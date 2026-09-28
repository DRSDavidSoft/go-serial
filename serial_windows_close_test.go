//go:build windows

package serial

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestWindowsCloseCancelsPendingOverlappedRead(t *testing.T) {
	server, client := connectedOverlappedPipe(t)
	port := &windowsPort{handle: server}
	pending := make(chan struct{})
	originalWait := waitForOverlappedResult
	waitForOverlappedResult = func(
		handle windows.Handle,
		overlapped *windows.Overlapped,
		transferred *uint32,
		wait bool,
	) error {
		close(pending)
		return originalWait(handle, overlapped, transferred, wait)
	}
	defer func() { waitForOverlappedResult = originalWait }()

	readDone := make(chan error, 1)
	go func() {
		buffer := make([]byte, 1)
		_, err := port.Read(buffer)
		readDone <- err
	}()

	// The peer deliberately sends no data. Wait until ReadFile has explicitly
	// reported ERROR_IO_PENDING before closing from another goroutine.
	select {
	case <-pending:
	case <-time.After(time.Second):
		t.Fatal("ReadFile did not enter the pending overlapped state")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- port.Close() }()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close pending read: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after cancelling a pending overlapped read")
	}

	select {
	case err := <-readDone:
		var portErr *PortError
		if !errors.As(err, &portErr) || portErr.Code() != PortClosed {
			t.Fatalf("read error = %v, want PortClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending Read did not return after Close")
	}

	var duplicate windows.Handle
	duplicateErr := windows.DuplicateHandle(
		windows.CurrentProcess(), server,
		windows.CurrentProcess(), &duplicate,
		0, false, windows.DUPLICATE_SAME_ACCESS,
	)
	if duplicateErr == nil {
		windows.CloseHandle(duplicate)
		t.Fatal("serial handle remained valid after Close")
	}
	if !errors.Is(duplicateErr, windows.ERROR_INVALID_HANDLE) {
		t.Fatalf("duplicate closed serial handle: %v, want ERROR_INVALID_HANDLE", duplicateErr)
	}

	if err := windows.CloseHandle(client); err != nil {
		t.Fatalf("close pipe peer: %v", err)
	}
}

func TestWindowsCloseWaitsAndReleasesHandleWhenCancellationFails(t *testing.T) {
	server, client := connectedOverlappedPipe(t)
	port := &windowsPort{handle: server}
	cancelErr := windows.ERROR_ACCESS_DENIED
	closeErr := windows.ERROR_INVALID_FUNCTION
	pending := make(chan struct{})
	originalWait := waitForOverlappedResult
	originalCancel := cancelPendingIO
	originalClose := closeWindowsHandle
	cancelCalls := 0
	closeCalls := 0
	waitForOverlappedResult = func(
		handle windows.Handle,
		overlapped *windows.Overlapped,
		transferred *uint32,
		wait bool,
	) error {
		close(pending)
		return originalWait(handle, overlapped, transferred, wait)
	}
	cancelPendingIO = func(windows.Handle, *windows.Overlapped) error {
		cancelCalls++
		return cancelErr
	}
	closeWindowsHandle = func(handle windows.Handle) error {
		closeCalls++
		return errors.Join(originalClose(handle), closeErr)
	}
	defer func() {
		waitForOverlappedResult = originalWait
		cancelPendingIO = originalCancel
		closeWindowsHandle = originalClose
	}()

	readDone := make(chan error, 1)
	go func() {
		buffer := make([]byte, 1)
		_, err := port.Read(buffer)
		readDone <- err
	}()
	select {
	case <-pending:
	case <-time.After(time.Second):
		t.Fatal("ReadFile did not enter the pending overlapped state")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- port.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before failed-cancellation I/O completed: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Required: Close keeps the handle and OVERLAPPED storage alive.
	}
	if closeCalls != 0 {
		t.Fatalf("CloseHandle calls before pending I/O completed = %d, want 0", closeCalls)
	}

	data := []byte{0xA5}
	var written uint32
	if err := windows.WriteFile(client, data, &written, nil); err != nil {
		t.Fatalf("complete pending pipe read: %v", err)
	}
	if written != 1 {
		t.Fatalf("pipe bytes written = %d, want 1", written)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("completed read: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending read did not finish after peer completion")
	}

	var err error
	select {
	case err = <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after pending I/O completed")
	}
	if !errors.Is(err, cancelErr) || !errors.Is(err, closeErr) {
		t.Fatalf("close error = %v, want joined cancellation and close errors", err)
	}
	if cancelCalls != 1 || closeCalls != 1 {
		t.Fatalf("calls = cancel %d, close %d; want one each", cancelCalls, closeCalls)
	}
	if port.handle != 0 {
		t.Fatalf("handle = %v after completed close, want 0", port.handle)
	}
	if repeatedErr := port.Close(); repeatedErr != err {
		t.Fatalf("repeated close error = %v, want stored result %v", repeatedErr, err)
	}
	if err := windows.CloseHandle(client); err != nil {
		t.Fatalf("close pipe peer: %v", err)
	}
}

func TestWindowsOperationAbortedMapsToPortClosed(t *testing.T) {
	err := windowsIOError(windows.ERROR_OPERATION_ABORTED)
	var portErr *PortError
	if !errors.As(err, &portErr) || portErr.Code() != PortClosed {
		t.Fatalf("error = %v, want PortClosed", err)
	}
}

func connectedOverlappedPipe(t *testing.T) (windows.Handle, windows.Handle) {
	t.Helper()
	name, err := windows.UTF16PtrFromString(fmt.Sprintf(
		`\\.\pipe\go-serial-close-%d-%d`, os.Getpid(), time.Now().UnixNano(),
	))
	if err != nil {
		t.Fatalf("pipe name: %v", err)
	}

	server, err := windows.CreateNamedPipe(
		name,
		windows.PIPE_ACCESS_INBOUND|windows.FILE_FLAG_OVERLAPPED,
		windows.PIPE_TYPE_BYTE|windows.PIPE_WAIT,
		1,
		4096,
		4096,
		0,
		nil,
	)
	if err != nil {
		t.Fatalf("create named pipe: %v", err)
	}

	event, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		windows.CloseHandle(server)
		t.Fatalf("create connect event: %v", err)
	}
	defer windows.CloseHandle(event)
	overlapped := &windows.Overlapped{HEvent: event}
	connectErr := windows.ConnectNamedPipe(server, overlapped)
	if connectErr != nil && !errors.Is(connectErr, windows.ERROR_IO_PENDING) {
		windows.CloseHandle(server)
		t.Fatalf("connect named pipe: %v", connectErr)
	}

	client, err := windows.CreateFile(
		name,
		windows.GENERIC_WRITE,
		0,
		nil,
		windows.OPEN_EXISTING,
		0,
		0,
	)
	if err != nil {
		windows.CloseHandle(server)
		t.Fatalf("open named pipe peer: %v", err)
	}

	if errors.Is(connectErr, windows.ERROR_IO_PENDING) {
		var transferred uint32
		if err := windows.GetOverlappedResult(server, overlapped, &transferred, true); err != nil {
			windows.CloseHandle(client)
			windows.CloseHandle(server)
			t.Fatalf("finish named pipe connection: %v", err)
		}
	}
	return server, client
}
