//go:build windows

package serial

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
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

func TestWindowsCloseReleasesHandleBeforeJoiningDriverCancellation(t *testing.T) {
	port := &windowsPort{handle: windows.Handle(1)}
	readStarted := make(chan struct{})
	handleClosed := make(chan struct{})
	originalRead := readWindowsFile
	originalWait := waitForOverlappedResult
	originalCancel := cancelPendingIO
	originalClose := closeWindowsHandle
	readWindowsFile = func(
		windows.Handle,
		[]byte,
		*uint32,
		*windows.Overlapped,
	) error {
		close(readStarted)
		return windows.ERROR_IO_PENDING
	}
	waitForOverlappedResult = func(
		windows.Handle,
		*windows.Overlapped,
		*uint32,
		bool,
	) error {
		// Model the CH340 behavior observed on Cafe: CancelIoEx is accepted,
		// but completion is not delivered until CloseHandle releases the device.
		<-handleClosed
		return windows.ERROR_OPERATION_ABORTED
	}
	cancelPendingIO = func(windows.Handle, *windows.Overlapped) error { return nil }
	closeWindowsHandle = func(windows.Handle) error {
		close(handleClosed)
		return nil
	}
	defer func() {
		readWindowsFile = originalRead
		waitForOverlappedResult = originalWait
		cancelPendingIO = originalCancel
		closeWindowsHandle = originalClose
	}()

	readDone := make(chan error, 1)
	go func() {
		_, err := port.Read(make([]byte, 1))
		readDone <- err
	}()
	select {
	case <-readStarted:
	case <-time.After(time.Second):
		t.Fatal("ReadFile was not issued")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- port.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close waited for driver completion before releasing the handle")
	}
	select {
	case err := <-readDone:
		var portErr *PortError
		if !errors.As(err, &portErr) || portErr.Code() != PortClosed {
			t.Fatalf("read error = %v, want PortClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read did not finish after handle release")
	}
}

func TestWindowsCloseCanCancelDriverBlockedInsideReadFile(t *testing.T) {
	port := &windowsPort{handle: windows.Handle(1)}
	readStarted := make(chan struct{})
	canceled := make(chan struct{})
	originalRead := readWindowsFile
	originalCancel := cancelPendingIO
	originalClose := closeWindowsHandle
	readWindowsFile = func(
		windows.Handle,
		[]byte,
		*uint32,
		*windows.Overlapped,
	) error {
		close(readStarted)
		<-canceled
		return windows.ERROR_OPERATION_ABORTED
	}
	cancelPendingIO = func(windows.Handle, *windows.Overlapped) error {
		close(canceled)
		return nil
	}
	closeWindowsHandle = func(windows.Handle) error { return nil }
	defer func() {
		readWindowsFile = originalRead
		cancelPendingIO = originalCancel
		closeWindowsHandle = originalClose
	}()

	readDone := make(chan error, 1)
	go func() {
		_, err := port.Read(make([]byte, 1))
		readDone <- err
	}()
	select {
	case <-readStarted:
	case <-time.After(time.Second):
		t.Fatal("ReadFile was not entered")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- port.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close could not reach CancelIoEx while ReadFile was blocked")
	}
	select {
	case err := <-readDone:
		var portErr *PortError
		if !errors.As(err, &portErr) || portErr.Code() != PortClosed {
			t.Fatalf("read error = %v, want PortClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked ReadFile was not canceled")
	}
}

func TestWindowsCloseCancelsOperationIssuedAfterInitialCancel(t *testing.T) {
	port := &windowsPort{handle: windows.Handle(1)}
	readEntered := make(chan struct{})
	initialCancel := make(chan struct{})
	targetedCancel := make(chan struct{})
	originalRead := readWindowsFile
	originalWait := waitForOverlappedResult
	originalCancel := cancelPendingIO
	originalClose := closeWindowsHandle
	readWindowsFile = func(
		windows.Handle,
		[]byte,
		*uint32,
		*windows.Overlapped,
	) error {
		close(readEntered)
		<-initialCancel
		return windows.ERROR_IO_PENDING
	}
	waitForOverlappedResult = func(
		windows.Handle,
		*windows.Overlapped,
		*uint32,
		bool,
	) error {
		<-targetedCancel
		return windows.ERROR_OPERATION_ABORTED
	}
	cancelPendingIO = func(_ windows.Handle, overlapped *windows.Overlapped) error {
		if overlapped == nil {
			close(initialCancel)
			return windows.ERROR_NOT_FOUND
		}
		close(targetedCancel)
		return nil
	}
	closeWindowsHandle = func(windows.Handle) error { return nil }
	defer func() {
		readWindowsFile = originalRead
		waitForOverlappedResult = originalWait
		cancelPendingIO = originalCancel
		closeWindowsHandle = originalClose
	}()

	readDone := make(chan error, 1)
	go func() {
		_, err := port.Read(make([]byte, 1))
		readDone <- err
	}()
	select {
	case <-readEntered:
	case <-time.After(time.Second):
		t.Fatal("ReadFile was not entered")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- port.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close stranded an operation issued after the initial CancelIoEx")
	}
	select {
	case err := <-readDone:
		var portErr *PortError
		if !errors.As(err, &portErr) || portErr.Code() != PortClosed {
			t.Fatalf("read error = %v, want PortClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late-issued read was not canceled")
	}
}

func TestWindowsCloseReturnsCancellationErrorAndLaterCloseRetries(t *testing.T) {
	server, client := connectedOverlappedPipe(t)
	port := &windowsPort{handle: server}
	cancelErr := windows.ERROR_ACCESS_DENIED
	pending := make(chan struct{})
	originalWait := waitForOverlappedResult
	originalCancel := cancelPendingIO
	originalClose := closeWindowsHandle
	cancelCalls := 0
	var closeCalls atomic.Int32
	waitForOverlappedResult = func(
		handle windows.Handle,
		overlapped *windows.Overlapped,
		transferred *uint32,
		wait bool,
	) error {
		close(pending)
		return originalWait(handle, overlapped, transferred, wait)
	}
	cancelPendingIO = func(handle windows.Handle, overlapped *windows.Overlapped) error {
		cancelCalls++
		if cancelCalls == 1 {
			return cancelErr
		}
		return originalCancel(handle, overlapped)
	}
	closeWindowsHandle = func(handle windows.Handle) error {
		closeCalls.Add(1)
		return originalClose(handle)
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
		if !errors.Is(err, cancelErr) {
			t.Fatalf("first Close error = %v, want cancellation error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Close did not return after cancellation failed")
	}
	if calls := closeCalls.Load(); calls != 0 {
		t.Fatalf("CloseHandle calls after failed cancellation = %d, want 0", calls)
	}
	if port.handle != server {
		t.Fatalf("handle = %v after failed cancellation, want retained %v", port.handle, server)
	}
	select {
	case err := <-readDone:
		t.Fatalf("pending read returned after failed cancellation: %v", err)
	default:
	}

	var duplicate windows.Handle
	if err := windows.DuplicateHandle(
		windows.CurrentProcess(), server,
		windows.CurrentProcess(), &duplicate,
		0, false, windows.DUPLICATE_SAME_ACCESS,
	); err != nil {
		t.Fatalf("duplicate retained serial handle: %v", err)
	}
	if err := windows.CloseHandle(duplicate); err != nil {
		t.Fatalf("close duplicate retained handle: %v", err)
	}

	// A later Close retries cancellation. This time the real CancelIoEx runs,
	// the waiter observes ERROR_OPERATION_ABORTED, and only then is the handle
	// released.
	go func() { closeDone <- port.Close() }()
	select {
	case err := <-readDone:
		var portErr *PortError
		if !errors.As(err, &portErr) || portErr.Code() != PortClosed {
			t.Fatalf("read error = %v, want PortClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending read did not return after retrying Close")
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("retry Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry Close did not finish after cancellation succeeded")
	}
	if calls := closeCalls.Load(); cancelCalls != 2 || calls != 1 {
		t.Fatalf("calls = cancel %d, close %d; want two cancellations and one close", cancelCalls, calls)
	}
	if port.handle != 0 {
		t.Fatalf("handle = %v after completed close, want 0", port.handle)
	}
	if repeatedErr := port.Close(); repeatedErr != nil {
		t.Fatalf("repeated Close after successful retry = %v, want nil", repeatedErr)
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
