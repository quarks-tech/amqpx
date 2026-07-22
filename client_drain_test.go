package amqpx

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Drain mode: after cancellation the command keeps its connection and its
// clean return is honored — no force-close, nil error.
func TestRunCommandWithContextDrainWaitsForCleanReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	releaseCommand := make(chan struct{})
	closeCalled := make(chan struct{}, 1)

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: 5 * time.Second},
			func() error {
				close(commandStarted)
				<-releaseCommand
				return nil
			},
			func(time.Time) error {
				closeCalled <- struct{}{}
				return nil
			},
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		t.Fatalf("returned %v before the command finished draining", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCommand)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("error = %v, want nil (clean drain)", err)
		}
	case <-time.After(time.Second):
		t.Fatal("did not return after the command drained")
	}
	select {
	case <-closeCalled:
		t.Fatal("connection was closed during a clean drain")
	default:
	}
}

// Drain mode: the command's own error passes through verbatim.
func TestRunCommandWithContextDrainReturnsCommandErrorVerbatim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	cmdErr := errors.New("handler failed mid-drain")

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: 5 * time.Second},
			func() error {
				close(commandStarted)
				return cmdErr
			},
			func(time.Time) error { return nil },
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, cmdErr) {
			t.Fatalf("error = %v, want the command's own error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("did not return the command error")
	}
}

// Drain mode backstop: a command that ignores cancellation is force-closed at
// the drain deadline; the error matches BOTH ErrDrainTimeout and the ctx error.
func TestRunCommandWithContextDrainTimeoutForceCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	releaseCommand := make(chan struct{})
	defer close(releaseCommand)
	closeCalled := make(chan struct{}, 1)

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: 50 * time.Millisecond},
			func() error {
				close(commandStarted)
				<-releaseCommand
				return nil
			},
			func(time.Time) error {
				closeCalled <- struct{}{}
				return nil
			},
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("error = %v, want ErrDrainTimeout", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled preserved for planned-shutdown checks", err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain timeout did not fire")
	}
	select {
	case <-closeCalled:
	case <-time.After(time.Second):
		t.Fatal("connection was not force-closed at the drain deadline")
	}
}

// Negative drainTimeout waits forever: no timer, no close, command owns exit.
func TestRunCommandWithContextDrainNegativeWaitsForever(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	releaseCommand := make(chan struct{})
	closeCalled := make(chan struct{}, 1)

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: -1},
			func() error {
				close(commandStarted)
				<-releaseCommand
				return nil
			},
			func(time.Time) error {
				closeCalled <- struct{}{}
				return nil
			},
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		t.Fatalf("returned %v; negative drain timeout must wait for the command", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(releaseCommand)
	if err := <-result; err != nil {
		t.Fatalf("error = %v, want nil", err)
	}
	select {
	case <-closeCalled:
		t.Fatal("connection was closed despite wait-forever drain")
	default:
	}
}

// Drain mode inherits the pre-cancel guard: an already-canceled ctx never
// starts the command (identical to abort mode).
func TestRunCommandWithContextDrainDoesNotStartAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	commandCalled := make(chan struct{}, 1)
	err := runCommandWithContext(
		ctx,
		cancelPolicy{drainTimeout: time.Second},
		func() error {
			commandCalled <- struct{}{}
			return nil
		},
		func(time.Time) error { return nil },
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	select {
	case <-commandCalled:
		t.Fatal("command started with an already canceled context")
	default:
	}
}
