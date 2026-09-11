package nethernet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestListenerTimeoutReplyUsesConnContext(t *testing.T) {
	for _, test := range []struct {
		name    string
		expired bool
		cause   error
	}{
		{"transport closed after deadline", true, errors.New("ICE transport closed")},
		{"candidate signaling deadline", false, fmt.Errorf("signal candidate: %w", context.DeadlineExceeded)},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			if test.expired {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
			}
			log := slog.New(slog.NewTextHandler(io.Discard, nil))
			release := make(chan struct{})
			defer close(release)
			signaling := blockedErrorSignaling{started: make(chan context.Context, 1), release: release}
			l := &Listener{
				conf: ListenConfig{Log: log, ConnContext: func(context.Context, *Conn) (context.Context, context.CancelFunc) {
					return ctx, func() {}
				}},
				signaling: signaling,
				closed:    make(chan struct{}),
			}
			connCtx, cancel := context.WithCancelCause(context.Background())
			cancel(test.cause)
			conn := &Conn{ctx: connCtx, log: log}
			// Model transports that have already closed with the specified cause.
			conn.once.Do(func() {})
			n := &listenerNegotiator{Listener: l, closed: make(chan struct{})}
			close(n.closed)
			n.finaliseConn(conn, nil, make(chan struct{}))
			select {
			case <-signaling.started:
				if !test.expired {
					t.Fatal("timeout reply dispatched for a failure that was not a deadline")
				}
			case <-time.After(200 * time.Millisecond):
				if test.expired {
					t.Fatal("timeout reply was not dispatched")
				}
			}
		})
	}
}

func TestListenerFinaliseConnPreservesFailureCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	want := errors.New("remote negotiation failed")
	cancel(want)
	conn := &Conn{ctx: ctx}
	n := &listenerNegotiator{Listener: &Listener{}, closed: make(chan struct{})}
	close(n.closed)
	if err := n.finaliseConn(conn, nil, make(chan struct{})); !errors.Is(err, want) {
		t.Fatalf("finaliseConn() error = %v, want %v", err, want)
	}
}

func TestListenerWaitForChannelsReadyReturnsConnCause(t *testing.T) {
	n := &listenerNegotiator{closed: make(chan struct{})}
	ctx := context.Background()
	connCtx, cancel := context.WithCancelCause(context.Background())
	conn := &Conn{ctx: connCtx}

	want := errors.New("connection closed early")
	cancel(want)
	close(n.closed)

	err := n.waitForChannelsReady(ctx, conn, make(chan struct{}))
	if !errors.Is(err, want) {
		t.Fatalf("waitForChannelsReady() error = %v, want %v", err, want)
	}
}

func TestListenerWaitForChannelsReadyReturnsNilWhenReady(t *testing.T) {
	n := &listenerNegotiator{closed: make(chan struct{})}
	conn := &Conn{ctx: context.Background()}
	channelsReady := make(chan struct{})
	close(channelsReady)

	if err := n.waitForChannelsReady(context.Background(), conn, channelsReady); err != nil {
		t.Fatalf("waitForChannelsReady() error = %v, want nil", err)
	}
}

func TestListenerConnectionOwnership(t *testing.T) {
	l := &Listener{negotiations: make(map[negotiationKey]*listenerNegotiator)}
	key := negotiationKey{networkID: "remote", connectionID: 7}
	first := &listenerNegotiator{Listener: l, key: key, closed: make(chan struct{})}
	duplicate := &listenerNegotiator{Listener: l, key: key, closed: make(chan struct{})}
	l.negotiations[key] = first

	// Cleanup from a stale owner must not unregister its replacement.
	duplicate.close()
	if got := l.negotiations[key]; got != first {
		t.Fatalf("owner after stale close = %p, want %p", got, first)
	}
	first.close()
	if _, ok := l.negotiations[key]; ok {
		t.Fatal("closed owner remains registered")
	}
}

func TestWrapSignalErrorPreservesExistingCode(t *testing.T) {
	inner := wrapSignalError(errors.New("bad offer"), ErrorCodeFailedToSetRemoteDescription)
	outer := fmt.Errorf("negotiate: %w", inner)
	if got := wrapSignalError(outer, ErrorCodeFailedToCreateAnswer); got != outer {
		t.Fatalf("wrapSignalError replaced an existing signal error: %v", got)
	}
}
