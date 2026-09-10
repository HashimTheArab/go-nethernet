package nethernet

import (
	"context"
	"errors"
	"testing"
)

func TestListenerWaitForChannelsReadyReturnsConnCause(t *testing.T) {
	n := &listenerNegotiator{closed: make(chan struct{})}
	ctx := context.Background()
	connCtx, cancel := context.WithCancelCause(context.Background())
	conn := &Conn{ctx: connCtx}

	want := errors.New("connection closed early")
	cancel(want)

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
