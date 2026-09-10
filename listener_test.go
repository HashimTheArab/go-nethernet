package nethernet

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestListenerHandleConnPreservesFailureCause(t *testing.T) {
	var output bytes.Buffer
	ctx, cancel := context.WithCancelCause(context.Background())
	want := errors.New("remote negotiation failed")
	cancel(want)
	conn := &Conn{ctx: ctx, log: slog.New(slog.NewTextHandler(&output, nil))}
	// Model a Conn whose transports have already completed closure.
	conn.once.Do(func() {})
	n := &listenerNegotiator{Listener: &Listener{}, closed: make(chan struct{})}
	close(n.closed)
	n.handleConn(conn, nil, make(chan struct{}))
	if !strings.Contains(output.String(), want.Error()) {
		t.Fatalf("failure log = %q, want original connection cause", output.String())
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
