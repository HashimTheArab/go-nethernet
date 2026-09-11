package nethernet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestAcceptedConnUsesNegotiatedMessageSize(t *testing.T) {
	client, server := newMemorySignalingPair("client", "server")
	t.Cleanup(client.close)
	t.Cleanup(server.close)

	_, clientConn, serverConn := dialAcceptedListener(t, smallMessageSignaling{client}, server)
	checkConnPayload(t, serverConn, clientConn, bytes.Repeat([]byte{0x5a}, 2048))
}

func TestListenerIgnoresMalformedDeferredSignals(t *testing.T) {
	for _, kind := range []string{SignalTypeCandidate, SignalTypeError} {
		t.Run(kind, func(t *testing.T) {
			client, server := newMemorySignalingPair("client", "server")
			t.Cleanup(client.close)
			t.Cleanup(server.close)
			gate := newBlockingCredentialsSignaling()
			t.Cleanup(gate.close)
			_, clientConn, serverConn := dialAcceptedListener(t,
				earlyMalformedSignaling{Signaling: client, gate: gate, kind: kind},
				gatedOfferSignaling{Signaling: server, gate: gate},
			)
			checkConnPayload(t, clientConn, serverConn, []byte("handshake survived malformed signal"))
		})
	}
}

// gatedOfferSignaling holds offer processing until an early signal is delivered.
type gatedOfferSignaling struct {
	Signaling
	gate *blockingCredentialsSignaling
}

// Credentials pauses the offer while the client injects a malformed signal.
func (s gatedOfferSignaling) Credentials(ctx context.Context) (*Credentials, error) {
	return s.gate.Credentials(ctx)
}

// earlyMalformedSignaling injects one malformed signal before the answer is built.
type earlyMalformedSignaling struct {
	Signaling
	gate *blockingCredentialsSignaling
	kind string
}

// Signal defers an invalid candidate or error while offer credentials are blocked.
func (s earlyMalformedSignaling) Signal(ctx context.Context, signal *Signal) error {
	if err := s.Signaling.Signal(ctx, signal); err != nil {
		return err
	}
	if signal.Type != SignalTypeOffer {
		return nil
	}
	select {
	case <-s.gate.started:
	case <-ctx.Done():
		return ctx.Err()
	}
	err := s.Signaling.Signal(ctx, &Signal{Type: s.kind, NetworkID: signal.NetworkID, ConnectionID: signal.ConnectionID, Data: "invalid"})
	close(s.gate.release)
	return err
}

func TestListenerRejectsDuplicateAfterAccept(t *testing.T) {
	client, server := newMemorySignalingPair("client", "server")
	t.Cleanup(client.close)
	t.Cleanup(server.close)
	responses := make(chan Signal, 4)
	l, clientConn, serverConn := dialAcceptedListener(t, client, listenerResponseSignaling{server, responses})

	// Consume the original answer before observing the responses to duplicates.
	if response := waitListenerResponse(t, responses); response.Type != SignalTypeAnswer {
		t.Fatalf("initial response type = %q, want answer", response.Type)
	}
	addr := serverConn.RemoteAddr().(*Addr)
	for range 2 {
		if !l.NotifySignal(&Signal{
			Type:         SignalTypeOffer,
			ConnectionID: addr.ConnectionID,
			NetworkID:    addr.NetworkID,
			Data:         testOffer(t),
		}) {
			t.Fatal("NotifySignal(duplicate offer) = false, want handled as a rejection")
		}
		response := waitListenerResponse(t, responses)
		if response.Type != SignalTypeError {
			t.Fatalf("duplicate response type = %q, want error", response.Type)
		}
		if response.Data != strconv.Itoa(ErrorCodeIncomingConnectionIgnored) {
			t.Fatalf("duplicate error code = %q, want incoming connection ignored", response.Data)
		}
		checkConnPayload(t, clientConn, serverConn, []byte("original connection still works"))
	}
}

func TestAcceptedConnHandlesLateErrorSignal(t *testing.T) {
	client, server := newMemorySignalingPair("client", "server")
	t.Cleanup(client.close)
	t.Cleanup(server.close)
	l, _, serverConn := dialAcceptedListener(t, client, server)
	waitListenerState(t, l, 0, 1)
	addr := serverConn.RemoteAddr().(*Addr)
	if !l.NotifySignal(&Signal{
		Type:         SignalTypeError,
		ConnectionID: addr.ConnectionID,
		NetworkID:    addr.NetworkID,
		Data:         strconv.Itoa(ErrorCodeGenericFailure),
	}) {
		t.Fatal("NotifySignal(error after Accept) = false, want true")
	}
	select {
	case <-serverConn.Context().Done():
		if cause := context.Cause(serverConn.Context()); !strings.Contains(cause.Error(), "remote peer notified connection failure") {
			t.Fatalf("accepted connection closed with cause %v, want remote failure", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("accepted connection did not close after remote error")
	}
}

func TestAcceptedConnSurvivesListenerClose(t *testing.T) {
	client, server := newMemorySignalingPair("client", "server")
	t.Cleanup(client.close)
	t.Cleanup(server.close)
	l, clientConn, serverConn := dialAcceptedListener(t, client, server)
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	checkConnPayload(t, serverConn, clientConn, []byte("after listener close"))
	checkConnPayload(t, clientConn, serverConn, []byte("reply after listener close"))
}

// smallMessageSignaling advertises a smaller receive limit in the client's offer.
type smallMessageSignaling struct{ Signaling }

// Signal changes a copy of the offer so the listener must fragment its writes.
func (s smallMessageSignaling) Signal(ctx context.Context, signal *Signal) error {
	copy := *signal
	if copy.Type == SignalTypeOffer {
		const attribute = "a=max-message-size:262144"
		if !strings.Contains(copy.Data, attribute) {
			return fmt.Errorf("offer is missing %q", attribute)
		}
		copy.Data = strings.Replace(copy.Data, attribute, "a=max-message-size:1024", 1)
	}
	return s.Signaling.Signal(ctx, &copy)
}

// listenerResponseSignaling records negotiation replies without sending duplicate
// rejection errors to the already established client with the same connection ID.
type listenerResponseSignaling struct {
	Signaling
	responses chan<- Signal
}

// Signal records answers and errors, forwarding everything except error replies.
func (s listenerResponseSignaling) Signal(ctx context.Context, signal *Signal) error {
	if signal.Type == SignalTypeAnswer || signal.Type == SignalTypeError {
		select {
		case s.responses <- *signal:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if signal.Type == SignalTypeError {
		return nil
	}
	return s.Signaling.Signal(ctx, signal)
}

// dialAcceptedListener establishes a real WebRTC connection and owns its cleanup.
func dialAcceptedListener(t *testing.T, client, server Signaling) (*Listener, *Conn, *Conn) {
	t.Helper()
	l, err := (ListenConfig{AllowAnonymous: true}).Listen(server)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	accepted := make(chan net.Conn)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		select {
		case accepted <- conn:
		case <-ctx.Done():
			_ = conn.Close()
		}
	}()
	clientConn, err := (Dialer{}).DialContext(ctx, server.NetworkID(), client)
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	t.Cleanup(func() { _ = clientConn.Close() })
	select {
	case acceptedConn := <-accepted:
		serverConn := acceptedConn.(*Conn)
		t.Cleanup(func() { _ = serverConn.Close() })
		return l, clientConn, serverConn
	case err := <-acceptErr:
		t.Fatalf("Accept() error = %v", err)
	case <-ctx.Done():
		t.Fatalf("Accept() timed out: %v", ctx.Err())
	}
	return nil, nil, nil
}

// checkConnPayload checks that one write arrives completely and unchanged.
func checkConnPayload(t *testing.T, sender, receiver *Conn, payload []byte) {
	t.Helper()
	if n, err := sender.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write() = (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	got := make([]byte, len(payload))
	read := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(receiver, got)
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatalf("ReadFull() error = %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("ReadFull() = %x, want %x", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for payload")
	}
}

// waitListenerResponse waits for an answer or rejection from the listener.
func waitListenerResponse(t *testing.T, responses <-chan Signal) Signal {
	t.Helper()
	select {
	case signal := <-responses:
		return signal
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for listener response")
		return Signal{}
	}
}
