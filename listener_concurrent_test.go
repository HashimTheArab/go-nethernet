package nethernet

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// blockingCredentialsSignaling lets tests pause offer handling inside
// Signaling.Credentials.
type blockingCredentialsSignaling struct {
	ctx     context.Context
	cancel  context.CancelFunc
	started chan struct{}
	release chan struct{}
}

// newBlockingCredentialsSignaling creates a signaling connection whose
// credential requests wait for release to close.
func newBlockingCredentialsSignaling() *blockingCredentialsSignaling {
	ctx, cancel := context.WithCancel(context.Background())
	return &blockingCredentialsSignaling{
		ctx:     ctx,
		cancel:  cancel,
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
}

// Signal accepts an outbound signal for this test signaling connection.
func (*blockingCredentialsSignaling) Signal(context.Context, *Signal) error { return nil }

// Notify accepts a notifier. Tests deliver signals directly to the listener.
func (*blockingCredentialsSignaling) Notify(Notifier) func() { return func() {} }

// Context returns the test signaling connection's lifetime context.
func (s *blockingCredentialsSignaling) Context() context.Context { return s.ctx }

// Credentials reports that a request started and waits until the test releases it.
func (s *blockingCredentialsSignaling) Credentials(ctx context.Context) (*Credentials, error) {
	select {
	case s.started <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-s.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, context.Cause(s.ctx)
	}
}

// NetworkID returns the listener's test network ID.
func (*blockingCredentialsSignaling) NetworkID() string { return "listener" }

// PongData accepts LAN discovery data for the Signaling interface.
func (*blockingCredentialsSignaling) PongData([]byte) {}

// close ends the test signaling connection.
func (s *blockingCredentialsSignaling) close() { s.cancel() }

// testOffer returns a valid offer that reaches the credential lookup step.
func testOffer(t *testing.T) string {
	t.Helper()
	b, err := (description{
		ice: webrtc.ICEParameters{UsernameFragment: "user", Password: "password"},
		dtls: webrtc.DTLSParameters{
			Role: webrtc.DTLSRoleAuto,
			Fingerprints: []webrtc.DTLSFingerprint{{
				Algorithm: "sha-256",
				Value:     "00",
			}},
		},
	}).encode()
	if err != nil {
		t.Fatalf("encode offer: %v", err)
	}
	return string(b)
}

func TestListenerProcessesConnectionsIndependently(t *testing.T) {
	signaling := newBlockingCredentialsSignaling()
	defer signaling.close()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	l, err := (ListenConfig{Log: log, AllowAnonymous: true}).Listen(signaling)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer l.Close()

	offer := testOffer(t)
	if !l.NotifySignal(&Signal{Type: SignalTypeOffer, ConnectionID: 1, NetworkID: "remote", Data: offer}) {
		t.Fatal("NotifySignal(first offer) = false, want true")
	}
	waitForCredentialRequest(t, signaling.started, "first offer")

	if !l.NotifySignal(&Signal{Type: SignalTypeOffer, ConnectionID: 2, NetworkID: "remote", Data: offer}) {
		t.Fatal("NotifySignal(second offer) = false, want true")
	}
	waitForCredentialRequest(t, signaling.started, "second offer")

	// Keep the first connection blocked and fill only its queue. The second
	// connection continues independently.
	for i := range maxPendingSignalsPerNegotiation {
		if !l.NotifySignal(&Signal{
			Type:         SignalTypeCandidate,
			ConnectionID: 1,
			NetworkID:    "remote",
			Data:         "candidate",
		}) {
			t.Fatalf("NotifySignal(candidate #%d) = false, want true", i)
		}
	}
	if l.NotifySignal(&Signal{
		Type:         SignalTypeCandidate,
		ConnectionID: 1,
		NetworkID:    "remote",
		Data:         "candidate",
	}) {
		t.Fatal("NotifySignal(over capacity) = true, want false")
	}
}

// waitForCredentialRequest waits for one offer to enter the blocked credential lookup.
func waitForCredentialRequest(t *testing.T, started <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s credential request", name)
	}
}

func TestListenerClosedSignalingDuringListen(t *testing.T) {
	for _, alreadyClosed := range []bool{true, false} {
		t.Run(strconv.FormatBool(alreadyClosed), func(t *testing.T) {
			client, server := newMemorySignalingPair("client", "server")
			t.Cleanup(client.close)
			t.Cleanup(server.close)
			if alreadyClosed {
				server.close()
			}
			l, err := (ListenConfig{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Listen(cancelOnNotifySignaling{server})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			select {
			case <-l.Context().Done():
			case <-time.After(time.Second):
				t.Fatal("listener did not close with its signaling connection")
			}
			_ = l.Close()
			server.mu.Lock()
			defer server.mu.Unlock()
			if len(server.notifiers) != 0 {
				t.Fatal("closed listener remains subscribed")
			}
		})
	}
}

// cancelOnNotifySignaling cancels signaling while the listener registers itself.
type cancelOnNotifySignaling struct{ *memorySignaling }

// Notify registers the subscription before canceling the signaling connection.
func (s cancelOnNotifySignaling) Notify(n Notifier) func() {
	stop := s.memorySignaling.Notify(n)
	s.close()
	return stop
}

func TestListenerBoundsPendingNegotiations(t *testing.T) {
	signaling := newBlockingCredentialsSignaling()
	signaling.started = make(chan struct{}, maxListenerNegotiations)
	t.Cleanup(signaling.close)
	cancels := make(chan context.CancelFunc, maxListenerNegotiations)
	l, err := (ListenConfig{
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		NegotiationContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(parent)
			cancels <- cancel
			return ctx, cancel
		},
	}).Listen(signaling)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	offer := testOffer(t)
	for i := range maxListenerNegotiations {
		if !l.NotifySignal(&Signal{Type: SignalTypeOffer, NetworkID: "remote", ConnectionID: uint64(i), Data: offer}) {
			t.Fatalf("offer %d was rejected before the limit", i)
		}
	}
	for range maxListenerNegotiations {
		waitForCredentialRequest(t, signaling.started, "pending offer")
	}
	next := &Signal{Type: SignalTypeOffer, NetworkID: "remote", ConnectionID: maxListenerNegotiations, Data: offer}
	if l.NotifySignal(next) {
		t.Fatal("offer above the pending negotiation limit was admitted")
	}
	// Existing workers can still receive signals when admission is full.
	if !l.NotifySignal(&Signal{Type: SignalTypeCandidate, NetworkID: "remote", ConnectionID: 0, Data: "candidate"}) {
		t.Fatal("pending negotiation could not receive a candidate at capacity")
	}
	(<-cancels)()
	waitListenerState(t, l, maxListenerNegotiations-1, maxListenerNegotiations-1, maxListenerNegotiations-1)
	if !l.NotifySignal(next) {
		t.Fatal("failed negotiation did not release its admission slot")
	}
	_ = l.Close()
	waitListenerState(t, l, 0, 0, 0)
	if l.NotifySignal(next) {
		t.Fatal("closed listener admitted an offer")
	}
}

func TestListenerPendingOwnerSurvivesIdleWorkerAndDuplicate(t *testing.T) {
	signaling := newBlockingCredentialsSignaling()
	close(signaling.release)
	t.Cleanup(signaling.close)
	responses := make(chan Signal, 4)
	conns := make(chan *Conn, 1)
	l, err := (ListenConfig{
		AllowAnonymous: true,
		Log:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		ConnContext: func(parent context.Context, conn *Conn) (context.Context, context.CancelFunc) {
			conns <- conn
			return context.WithTimeout(parent, 5*time.Second)
		},
	}).Listen(listenerResponseSignaling{signaling, responses})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	offer := &Signal{Type: SignalTypeOffer, ConnectionID: 1, NetworkID: "remote", Data: testOffer(t)}
	if !l.NotifySignal(offer) {
		t.Fatal("initial offer rejected")
	}
	var conn *Conn
	select {
	case conn = <-conns:
	case <-time.After(time.Second):
		t.Fatal("offer did not start a connection")
	}
	t.Cleanup(func() { _ = conn.Close() })
	if got := waitListenerResponse(t, responses); got.Type != SignalTypeAnswer {
		t.Fatalf("initial response = %s, want answer", got.Type)
	}
	waitListenerState(t, l, 1, 0, 1)
	if !l.NotifySignal(offer) {
		t.Fatal("duplicate offer was not queued for rejection")
	}
	if got := waitListenerResponse(t, responses); got.Type != SignalTypeError || got.Data != strconv.Itoa(ErrorCodeIncomingConnectionIgnored) {
		t.Fatalf("duplicate response = %s, want incoming connection ignored", got.String())
	}
	waitListenerState(t, l, 1, 0, 1)
	if conn.Context().Err() != nil {
		t.Fatal("duplicate offer closed the original pending connection")
	}
	if !l.NotifySignal(&Signal{Type: SignalTypeCandidate, ConnectionID: 1, NetworkID: "remote", Data: "candidate:1 1 udp 2130706431 127.0.0.1 9 typ host"}) {
		t.Fatal("late candidate was rejected after the worker became idle")
	}
	select {
	case <-conn.candidateReceived:
	case <-time.After(time.Second):
		t.Fatal("restarted worker did not deliver the late candidate")
	}
	if !l.NotifySignal(&Signal{Type: SignalTypeError, ConnectionID: 1, NetworkID: "remote", Data: strconv.Itoa(ErrorCodeGenericFailure)}) {
		t.Fatal("remote error was rejected")
	}
	waitListenerState(t, l, 0, 0, 0)
}

func TestListenerAcceptReleasesAdmission(t *testing.T) {
	client, server := newMemorySignalingPair("client", "server")
	t.Cleanup(client.close)
	t.Cleanup(server.close)
	l, _, conn := dialAcceptedListener(t, client, server)
	waitListenerState(t, l, 0, 0, 1)
	_ = conn.Close()
	waitListenerState(t, l, 0, 0, 0)
}

func TestListenerBoundsErrorWorkers(t *testing.T) {
	signaling := blockedErrorSignaling{
		blockingCredentialsSignaling: newBlockingCredentialsSignaling(),
		started:                      make(chan context.Context, maxListenerSignalWorkers),
	}
	t.Cleanup(signaling.close)
	l, err := (ListenConfig{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Listen(signaling)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	for i := range maxListenerSignalWorkers {
		if !l.NotifySignal(&Signal{Type: SignalTypeOffer, NetworkID: "remote", ConnectionID: uint64(i), Data: "invalid"}) {
			t.Fatalf("offer %d rejected before worker limit", i)
		}
	}
	for range maxListenerSignalWorkers {
		select {
		case ctx := <-signaling.started:
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("error delivery has no deadline")
			}
		case <-time.After(time.Second):
			t.Fatal("worker did not attempt to report its error")
		}
	}
	waitListenerState(t, l, 0, maxListenerSignalWorkers, 0)
	if l.NotifySignal(&Signal{Type: SignalTypeOffer, NetworkID: "remote", ConnectionID: maxListenerSignalWorkers, Data: "invalid"}) {
		t.Fatal("error responses did not count towards the worker limit")
	}
	_ = l.Close()
	waitListenerState(t, l, 0, 0, 0)
}

// blockedErrorSignaling holds error responses until their delivery context ends.
type blockedErrorSignaling struct {
	*blockingCredentialsSignaling
	started chan context.Context
}

// Signal observes the delivery context and blocks as a stalled signaling sender would.
func (s blockedErrorSignaling) Signal(ctx context.Context, signal *Signal) error {
	if signal.Type != SignalTypeError {
		return errors.New("unexpected non-error signal")
	}
	s.started <- ctx
	<-ctx.Done()
	return ctx.Err()
}

// waitListenerState waits for asynchronous workers to reach the expected ownership state.
func waitListenerState(t *testing.T, l *Listener, pending, workers, owners int) {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		l.negotiationsMu.Lock()
		gotPending, gotWorkers, gotOwners := l.pending, l.workers, len(l.negotiations)
		l.negotiationsMu.Unlock()
		if gotPending == pending && gotWorkers == workers && gotOwners == owners {
			return
		}
		select {
		case <-timeout.C:
			t.Fatalf("listener state = (%d pending, %d workers, %d owners), want (%d, %d, %d)", gotPending, gotWorkers, gotOwners, pending, workers, owners)
		case <-tick.C:
		}
	}
}
