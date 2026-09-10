package nethernet

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func TestListenerNonTrickleGatheringDoesNotBlockOtherOffers(t *testing.T) {
	stun, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stun.Close() })
	base := newBlockingCredentialsSignaling()
	t.Cleanup(base.close)
	responses := make(chan Signal, 4)
	signaling := &firstOfferSTUNSignaling{Signaling: listenerResponseSignaling{base, responses}, url: "stun:" + stun.LocalAddr().String()}
	var settings webrtc.SettingEngine
	settings.SetIncludeLoopbackCandidate(true)
	settings.SetSTUNGatherTimeout(time.Minute)
	l, err := (ListenConfig{
		API:               webrtc.NewAPI(webrtc.WithSettingEngine(settings)),
		Log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		AllowAnonymous:    true,
		DisableTrickleICE: true,
	}).Listen(signaling)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	offer := testOffer(t)
	if !l.NotifySignal(&Signal{Type: SignalTypeOffer, NetworkID: "remote", ConnectionID: 1, Data: offer}) {
		t.Fatal("first offer rejected")
	}
	// Receipt of a STUN request proves the first offer entered ICE gathering.
	// Leave it unanswered while a second offer gathers only local candidates.
	if err := stun.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := stun.ReadFrom(make([]byte, 1500)); err != nil {
		t.Fatalf("first offer did not start gathering: %v", err)
	}
	if !l.NotifySignal(&Signal{Type: SignalTypeOffer, NetworkID: "remote", ConnectionID: 2, Data: offer}) {
		t.Fatal("second offer rejected during ICE gathering")
	}
	if response := waitListenerResponse(t, responses); response.Type != SignalTypeAnswer || response.ConnectionID != 2 {
		t.Fatalf("response = %s, want second offer answered while first is gathering", response.String())
	}
}

// firstOfferSTUNSignaling supplies a STUN server only to the first offer.
type firstOfferSTUNSignaling struct {
	Signaling
	url   string
	calls atomic.Uint32
}

// Credentials makes the first offer wait for STUN while later offers gather locally.
func (s *firstOfferSTUNSignaling) Credentials(context.Context) (*Credentials, error) {
	if s.calls.Add(1) == 1 {
		return &Credentials{ICEServers: []ICEServer{{URLs: []string{s.url}}}}, nil
	}
	return nil, nil
}

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
	accepted := make(chan bool, maxPendingSignalsPerNegotiation)
	for range maxPendingSignalsPerNegotiation {
		go func() {
			accepted <- l.NotifySignal(&Signal{
				Type:         SignalTypeCandidate,
				ConnectionID: 1,
				NetworkID:    "remote",
				Data:         "candidate",
			})
		}()
	}
	for i := range maxPendingSignalsPerNegotiation {
		if !<-accepted {
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

func TestListenerRejectsUnownedSignal(t *testing.T) {
	signaling := newBlockingCredentialsSignaling()
	t.Cleanup(signaling.close)
	l, err := (ListenConfig{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Listen(signaling)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if l.NotifySignal(&Signal{Type: SignalTypeCandidate, NetworkID: "unknown", ConnectionID: 1, Data: "candidate"}) {
		t.Fatal("listener claimed a signal without a connection owner")
	}
	waitListenerState(t, l, 0, 0)
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
	// Pending connections can still receive signals when admission is full.
	if !l.NotifySignal(&Signal{Type: SignalTypeCandidate, NetworkID: "remote", ConnectionID: 0, Data: "candidate"}) {
		t.Fatal("pending negotiation could not receive a candidate at capacity")
	}
	(<-cancels)()
	waitListenerState(t, l, maxListenerNegotiations-1, maxListenerNegotiations-1)
	if !l.NotifySignal(next) {
		t.Fatal("failed negotiation did not release its admission slot")
	}
	_ = l.Close()
	waitListenerState(t, l, 0, 0)
	if l.NotifySignal(next) {
		t.Fatal("closed listener admitted an offer")
	}
}

func TestListenerPendingOwnerHandlesDirectSignalsAndDuplicates(t *testing.T) {
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
	waitListenerState(t, l, 1, 1)
	if !l.NotifySignal(offer) {
		t.Fatal("duplicate offer was not handled as a rejection")
	}
	if got := waitListenerResponse(t, responses); got.Type != SignalTypeError || got.Data != strconv.Itoa(ErrorCodeIncomingConnectionIgnored) {
		t.Fatalf("duplicate response = %s, want incoming connection ignored", got.String())
	}
	waitListenerState(t, l, 1, 1)
	if conn.Context().Err() != nil {
		t.Fatal("duplicate offer closed the original pending connection")
	}
	if !l.NotifySignal(&Signal{Type: SignalTypeCandidate, ConnectionID: 1, NetworkID: "remote", Data: "candidate:1 1 udp 2130706431 127.0.0.1 9 typ host"}) {
		t.Fatal("candidate was rejected after Conn publication")
	}
	select {
	case <-conn.candidateReceived:
	default:
		t.Fatal("NotifySignal returned before delivering the candidate")
	}
	if !l.NotifySignal(&Signal{Type: SignalTypeError, ConnectionID: 1, NetworkID: "remote", Data: strconv.Itoa(ErrorCodeGenericFailure)}) {
		t.Fatal("remote error was rejected")
	}
	waitListenerState(t, l, 0, 0)
}

func TestListenerAcceptReleasesAdmission(t *testing.T) {
	client, server := newMemorySignalingPair("client", "server")
	t.Cleanup(client.close)
	t.Cleanup(server.close)
	l, _, conn := dialAcceptedListener(t, client, server)
	waitListenerState(t, l, 0, 1)
	_ = conn.Close()
	waitListenerState(t, l, 0, 0)
}

func TestListenerErrorRepliesDoNotBlockNegotiations(t *testing.T) {
	base := newBlockingCredentialsSignaling()
	t.Cleanup(base.close)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	signaling := blockedErrorSignaling{Signaling: base, started: make(chan context.Context, maxListenerSignalErrors), release: release}
	l, err := (ListenConfig{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Listen(signaling)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	fillListenerErrorReplies(t, l, signaling.started)
	waitListenerState(t, l, 0, 0)
	if !l.NotifySignal(&Signal{Type: SignalTypeOffer, NetworkID: "remote", ConnectionID: 1, Data: testOffer(t)}) {
		t.Fatal("blocked error responses prevented a new negotiation")
	}
	waitForCredentialRequest(t, base.started, "offer while error delivery is full")
	if !l.NotifySignal(&Signal{Type: SignalTypeOffer, NetworkID: "invalid", ConnectionID: maxListenerSignalErrors, Data: "invalid"}) {
		t.Fatal("full error budget blocked signal processing")
	}
	waitListenerState(t, l, 1, 1)
	select {
	case <-signaling.started:
		t.Fatal("error reply exceeded the delivery limit")
	default:
	}
	_ = l.Close()
	waitListenerState(t, l, 0, 0)
}

// blockedErrorSignaling holds error responses until their delivery context ends,
// or until an explicit release when testing a backend that stalls past cancellation.
type blockedErrorSignaling struct {
	Signaling
	started chan context.Context
	release <-chan struct{}
}

// Signal stalls error replies while forwarding normal negotiation signals.
func (s blockedErrorSignaling) Signal(ctx context.Context, signal *Signal) error {
	if signal.Type != SignalTypeError {
		return s.Signaling.Signal(ctx, signal)
	}
	s.started <- ctx
	if s.release != nil {
		<-s.release
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

// fillListenerErrorReplies occupies the error budget with malformed offers.
func fillListenerErrorReplies(t *testing.T, l *Listener, started <-chan context.Context) {
	t.Helper()
	for i := range maxListenerSignalErrors {
		if !l.NotifySignal(&Signal{Type: SignalTypeOffer, NetworkID: "invalid", ConnectionID: uint64(i), Data: "invalid"}) {
			t.Fatalf("offer %d rejected before error limit", i)
		}
	}
	for range maxListenerSignalErrors {
		select {
		case ctx := <-started:
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("error delivery has no deadline")
			}
		case <-time.After(time.Second):
			t.Fatal("worker did not attempt to report its error")
		}
	}
}

// waitListenerState waits for offers to reach the expected admission and ownership state.
func waitListenerState(t *testing.T, l *Listener, pending, owners int) {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		l.negotiationsMu.Lock()
		gotPending, gotOwners := len(l.sem), len(l.negotiations)
		l.negotiationsMu.Unlock()
		if gotPending == pending && gotOwners == owners {
			return
		}
		select {
		case <-timeout.C:
			t.Fatalf("listener state = (%d pending, %d owners), want (%d, %d)", gotPending, gotOwners, pending, owners)
		case <-tick.C:
		}
	}
}
