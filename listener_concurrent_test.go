package nethernet

import (
	"context"
	"log/slog"
	"os"
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

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
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
	for i := range 32 {
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
