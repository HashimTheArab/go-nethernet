package nethernet

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// Like vanilla, a client never reports its own failures to the remote network.
func TestDialDeadlineSignalsNoError(t *testing.T) {
	signaling := newBlockingErrorSignaling("client")

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := (Dialer{}).DialContext(ctx, "server", signaling)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("DialContext() error = %v, want context deadline exceeded", err)
		}
	case <-time.After(time.Millisecond * 250):
		t.Fatal("DialContext() did not return promptly after its context deadline")
	}

	select {
	case <-signaling.errorSignalStarted:
		t.Fatal("DialContext() signaled its timeout to the remote network")
	case <-time.After(100 * time.Millisecond):
	}
}

type blockingErrorSignaling struct {
	id string

	ctx    context.Context
	cancel context.CancelCauseFunc

	once               sync.Once
	errorSignalStarted chan struct{}
}

func newBlockingErrorSignaling(id string) *blockingErrorSignaling {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &blockingErrorSignaling{
		id:                 id,
		ctx:                ctx,
		cancel:             cancel,
		errorSignalStarted: make(chan struct{}),
	}
}

func (s *blockingErrorSignaling) Signal(ctx context.Context, signal *Signal) error {
	if signal.Type != SignalTypeError {
		return nil
	}
	s.once.Do(func() {
		close(s.errorSignalStarted)
	})
	<-ctx.Done()
	return ctx.Err()
}

func (*blockingErrorSignaling) Notify(Notifier) func() {
	return func() {}
}

func (s *blockingErrorSignaling) Context() context.Context {
	return s.ctx
}

func (*blockingErrorSignaling) Credentials(context.Context) (*Credentials, error) {
	return nil, nil
}

func (s *blockingErrorSignaling) NetworkID() string {
	return s.id
}

func (*blockingErrorSignaling) PongData([]byte) {}

func TestDialerNotifierDropsMalformedSignals(t *testing.T) {
	n := &dialerNotifier{Dialer: Dialer{ConnectionID: 42, Log: slog.New(slog.DiscardHandler)}, networkID: "server", ctx: t.Context(), signals: make(chan *Signal, 1)}
	for _, signal := range []*Signal{
		{Type: SignalTypeError, Data: "invalid"},
		{Type: SignalTypeError, Data: "2147483648"},
		{Type: "UNKNOWN", Data: "data"},
	} {
		signal.ConnectionID, signal.NetworkID = 42, "server"
		if n.NotifySignal(signal) {
			t.Fatalf("NotifySignal(%q, %q) accepted malformed signal", signal.Type, signal.Data)
		}
		if len(n.signals) != 0 {
			t.Fatal("malformed signal consumed dialer queue capacity")
		}
	}
}

func TestDialerIgnoresMalformedSignalsBeforeAnswer(t *testing.T) {
	client, server := newMemorySignalingPair("client", "server")
	t.Cleanup(client.close)
	t.Cleanup(server.close)
	_, clientConn, serverConn := dialAcceptedListener(t, client, malformedBeforeAnswerSignaling{server})
	checkConnPayload(t, clientConn, serverConn, []byte("handshake survived malformed signals"))
	if got := client.signalCount(SignalTypeError); got != 0 {
		t.Fatalf("client sent %d error replies to malformed signals", got)
	}
}

// malformedBeforeAnswerSignaling injects invalid signals before forwarding the answer.
type malformedBeforeAnswerSignaling struct{ Signaling }

// Signal delivers malformed signals while the dialer is still waiting for its answer.
func (s malformedBeforeAnswerSignaling) Signal(ctx context.Context, signal *Signal) error {
	if signal.Type == SignalTypeAnswer {
		for _, injected := range []Signal{
			{Type: SignalTypeError, Data: "invalid"},
			{Type: SignalTypeError, Data: ""},
			{Type: SignalTypeError, Data: "+1"},
			{Type: SignalTypeError, Data: "2147483648"},
			{Type: "UNKNOWN", Data: "data"},
		} {
			injected.NetworkID, injected.ConnectionID = signal.NetworkID, signal.ConnectionID
			if err := s.Signaling.Signal(ctx, &injected); err != nil {
				return err
			}
		}
	}
	return s.Signaling.Signal(ctx, signal)
}

// awaitingSignaling answers Credentials with no servers and AwaitCredentials with servers or err.
type awaitingSignaling struct {
	recordingSignaling
	servers []ICEServer
	err     error
	awaited atomic.Bool
}

func (s *awaitingSignaling) AwaitCredentials(context.Context) (*Credentials, error) {
	s.awaited.Store(true)
	return &Credentials{ICEServers: s.servers}, s.err
}

// A relay-only dial waits for TURN servers through AwaitCredentials and fails at once when the
// answer is an error or names no TURN server it can authenticate to; other dials never wait.
func TestRelayOnlyDialRequiresTURNServer(t *testing.T) {
	stun := ICEServer{URLs: []string{"stun:relay.example:3478"}}
	anonymous := ICEServer{URLs: []string{"turn:relay.example:3478"}}
	malformed := ICEServer{Username: "u", URLs: []string{"turn:"}}
	turnFailed := errors.New("turn unavailable")
	for name, test := range map[string]struct {
		servers []ICEServer
		err     error
		want    error
	}{
		"failed request": {err: turnFailed, want: turnFailed},
		"no servers":     {want: ErrNoTURNServer},
		"stun only":      {servers: []ICEServer{stun}, want: ErrNoTURNServer},
		"no username":    {servers: []ICEServer{anonymous}, want: ErrNoTURNServer},
		"malformed URL":  {servers: []ICEServer{malformed}, want: ErrNoTURNServer},
	} {
		signaling := &awaitingSignaling{recordingSignaling: recordingSignaling{id: "1", ctx: context.Background(), signals: make(chan *Signal, 8)}, servers: test.servers, err: test.err}
		_, err := (Dialer{ICEGatherPolicy: webrtc.ICETransportPolicyRelay}).DialContext(context.Background(), "2", signaling)
		if !errors.Is(err, test.want) {
			t.Fatalf("%s: DialContext() error = %v, want %v", name, err, test.want)
		}
		if len(signaling.signals) != 0 {
			t.Fatalf("%s: dial signaled an offer", name)
		}
	}

	signaling := &awaitingSignaling{recordingSignaling: recordingSignaling{id: "1", ctx: context.Background(), signals: make(chan *Signal, 8)}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, _ = (Dialer{}).DialContext(ctx, "2", signaling)
	if signaling.awaited.Load() {
		t.Fatal("a dial gathering every candidate type waited for TURN servers")
	}
}

// A relay-only dial whose TURN server refuses the allocation fails once gathering ends, rather than
// waiting for an answer to an offer the remote peer could never reach.
func TestRelayOnlyDialFailsWithoutRelayCandidate(t *testing.T) {
	turnURL := startTURNServer(t, false)
	for _, bundled := range []bool{false, true} {
		signaling := &recordingSignaling{id: "1", ctx: context.Background(), signals: make(chan *Signal, 64)}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		start := time.Now()
		_, err := (Dialer{
			ICEGatherPolicy:   webrtc.ICETransportPolicyRelay,
			DisableTrickleICE: bundled,
			Credentials: func(context.Context) (*Credentials, error) {
				return &Credentials{ICEServers: []ICEServer{{Username: "user", Password: "pass", URLs: []string{turnURL}}}}, nil
			},
		}).DialContext(ctx, "2", signaling)
		cancel()
		if !errors.Is(err, ErrNoCandidates) {
			t.Fatalf("bundled=%v: DialContext() error = %v after %v, want %v", bundled, err, time.Since(start), ErrNoCandidates)
		}
	}
}

// A relay-only listener waits for TURN servers through AwaitCredentials and, with none, answers the
// offer with ErrorCodeSignalingTurnAuthFailed.
func TestRelayOnlyListenerRequiresTURNServer(t *testing.T) {
	signaling := &awaitingSignaling{recordingSignaling: recordingSignaling{id: "1", ctx: context.Background(), signals: make(chan *Signal, 8)}}
	l, err := (ListenConfig{ICEGatherPolicy: webrtc.ICETransportPolicyRelay, Log: slog.New(slog.DiscardHandler)}).Listen(signaling)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if !l.NotifySignal(&Signal{Type: SignalTypeOffer, NetworkID: "remote", ConnectionID: 1, Data: testOffer(t)}) {
		t.Fatal("offer rejected")
	}
	select {
	case signal := <-signaling.signals:
		if signal.Type != SignalTypeError || signal.Data != strconv.Itoa(ErrorCodeSignalingTurnAuthFailed) {
			t.Fatalf("listener signaled %v, want error code %d", signal, ErrorCodeSignalingTurnAuthFailed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listener sent no error reply")
	}
	if !signaling.awaited.Load() {
		t.Fatal("relay-only listener did not wait for TURN servers")
	}
}
