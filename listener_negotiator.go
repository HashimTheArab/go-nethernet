package nethernet

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
)

// listenerNegotiator owns one connection and processes its signals in order.
// Its worker exits when the queue empties and restarts for later signals.
// All fields except conn are guarded by Listener.negotiationsMu. Only the
// signal worker sets conn, before starting transports or handing it to Accept.
type listenerNegotiator struct {
	*Listener
	key     negotiationKey
	queue   []*Signal
	running bool
	pending bool
	closed  chan struct{}
	conn    *Conn
}

const (
	// maxListenerNegotiations bounds connections that have not reached Accept.
	maxListenerNegotiations = 64
	// maxListenerSignalWorkers bounds workers, including those sending errors.
	maxListenerSignalWorkers = 64
	// maxPendingSignalsPerNegotiation bounds the queue for one connection.
	maxPendingSignalsPerNegotiation = 32
)

// background processes queued signals until this worker becomes idle or closes.
func (n *listenerNegotiator) background() {
	for {
		signal, ok := n.nextSignal()
		if !ok {
			return
		}
		var err error
		switch signal.Type {
		case SignalTypeOffer:
			err = n.handleOffer(signal)
		default:
			err = n.handleSignal(signal)
		}
		if err == nil {
			continue
		}
		if signal.Type == SignalTypeOffer && n.conn == nil {
			n.close()
		}
		var s *signalError
		if errors.As(err, &s) {
			// The negotiation may already have closed. Bound error delivery with
			// a fresh listener context so a stalled sender cannot retain a worker.
			ctx, cancel := context.WithTimeout(n.Listener.Context(), 2*time.Second)
			signalErr := n.signaling.Signal(ctx, &Signal{
				Type:         SignalTypeError,
				ConnectionID: signal.ConnectionID,
				Data:         strconv.FormatUint(uint64(s.code), 10),
				NetworkID:    signal.NetworkID,
			})
			cancel()
			if signalErr != nil {
				n.conf.Log.Error("error signaling error", slog.Any("error", signalErr))
			}
		}
		n.conf.Log.Error("error handling signal", slog.Any("signal", signal), slog.Any("error", err))
	}
}

// nextSignal takes the next signal or releases the worker when its queue is empty.
func (n *listenerNegotiator) nextSignal() (*Signal, bool) {
	n.negotiationsMu.Lock()
	defer n.negotiationsMu.Unlock()
	if len(n.queue) == 0 {
		n.running = false
		n.workers--
		return nil, false
	}
	signal := n.queue[0]
	n.queue[0] = nil
	n.queue = n.queue[1:]
	if len(n.queue) == 0 {
		n.queue = nil
	}
	return signal, true
}

// Context is canceled when the connection owner or its listener closes.
func (n *listenerNegotiator) Context() context.Context {
	return listenerContext{n.closed}
}

// releasePending frees an admission slot after Accept takes ownership of the Conn.
func (n *listenerNegotiator) releasePending() {
	n.negotiationsMu.Lock()
	defer n.negotiationsMu.Unlock()
	n.releasePendingLocked()
}

// releasePendingLocked frees an admission slot once, with negotiationsMu held.
func (n *listenerNegotiator) releasePendingLocked() {
	if n.pending {
		n.pending = false
		n.Listener.pending--
	}
}

// close unregisters this connection owner and cancels pending negotiation work.
func (n *listenerNegotiator) close() {
	n.negotiationsMu.Lock()
	defer n.negotiationsMu.Unlock()
	n.closeLocked()
}

// closeLocked closes this owner once, with negotiationsMu held.
func (n *listenerNegotiator) closeLocked() {
	select {
	case <-n.closed:
		return
	default:
	}
	close(n.closed)
	n.queue = nil
	n.releasePendingLocked()
	if n.negotiations[n.key] == n {
		delete(n.negotiations, n.key)
	}
}

// handleOffer handles an incoming Signal of SignalTypeOffer. It parses the data of Signal into [sdp.SessionDescription]
// and transforms into remote description for later use in negotiation. An answer will be created from local parameters of
// each transport and signaled back to the remote connection referenced in the offer.
func (n *listenerNegotiator) handleOffer(signal *Signal) error {
	if n.conn != nil {
		return wrapSignalError(errors.New("duplicate offer for same connection"), ErrorCodeIncomingConnectionIgnored)
	}

	d := &sdp.SessionDescription{}
	if err := d.UnmarshalString(signal.Data); err != nil {
		return wrapSignalError(fmt.Errorf("decode offer: %w", err), ErrorCodeFailedToSetRemoteDescription)
	}
	desc, err := parseDescription(d)
	if err != nil {
		return wrapSignalError(fmt.Errorf("parse offer: %w", err), ErrorCodeFailedToSetRemoteDescription)
	}

	var (
		ctx    context.Context
		parent = n.Context()
	)
	if n.conf.NegotiationContext != nil {
		var cancel context.CancelFunc
		ctx, cancel = n.conf.NegotiationContext(parent)
		if ctx == nil {
			panic("nethernet: Listener: NegotiationContext returned nil")
		}
		defer cancel()
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parent, time.Second*15)
		defer cancel()
	}
	credentials, err := n.signaling.Credentials(ctx)
	if err != nil {
		return wrapSignalError(fmt.Errorf("obtain credentials: %w", err), ErrorCodeSignalingTurnAuthFailed)
	}

	c, err := newConn(
		n.conf.API,
		gatherOptions(credentials, n.conf.ICEGatherPolicy),
		signal.ConnectionID,
		signal.NetworkID,
		n.networkID,
		n,
		ErrorCodeFailedToCreateAnswer,
	)
	if err != nil {
		return fmt.Errorf("create peer connection: %w", err)
	}
	established := false
	defer func() {
		if !established {
			_ = c.Close()
		}
	}()
	disableTrickleICE := shouldDisableTrickleICE(n.conf.DisableTrickleICE, n.signaling)
	if disableTrickleICE {
		c.description.candidates, err = c.gatherCandidates(ctx)
		if err != nil {
			return wrapSignalError(fmt.Errorf("gather local candidates: %w", err), ErrorCodeICE)
		}
	}
	for _, candidate := range desc.candidates {
		// Non-trickle ICE connection may include candidates in a single SDP.
		if err := c.addRemoteCandidate(candidate); err != nil {
			return wrapSignalError(fmt.Errorf("add inline candidate: %w", err), ErrorCodeFailedToSetRemoteDescription)
		}
	}
	c.description.dtls.Role = n.answererRole(desc.dtls.Role)

	if desc.identity != nil {
		publicKey, err := n.conf.VerifyClientToken(ctx, desc.identity.Assertion.Token)
		if err != nil {
			return wrapSignalError(fmt.Errorf("verify client token: %w", err), ErrorCodeIdentityNotAllowed)
		}
		if publicKey == nil {
			publicKey, err = claimPublicKey(desc.identity.Assertion.Token, false)
			if err != nil {
				return wrapSignalError(fmt.Errorf("claim public key: %w", err), ErrorCodeIdentityNotAllowed)
			}
		}
		if err := desc.identity.verify(desc, publicKey); err != nil {
			return wrapSignalError(fmt.Errorf("verify identity assertion: %w", err), ErrorCodeIdentityNotAllowed)
		}
		c.publicKey = publicKey
	} else if !n.conf.AllowAnonymous {
		n.conf.Log.Warn("rejecting anonymous identity because AllowAnonymous is false",
			slog.Uint64("connectionID", signal.ConnectionID),
			slog.String("networkID", signal.NetworkID),
		)
		return wrapSignalError(errors.New("nethernet: anonymous identity not allowed"), ErrorCodeIdentityNotAllowed)
	}
	identity, err := n.conf.IssueServerIdentity(ctx)
	if err != nil {
		return wrapSignalError(fmt.Errorf("issue server identity: %w", err), ErrorCodeFailedToCreateIdentityAssertion)
	}
	if err := identity.sign(c.description); err != nil {
		return wrapSignalError(fmt.Errorf("generate identity assertion: %w", err), ErrorCodeFailedToCreateIdentityAssertion)
	}

	// Register a callback function immediately since the remote peer
	// may open data channels at any time while ICE candidates are being signaled.
	var (
		opened        atomic.Uint32
		channelsReady = make(chan struct{})
	)
	c.sctp.OnDataChannel(func(channel *webrtc.DataChannel) {
		for r := range messageReliabilityCapacity {
			if r.Valid(channel) {
				ch := wrapDataChannel(channel, r, c)
				if existing := c.storeChannel(r, ch); existing != nil {
					go c.close(fmt.Errorf("data channel created for same reliability parameters: %q", r.Parameters().Label))
					return
				}
				channel.OnOpen(sync.OnceFunc(func() {
					// If all data channels have been opened by remote peer, we can signal that the connection is ready.
					if opened.Add(1) == uint32(messageReliabilityCapacity) {
						close(channelsReady)
					}
				}))
				return
			}
		}
		go c.close(fmt.Errorf("invalid data channel opened: %q", channel.Label()))
	})

	// Encode an answer using the local parameters!
	answer, err := c.description.encode()
	if err != nil {
		return wrapSignalError(fmt.Errorf("encode answer: %w", err), ErrorCodeFailedToCreateAnswer)
	}

	if err := n.signaling.Signal(ctx, &Signal{
		Type:         SignalTypeAnswer,
		ConnectionID: signal.ConnectionID,
		Data:         string(answer),
		NetworkID:    signal.NetworkID,
	}); err != nil {
		// I don't think the error code will be signaled back to the remote connection, but just in case.
		return wrapSignalError(fmt.Errorf("signal answer: %w", err), ErrorCodeSignalingFailedToSend)
	}

	if !disableTrickleICE {
		if err := c.trickleCandidates(n.signaling); err != nil {
			return wrapSignalError(fmt.Errorf("start gathering local candidates: %w", err), ErrorCodeFailedToCreatePeerConnection)
		}
	}

	n.conn = c
	go n.handleConn(c, desc, channelsReady)
	established = true
	return nil
}

// answererRole returns the local [webrtc.DTLSRole] for an answer based on the
// role signaled by the remote peer. If the remote peer uses
// [webrtc.DTLSRoleAuto], it will be [webrtc.DTLSRoleClient] since the ICE
// transport will always start as controlled.
func (n *listenerNegotiator) answererRole(role webrtc.DTLSRole) webrtc.DTLSRole {
	switch role {
	case webrtc.DTLSRoleServer:
		return webrtc.DTLSRoleClient
	case webrtc.DTLSRoleClient:
		return webrtc.DTLSRoleServer
	default:
		return webrtc.DTLSRoleClient
	}
}

// handleSignal handles the given Signal received from the remote network in the Conn.
func (n *listenerNegotiator) handleSignal(signal *Signal) error {
	if n.conn == nil {
		// Should not happen as long as we don't accept ICE candidates before offer in Listener.NotifySignal.
		return fmt.Errorf("attempting to handle non-offer signal before creating a Conn")
	}
	return n.conn.handleSignal(signal)
}

// handleClose deletes the Conn from the Listener, since it is closed and can no longer be negotiated.
func (n *listenerNegotiator) handleClose(*Conn) {
	n.close()
}

// log extends the [slog.Logger] from [ListenConfig.Log] with an additional [slog.Attr] of "src" with the
// value "listener" to mark that the Conn has been negotiated by Listener, and returns it to be used as the logger
// of a Conn.
func (n *listenerNegotiator) log() *slog.Logger {
	return n.conf.Log.With(slog.String("src", "listener"))
}

// handleConn finalises the Conn. Once an ICE candidate for the Conn has been signaled from the remote
// connection, it starts the transports of the Conn using the remote description and a context.Context]
// returned from [ListenConfig.ConnContext].
func (n *listenerNegotiator) handleConn(conn *Conn, d *description, channelsReady <-chan struct{}) {
	var (
		ctx    context.Context
		parent = n.Context()
	)
	if n.conf.ConnContext != nil {
		var cancel context.CancelFunc
		ctx, cancel = n.conf.ConnContext(parent, conn)
		if ctx == nil {
			panic("nethernet: ConnContext returned nil")
		}
		defer cancel()
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parent, time.Second*5)
		defer cancel()
	}

	var err error
	defer func() {
		if err != nil {
			_ = conn.Close() // Stop notifying for the Conn.

			if errors.Is(err, context.DeadlineExceeded) {
				// ctx is already expired: use a fresh context so the signal has a chance to be delivered.
				sigCtx, cancel := context.WithTimeout(n.Listener.Context(), time.Second*2)
				defer cancel()
				if err := n.signaling.Signal(sigCtx, &Signal{
					Type:         SignalTypeError,
					ConnectionID: conn.id,
					Data:         strconv.Itoa(ErrorCodeNegotiationTimeoutWaitingForAccept),
					NetworkID:    conn.networkID,
				}); err != nil {
					conn.log.Error("error signaling timeout", slog.Any("error", err))
				}
			}
			if !errors.Is(err, net.ErrClosed) {
				conn.log.Error("error starting transports", slog.Any("error", err))
			}
		}
	}()

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-n.closed:
		err = net.ErrClosed
	case <-conn.ctx.Done():
		err = context.Cause(conn.ctx)
	case <-conn.candidateReceived:
		conn.log.Debug("received first candidate")
		if err = n.startTransports(ctx, conn, d, channelsReady); err != nil {
			conn.log.Error("error starting transports", slog.Any("error", err))
			return
		}

		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-conn.ctx.Done():
			err = context.Cause(conn.ctx)
		case <-n.closed:
			_ = conn.Close()
		case n.incoming <- n:
			// Accept releases the admission slot. Keep the owner registered so
			// late signals and duplicate offers still reach this connection.
		}
	}
}

// startTransports starts ICE as [webrtc.ICERoleControlled], then starts DTLS
// and SCTP using the remote description. It blocks until the remote peer has
// created both 'ReliableDataChannel' and 'UnreliableDataChannel'. The provided
// [context.Context] is used to control the deadline.
func (n *listenerNegotiator) startTransports(ctx context.Context, conn *Conn, d *description, channelsReady <-chan struct{}) error {
	conn.log.Debug("starting ICE transport as controlled")
	iceRole := webrtc.ICERoleControlled
	if err := conn.ice.StartContext(ctx, nil, d.ice, &iceRole); err != nil {
		return fmt.Errorf("start ICE: %w", err)
	}

	conn.log.Debug("starting DTLS transport", slog.String("remoteRole", d.dtls.Role.String()))
	if err := conn.dtls.StartContext(ctx, d.dtls); err != nil {
		return fmt.Errorf("start DTLS: %w", err)
	}

	conn.log.Debug("starting SCTP transport")
	if err := withContextCancel(ctx, func() error {
		return conn.sctp.Start(d.sctp)
	}, func() {
		_ = conn.sctp.Stop()
	}); err != nil {
		return fmt.Errorf("start SCTP: %w", err)
	}

	conn.maxSegmentPayload.Store(conn.sctp.GetCapabilities().MaxMessageSize - 1)
	return n.waitForChannelsReady(ctx, conn, channelsReady)
}

// waitForChannelsReady blocks until all data channels have been opened by the
// remote peer, or until the Listener, Conn, or context is closed.
func (n *listenerNegotiator) waitForChannelsReady(ctx context.Context, conn *Conn, channelsReady <-chan struct{}) error {
	select {
	case <-n.closed:
		return net.ErrClosed
	case <-channelsReady:
		return nil
	case <-conn.ctx.Done():
		return context.Cause(conn.ctx)
	case <-ctx.Done():
		if err := context.Cause(conn.ctx); err != nil {
			return err
		}
		return context.Cause(ctx)
	}
}
