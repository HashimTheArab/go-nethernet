package nethernet

import (
	"cmp"
	"math/bits"
	"net/netip"
	"slices"

	"github.com/pion/webrtc/v4"
)

// fitAnswer encodes desc as the answer to offer. With a non-zero limit, it drops the bundled candidates
// least likely to reach the client until the answer fits, keeping at least one. Those are the
// candidates sharing the smallest leading share of their address with any of remote.
func fitAnswer(desc *Description, remote []webrtc.ICECandidate, offer *Signal, limit int) (*Signal, error) {
	if limit > 0 && len(desc.Candidates) > 1 {
		desc.Candidates = rankCandidates(desc.Candidates, remote)
	}
	for {
		data, err := desc.Encode()
		if err != nil {
			return nil, err
		}
		answer := &Signal{
			Type:         SignalTypeAnswer,
			ConnectionID: offer.ConnectionID,
			Data:         string(data),
			NetworkID:    offer.NetworkID,
		}
		if limit == 0 || len(desc.Candidates) <= 1 || len(answer.String()) <= limit {
			return answer, nil
		}
		desc.Candidates = desc.Candidates[:len(desc.Candidates)-1]
	}
}

// rankCandidates returns local ordered by the largest leading share of its address each has in common
// with a remote candidate of the same family, so a match on an IPv4 /24 outranks one on an IPv6 /48.
// Equal candidates keep their order, so with no comparable remote address the order is unchanged.
func rankCandidates(local, remote []webrtc.ICECandidate) []webrtc.ICECandidate {
	type ranked struct {
		candidate webrtc.ICECandidate
		shared    int
	}
	r := make([]ranked, len(local))
	for i, c := range local {
		r[i] = ranked{candidate: c, shared: sharedPrefixShare(c.Address, remote)}
	}
	slices.SortStableFunc(r, func(a, b ranked) int {
		return cmp.Compare(b.shared, a.shared)
	})
	out := make([]webrtc.ICECandidate, len(r))
	for i, c := range r {
		out[i] = c.candidate
	}
	return out
}

// sharedPrefixShare returns the most leading bits address shares with a remote candidate's address of
// the same family, scaled to a 128-bit address, or -1 if none is comparable.
func sharedPrefixShare(address string, remote []webrtc.ICECandidate) int {
	local, err := netip.ParseAddr(address)
	if err != nil {
		return -1
	}
	local = local.Unmap()
	best := -1
	for _, c := range remote {
		addr, err := netip.ParseAddr(c.Address)
		if err != nil {
			continue
		}
		addr = addr.Unmap()
		if addr.Is4() != local.Is4() {
			continue
		}
		best = max(best, commonPrefixBits(local.AsSlice(), addr.AsSlice())*128/local.BitLen())
	}
	return best
}

// commonPrefixBits returns the number of leading bits a and b, of equal length, share.
func commonPrefixBits(a, b []byte) int {
	for i := range a {
		if x := a[i] ^ b[i]; x != 0 {
			return i*8 + bits.LeadingZeros8(x)
		}
	}
	return len(a) * 8
}
