package nethernet

import (
	"errors"
	"slices"

	"github.com/pion/stun/v4"
	"github.com/pion/webrtc/v4"
)

// Credentials holds the configuration for ICE servers used for gathering local ICE candidates.
type Credentials struct {
	ExpirationInSeconds int         `json:"ExpirationInSeconds"`
	ICEServers          []ICEServer `json:"TurnAuthServers"`
}

// ICEServer represents a single ICE server configuration, including its authentication details
// and connection URLs. Each server requires a username and password for authentication.
type ICEServer struct {
	Username string   `json:"Username"`
	Password string   `json:"Password"`
	URLs     []string `json:"Urls"`
}

var (
	// errNoTURNServer reports relay-only gathering with no TURN server it can authenticate to.
	errNoTURNServer = errors.New("relay-only gathering: no TURN server with a username")
	// errNoCandidates reports gathering that finished without a single local candidate.
	errNoCandidates = errors.New("no local candidates were gathered")
)

// hasTURNServer reports whether credentials name a TURN server with a username.
func hasTURNServer(credentials *Credentials) bool {
	return credentials != nil && slices.ContainsFunc(credentials.ICEServers, func(server ICEServer) bool {
		return server.Username != "" && slices.ContainsFunc(server.URLs, func(u string) bool {
			uri, err := stun.ParseURI(u)
			return err == nil && (uri.Scheme == stun.SchemeTypeTURN || uri.Scheme == stun.SchemeTypeTURNS)
		})
	})
}

// gatherOptions transforms the given Credentials and gather policy into a
// [webrtc.ICEGatherOptions] for gathering local ICE candidates with
// [webrtc.ICEGatherer]. If the given credentials are nil or contain no ICE
// servers, only the gather policy is populated.
func gatherOptions(credentials *Credentials, policy webrtc.ICEGatherPolicy) webrtc.ICEGatherOptions {
	opts := webrtc.ICEGatherOptions{ICEGatherPolicy: policy}
	if credentials != nil && len(credentials.ICEServers) > 0 {
		opts.ICEServers = make([]webrtc.ICEServer, len(credentials.ICEServers))
		for i, server := range credentials.ICEServers {
			opts.ICEServers[i] = webrtc.ICEServer{
				Username:       server.Username,
				Credential:     server.Password,
				CredentialType: webrtc.ICECredentialTypePassword,
				URLs:           server.URLs,
			}
		}
	}
	return opts
}
