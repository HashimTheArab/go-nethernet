package discovery

import (
	"crypto/aes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

// maxApplicationData is the longest ApplicationData whose response fits maxDatagramSize: a SHA-256 HMAC,
// then whole AES blocks holding at least one padding byte, the length, the 18-byte header, the data's
// length prefix and the hex-encoded data.
const maxApplicationData = ((maxDatagramSize-sha256.Size)/aes.BlockSize*aes.BlockSize - 1 - 2 - 18 - 4) / 2

// ResponsePacket is a packet sent by servers in response to a RequestPacket from clients to advertise the world.
type ResponsePacket struct {
	// ApplicationData contains application-specific data for the packet. In Minecraft: Bedrock Edition, it is
	// typically structured as ServerData.
	ApplicationData []byte
}

// ID ...
func (*ResponsePacket) ID() uint16 { return IDResponsePacket }

// Read ...
func (pk *ResponsePacket) Read(r io.Reader) error {
	data, err := readRemaining(r)
	if err != nil {
		return fmt.Errorf("read application data: %w", err)
	}
	n, err := hex.Decode(data, data)
	if err != nil {
		return fmt.Errorf("decode application data: %w", err)
	}
	pk.ApplicationData = data[:n]
	return nil
}

// Write ...
func (pk *ResponsePacket) Write(w io.Writer) {
	data := make([]byte, hex.EncodedLen(len(pk.ApplicationData)))
	hex.Encode(data, pk.ApplicationData)
	writeBytes[uint32](w, data)
}
