package discovery

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// GameType represents the default game mode of a world.
const (
	GameTypeSurvival       int32 = 0
	GameTypeCreative       int32 = 1
	GameTypeAdventure      int32 = 2
	GameTypeSurvivalViewer int32 = 3
	GameTypeCreativeViewer int32 = 4
	GameTypeDefault        int32 = 5
)

// TransportLayer indicates the transport protocol used by a server.
const (
	TransportLayerRakNet    int32 = 0
	TransportLayerNetherNet int32 = 2
	TransportLayerLocal     int32 = 4
)

// ServerData defines the binary structure representing worlds in Minecraft: Bedrock Edition.
// It is encapsulated in [ResponsePacket.ApplicationData] and sent in response to [RequestPacket]
// broadcasted by clients on port 7551.
type ServerData struct {
	// ServerName is the name of the server. It is typically the player name of the owner
	// hosting the server and is displayed below the LevelName in the world card.
	ServerName string
	// LevelName identifies the name of the world and appears at the top of ServerName in the world card.
	LevelName string
	// GameType is the default game mode of the world. Players receive this game mode when they
	// join. It remains unchanged during gameplay and may be updated the next time the world is hosted.
	GameType int32
	// PlayerCount is the amount of players currently connected to the world. Worlds
	// with a player count of 0 or less are not displayed by clients, so it should be at
	// least 1 even if the server reports 0 to prevent world cards not appearing for the server.
	PlayerCount int32
	// MaxPlayerCount is the maximum amount of players allowed to join the world.
	MaxPlayerCount int32
	// EditorWorld indicates whether the world was created as a project in Editor Mode.
	// When enabled, the server or world card is only visible to clients in Editor Mode.
	EditorWorld bool
	// Hardcore indicates that the world is in hardcore mode. When enabled, it is common to also set
	// GameType to Survival (0) to reproduce expected behavior.
	Hardcore bool
	// AcceptsOnlineAuth indicates whether the server accepts online-authenticated (Xbox Live) players.
	AcceptsOnlineAuth bool
	// AcceptsSelfSignedAuth indicates whether the server accepts self-signed (LAN) authentication.
	AcceptsSelfSignedAuth bool
	// TransportLayer indicates the transport layer used by the server. Known values are
	// TransportLayerRakNet (0), TransportLayerNetherNet (2), and TransportLayerLocal (4).
	TransportLayer int32
	// ConnectionType indicates the connection type used alongside the transport layer. The
	// exact meaning of its values is not fully documented; vanilla NetherNet LAN discovery
	// typically sends 4.
	ConnectionType int32
}

// MarshalBinary encodes the ServerData into its binary wire representation.
func (d *ServerData) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteByte(version)
	writeString(buf, d.ServerName)
	writeString(buf, d.LevelName)
	writeVarInt(buf, d.GameType)
	_ = binary.Write(buf, binary.LittleEndian, d.PlayerCount)
	_ = binary.Write(buf, binary.LittleEndian, d.MaxPlayerCount)
	buf.WriteByte(boolByte(d.EditorWorld))
	buf.WriteByte(boolByte(d.Hardcore))
	buf.WriteByte(boolByte(d.AcceptsOnlineAuth))
	buf.WriteByte(boolByte(d.AcceptsSelfSignedAuth))
	writeVarInt(buf, d.TransportLayer)
	writeVarInt(buf, d.ConnectionType)
	return buf.Bytes(), nil
}

// UnmarshalBinary decodes the ServerData from its binary wire representation.
func (d *ServerData) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)

	v, err := buf.ReadByte()
	if err != nil {
		return fmt.Errorf("read version: %w", err)
	}
	if v != version {
		return fmt.Errorf("version mismatch: got %d, want %d", v, version)
	}
	d.ServerName, err = readString(buf)
	if err != nil {
		return fmt.Errorf("read server name: %w", err)
	}
	d.LevelName, err = readString(buf)
	if err != nil {
		return fmt.Errorf("read level name: %w", err)
	}
	d.GameType, err = readVarInt(buf)
	if err != nil {
		return fmt.Errorf("read game type: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &d.PlayerCount); err != nil {
		return fmt.Errorf("read player count: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &d.MaxPlayerCount); err != nil {
		return fmt.Errorf("read max player count: %w", err)
	}
	d.EditorWorld, err = readBool(buf)
	if err != nil {
		return fmt.Errorf("read editor world: %w", err)
	}
	d.Hardcore, err = readBool(buf)
	if err != nil {
		return fmt.Errorf("read hardcore: %w", err)
	}
	d.AcceptsOnlineAuth, err = readBool(buf)
	if err != nil {
		return fmt.Errorf("read accepts online auth: %w", err)
	}
	d.AcceptsSelfSignedAuth, err = readBool(buf)
	if err != nil {
		return fmt.Errorf("read accepts self-signed auth: %w", err)
	}
	d.TransportLayer, err = readVarInt(buf)
	if err != nil {
		return fmt.Errorf("read transport layer: %w", err)
	}
	d.ConnectionType, err = readVarInt(buf)
	if err != nil {
		return fmt.Errorf("read connection type: %w", err)
	}
	if remaining := buf.Len(); remaining != 0 {
		return fmt.Errorf("unread %d bytes", remaining)
	}
	return nil
}

// version is the current version of ServerData as supported by the discovery package.
const version uint8 = 5

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func readBool(r io.ByteReader) (bool, error) {
	b, err := r.ReadByte()
	return b != 0, err
}

// writeString writes a string with a varint-length prefix, matching BinaryStream::writeString.
func writeString(buf *bytes.Buffer, s string) {
	writeUnsignedVarInt(buf, uint32(len(s)))
	buf.WriteString(s)
}

// readString reads a varint-length-prefixed string, matching ReadOnlyBinaryStream::getString.
func readString(buf *bytes.Buffer) (string, error) {
	length, err := readUnsignedVarInt(buf)
	if err != nil {
		return "", err
	}
	if int(length) > buf.Len() {
		return "", fmt.Errorf("string length %d exceeds remaining %d bytes", length, buf.Len())
	}
	b := make([]byte, length)
	if _, err := io.ReadFull(buf, b); err != nil {
		return "", err
	}
	return string(b), nil
}

// writeVarInt writes a signed 32-bit integer as a zigzag-encoded variable-length integer,
// matching BinaryStream::writeVarInt.
func writeVarInt(buf *bytes.Buffer, v int32) {
	writeUnsignedVarInt(buf, uint32((v<<1)^(v>>31)))
}

// readVarInt reads a zigzag-encoded variable-length integer and decodes it as a signed int32,
// matching ReadOnlyBinaryStream::getVarInt.
func readVarInt(r io.ByteReader) (int32, error) {
	u, err := readUnsignedVarInt(r)
	if err != nil {
		return 0, err
	}
	return int32((u >> 1) ^ -(u & 1)), nil
}

func writeUnsignedVarInt(buf *bytes.Buffer, v uint32) {
	for v >= 0x80 {
		buf.WriteByte(byte(v) | 0x80)
		v >>= 7
	}
	buf.WriteByte(byte(v))
}

func readUnsignedVarInt(r io.ByteReader) (uint32, error) {
	var v uint32
	for i := uint(0); i < 35; i += 7 {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		v |= uint32(b&0x7F) << i
		if b&0x80 == 0 {
			return v, nil
		}
	}
	return 0, fmt.Errorf("varint too long")
}
