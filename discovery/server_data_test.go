package discovery

import (
	"testing"
)

func TestServerDataMarshalUnmarshalBinary(t *testing.T) {
	original := &ServerData{
		ServerName:            "server",
		LevelName:             "world",
		GameType:              GameTypeAdventure,
		PlayerCount:           1,
		MaxPlayerCount:        8,
		EditorWorld:           false,
		Hardcore:              false,
		AcceptsOnlineAuth:     true,
		AcceptsSelfSignedAuth: true,
		TransportLayer:        TransportLayerNetherNet,
		ConnectionType:        4,
	}
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}

	decoded := &ServerData{}
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}

	if *original != *decoded {
		t.Fatalf("decoded ServerData does not match original:\ngot:  %+v\nwant: %+v", decoded, original)
	}
}
