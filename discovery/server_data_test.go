package discovery

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestServerDataMarshalBinary(t *testing.T) {
	data := testServerData("server", "world")
	data.Hardcore = true
	data.AcceptsOnlineAuth = true

	got, err := data.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	want := []byte{
		0x07,
		0x06, 's', 'e', 'r', 'v', 'e', 'r',
		0x8A, 0x22,
		0x7, '1', '.', '2', '6', '.', '5', '0',
		0x05, 'w', 'o', 'r', 'l', 'd',
		0x02,
		0x10,
		0x04,
		0x00,
		0x01,
		0x01,
		0x01,
		0x5, 'n', 'o', 'n', 'c', 'e',
		0x08,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("MarshalBinary() = % x, want % x", got, want)
	}
}

func TestServerDataUnmarshalBinary(t *testing.T) {
	var data ServerData
	if err := data.UnmarshalBinary([]byte{
		0x07,
		0x06, 's', 'e', 'r', 'v', 'e', 'r',
		0x8A, 0x22,
		0x7, '1', '.', '2', '6', '.', '5', '0',
		0x05, 'w', 'o', 'r', 'l', 'd',
		0x02,
		0x10,
		0x04,
		0x00,
		0x01,
		0x01,
		0x00,
		0x5, 'n', 'o', 'n', 'c', 'e',
		0x08,
	}); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}
	if data.ServerName != "server" || data.LevelName != "world" || data.GameType != GameTypeAdventure {
		t.Fatalf("UnmarshalBinary() = %#v", data)
	}
	if data.Protocol != 2181 {
		t.Fatalf("protocol: %d != 2181", data.Protocol)
	}
	if data.Version != "1.26.50" {
		t.Fatalf("version: %q != \"1.26.50\"", data.Version)
	}
	if data.PlayerCount != 1 || data.MaxPlayerCount != 8 {
		t.Fatalf("UnmarshalBinary() player counts = %d/%d, want 1/8", data.PlayerCount, data.MaxPlayerCount)
	}
	if data.EditorWorld || !data.Hardcore || !data.AcceptsOnlineAuth || data.AcceptsSelfSignedAuth {
		t.Fatalf("UnmarshalBinary() bools = editor %v hardcore %v online %v self-signed %v", data.EditorWorld, data.Hardcore, data.AcceptsOnlineAuth, data.AcceptsSelfSignedAuth)
	}
	if data.ConnectionType != 4 {
		t.Fatalf("connection type: %d != 4", data.ConnectionType)
	}
}

func TestServerDataMarshalBinaryAllowsLongVarintStrings(t *testing.T) {
	data := testServerData(strings.Repeat("s", 300), strings.Repeat("l", 300))
	if _, err := data.MarshalBinary(); err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
}

func testServerData(serverName, levelName string) *ServerData {
	return &ServerData{
		ServerName:            serverName,
		Protocol:              2181,
		Version:               "1.26.50",
		LevelName:             levelName,
		GameType:              GameTypeAdventure,
		PlayerCount:           1,
		MaxPlayerCount:        8,
		AcceptsSelfSignedAuth: true,
		ConnectionType:        4,
		Nonce:                 "nonce",
	}
}

// Long names are shortened so the response fits one datagram, keeping the fields the client relies on.
func TestListenerServerDataFitsOneDatagram(t *testing.T) {
	long := strings.Repeat("Long Server Name ", 40)
	for name, set := range map[string]func(l *Listener){
		"ServerData": func(l *Listener) {
			l.ServerData(&ServerData{ServerName: long, LevelName: strings.Repeat("𝓦", 200), Version: "1.26.60", Nonce: "6e6f6e6365", ConnectionType: 4})
		},
		"PongData": func(l *Listener) {
			l.PongData([]byte("MCPE;" + long + ";924;1.26.60;1;10;1234;" + long + ";Survival;1;19132;19133;0;"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			l := &Listener{}
			set(l)
			data := *l.pongData.Load()
			if n := len(Marshal(&ResponsePacket{ApplicationData: data}, 1<<64-1)); n > maxDatagramSize {
				t.Fatalf("response is %d bytes, want at most %d", n, maxDatagramSize)
			}
			var d ServerData
			if err := d.UnmarshalBinary(data); err != nil {
				t.Fatalf("UnmarshalBinary() error = %v", err)
			}
			if !utf8.ValidString(d.ServerName) || !utf8.ValidString(d.LevelName) || d.ServerName == "" || d.LevelName == "" {
				t.Fatalf("names = %q, %q, want valid, non-empty prefixes", d.ServerName, d.LevelName)
			}
			if name == "ServerData" && (d.Version != "1.26.60" || d.Nonce != "6e6f6e6365") {
				t.Fatalf("Version, Nonce = %q, %q, want them unchanged", d.Version, d.Nonce)
			}
		})
	}
}

func TestServerDataFitNames(t *testing.T) {
	for _, tc := range []struct {
		name          string
		server, level string
		// wantServer and wantLevel are checked when set.
		wantServer, wantLevel string
	}{
		{name: "short data is unchanged", server: "Lunar Proxy", level: "World", wantServer: "Lunar Proxy", wantLevel: "World"},
		{name: "only the longer name is cut", server: strings.Repeat("s", 700), level: "World", wantLevel: "World"},
		{name: "a cut keeps whole runes", server: strings.Repeat("é", 400), level: strings.Repeat("ü", 400)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := ServerData{ServerName: tc.server, LevelName: tc.level, Version: "1.26.60", Nonce: "n"}
			got := d.fitNames(maxApplicationData)
			if (tc.wantServer != "" && got.ServerName != tc.wantServer) || (tc.wantLevel != "" && got.LevelName != tc.wantLevel) {
				t.Fatalf("fitNames() names = %q, %q, want %q, %q", got.ServerName, got.LevelName, tc.wantServer, tc.wantLevel)
			}
			if !strings.HasPrefix(tc.server, got.ServerName) || !strings.HasPrefix(tc.level, got.LevelName) {
				t.Fatalf("fitNames() names = %q, %q, want prefixes of the originals", got.ServerName, got.LevelName)
			}
			if !utf8.ValidString(got.ServerName) || !utf8.ValidString(got.LevelName) {
				t.Fatalf("fitNames() names = %q, %q, want whole runes", got.ServerName, got.LevelName)
			}
			if b, _ := got.MarshalBinary(); len(b) > maxApplicationData {
				t.Fatalf("fitNames() marshals to %d bytes, want at most %d", len(b), maxApplicationData)
			}
		})
	}
}

// A cut never leaves a formatting prefix without its code.
func TestCutNameDropsDanglingFormattingPrefix(t *testing.T) {
	if got := cutName("ab§c", 4); got != "ab" {
		t.Fatalf("cutName() = %q, want %q", got, "ab")
	}
}
