package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/zeroconfig"
	flag "maunium.net/go/mauflag"
)

const secret = "SENTINEL-SECRET-BODY"

func reqLine(url string) []byte {
	return []byte(`{"level":"debug","method":"PUT","url":"` + url + `","req_body":{"msgtype":"m.text","body":"` + secret + `"},"duration":3}` + "\n")
}

func TestScrubLineSendBody(t *testing.T) {
	out := scrubLine(reqLine("https://hs.example.com/_matrix/client/v3/rooms/!r:example.com/send/m.room.message/txn1"))
	if bytes.Contains(out, []byte(secret)) || !bytes.Contains(out, redactedBody) || !bytes.Contains(out, []byte(`"duration":3`)) {
		t.Fatalf("unexpected: %s", out)
	}
}

func TestScrubLineStateTopic(t *testing.T) {
	out := scrubLine(reqLine("https://hs.example.com/_matrix/client/v3/rooms/!r:example.com/state/m.room.topic"))
	if bytes.Contains(out, []byte(secret)) {
		t.Fatalf("not scrubbed: %s", out)
	}
}

func TestScrubLineRedactReason(t *testing.T) {
	out := scrubLine(reqLine("https://hs.example.com/_matrix/client/v3/rooms/!r:example.com/redact/$e/txn1"))
	if bytes.Contains(out, []byte(secret)) {
		t.Fatalf("not scrubbed: %s", out)
	}
}

func TestScrubLineKeepsOtherRequests(t *testing.T) {
	for _, u := range []string{
		"https://hs.example.com/_matrix/client/v3/join/!r:example.com",
		"https://hs.example.com/_matrix/media/v3/upload",
	} {
		in := reqLine(u)
		if out := scrubLine(in); !bytes.Equal(in, out) {
			t.Fatalf("changed %s: %s", u, out)
		}
	}
}

func TestScrubLineMalformed(t *testing.T) {
	for _, in := range []string{
		`not json "req_body":{"a":1} /send/`,
		`{"url":"https://x/send/a","req_body":{"a":`,
		`{"level":"info","message":"hello"}`,
	} {
		if out := scrubLine([]byte(in)); string(out) != in {
			t.Fatalf("changed %q: %q", in, out)
		}
	}
}

type levelRecorder struct {
	level zerolog.Level
	data  []byte
}

func (l *levelRecorder) Write(p []byte) (int, error) {
	l.data = append([]byte{}, p...)
	return len(p), nil
}
func (l *levelRecorder) WriteLevel(lv zerolog.Level, p []byte) (int, error) {
	l.level = lv
	return l.Write(p)
}

func TestScrubWriterPassesLevel(t *testing.T) {
	rec := &levelRecorder{}
	w := scrubWriter{inner: rec}
	in := reqLine("https://hs.example.com/rooms/!r/send/m.room.message/t")
	n, err := w.WriteLevel(zerolog.WarnLevel, in)
	if err != nil || n != len(in) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if rec.level != zerolog.WarnLevel || bytes.Contains(rec.data, []byte(secret)) {
		t.Fatalf("level=%v data=%s", rec.level, rec.data)
	}
}

func TestScrubLoggingCompilesAllWriters(t *testing.T) {
	cfg := zeroconfig.Config{
		MinLevel: nil,
		Writers: []zeroconfig.WriterConfig{
			{Type: zeroconfig.WriterTypeStdout, Format: zeroconfig.LogFormatPrettyColored},
			{Type: zeroconfig.WriterTypeStderr, Format: zeroconfig.LogFormatJSON},
		},
	}
	scrubLogging(&cfg)
	for _, w := range cfg.Writers {
		if w.Format != zeroconfig.LogFormatJSON || !strings.HasPrefix(string(w.Type), "jmpchat-scrubbed-") {
			t.Fatalf("writer not replaced: %+v", w)
		}
	}
	if _, err := cfg.Compile(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigPathFromArgs(t *testing.T) {
	cases := []struct {
		args []string
		want string
		ok   bool
	}{
		{[]string{"-c", "a.yaml"}, "a.yaml", true},
		{[]string{"--config", "b.yaml"}, "b.yaml", true},
		{[]string{"--config=c.yaml"}, "c.yaml", true},
		{[]string{"-cd.yaml"}, "d.yaml", true},
		{[]string{"-g"}, "", false},
	}
	for _, c := range cases {
		got, ok := configPathFromArgs(c.args)
		if got != c.want || ok != c.ok {
			t.Errorf("%v: got %q,%v", c.args, got, ok)
		}
	}
}

func withArgs(t *testing.T, args []string) {
	t.Helper()
	origArgs := os.Args
	origIn := flag.DefaultSet().InputArgs
	os.Args = args
	t.Cleanup(func() { os.Args = origArgs; flag.DefaultSet().InputArgs = origIn })
}

func TestPreflightAddsNoUpdateWhenReadOnly(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write anything")
	}
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte("a: 1\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	withArgs(t, []string{"bridge", "-c", p})
	preflight()
	if os.Args[len(os.Args)-1] != "--no-update" {
		t.Fatalf("args: %v", os.Args)
	}
	if got := flag.DefaultSet().InputArgs; len(got) == 0 || got[len(got)-1] != "--no-update" {
		t.Fatalf("mauflag args: %v", got)
	}
}

func TestPreflightSkipsGenerateRegistration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write anything")
	}
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte("a: 1\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	withArgs(t, []string{"bridge", "-g", "-c", p})
	preflight()
	if strings.Contains(strings.Join(os.Args, " "), "--no-update") {
		t.Fatalf("args: %v", os.Args)
	}
}

func TestPreflightLeavesWritableAlone(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte("a: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	withArgs(t, []string{"bridge", "-c", p})
	preflight()
	if len(os.Args) != 3 {
		t.Fatalf("args: %v", os.Args)
	}
}

func TestPreflightSkipsExampleConfig(t *testing.T) {
	// -e -c - (Docker first run) and -e with a missing file must not exit.
	withArgs(t, []string{"bridge", "-e", "-c", "/nonexistent/config.yaml"})
	preflight()
	withArgs(t, []string{"bridge", "-c", "-"})
	preflight()
}

func TestScrubLineCreateRoomAndProfile(t *testing.T) {
	for _, u := range []string{
		"https://hs.example.com/_matrix/client/v3/createRoom",
		"https://hs.example.com/_matrix/client/v3/profile/@u:example.com/displayname",
	} {
		out := scrubLine(reqLine(u))
		if bytes.Contains(out, []byte(secret)) || !bytes.Contains(out, redactedBody) {
			t.Fatalf("not scrubbed %s: %s", u, out)
		}
	}
}

func TestScrubLineDecoyField(t *testing.T) {
	in := []byte(`{"level":"debug","note":"x \"req_body\":{\"a\":1} y","url":"https://hs.example.com/_matrix/client/v3/rooms/!r:example.com/send/m.room.message/t","req_body":{"body":"` + secret + `"},"duration":3}`)
	out := scrubLine(in)
	if bytes.Contains(out, []byte(secret)) || !bytes.Contains(out, []byte(`"duration":3`)) || !bytes.Contains(out, []byte(`\"req_body\":{\"a\":1} y`)) {
		t.Fatalf("unexpected: %s", out)
	}
	// A decoy in a field that comes before, with a non-scrubbed URL, stays untouched.
	other := []byte(`{"note":"\"req_body\":","url":"https://hs.example.com/_matrix/client/v3/join/!r","req_body":{"a":1}}`)
	if got := scrubLine(other); !bytes.Equal(got, other) {
		t.Fatalf("changed: %s", got)
	}
}
