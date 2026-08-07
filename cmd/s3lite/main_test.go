package main

import (
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/atmin/s3lite"
)

func TestParseArgsDefaults(t *testing.T) {
	for _, env := range []string{"AWS_REGION", "AWS_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
		t.Setenv(env, "")
	}
	opts, err := parseArgs([]string{"s3://bucket/db"}, io.Discard)
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	if opts.role != s3lite.RoleAuto {
		t.Errorf("role = %v, want RoleAuto", opts.role)
	}
	if opts.mode != "list" || opts.headers {
		t.Errorf("mode/headers = %q/%v, want list/false", opts.mode, opts.headers)
	}
	if opts.idleYield != 30*time.Second {
		t.Errorf("idle-yield = %v, want 30s", opts.idleYield)
	}
	if !strings.HasSuffix(opts.local, filepath.Join("s3lite", "bucket", "db.sqlite3")) {
		t.Errorf("default local path = %q, want it keyed by the replica URL", opts.local)
	}
}

// TestParseArgsRoles covers the flag the two documented invocations turn on.
func TestParseArgsRoles(t *testing.T) {
	for flag, want := range map[string]s3lite.Role{
		"auto":     s3lite.RoleAuto,
		"writer":   s3lite.RoleWriter,
		"follower": s3lite.RoleFollower,
	} {
		opts, err := parseArgs([]string{"--role=" + flag, "s3://bucket/db"}, io.Discard)
		if err != nil {
			t.Fatalf("--role=%s: %v", flag, err)
		}
		if opts.role != want {
			t.Errorf("--role=%s parsed as %v, want %v", flag, opts.role, want)
		}
	}
	if _, err := parseArgs([]string{"--role=leader", "s3://bucket/db"}, io.Discard); err == nil {
		t.Error("an unknown --role was accepted")
	}
}

func TestParseArgsRejectsBadInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"no replica", []string{}},
		{"two replicas", []string{"s3://a/db", "s3://b/db"}},
		{"unknown mode", []string{"--mode=fancy", "s3://bucket/db"}},
		{"a bare path is not a replica", []string{"/tmp/db.sqlite3"}},
		{"an unsupported scheme", []string{"gs://bucket/db"}},
		{"an unsupported scheme with an explicit local path", []string{"--local=/tmp/db.sqlite3", "gs://bucket/db"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseArgs(tc.args, io.Discard); err == nil {
				t.Fatalf("parseArgs(%q) accepted it", tc.args)
			}
		})
	}
}

// TestParseArgsS3EnvFallback: the flags win, the documented AWS variables fill in,
// and what neither supplies is left for the SDK's own chain.
func TestParseArgsS3EnvFallback(t *testing.T) {
	t.Setenv("AWS_REGION", "eu-central-1")
	t.Setenv("AWS_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("AWS_ACCESS_KEY_ID", "from-env")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	opts, err := parseArgs([]string{"--access-key-id=from-flag", "s3://bucket/db"}, io.Discard)
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	want := s3lite.S3Config{Region: "eu-central-1", Endpoint: "http://minio:9000", AccessKeyID: "from-flag"}
	if opts.s3 != want {
		t.Fatalf("S3 settings = %+v, want %+v", opts.s3, want)
	}
}

func TestParseArgsKeyFile(t *testing.T) {
	dir := t.TempDir()
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i)
	}
	rawPath := filepath.Join(dir, "raw.key")
	hexPath := filepath.Join(dir, "hex.key")
	shortPath := filepath.Join(dir, "short.key")
	for path, content := range map[string][]byte{
		rawPath:   raw,
		hexPath:   []byte(hex.EncodeToString(raw) + "\n"),
		shortPath: []byte("too short"),
	} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	for _, path := range []string{rawPath, hexPath} {
		opts, err := parseArgs([]string{"--key-file=" + path, "s3://bucket/db"}, io.Discard)
		if err != nil {
			t.Fatalf("--key-file=%s: %v", path, err)
		}
		if string(opts.key) != string(raw) {
			t.Errorf("--key-file=%s decoded to %x, want %x", path, opts.key, raw)
		}
	}
	if _, err := parseArgs([]string{"--key-file=" + shortPath, "s3://bucket/db"}, io.Discard); err == nil {
		t.Error("a key of the wrong length was accepted")
	}
}

// TestDefaultLocalPathIsContained: the local copy is keyed by the replica URL, and
// a URL cannot climb out of the cache directory with a "..".
func TestDefaultLocalPathIsContained(t *testing.T) {
	cache, err := os.UserCacheDir()
	if err != nil {
		t.Skipf("no user cache directory: %v", err)
	}
	root := filepath.Join(cache, "s3lite")
	localFor := func(raw string) string {
		t.Helper()
		u, err := replicaURL(raw)
		if err != nil {
			t.Fatalf("replicaURL(%q): %v", raw, err)
		}
		local, err := defaultLocalPath(u)
		if err != nil {
			t.Fatalf("defaultLocalPath(%q): %v", raw, err)
		}
		return local
	}

	stable := localFor("s3://bucket/nested/db")
	if want := filepath.Join(root, "bucket", "nested", "db.sqlite3"); stable != want {
		t.Fatalf("local path = %q, want %q", stable, want)
	}
	if again := localFor("s3://bucket/nested/db"); again != stable {
		t.Fatalf("the same URL resolved to %q then %q", stable, again)
	}

	escaped := localFor("s3://bucket/../../etc/db")
	if !strings.HasPrefix(escaped, root+string(filepath.Separator)) {
		t.Fatalf("a %q in the URL escaped the cache directory: %q", "..", escaped)
	}
}
