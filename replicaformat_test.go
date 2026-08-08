package s3lite

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

// TestReplicaWrittenByANewerBuildIsNamed is the forward-compatibility diagnosis: an
// object whose bytes are authentic but whose LTX framing this build cannot parse
// reports ErrReplicaFormatNewer — "upgrade" — and never something a reader would take
// for a corrupt database.
//
// The newer build is simulated the way a real one looks from here: a real object of
// this replica's own, re-sealed with one unknown bit set in its first page header's
// flags. ltx rejects unknown page flags by design — "reserved for future use" is
// exactly what a page format the reader predates trips over — so this is the shape of
// the real incident that motivated the classifier, where an ltx v0.5.2 writer set
// PageHeaderFlagSize and a v0.5.1 reader refused it.
//
// Run keyed and unkeyed, because the two paths reach the format probe differently: the
// keyed one after a frame authenticates, the plaintext one after the magic check
// declines to claim it is encrypted.
func TestReplicaWrittenByANewerBuildIsNamed(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{"Encrypted", testKey(0xc1)},
		{"Plaintext", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			replicaURL := "file://" + t.TempDir()
			cfg := replicaConfig{EncryptionKey: tc.key}

			db, err := Open(ctx, Config{
				LocalPath:     filepath.Join(t.TempDir(), "src.sqlite3"),
				BackupTo:      replicaURL,
				EncryptionKey: tc.key,
				Migrations:    []string{itemsSchema},
			})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 50; i++ {
				if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('x')`); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			// The replica restores before the tamper, so what the tamper proves is the
			// tamper and not a broken fixture.
			if err := restoreDB(ctx, cfg, replicaURL, filepath.Join(t.TempDir(), "before.sqlite3"), discardLogger(), nil); err != nil {
				t.Fatalf("restore before planting the newer object: %v", err)
			}

			plantFutureFormatObject(t, ctx, cfg, replicaURL)

			dest := filepath.Join(t.TempDir(), "after.sqlite3")
			err = restoreDB(ctx, cfg, replicaURL, dest, discardLogger(), nil)
			if !errors.Is(err, ErrReplicaFormatNewer) {
				t.Fatalf("restore of a newer-format replica: got %v, want ErrReplicaFormatNewer", err)
			}
			if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
				t.Fatal("a failed restore must leave no database behind")
			}
		})
	}
}

// plantFutureFormatObject rewrites the newest object of the replica's restore plan
// with one unknown flag bit set in its first page header, through the same client that
// wrote it — so on an encrypted replica it is re-sealed and authenticates normally, and
// only its LTX framing is from the future.
func plantFutureFormatObject(t *testing.T, ctx context.Context, cfg replicaConfig, rawURL string) {
	t.Helper()

	client, err := newReplicaClient(cfg, rawURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Init(ctx); err != nil {
		t.Fatal(err)
	}
	infos, err := litestream.CalcRestorePlan(ctx, client, 0, time.Time{}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) == 0 {
		t.Fatal("restore plan is empty — nothing to plant into")
	}
	info := infos[len(infos)-1]

	rc, err := client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}

	// The first page header sits immediately after the file header: Pgno(4) then
	// Flags(2). Bit 15 is one no ltx release has defined, which is what makes it stand
	// in for a format this build is behind rather than for damage.
	const flagsOff = ltx.HeaderSize + 4
	if len(body) < flagsOff+2 {
		t.Fatalf("object is %d bytes, too short to hold a page header", len(body))
	}
	binary.BigEndian.PutUint16(body[flagsOff:], binary.BigEndian.Uint16(body[flagsOff:flagsOff+2])|1<<15)

	if _, err := client.WriteLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
}

// TestProbeFormatCauseDeclinesWhatItCannotAttribute pins the other half of the
// classifier: the cases it must stay quiet on. Over-claiming here would be worse than
// the raw ltx string it replaces — it would tell someone to upgrade over a replica that
// is damaged, or not a replica at all, and send them away from the real problem.
func TestProbeFormatCauseDeclinesWhatItCannotAttribute(t *testing.T) {
	whole := mustLTXBytes(t, 1, 1, 2, time.UnixMilli(0).UTC())

	future := append([]byte(nil), whole...)
	const flagsOff = ltx.HeaderSize + 4
	binary.BigEndian.PutUint16(future[flagsOff:], binary.BigEndian.Uint16(future[flagsOff:flagsOff+2])|1<<15)

	for _, tc := range []struct {
		name string
		body []byte
		want error
	}{
		{"ParsesHere", whole, nil},
		{"NotAnLTXFileAtAll", []byte("not an ltx file, whatever else it may be"), nil},
		{"Empty", nil, nil},
		{"TruncatedMidHeader", whole[:ltx.HeaderSize-1], nil},
		{"TruncatedMidPage", whole[:ltx.HeaderSize+8], nil},
		{"UnknownPageFlag", future, ErrReplicaFormatNewer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := probeFormatCause(bytes.NewReader(tc.body)); !errors.Is(got, tc.want) {
				t.Fatalf("probeFormatCause = %v, want %v", got, tc.want)
			}
		})
	}
}
