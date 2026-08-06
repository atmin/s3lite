package s3lite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/superfly/ltx"
)

// The decorator's tests (against a real file backend, and through litestream's own
// ResumableReader) plus the end-to-end proofs that an encrypted replica round-trips
// a database and that an unencrypted one is untouched.

// mustLTXBytes builds a valid LTX file carrying nPages pages, so the object has a
// real header (the write path peeks it for the timestamp) and a size that spans
// several frames when asked to. Page bodies are pseudo-random because the LTX
// encoder compresses them — patterned pages would collapse to a fraction of the
// intended size and quietly stop crossing frame boundaries.
func mustLTXBytes(t *testing.T, minTXID, maxTXID ltx.TXID, nPages int, ts time.Time) []byte {
	t.Helper()
	const pageSize = 4096
	if nPages < 1 {
		t.Fatal("mustLTXBytes needs at least one page")
	}

	buf := new(bytes.Buffer)
	enc, err := ltx.NewEncoder(buf)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    uint32(nPages),
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: ts.UnixMilli(),
	}); err != nil {
		t.Fatalf("EncodeHeader: %v", err)
	}
	rnd := rand.New(rand.NewPCG(uint64(minTXID), uint64(maxTXID)))
	page := make([]byte, pageSize)
	for i := 1; i <= nPages; i++ {
		for j := range page {
			page[j] = byte(rnd.Uint32())
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: uint32(i)}, page); err != nil {
			t.Fatalf("EncodePage: %v", err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// newFileBackedClient returns an encrypting client over a real file backend plus the
// directory it writes to, so a test can inspect the bytes that actually land.
func newFileBackedClient(t *testing.T, key []byte, requireEncrypted bool) (*encryptedClient, *file.ReplicaClient, string) {
	t.Helper()
	dir := t.TempDir()
	inner := file.NewReplicaClient(dir)
	return newEncryptedClient(inner, key, requireEncrypted), inner, dir
}

// TestEncryptedClientRoundTripFileBackend is the decorator's core contract against a
// real backend: the object on disk is ciphertext, the listed size is the *plaintext*
// size, and reads — whole and ranged — return the original bytes.
func TestEncryptedClientRoundTripFileBackend(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xa1)
	client, inner, _ := newFileBackedClient(t, key, false)

	ts := time.UnixMilli(time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC).UnixMilli()).UTC()
	plain := mustLTXBytes(t, 1, 4, 40, ts) // ~160 KiB: three 64 KiB frames
	if int64(len(plain)) <= 2*encFrameSize {
		t.Fatalf("test payload %d bytes should span more than two frames", len(plain))
	}

	info, err := client.WriteLTXFile(ctx, 0, 1, 4, bytes.NewReader(plain))
	if err != nil {
		t.Fatalf("WriteLTXFile: %v", err)
	}
	if info.Size != int64(len(plain)) {
		t.Fatalf("reported Size=%d, want the plaintext size %d", info.Size, len(plain))
	}
	if !info.CreatedAt.Equal(ts) {
		t.Fatalf("CreatedAt=%v, want the LTX header timestamp %v", info.CreatedAt, ts)
	}

	// What landed on disk must be ciphertext of the expected length.
	onDisk, err := os.ReadFile(inner.LTXFilePath(0, 1, 4))
	if err != nil {
		t.Fatal(err)
	}
	if string(onDisk[:4]) != encMagic {
		t.Fatalf("stored object magic = %q, want %q", onDisk[:4], encMagic)
	}
	if want := encCiphertextSize(int64(len(plain)), encFrameSize); int64(len(onDisk)) != want {
		t.Fatalf("stored object is %d bytes, want %d", len(onDisk), want)
	}
	if bytes.Contains(onDisk, plain[:200]) {
		t.Fatal("stored object contains plaintext")
	}

	// The listing reports plaintext sizes, which is what litestream's premature-EOF
	// detection and minimum-size check compare against.
	itr, err := client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	var listed []*ltx.FileInfo
	for itr.Next() {
		listed = append(listed, itr.Item())
	}
	if err := itr.Err(); err != nil {
		t.Fatal(err)
	}
	if err := itr.Close(); err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 {
		t.Fatalf("listed %d files, want 1", len(listed))
	}
	if listed[0].Size != int64(len(plain)) {
		t.Fatalf("listed Size=%d, want %d", listed[0].Size, len(plain))
	}

	// Whole-object read.
	rc, err := client.OpenLTXFile(ctx, 0, 1, 4, 0, 0)
	if err != nil {
		t.Fatalf("OpenLTXFile: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatal("whole-object read did not return the plaintext")
	}

	// Ranged reads at and around every frame boundary — the resume path's arithmetic.
	for _, off := range []int64{1, encFrameSize - 1, encFrameSize, encFrameSize + 1,
		2 * encFrameSize, int64(len(plain)) - 1} {
		rc, err := client.OpenLTXFile(ctx, 0, 1, 4, off, 0)
		if err != nil {
			t.Fatalf("offset=%d: OpenLTXFile: %v", off, err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("offset=%d: read: %v", off, err)
		}
		if want := plain[off:]; !bytes.Equal(got, want) {
			t.Fatalf("offset=%d: got %d bytes, want %d", off, len(got), len(want))
		}
	}

	// A bounded range (size != 0) is not on s3lite's path but must still be correct.
	rc, err = client.OpenLTXFile(ctx, 0, 1, 4, encFrameSize-5, 10)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if want := plain[encFrameSize-5 : encFrameSize+5]; !bytes.Equal(got, want) {
		t.Fatalf("bounded range: got %x, want %x", got, want)
	}
}

// TestEncryptedClientSingleFrameObject covers the degenerate framing: an object that
// fits in one frame, and a read starting exactly at its plaintext end.
func TestEncryptedClientSingleFrameObject(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xa2)
	client, _, _ := newFileBackedClient(t, key, false)

	// The smallest real LTX file: one page, comfortably inside a single frame.
	plain := mustLTXBytes(t, 2, 2, 1, time.UnixMilli(0).UTC())
	if _, err := client.WriteLTXFile(ctx, 0, 2, 2, bytes.NewReader(plain)); err != nil {
		t.Fatalf("WriteLTXFile: %v", err)
	}
	rc, err := client.OpenLTXFile(ctx, 0, 2, 2, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatal("a single-frame object did not round-trip")
	}

	// Reading from exactly the end yields nothing, not an error.
	rc, err = client.OpenLTXFile(ctx, 0, 2, 2, int64(len(plain)), 0)
	if err != nil {
		t.Fatalf("open at the plaintext end: %v", err)
	}
	got, err = io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("read at the plaintext end: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("read at the plaintext end returned %d bytes", len(got))
	}
}

// flakyClient wraps a replica client and truncates the body of the first n reads of
// an object, which is exactly the failure ResumableReader exists for: a provider
// closing an idle connection mid-object.
type flakyClient struct {
	litestream.ReplicaClient
	mu     sync.Mutex
	dropAt int64 // bytes to deliver before cutting the stream
	drops  int   // how many more opens to cut
	opens  int
}

func (c *flakyClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	rc, err := c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opens++
	// Only cut full-body reads; a header fetch (a small bounded range) is left alone.
	if c.drops > 0 && size == 0 {
		c.drops--
		return &cuttingReadCloser{rc: rc, left: c.dropAt}, nil
	}
	return rc, nil
}

func (c *flakyClient) openCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opens
}

// cuttingReadCloser delivers left bytes then reports a connection drop.
type cuttingReadCloser struct {
	rc   io.ReadCloser
	left int64
}

func (r *cuttingReadCloser) Read(p []byte) (int, error) {
	if r.left <= 0 {
		return 0, errors.New("connection reset by peer")
	}
	if int64(len(p)) > r.left {
		p = p[:r.left]
	}
	n, err := r.rc.Read(p)
	r.left -= int64(n)
	return n, err
}

func (r *cuttingReadCloser) Close() error { return r.rc.Close() }

// TestEncryptedRestoreResumesThroughDroppedConnections is the resume proof, driven
// through litestream's own restore (which wraps every stream in its internal
// ResumableReader) with a client that cuts the body mid-object — the exact failure
// that reader exists for, a provider closing an idle connection.
//
// This is the path that makes whole-object sealing impossible: recovery reopens at an
// arbitrary *plaintext* offset and expects a seamless byte stream from there.
func TestEncryptedRestoreResumesThroughDroppedConnections(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xa3)
	replicaDir := t.TempDir()
	replicaURL := "file://" + replicaDir

	db, err := Open(ctx, Config{
		LocalPath:     filepath.Join(t.TempDir(), "enc.sqlite3"),
		BackupTo:      replicaURL,
		EncryptionKey: key,
		Migrations:    []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Enough rows that a snapshot spans several 64 KiB frames.
	for i := 0; i < 4000; i++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES (?)`,
			fmt.Sprintf("row-%06d-padding-padding-padding", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	for _, dropAt := range []int64{1, encFrameSize / 2, encFrameSize, encFrameSize + 7, 2 * encFrameSize} {
		t.Run(fmt.Sprintf("DropAt%d", dropAt), func(t *testing.T) {
			inner, err := newReplicaClient(replicaConfig{}, replicaURL) // unwrapped backend
			if err != nil {
				t.Fatal(err)
			}
			flaky := &flakyClient{ReplicaClient: inner, dropAt: dropAt, drops: 3}
			client := newEncryptedClient(flaky, key, false)

			dest := filepath.Join(t.TempDir(), "restored.sqlite3")
			replica := litestream.NewReplicaWithClient(nil, client)
			opt := litestream.NewRestoreOptions()
			opt.OutputPath = dest
			if err := replica.Restore(ctx, opt); err != nil {
				t.Fatalf("restore through dropped connections: %v", err)
			}
			if flaky.openCount() < 2 {
				t.Fatalf("the restore should have reopened at least once, got %d opens", flaky.openCount())
			}

			fresh, err := Open(ctx, Config{LocalPath: dest})
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			var n int
			if err := fresh.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 4000 {
				t.Fatalf("restored %d rows, want 4000", n)
			}
		})
	}
}

// TestEncryptedClientKeyCache pins the cost claim in the docs: a resumed read costs
// no extra round trip when the same client already read the object from the start,
// and exactly one (the header fetch) when it did not.
func TestEncryptedClientKeyCache(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xa4)
	dir := t.TempDir()
	inner := file.NewReplicaClient(dir)

	plain := mustLTXBytes(t, 1, 2, 40, time.UnixMilli(0).UTC())
	if _, err := newEncryptedClient(inner, key, false).WriteLTXFile(ctx, 0, 1, 2, bytes.NewReader(plain)); err != nil {
		t.Fatal(err)
	}

	t.Run("WarmAfterWholeObjectRead", func(t *testing.T) {
		counter := &flakyClient{ReplicaClient: inner}
		client := newEncryptedClient(counter, key, false)

		rc, err := client.OpenLTXFile(ctx, 0, 1, 2, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, rc)
		_ = rc.Close()
		if got := counter.openCount(); got != 1 {
			t.Fatalf("a whole-object read took %d opens, want 1", got)
		}

		rc, err = client.OpenLTXFile(ctx, 0, 1, 2, encFrameSize+9, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, rc)
		_ = rc.Close()
		if got := counter.openCount(); got != 2 {
			t.Fatalf("a warm resume took %d opens total, want 2 (no header fetch)", got)
		}
	})

	t.Run("ColdResumeFetchesTheHeader", func(t *testing.T) {
		counter := &flakyClient{ReplicaClient: inner}
		client := newEncryptedClient(counter, key, false)

		rc, err := client.OpenLTXFile(ctx, 0, 1, 2, encFrameSize+9, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, rc)
		_ = rc.Close()
		if got := counter.openCount(); got != 2 {
			t.Fatalf("a cold resume took %d opens, want 2 (header + body)", got)
		}
	})
}

// TestEncryptedClientRewrittenObjectSelfHeals covers the one way a cached key can go
// stale: an object name rewritten with a fresh salt (a retried upload, a re-run
// compaction) by *another* writer. The cached entry must be dropped on the
// authentication failure so litestream's retry succeeds.
func TestEncryptedClientRewrittenObjectSelfHeals(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xa5)
	dir := t.TempDir()
	inner := file.NewReplicaClient(dir)

	reader := newEncryptedClient(inner, key, false)
	first := mustLTXBytes(t, 1, 2, 40, time.UnixMilli(1000).UTC())
	if _, err := newEncryptedClient(inner, key, false).WriteLTXFile(ctx, 0, 1, 2, bytes.NewReader(first)); err != nil {
		t.Fatal(err)
	}

	// Warm this reader's cache with the first body's salt.
	rc, err := reader.OpenLTXFile(ctx, 0, 1, 2, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, rc)
	_ = rc.Close()
	if _, ok := reader.lookup(0, 1, 2); !ok {
		t.Fatal("the key cache should be warm after a whole-object read")
	}

	// Someone else rewrites the same object name under a fresh salt.
	second := mustLTXBytes(t, 1, 2, 41, time.UnixMilli(2000).UTC())
	if _, err := newEncryptedClient(inner, key, false).WriteLTXFile(ctx, 0, 1, 2, bytes.NewReader(second)); err != nil {
		t.Fatal(err)
	}

	// A resumed read under the stale key must fail *and* invalidate...
	if _, err := reader.OpenLTXFile(ctx, 0, 1, 2, encFrameSize, 0); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("a stale cached key should fail with ErrKeyMismatch, got %v", err)
	}
	if _, ok := reader.lookup(0, 1, 2); ok {
		t.Fatal("the failed read must have dropped the stale cache entry")
	}

	// ...so the retry (what ResumableReader does) succeeds.
	rc, err = reader.OpenLTXFile(ctx, 0, 1, 2, encFrameSize, 0)
	if err != nil {
		t.Fatalf("the retry after invalidation must succeed: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if want := second[encFrameSize:]; !bytes.Equal(got, want) {
		t.Fatalf("retry returned %d bytes, want %d", len(got), len(want))
	}
}

// TestEncryptedClientMixedMode covers the window in which a previously-plaintext
// replica still holds pre-key objects: they pass through while RequireEncrypted is
// false, and are refused once it is set.
func TestEncryptedClientMixedMode(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xa6)
	dir := t.TempDir()
	inner := file.NewReplicaClient(dir)

	// A pre-key object: written with no wrapper at all.
	oldPlain := mustLTXBytes(t, 1, 2, 40, time.UnixMilli(0).UTC())
	if _, err := inner.WriteLTXFile(ctx, 0, 1, 2, bytes.NewReader(oldPlain)); err != nil {
		t.Fatal(err)
	}
	// ...and one written after the key was configured.
	newPlain := mustLTXBytes(t, 3, 4, 40, time.UnixMilli(0).UTC())
	if _, err := newEncryptedClient(inner, key, false).WriteLTXFile(ctx, 0, 3, 4, bytes.NewReader(newPlain)); err != nil {
		t.Fatal(err)
	}

	t.Run("PlaintextPassesThrough", func(t *testing.T) {
		client := newEncryptedClient(inner, key, false)
		for _, tc := range []struct {
			name             string
			minTXID, maxTXID ltx.TXID
			want             []byte
		}{
			{"PreKeyObject", 1, 2, oldPlain},
			{"EncryptedObject", 3, 4, newPlain},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rc, err := client.OpenLTXFile(ctx, 0, tc.minTXID, tc.maxTXID, 0, 0)
				if err != nil {
					t.Fatalf("open: %v", err)
				}
				got, err := io.ReadAll(rc)
				_ = rc.Close()
				if err != nil {
					t.Fatalf("read: %v", err)
				}
				if !bytes.Equal(got, tc.want) {
					t.Fatalf("got %d bytes, want %d", len(got), len(tc.want))
				}

				// A resumed read of either kind must also work.
				rc, err = client.OpenLTXFile(ctx, 0, tc.minTXID, tc.maxTXID, encFrameSize+3, 0)
				if err != nil {
					t.Fatalf("resume open: %v", err)
				}
				got, err = io.ReadAll(rc)
				_ = rc.Close()
				if err != nil {
					t.Fatalf("resume read: %v", err)
				}
				if want := tc.want[encFrameSize+3:]; !bytes.Equal(got, want) {
					t.Fatalf("resume got %d bytes, want %d", len(got), len(want))
				}
			})
		}
	})

	t.Run("RequireEncryptedRefusesPlaintext", func(t *testing.T) {
		client := newEncryptedClient(inner, key, true)

		if _, err := client.OpenLTXFile(ctx, 0, 1, 2, 0, 0); !errors.Is(err, ErrObjectNotEncrypted) {
			t.Fatalf("a plaintext object must be refused with ErrObjectNotEncrypted, got %v", err)
		}
		// A resumed read of a plaintext object is refused too — the downgrade path
		// must not reopen through a different door.
		if _, err := client.OpenLTXFile(ctx, 0, 1, 2, encFrameSize, 0); !errors.Is(err, ErrObjectNotEncrypted) {
			t.Fatalf("a resumed plaintext read must be refused, got %v", err)
		}
		// The encrypted object is unaffected.
		rc, err := client.OpenLTXFile(ctx, 0, 3, 4, 0, 0)
		if err != nil {
			t.Fatalf("an encrypted object must still open: %v", err)
		}
		_, _ = io.Copy(io.Discard, rc)
		_ = rc.Close()
	})
}

// TestEncryptedClientDelegates pins that everything the decorator does not transform
// reaches the inner client, including the concrete-type unwrap wireReplica needs.
func TestEncryptedClientDelegates(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	inner := file.NewReplicaClient(dir)
	client := newEncryptedClient(inner, testKey(0xa7), false)

	if got, want := client.Type(), inner.Type(); got != want {
		t.Fatalf("Type()=%q, want %q", got, want)
	}
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	client.SetLogger(slog.New(slog.DiscardHandler))

	if got := unwrapReplicaClient(client); got != litestream.ReplicaClient(inner) {
		t.Fatalf("unwrapReplicaClient returned %T, want the inner *file.ReplicaClient", got)
	}

	// wireReplica must reach through the wrapper to set the file backend's
	// back-reference; without that, a file:// replica silently loses it.
	replica := litestream.NewReplicaWithClient(nil, client)
	wireReplica(client, replica)
	if inner.Replica != replica {
		t.Fatal("wireReplica did not set the back-reference through the wrapper")
	}

	plain := mustLTXBytes(t, 1, 2, 2, time.UnixMilli(0).UTC())
	info, err := client.WriteLTXFile(ctx, 0, 1, 2, bytes.NewReader(plain))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteLTXFiles(ctx, []*ltx.FileInfo{info}); err != nil {
		t.Fatalf("DeleteLTXFiles: %v", err)
	}
	if _, err := client.OpenLTXFile(ctx, 0, 1, 2, 0, 0); err == nil {
		t.Fatal("the object should be gone after DeleteLTXFiles")
	}

	if _, err := client.WriteLTXFile(ctx, 0, 5, 6, bytes.NewReader(plain)); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteAll(ctx); err != nil {
		t.Fatalf("DeleteAll: %v", err)
	}
	if _, err := client.OpenLTXFile(ctx, 0, 5, 6, 0, 0); err == nil {
		t.Fatal("everything should be gone after DeleteAll")
	}
}

// --- end to end through Open/Close -------------------------------------------

// encryptedReplicaFiles returns every LTX object under a file:// replica directory.
func encryptedReplicaFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".ltx") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		out[rel] = b
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// assertAllEncrypted fails unless every LTX object under a file:// replica directory
// carries the encryption magic, and returns how many it checked.
func assertAllEncrypted(t *testing.T, dir string) int {
	t.Helper()
	objects := encryptedReplicaFiles(t, dir)
	if len(objects) == 0 {
		t.Fatal("nothing was replicated")
	}
	for name, body := range objects {
		if len(body) < 4 || string(body[:4]) != encMagic {
			t.Fatalf("%s: object magic = %q, want %q", name, body[:min(4, len(body))], encMagic)
		}
	}
	return len(objects)
}

// TestEncryptedReplicaRoundTrip is the end-to-end proof: a database replicated under
// a key restores into a fresh instance with the same key, and the bucket holds only
// ciphertext in the meantime.
func TestEncryptedReplicaRoundTrip(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xb1)
	replicaDir := t.TempDir()
	replicaURL := "file://" + replicaDir

	db, err := Open(ctx, Config{
		LocalPath:     filepath.Join(t.TempDir(), "enc.sqlite3"),
		BackupTo:      replicaURL,
		EncryptionKey: key,
		Migrations:    []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	const secret = "zz-unmistakable-plaintext-marker-zz"
	for i := 0; i < 200; i++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES (?)`, fmt.Sprintf("%s-%d", secret, i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Every object on the replica is ciphertext, and none of them leak a row value.
	assertAllEncrypted(t, replicaDir)
	for name, body := range encryptedReplicaFiles(t, replicaDir) {
		if bytes.Contains(body, []byte(secret)) {
			t.Fatalf("%s: object body contains plaintext row data", name)
		}
	}

	// A fresh instance with the same key restores everything.
	fresh, err := Open(ctx, Config{
		LocalPath:     filepath.Join(t.TempDir(), "restored.sqlite3"),
		RestoreFrom:   replicaURL,
		EncryptionKey: key,
	})
	if err != nil {
		t.Fatalf("restore with the key: %v", err)
	}
	defer fresh.Close()

	var n int
	if err := fresh.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 200 {
		t.Fatalf("restored %d rows, want 200", n)
	}
	var name string
	if err := fresh.QueryRowContext(ctx, `SELECT name FROM items WHERE id = 7`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("%s-%d", secret, 6); name != want {
		t.Fatalf("row 7 = %q, want %q", name, want)
	}
	if err := fresh.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "ok" {
		t.Fatalf("integrity_check = %q", name)
	}
}

// TestEncryptedReplicaSurvivesCompaction covers "write, restore, compact, restore
// again": remote compaction reads and rewrites objects through the same client, so it
// must decrypt its sources and re-encrypt its output.
func TestEncryptedReplicaSurvivesCompaction(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xb2)
	replicaDir := t.TempDir()
	replicaURL := "file://" + replicaDir

	db, err := Open(ctx, Config{
		LocalPath:     filepath.Join(t.TempDir(), "enc.sqlite3"),
		BackupTo:      replicaURL,
		EncryptionKey: key,
		Migrations:    []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Several sync boundaries, so compaction has more than one source object.
	for round := 0; round < 4; round++ {
		for i := 0; i < 50; i++ {
			if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES (?)`,
				fmt.Sprintf("round-%d-row-%d", round, i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// A restore before compaction.
	before := filepath.Join(t.TempDir(), "before.sqlite3")
	if err := restoreDB(ctx, db.cfg.replica(), replicaURL, before, discardLogger()); err != nil {
		t.Fatalf("restore before compaction: %v", err)
	}

	// Compact level 0 into level 1, then into the snapshot level, using the live
	// store's own client — the encrypting one.
	lsDB := db.store.DBs()[0]
	if _, err := db.store.CompactDB(ctx, lsDB, &litestream.CompactionLevel{Level: 1}); err != nil {
		t.Fatalf("compact to level 1: %v", err)
	}
	if _, err := db.store.CompactDB(ctx, lsDB, db.store.SnapshotLevel()); err != nil {
		t.Fatalf("compact to the snapshot level: %v", err)
	}

	assertAllEncrypted(t, replicaDir) // compacted output must be ciphertext too

	// And a restore after compaction sees the same rows.
	after := filepath.Join(t.TempDir(), "after.sqlite3")
	if err := restoreDB(ctx, db.cfg.replica(), replicaURL, after, discardLogger()); err != nil {
		t.Fatalf("restore after compaction: %v", err)
	}
	for _, path := range []string{before, after} {
		fresh, err := Open(ctx, Config{LocalPath: path})
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if err := fresh.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 200 {
			t.Errorf("%s: restored %d rows, want 200", filepath.Base(path), n)
		}
		fresh.Close()
	}
}

// TestEncryptedReplicaRetentionExpiresSuperseded pins that retention still expires
// superseded files with encryption on. Retention deletes by each object's CreatedAt,
// which comes from the LTX header timestamp the write path now supplies to the backend
// directly — so a regressed timestamp hook would leave superseded snapshots to
// accumulate forever, which is exactly the kind of silent failure worth a test.
func TestEncryptedReplicaRetentionExpiresSuperseded(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xb6)
	replicaDir := t.TempDir()
	replicaURL := "file://" + replicaDir

	db, err := Open(ctx, Config{
		LocalPath:     filepath.Join(t.TempDir(), "enc.sqlite3"),
		BackupTo:      replicaURL,
		EncryptionKey: key,
		Migrations:    []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Several snapshots, so there is something to supersede. litestream rate-limits
	// snapshots to SnapshotInterval (24h by default) and refuses an earlier one with
	// ErrCompactionTooEarly, so pass a level with a short interval rather than mutating
	// the live store's fields — its compaction monitor reads those concurrently.
	lsDB := db.store.DBs()[0]
	snapLevel := &litestream.CompactionLevel{Level: litestream.SnapshotLevel, Interval: time.Millisecond}
	for round := 0; round < 3; round++ {
		for i := 0; i < 50; i++ {
			if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES (?)`,
				fmt.Sprintf("round-%d-row-%d", round, i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // clear the snapshot rate limit
		if _, err := db.store.CompactDB(ctx, lsDB, snapLevel); err != nil {
			t.Fatalf("snapshot round %d: %v", round, err)
		}
	}

	before := assertAllEncrypted(t, replicaDir)

	// A cutoff of "now" is a zero retention window: everything superseded expires
	// immediately. Passing the cutoff directly keeps the live store's fields untouched.
	minTXID, err := lsDB.EnforceSnapshotRetention(ctx, time.Now())
	if err != nil {
		t.Fatalf("enforce snapshot retention: %v", err)
	}
	if err := lsDB.EnforceRetentionByTXID(ctx, 1, minTXID); err != nil {
		t.Fatalf("enforce L1 retention: %v", err)
	}

	after := assertAllEncrypted(t, replicaDir) // survivors are still encrypted
	if after >= before {
		t.Fatalf("retention expired nothing: %d objects before, %d after", before, after)
	}

	// And the pruned replica still restores in full.
	dest := filepath.Join(t.TempDir(), "restored.sqlite3")
	n, err := restoreAndCount(ctx, db.cfg.replica(), replicaURL, dest)
	if err != nil {
		t.Fatalf("restore after retention: %v", err)
	}
	if n != 150 {
		t.Fatalf("restored %d rows after retention, want 150", n)
	}
}

// TestEncryptedReplicaKeyHandling pins the two typed failures a consumer needs to
// tell apart, and that neither leaves a partially-restored database behind.
func TestEncryptedReplicaKeyHandling(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xb3)
	replicaDir := t.TempDir()
	replicaURL := "file://" + replicaDir

	db, err := Open(ctx, Config{
		LocalPath:     filepath.Join(t.TempDir(), "enc.sqlite3"),
		BackupTo:      replicaURL,
		EncryptionKey: key,
		Migrations:    []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('x')`); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	t.Run("WrongKey", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "wrong.sqlite3")
		err := restoreDB(ctx, replicaConfig{EncryptionKey: testKey(0xb4)}, replicaURL, dest, discardLogger())
		if !errors.Is(err, ErrKeyMismatch) {
			t.Fatalf("wrong key: got %v, want ErrKeyMismatch", err)
		}
		if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
			t.Fatal("a failed restore must leave no database behind")
		}
	})

	t.Run("AbsentKey", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "nokey.sqlite3")
		err := restoreDB(ctx, replicaConfig{}, replicaURL, dest, discardLogger())
		if !errors.Is(err, ErrReplicaEncrypted) {
			t.Fatalf("absent key: got %v, want ErrReplicaEncrypted", err)
		}
		if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
			t.Fatal("a failed restore must leave no database behind")
		}
	})

	t.Run("OpenSurfacesTheTypedError", func(t *testing.T) {
		_, err := Open(ctx, Config{
			LocalPath:   filepath.Join(t.TempDir(), "nokey.sqlite3"),
			RestoreFrom: replicaURL,
		})
		if !errors.Is(err, ErrReplicaEncrypted) {
			t.Fatalf("Open without the key: got %v, want ErrReplicaEncrypted", err)
		}
	})

	t.Run("RightKeyStillWorks", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "right.sqlite3")
		if err := restoreDB(ctx, replicaConfig{EncryptionKey: key}, replicaURL, dest, discardLogger()); err != nil {
			t.Fatalf("restore with the right key: %v", err)
		}
	})
}

// TestEncryptedReplicaMixedWindow is the migration story: a replica that starts
// plaintext and gains a key keeps restoring, because the pre-key objects pass through
// while the new ones decrypt.
func TestEncryptedReplicaMixedWindow(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xb5)
	replicaDir := t.TempDir()
	replicaURL := "file://" + replicaDir
	localPath := filepath.Join(t.TempDir(), "mixed.sqlite3")

	// Phase 1: no key at all.
	db, err := Open(ctx, Config{
		LocalPath:  localPath,
		BackupTo:   replicaURL,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('before')`); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	plaintextObjects := len(encryptedReplicaFiles(t, replicaDir))
	if plaintextObjects == 0 {
		t.Fatal("phase 1 replicated nothing")
	}

	// Phase 2: same replica, now with a key. The local file carries on, so new LTX
	// lands encrypted next to the old plaintext.
	db, err = Open(ctx, Config{
		LocalPath:     localPath,
		BackupTo:      replicaURL,
		EncryptionKey: key,
	})
	if err != nil {
		t.Fatalf("reopen with a key: %v", err)
	}
	for i := 0; i < 50; i++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('after')`); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The replica really is mixed.
	var encrypted, plain int
	for _, body := range encryptedReplicaFiles(t, replicaDir) {
		if len(body) >= 4 && string(body[:4]) == encMagic {
			encrypted++
		} else {
			plain++
		}
	}
	if encrypted == 0 || plain == 0 {
		t.Fatalf("expected a mixed replica, got %d encrypted and %d plaintext objects", encrypted, plain)
	}

	// A restore across the window sees every row.
	dest := filepath.Join(t.TempDir(), "restored.sqlite3")
	n, err := restoreAndCount(ctx, replicaConfig{EncryptionKey: key}, replicaURL, dest)
	if err != nil {
		t.Fatalf("restore across the mixed window: %v", err)
	}
	if n != 100 {
		t.Fatalf("restored %d rows across the mixed window, want 100", n)
	}

	// With RequireEncrypted the pre-key objects are refused instead.
	err = restoreDB(ctx, replicaConfig{EncryptionKey: key, RequireEncrypted: true}, replicaURL,
		filepath.Join(t.TempDir(), "strict.sqlite3"), discardLogger())
	if !errors.Is(err, ErrObjectNotEncrypted) {
		t.Fatalf("RequireEncrypted across the mixed window: got %v, want ErrObjectNotEncrypted", err)
	}
}

// TestUnencryptedReplicaInstallsNoWrapper is the opt-in proof: with no key
// configured, newReplicaClient returns the bare backend client and the objects are
// plain LTX.
func TestUnencryptedReplicaInstallsNoWrapper(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct{ name, url string }{
		{"File", "file://" + t.TempDir()},
		{"S3", "s3://bucket/path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := newReplicaClient(replicaConfig{}, tc.url)
			if err != nil {
				t.Fatal(err)
			}
			if _, wrapped := client.(*encryptedClient); wrapped {
				t.Fatal("no key configured must install no wrapper")
			}

			keyed, err := newReplicaClient(replicaConfig{EncryptionKey: testKey(1)}, tc.url)
			if err != nil {
				t.Fatal(err)
			}
			if _, wrapped := keyed.(*encryptedClient); !wrapped {
				t.Fatalf("a configured key must install the wrapper, got %T", keyed)
			}
		})
	}

	// And the bytes on the wire stay plain LTX.
	replicaDir := t.TempDir()
	db, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "plain.sqlite3"),
		BackupTo:   "file://" + replicaDir,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('plain')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	objects := encryptedReplicaFiles(t, replicaDir)
	if len(objects) == 0 {
		t.Fatal("nothing was replicated")
	}
	for name, body := range objects {
		if string(body[:4]) != ltx.Magic {
			t.Fatalf("%s: magic = %q, want plain LTX %q", name, body[:4], ltx.Magic)
		}
	}
}

// TestEncryptedConfigValidation pins that misconfiguration fails at Open.
func TestEncryptedConfigValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("WrongKeyLength", func(t *testing.T) {
		_, err := Open(ctx, Config{
			LocalPath:     filepath.Join(t.TempDir(), "db.sqlite3"),
			EncryptionKey: make([]byte, 16),
		})
		if err == nil || !strings.Contains(err.Error(), "EncryptionKey must be 32 bytes") {
			t.Fatalf("got %v, want a key-length error", err)
		}
	})

	t.Run("RequireEncryptedWithoutKey", func(t *testing.T) {
		_, err := Open(ctx, Config{
			LocalPath:        filepath.Join(t.TempDir(), "db.sqlite3"),
			RequireEncrypted: true,
		})
		if err == nil || !strings.Contains(err.Error(), "RequireEncrypted needs an EncryptionKey") {
			t.Fatalf("got %v, want a RequireEncrypted error", err)
		}
	})
}

// TestEncryptedInstanceOwnerIsOpaque pins the one lock.json change encryption makes:
// an encrypted instance stops publishing its hostname as the lease owner, while an
// explicit Owner and the unencrypted default are untouched.
func TestEncryptedInstanceOwnerIsOpaque(t *testing.T) {
	ctx := context.Background()
	hostname, _ := os.Hostname()

	// Open resolves the owner into cfg before it builds the DB, so db.cfg.Owner is
	// exactly what litestream's leaser was handed.
	open := func(t *testing.T, cfg Config) *DB {
		t.Helper()
		installFakeLeaser(t, &fakeLock{})
		cfg.LocalPath = filepath.Join(t.TempDir(), "db.sqlite3")
		cfg.BackupTo = "file://" + t.TempDir()
		cfg.Role = RoleWriter
		cfg.LeaseTTL = time.Minute
		cfg.Migrations = []string{itemsSchema}
		db, err := Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}

	t.Run("EncryptedDefaultsToOpaque", func(t *testing.T) {
		got := open(t, Config{EncryptionKey: testKey(0xc1)}).cfg.Owner
		if got == "" {
			t.Fatal("an encrypted instance must not leave the owner empty (litestream would derive it from the hostname)")
		}
		if hostname != "" && strings.Contains(got, hostname) {
			t.Fatalf("owner %q leaks the hostname", got)
		}
		if len(got) != 32 {
			t.Fatalf("owner %q is not an opaque 32-hex-char id", got)
		}
	})

	t.Run("ExplicitOwnerWins", func(t *testing.T) {
		got := open(t, Config{EncryptionKey: testKey(0xc2), Owner: "explicit-owner"}).cfg.Owner
		if got != "explicit-owner" {
			t.Fatalf("owner = %q, want the explicit one", got)
		}
	})

	t.Run("UnencryptedKeepsLitestreamDefault", func(t *testing.T) {
		if got := open(t, Config{}).cfg.Owner; got != "" {
			t.Fatalf("owner = %q, want empty so litestream keeps its diagnostic default", got)
		}
	})
}

// TestEncryptedFollowerRefresh pins that a follower's incremental catch-up decrypts
// too — it drives litestream's Restore(Follow) through the same client — and that a
// follower without the key fails cleanly instead of serving nothing.
func TestEncryptedFollowerRefresh(t *testing.T) {
	ctx := context.Background()
	key := testKey(0xd1)
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	replicaURL := "file://" + t.TempDir()

	leader, err := Open(ctx, Config{
		LocalPath:     filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:      replicaURL,
		Role:          RoleWriter,
		Owner:         "leader",
		LeaseTTL:      time.Minute,
		EncryptionKey: key,
		Migrations:    []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()
	if err := leader.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	follower, err := Open(ctx, Config{
		LocalPath:               filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:                replicaURL,
		Role:                    RoleFollower,
		Owner:                   "follower",
		LeaseTTL:                time.Minute,
		FollowerRefreshInterval: 50 * time.Millisecond,
		EncryptionKey:           key,
	})
	if err != nil {
		t.Fatalf("follower Open with the key: %v", err)
	}
	defer follower.Close()
	if follower.IsLeader() {
		t.Fatal("the second instance should be a follower")
	}
	cached := follower.DB

	if _, err := leader.ExecContext(ctx, `INSERT INTO items (name) VALUES ('encrypted-fresh')`); err != nil {
		t.Fatal(err)
	}
	if err := leader.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 10*time.Second, func() bool {
		var n int
		if err := cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
			return false
		}
		return n == 1
	}, "follower should see the leader's encrypted write through an incremental refresh")

	// A follower with no key cannot open at all: its Open-time restore fails with the
	// typed error rather than silently serving an empty database.
	_, err = Open(ctx, Config{
		LocalPath: filepath.Join(t.TempDir(), "nokey.sqlite3"),
		BackupTo:  replicaURL,
		Role:      RoleFollower,
		Owner:     "nokey",
		LeaseTTL:  time.Minute,
	})
	if !errors.Is(err, ErrReplicaEncrypted) {
		t.Fatalf("a follower without the key: got %v, want ErrReplicaEncrypted", err)
	}
}
