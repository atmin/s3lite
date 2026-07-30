package s3lite

import (
	"bytes"
	"context"
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/benbjohnson/litestream"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/superfly/ltx"
)

// encryptedClient is the litestream.ReplicaClient decorator that makes every LTX
// object s3lite ships ciphertext. newReplicaClient (replica.go) is the single
// constructor of a replica client — it feeds replication, restore, the follower's
// incremental advance, the latest-TXID probe and remote compaction — so wrapping
// there covers every path by construction.
//
// It is installed only when Config.EncryptionKey is set. Without a key nothing is
// wrapped at all: the object layout, the client types and the bytes on the wire
// are exactly what they were before this feature existed.
//
// The wire format, and why it is framed rather than a whole-object seal, is in
// encrypt.go.
//
// It implements litestream.ReplicaClient and nothing more. litestream also probes a
// client for optional capabilities — `ReplicaClientV3`, for reading v0.3.x backup
// layouts — and those are deliberately *not* forwarded: an s3lite replica is always
// v0.5 LTX, so masking them costs nothing here. If litestream grows an optional
// interface that matters, it has to be forwarded explicitly, or a keyed instance will
// silently behave differently from an unkeyed one.
type encryptedClient struct {
	inner            litestream.ReplicaClient
	key              []byte
	requireEncrypted bool

	// cache maps object identity to its derived key, so a resumed read does not
	// re-fetch the header. lru.Cache is itself locked, and the client is used
	// concurrently by litestream's replication and compaction goroutines.
	cache *lru.Cache[encObjectID, *encKeyEntry]
}

var _ litestream.ReplicaClient = (*encryptedClient)(nil)

func newEncryptedClient(inner litestream.ReplicaClient, key []byte, requireEncrypted bool) *encryptedClient {
	// lru.New only errors on a non-positive size, and encKeyCacheSize is a positive
	// constant.
	cache, _ := lru.New[encObjectID, *encKeyEntry](encKeyCacheSize)
	return &encryptedClient{
		inner:            inner,
		key:              key,
		requireEncrypted: requireEncrypted,
		cache:            cache,
	}
}

// Unwrap returns the wrapped client. wireReplica needs it because the file
// backend's back-reference is set by type-asserting the concrete client type.
func (c *encryptedClient) Unwrap() litestream.ReplicaClient { return c.inner }

func (c *encryptedClient) Type() string                   { return c.inner.Type() }
func (c *encryptedClient) Init(ctx context.Context) error { return c.inner.Init(ctx) }
func (c *encryptedClient) SetLogger(logger *slog.Logger)  { c.inner.SetLogger(logger) }
func (c *encryptedClient) DeleteAll(ctx context.Context) error {
	return c.inner.DeleteAll(ctx)
}

// DeleteLTXFiles deletes by level and TXID range, which encryption does not touch,
// so the infos pass straight through (their Size is not consulted).
func (c *encryptedClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	return c.inner.DeleteLTXFiles(ctx, a)
}

// WriteLTXFile encrypts the body and reports the *plaintext* size upward.
//
// The body handed to the inner client implements litestream.LTXTimestamper: every
// backend records the LTX header's timestamp as object metadata (timestamp-based
// restore and retention read it back) and normally peeks it out of the upload
// stream, which ciphertext cannot supply. That hook is the fork's second carried
// patch — see LITESTREAM-FORK.md.
func (c *encryptedClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	sealer, err := newSealingReader(c.key, level, minTXID, maxTXID, r)
	if err != nil {
		return nil, err
	}
	// This object name is about to hold a body under a fresh salt — a retried upload or
	// a re-run compaction rewrites the same name with different bytes — so any key
	// cached for it is stale. Deferred, because a write that fails partway can still
	// have replaced the object.
	defer c.forget(level, minTXID, maxTXID)

	info, err := c.inner.WriteLTXFile(ctx, level, minTXID, maxTXID, sealer)
	if err != nil {
		return nil, err
	}
	out := *info
	out.Size = sealer.plaintextSize()
	return &out, nil
}

// LTXFiles converts every listed size from ciphertext to plaintext. litestream
// treats the listed size as load-bearing — ResumableReader compares it against its
// offset to tell a premature EOF from a real one, and restore rejects an object
// smaller than an LTX header — so what is reported upward has to match the bytes
// this client delivers.
//
// A listing cannot say whether an individual object is encrypted (that is in its
// header, which listing does not read), so the conversion assumes every object is.
// While a previously-plaintext replica still holds pre-key objects (see the mixed
// window in OpenLTXFile), those objects' sizes are therefore under-reported by
// their would-be framing overhead. The consequence is bounded and transitional:
// litestream's premature-EOF *recovery* on such an object may not trigger in its
// last few bytes, and a truncated read then surfaces as an LTX decode error rather
// than being retried. Nothing silently returns short data. Setting
// RequireEncrypted once every live object is encrypted makes the conversion exact.
func (c *encryptedClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	itr, err := c.inner.LTXFiles(ctx, level, seek, useMetadata)
	if err != nil {
		return nil, err
	}
	return &encFileIterator{inner: itr}, nil
}

// encFileIterator is LTXFiles' size-converting iterator.
type encFileIterator struct {
	inner ltx.FileIterator
	item  *ltx.FileInfo
	err   error
}

func (i *encFileIterator) Next() bool {
	if i.err != nil {
		return false
	}
	if !i.inner.Next() {
		return false
	}
	item := *i.inner.Item()
	// The frame size is fixed per format version (see encVersion), so a listing
	// can size an object without reading its header.
	pt, err := encPlaintextSize(item.Size, encFrameSize)
	if err != nil {
		i.err = err
		return false
	}
	item.Size = pt
	i.item = &item
	return true
}

func (i *encFileIterator) Item() *ltx.FileInfo { return i.item }
func (i *encFileIterator) Close() error        { return i.inner.Close() }

func (i *encFileIterator) Err() error {
	if err := i.inner.Err(); err != nil {
		return err
	}
	return i.err
}

// OpenLTXFile returns the object's plaintext for the requested plaintext byte
// range. offset and size are in plaintext bytes, matching the sizes LTXFiles
// reports.
//
// **Mixed mode.** A replica that predates the key still holds plaintext LTX
// objects. Those are detected per object by their magic and passed through
// unchanged, so such a replica keeps restoring while retention ages the plaintext
// out — unless RequireEncrypted is set, which refuses them. That flag closes the
// downgrade path where anyone with bucket *write* access substitutes a crafted
// plaintext LTX file for an encrypted one.
func (c *encryptedClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if offset < 0 || size < 0 {
		return nil, fmt.Errorf("s3lite: invalid ltx range (offset=%d size=%d)", offset, size)
	}
	if offset == 0 {
		return c.openFromStart(ctx, level, minTXID, maxTXID, size)
	}
	return c.openAtOffset(ctx, level, minTXID, maxTXID, offset, size)
}

// openFromStart is the common path (restore, compaction, the follower's apply): a
// single GET whose own first bytes carry the header, so no extra round trip.
func (c *encryptedClient) openFromStart(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, size int64) (io.ReadCloser, error) {
	// Always read to the end of the object rather than requesting `size` bytes:
	// the last frame must be recognisable as the object's last, which a bounded
	// range cannot show. The plaintext is capped at `size` below, and closing the
	// body early stops the transfer. s3lite's own paths only ever pass size=0.
	rc, err := c.inner.OpenLTXFile(ctx, level, minTXID, maxTXID, 0, 0)
	if err != nil {
		return nil, err
	}

	prefix, err := readEncHeaderPrefix(rc)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}
	entry, err := c.classify(prefix, level, minTXID, maxTXID)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}
	if entry.plaintext {
		return limitReadCloser(newBorrowedReadCloser(io.MultiReader(bytes.NewReader(prefix), rc), rc), size), nil
	}
	// The header was consumed exactly (parseEncHeader rejects a short read), so the
	// stream now stands on the first frame boundary.
	body := newOpeningReader(entry.aead, entry.hdr, rc, 0, 0)
	return c.decryptingBody(level, minTXID, maxTXID, body, rc, size)
}

// readEncHeaderPrefix reads as much of an object header as the stream has. A short
// read is not an error here — parseEncHeader is what decides whether the bytes
// identify an encrypted object, a plaintext one, or nothing recognisable.
func readEncHeaderPrefix(r io.Reader) ([]byte, error) {
	prefix := make([]byte, encHeaderSize)
	n, err := io.ReadFull(r, prefix)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	return prefix[:n], nil
}

// classify turns an object's header bytes into a cache entry: an encrypted object's
// derived key, or the pass-through marker for a pre-key plaintext one. It is the
// single place the RequireEncrypted policy is enforced, so both open paths (and any
// future one) get it identically.
func (c *encryptedClient) classify(prefix []byte, level int, minTXID, maxTXID ltx.TXID) (*encKeyEntry, error) {
	hdr, err := parseEncHeader(prefix)
	switch {
	case err == nil:
	case errors.Is(err, ErrObjectNotEncrypted):
		if c.requireEncrypted {
			return nil, fmt.Errorf("%w (level=%d min=%s max=%s)", ErrObjectNotEncrypted, level, minTXID, maxTXID)
		}
		entry := &encKeyEntry{plaintext: true}
		c.remember(level, minTXID, maxTXID, entry)
		return entry, nil
	default:
		return nil, err
	}

	aead, err := encObjectKey(c.key, hdr.salt, level, minTXID, maxTXID)
	if err != nil {
		return nil, err
	}
	entry := &encKeyEntry{hdr: hdr, aead: aead}
	c.remember(level, minTXID, maxTXID, entry)
	return entry, nil
}

// openAtOffset serves a resumed read. litestream's ResumableReader reopens a
// dropped stream at the plaintext offset it reached (OpenLTXFile(offset, 0)) — the
// exact failure it exists for, a provider closing an idle connection — so this
// path has to work for encryption to be usable at all.
//
// The salt lives in the object header, which is not in a stream that starts past
// it, so a resume needs it separately. The key cache keeps that to one extra GET
// per object per client (and none at all when the same client already read the
// object from the start, which is the ResumableReader case).
func (c *encryptedClient) openAtOffset(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	entry, err := c.objectKey(ctx, level, minTXID, maxTXID)
	if err != nil {
		return nil, err
	}
	if entry.plaintext {
		// A pre-key plaintext object: plaintext offsets are raw offsets.
		return c.inner.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
	}

	idx, skip, ctOffset := encFrameLocation(offset, entry.hdr.frameSize)
	rc, err := c.inner.OpenLTXFile(ctx, level, minTXID, maxTXID, ctOffset, 0)
	if err != nil {
		return nil, err
	}
	body := newOpeningReader(entry.aead, entry.hdr, rc, idx, skip)
	return c.decryptingBody(level, minTXID, maxTXID, body, rc, size)
}

// decryptingBody authenticates the first frame before handing the body back, then
// caps it at size and keeps the cache honest for the rest of the stream. See
// openingReader.prime for why the check is eager.
func (c *encryptedClient) decryptingBody(level int, minTXID, maxTXID ltx.TXID, body *openingReader, closer io.Closer, size int64) (io.ReadCloser, error) {
	if err := body.prime(); err != nil {
		if errors.Is(err, ErrKeyMismatch) {
			// The cached key may simply be stale: another writer can rewrite an
			// object name with a fresh salt (a retried upload, a re-run
			// compaction). Dropping it makes litestream's own retry re-fetch the
			// header rather than fail for good.
			c.forget(level, minTXID, maxTXID)
		}
		_ = closer.Close()
		return nil, fmt.Errorf("%w (level=%d min=%s max=%s)", err, level, minTXID, maxTXID)
	}
	watched := &watchedReader{
		body:   body,
		closer: closer,
		client: c,
		id:     encObjectID{level, minTXID, maxTXID},
	}
	return limitReadCloser(watched, size), nil
}

// objectKey returns the cached key for an object, fetching just its header if the
// cache has nothing.
func (c *encryptedClient) objectKey(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (*encKeyEntry, error) {
	if entry, ok := c.lookup(level, minTXID, maxTXID); ok {
		return entry, nil
	}

	rc, err := c.inner.OpenLTXFile(ctx, level, minTXID, maxTXID, 0, encHeaderSize)
	if err != nil {
		return nil, err
	}
	prefix, err := readEncHeaderPrefix(rc)
	_ = rc.Close()
	if err != nil {
		return nil, err
	}
	return c.classify(prefix, level, minTXID, maxTXID)
}

// --- the per-object key cache ------------------------------------------------

// encKeyCacheSize bounds the cache. A restore opens a snapshot plus the deltas
// above it, so a few dozen entries covers a whole restore's working set; the cache
// is per client (each restore builds its own), not process-wide.
const encKeyCacheSize = 64

type encKeyEntry struct {
	hdr       *encHeader
	aead      cipher.AEAD
	plaintext bool // a pre-key plaintext LTX object, passed through
}

type encObjectID struct {
	level            int
	minTXID, maxTXID ltx.TXID
}

func (c *encryptedClient) lookup(level int, minTXID, maxTXID ltx.TXID) (*encKeyEntry, bool) {
	return c.cache.Get(encObjectID{level, minTXID, maxTXID})
}

func (c *encryptedClient) remember(level int, minTXID, maxTXID ltx.TXID, entry *encKeyEntry) {
	c.cache.Add(encObjectID{level, minTXID, maxTXID}, entry)
}

// forget drops an object's cached key. Called on write (the object now has a fresh
// salt) and when a read under a cached key fails to authenticate — which is what
// makes a cache entry stale after some other writer rewrote that object name, so
// litestream's own retry re-fetches the header instead of failing for good.
func (c *encryptedClient) forget(level int, minTXID, maxTXID ltx.TXID) {
	c.cache.Remove(encObjectID{level, minTXID, maxTXID})
}

// --- reader plumbing ---------------------------------------------------------

// watchedReader is a decrypting body paired with the underlying stream's Close. It
// drops the object's cached key the first time the body fails to authenticate: the
// entry may simply be stale, and litestream's own retry then re-fetches the header
// instead of failing for good. It holds the client and the object id rather than a
// closure so a long-lived body does not pin an enclosing scope.
type watchedReader struct {
	body   io.Reader
	closer io.Closer
	client *encryptedClient
	id     encObjectID
	fired  bool
}

func (r *watchedReader) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	if err != nil && !r.fired && errors.Is(err, ErrKeyMismatch) {
		r.fired = true
		r.client.cache.Remove(r.id)
	}
	return n, err
}

func (r *watchedReader) Close() error { return r.closer.Close() }

// borrowedReadCloser is a reader that closes something else — a transformed or
// truncated view of a stream whose Close still belongs to the original body.
type borrowedReadCloser struct {
	io.Reader
	closer io.Closer
}

func newBorrowedReadCloser(r io.Reader, closer io.Closer) io.ReadCloser {
	return &borrowedReadCloser{Reader: r, closer: closer}
}

func (r *borrowedReadCloser) Close() error { return r.closer.Close() }

// limitReadCloser caps a body at size plaintext bytes; size 0 means "to the end",
// which is what every s3lite path asks for.
func limitReadCloser(rc io.ReadCloser, size int64) io.ReadCloser {
	if size <= 0 {
		return rc
	}
	return newBorrowedReadCloser(io.LimitReader(rc, size), rc)
}
