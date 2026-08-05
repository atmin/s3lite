package s3lite

import (
	"bufio"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/superfly/ltx"
	"golang.org/x/crypto/chacha20poly1305"
)

// Client-side encryption of the LTX objects a replica client ships. The wire
// format lives here; the ReplicaClient decorator that uses it is in
// encryptclient.go.
//
// The format is framed AEAD rather than a whole-object seal because litestream
// reads objects at arbitrary byte offsets: ResumableReader reopens a dropped
// stream mid-object via OpenLTXFile(offset, 0), so decryption must be able to
// start at an interior boundary without the preceding bytes.
//
//	header : magic "S3LE" | version | frame-size code | reserved(2) | salt(32)
//	body   : frame(0) … frame(n-1)   each = encFrameSize plaintext bytes + a
//	                                 16-byte tag; the final frame is short (and
//	                                 empty only for an empty object)
//
// Per-frame key = HKDF-SHA256(EncryptionKey, salt, "s3lite-ltx-v1" ‖ level ‖
// minTXID ‖ maxTXID). Binding the object's identity into the derivation is what
// stops anyone with bucket write access from moving a body between object names:
// nothing else authenticates which object a body belongs to. The random salt is
// what stops keystream reuse when the *same* object name is rewritten with
// different bytes — a retried upload or a re-run compaction does exactly that
// (only the LTX header timestamp differs), so object identity alone is not a safe
// nonce.
//
// Each frame is sealed with nonce = frame index and AAD = the whole header ‖
// frame index ‖ final flag. The index and final flag are the STREAM
// construction: a reordered, duplicated, or dropped trailing frame fails to
// authenticate instead of decrypting to a short read. Putting the header in the
// AAD binds the version and frame-size code too (the salt is already bound
// through the derivation).

const (
	// encMagic marks an s3lite-encrypted object. It is compared against
	// ltx.Magic ("LTX1") to tell an encrypted object from a plaintext one.
	encMagic = "S3LE"

	// encVersion is the format version. A reader refuses anything else rather
	// than guessing, so a future format change is a clean error on old code.
	encVersion = 1

	// encFrameSizeCode is log2 of the plaintext bytes per frame, and is what the
	// header stores so a reader can frame an object written under a different
	// setting. 64 KiB keeps the streaming buffers small while costing 16 bytes of
	// tag per frame — 0.024% overhead — and makes a resumed read re-fetch at most
	// one frame's worth of bytes. Changing it is a format change: bump encVersion,
	// because LTXFiles has to size a listed object without reading its header.
	encFrameSizeCode = 16

	// encFrameSize is the plaintext bytes per frame.
	encFrameSize = 1 << encFrameSizeCode

	// encTagSize is the ChaCha20-Poly1305 authentication tag length.
	encTagSize = chacha20poly1305.Overhead

	// encSaltSize is the per-object random salt length (HKDF salt).
	encSaltSize = 32

	// encHeaderSize is the fixed object header: magic(4) | version(1) |
	// frame-size code(1) | reserved(2) | salt(32).
	encHeaderSize = 4 + 1 + 1 + 2 + encSaltSize

	// encKeySize is the required Config.EncryptionKey length.
	encKeySize = chacha20poly1305.KeySize

	// encKeyInfoPrefix domain-separates this key derivation from any other use of
	// the caller's key. It changes only if the format changes.
	encKeyInfoPrefix = "s3lite-ltx-v1"
)

// ErrReplicaEncrypted reports that the replica holds s3lite-encrypted objects but
// this instance has no Config.EncryptionKey — "you forgot the key", not a corrupt
// database. Restore, follower refresh and Open surface it wrapped.
var ErrReplicaEncrypted = errors.New("s3lite: replica is encrypted but no EncryptionKey is configured")

// ErrKeyMismatch reports that an encrypted object would not authenticate under the
// configured key — "this bucket is not yours (or its bytes were tampered with)".
// A wrong key and a modified object are cryptographically indistinguishable, so
// both report this; either way no plaintext is ever returned.
var ErrKeyMismatch = errors.New("s3lite: replica object does not authenticate under the configured EncryptionKey")

// ErrObjectNotEncrypted reports that a plaintext object was found on a replica
// opened with Config.RequireEncrypted. Leave that flag false while a
// previously-plaintext replica still holds pre-key objects.
var ErrObjectNotEncrypted = errors.New("s3lite: replica holds a plaintext object and RequireEncrypted is set")

// validateEncryptionKey checks the configured key length up front, so a typo
// fails Open loudly instead of at the first replication attempt.
func validateEncryptionKey(key []byte) error {
	if len(key) == 0 || len(key) == encKeySize {
		return nil
	}
	return fmt.Errorf("s3lite: EncryptionKey must be %d bytes, got %d", encKeySize, len(key))
}

// encObjectKey derives an object's frame key from the master key, the object's
// random salt and its identity. See the package comment above for why identity is
// part of the derivation.
func encObjectKey(master, salt []byte, level int, minTXID, maxTXID ltx.TXID) (cipher.AEAD, error) {
	info := make([]byte, 0, len(encKeyInfoPrefix)+4+8+8)
	info = append(info, encKeyInfoPrefix...)
	info = binary.BigEndian.AppendUint32(info, uint32(level))
	info = binary.BigEndian.AppendUint64(info, uint64(minTXID))
	info = binary.BigEndian.AppendUint64(info, uint64(maxTXID))

	sub, err := hkdf.Key(sha256.New, master, salt, string(info), encKeySize)
	if err != nil {
		return nil, fmt.Errorf("s3lite: derive object key: %w", err)
	}
	aead, err := chacha20poly1305.New(sub)
	if err != nil {
		return nil, fmt.Errorf("s3lite: new aead: %w", err)
	}
	return aead, nil
}

// encHeader is an encrypted object's fixed-size preamble.
type encHeader struct {
	frameSize int
	salt      []byte
	raw       []byte // the marshalled header, bound into every frame's AAD
}

// newEncHeader builds a header with a fresh random salt. frameCode is log2 of the
// frame size; production always passes encFrameSizeCode, and it is a parameter only
// so tests can exercise every plaintext length across several frames cheaply.
func newEncHeader(frameCode uint8) (*encHeader, error) {
	salt := make([]byte, encSaltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("s3lite: generate object salt: %w", err)
	}
	raw := make([]byte, encHeaderSize)
	copy(raw, encMagic)
	raw[4] = encVersion
	raw[5] = frameCode
	copy(raw[8:], salt)
	return &encHeader{frameSize: 1 << frameCode, salt: raw[8 : 8+encSaltSize], raw: raw}, nil
}

// parseEncHeader reads a header off the front of an object. It returns
// ErrObjectNotEncrypted for a plaintext LTX object so the caller can decide
// whether to pass it through (the mixed window) or refuse it.
func parseEncHeader(b []byte) (*encHeader, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("s3lite: replica object is too short to identify (%d bytes)", len(b))
	}
	switch string(b[:4]) {
	case encMagic:
	case ltx.Magic:
		return nil, ErrObjectNotEncrypted
	default:
		return nil, fmt.Errorf("s3lite: replica object has unknown magic %q", b[:4])
	}
	if len(b) < encHeaderSize {
		return nil, fmt.Errorf("s3lite: encrypted object header truncated (%d of %d bytes)", len(b), encHeaderSize)
	}
	if b[4] != encVersion {
		return nil, fmt.Errorf("s3lite: unsupported encrypted object version %d (this build writes %d)", b[4], encVersion)
	}
	code := b[5]
	if code < 10 || code > 30 { // 1 KiB … 1 GiB: anything else is not a frame size we wrote
		return nil, fmt.Errorf("s3lite: encrypted object has invalid frame-size code %d", code)
	}
	raw := make([]byte, encHeaderSize)
	copy(raw, b[:encHeaderSize])
	return &encHeader{
		frameSize: 1 << code,
		salt:      raw[8 : 8+encSaltSize],
		raw:       raw,
	}, nil
}

// frameSealer holds the per-frame nonce and AAD for one object, so the streaming
// readers can rewrite the few bytes that change per frame instead of rebuilding
// both on every 64 KiB. The header is the bulk of the AAD and never changes, so it
// is copied in once here.
type frameSealer struct {
	nonce [chacha20poly1305.NonceSize]byte
	aad   []byte // encHeaderSize bytes of header, then idx(8) and the final flag(1)
}

func newFrameSealer(hdr *encHeader) frameSealer {
	s := frameSealer{aad: make([]byte, encHeaderSize+9)}
	copy(s.aad, hdr.raw)
	return s
}

// set points the nonce and AAD at frame idx. The object key is derived from a fresh
// random salt on every write, so a bare frame counter never repeats under a key; the
// index and final flag are the STREAM construction (see the package comment).
func (s *frameSealer) set(idx uint64, final bool) {
	binary.BigEndian.PutUint64(s.nonce[4:], idx)
	binary.BigEndian.PutUint64(s.aad[encHeaderSize:], idx)
	s.aad[encHeaderSize+8] = 0
	if final {
		s.aad[encHeaderSize+8] = 1
	}
}

// encFrameLocation maps a plaintext offset onto the frame that contains it: the
// frame's index, how many of its plaintext bytes precede the offset, and where the
// frame starts in the object. It is the whole of the resume path's arithmetic, so it
// lives in one place — the ranged-read tests drive this same function rather than a
// copy of it.
func encFrameLocation(offset int64, frameSize int) (idx uint64, skip, ctOffset int64) {
	idx = uint64(offset / int64(frameSize))
	skip = offset % int64(frameSize)
	ctOffset = encHeaderSize + int64(idx)*(int64(frameSize)+encTagSize)
	return idx, skip, ctOffset
}

// encPlaintextSize is the inverse: the plaintext size of an encrypted object of
// ctSize bytes. It is exact arithmetic, never an estimate, because litestream
// treats the size it gets from a listing as load-bearing — ResumableReader
// compares it against its offset to tell a premature EOF from a real one, and
// restore rejects an object smaller than an LTX header. Sizes reported upward
// must therefore be plaintext sizes.
func encPlaintextSize(ctSize int64, frameSize int) (int64, error) {
	body := ctSize - encHeaderSize
	if body < encTagSize {
		return 0, fmt.Errorf("s3lite: encrypted object too small (%d bytes)", ctSize)
	}
	stride := int64(frameSize) + encTagSize
	frames := body / stride
	if body%stride != 0 {
		frames++
	}
	pt := body - frames*encTagSize
	if pt < 0 {
		return 0, fmt.Errorf("s3lite: encrypted object size %d is not a whole number of frames", ctSize)
	}
	return pt, nil
}

// sealingReader turns a plaintext LTX stream into an encrypted object stream. It
// is the body handed to the inner client's WriteLTXFile and works in constant
// memory: one frame in flight, whatever the database size.
//
// It implements litestream.LTXTimestamper, which is the whole reason the fork
// carries its second patch: every backend otherwise peeks the LTX header out of
// the upload stream to record the object's metadata timestamp, and ciphertext
// cannot be peeked. See docs/litestream-fork.md.
type sealingReader struct {
	src       *bufio.Reader
	aead      cipher.AEAD
	sealer    frameSealer
	timestamp time.Time

	idx     uint64
	plain   []byte // one frame of plaintext, reused
	ct      []byte // one frame of ciphertext, reused as Seal's destination
	out     []byte // the pending slice of ct not yet handed to the caller
	ptBytes int64  // plaintext bytes consumed, which is the size we report upward
	done    bool
	err     error
}

// newSealingReader peeks the plaintext LTX header for its timestamp — the value the
// inner client would otherwise have to peek out of the (now unreadable) upload
// stream itself — then wraps src.
func newSealingReader(master []byte, level int, minTXID, maxTXID ltx.TXID, src io.Reader) (*sealingReader, error) {
	ltxHdr, rest, err := ltx.PeekHeader(src)
	if err != nil {
		return nil, fmt.Errorf("s3lite: peek ltx header for encryption: %w", err)
	}
	return newRawSealingReader(master, level, minTXID, maxTXID, rest,
		time.UnixMilli(ltxHdr.Timestamp).UTC(), encFrameSizeCode)
}

// newRawSealingReader seals arbitrary bytes under a caller-supplied timestamp and
// frame size. Production always goes through newSealingReader (whose body must be a
// real LTX stream); this exists so the format itself can be exercised on any byte
// length, and across several frames without moving 64 KiB per frame.
func newRawSealingReader(master []byte, level int, minTXID, maxTXID ltx.TXID, src io.Reader, ts time.Time, frameCode uint8) (*sealingReader, error) {
	hdr, err := newEncHeader(frameCode)
	if err != nil {
		return nil, err
	}
	aead, err := encObjectKey(master, hdr.salt, level, minTXID, maxTXID)
	if err != nil {
		return nil, err
	}
	return &sealingReader{
		// bufio gives the one-byte lookahead that tells a full frame from the last
		// one, which is what keeps a plaintext that is an exact multiple of the frame
		// size from paying for a trailing empty frame.
		src:       bufio.NewReader(src),
		aead:      aead,
		sealer:    newFrameSealer(hdr),
		timestamp: ts,
		plain:     make([]byte, hdr.frameSize),
		ct:        make([]byte, 0, hdr.frameSize+encTagSize),
		out:       hdr.raw,
	}, nil
}

// LTXTimestamp implements litestream.LTXTimestamper.
func (r *sealingReader) LTXTimestamp() time.Time { return r.timestamp }

// plaintextSize reports the plaintext bytes consumed so far; after a complete read
// it is the object's plaintext size, which is what the decorator reports upward.
func (r *sealingReader) plaintextSize() int64 { return r.ptBytes }

func (r *sealingReader) Read(p []byte) (int, error) {
	for len(r.out) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if r.done {
			return 0, io.EOF
		}
		if err := r.sealNext(); err != nil {
			r.err = err
			return 0, err
		}
	}
	n := copy(p, r.out)
	r.out = r.out[n:]
	return n, nil
}

// sealNext reads one frame of plaintext and stages its ciphertext. A frame is
// final when nothing follows it; an empty object still gets one (empty) frame so
// that "the last frame carries the final flag" holds for every object.
func (r *sealingReader) sealNext() error {
	n, final, err := readFrame(r.src, r.plain)
	if err != nil {
		return err
	}
	r.done = final

	r.ptBytes += int64(n)
	r.sealer.set(r.idx, final)
	// Seal into the reused buffer: Read has already handed out the whole previous
	// frame (it only calls sealNext once out is drained), so overwriting is safe.
	r.out = r.aead.Seal(r.ct[:0], r.sealer.nonce[:], r.plain[:n], r.sealer.aad)
	r.idx++
	return nil
}

// readFrame fills buf with one frame's worth of bytes and reports whether that frame
// is the stream's last, using a one-byte lookahead. It is the framing rule both
// directions share: the sealer needs it so an exact multiple of the frame size does
// not emit a trailing empty frame, and the opener needs it to know which frame must
// carry the final flag — the property that turns a truncated body into an
// authentication failure rather than a short read.
func readFrame(src *bufio.Reader, buf []byte) (n int, final bool, err error) {
	n, err = io.ReadFull(src, buf)
	switch {
	case err == nil:
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return n, true, nil // short (possibly empty) frame: the stream ends here
	default:
		return n, false, err
	}
	// A full frame is the last only if nothing follows it.
	if _, err := src.Peek(1); err != nil {
		if !errors.Is(err, io.EOF) {
			return n, false, err
		}
		return n, true, nil
	}
	return n, false, nil
}

// openingReader turns an encrypted object stream back into plaintext. src must
// begin at a frame boundary; firstIdx is that frame's index (0 for a whole-object
// read, higher for a resumed one) and skip is how many plaintext bytes of that
// first frame the caller does not want.
//
// A frame is decrypted as the object's final frame exactly when src ends right
// after it. That is what makes truncation an error rather than a short read: a
// stream cut at a frame boundary fails to authenticate (the frame was sealed
// without the final flag), and a stream cut inside a frame fails the tag. Both
// are errors the caller can retry — litestream's ResumableReader reopens at the
// plaintext offset it reached, which lands back on a frame boundary — while a
// genuinely tampered object simply keeps failing.
type openingReader struct {
	src    *bufio.Reader
	aead   cipher.AEAD
	sealer frameSealer

	idx  uint64
	skip int64
	ct   []byte // one frame of ciphertext, reused
	pt   []byte // one frame of plaintext, reused as Open's destination
	out  []byte // the pending slice of pt not yet handed to the caller
	eof  bool
	err  error
}

func newOpeningReader(aead cipher.AEAD, hdr *encHeader, src io.Reader, firstIdx uint64, skip int64) *openingReader {
	return &openingReader{
		src:    bufio.NewReader(src),
		aead:   aead,
		sealer: newFrameSealer(hdr),
		idx:    firstIdx,
		skip:   skip,
		ct:     make([]byte, hdr.frameSize+encTagSize),
		pt:     make([]byte, 0, hdr.frameSize),
	}
}

// prime decrypts and authenticates the object's first frame up front, so a wrong
// key or a tampered head fails at open time. That matters for the error a consumer
// sees: litestream wraps an *open* failure (`reopen ltx file at offset %d: %w`), so
// the typed ErrKeyMismatch survives to the caller, whereas a mid-stream read error
// is swallowed by ResumableReader's retry loop and reported as a bare "max retries
// exceeded". Either way no plaintext is returned; this is about diagnosis.
func (r *openingReader) prime() error {
	for len(r.out) == 0 && !r.eof && r.err == nil {
		if err := r.openNext(); err != nil {
			r.err = err
			return err
		}
	}
	return nil
}

func (r *openingReader) Read(p []byte) (int, error) {
	for len(r.out) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if r.eof {
			return 0, io.EOF
		}
		if err := r.openNext(); err != nil {
			r.err = err
			return 0, err
		}
	}
	n := copy(p, r.out)
	r.out = r.out[n:]
	return n, nil
}

// openNext reads one ciphertext frame and stages its plaintext.
func (r *openingReader) openNext() error {
	n, final, err := readFrame(r.src, r.ct)
	if err != nil {
		return err
	}
	r.eof = final
	if n < encTagSize {
		// Structurally impossible for an object we wrote: every frame carries at
		// least its tag. Either the body was cut here or it has no frames at all.
		return fmt.Errorf("s3lite: encrypted object frame %d is %d bytes, shorter than a tag: %w",
			r.idx, n, io.ErrUnexpectedEOF)
	}

	r.sealer.set(r.idx, final)
	// Open into the reused buffer: Read has already handed out the whole previous
	// frame (it only calls openNext once out is drained), so overwriting is safe.
	plain, err := r.aead.Open(r.pt[:0], r.sealer.nonce[:], r.ct[:n], r.sealer.aad)
	if err != nil {
		return fmt.Errorf("%w (frame %d)", ErrKeyMismatch, r.idx)
	}
	r.idx++

	if r.skip > 0 {
		if r.skip >= int64(len(plain)) {
			r.skip -= int64(len(plain))
			return nil
		}
		plain = plain[r.skip:]
		r.skip = 0
	}
	r.out = plain
	return nil
}
