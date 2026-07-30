package s3lite

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

// The object format's own tests: framing, the size arithmetic litestream depends
// on, ranged reads, and the guarantee that tampering is an error rather than
// bytes. The decorator and end-to-end tests live in encryptclient_test.go.

// testFrameCode gives a 1 KiB frame so a property test can walk every plaintext
// length across several frames without moving 64 KiB per frame. The format carries
// the frame-size code in each object header, so a reader frames correctly either
// way — which these tests also prove by round-tripping at both sizes.
const (
	testFrameCode = 10
	testFrameSize = 1 << testFrameCode
)

// encCiphertextSize is the forward direction of the size arithmetic: the object size
// that sealing ptSize plaintext bytes produces. Production never predicts a size (the
// write path counts bytes and the read path derives with encPlaintextSize), so this
// lives with the tests that prove the two directions agree — with each other and with
// really-sealed objects.
func encCiphertextSize(ptSize int64, frameSize int) int64 {
	frames := (ptSize + int64(frameSize) - 1) / int64(frameSize)
	if frames == 0 {
		frames = 1 // an empty object still carries one (empty) final frame
	}
	return encHeaderSize + ptSize + frames*encTagSize
}

func testKey(b byte) []byte {
	key := make([]byte, encKeySize)
	for i := range key {
		key[i] = b ^ byte(i)
	}
	return key
}

// sealForTest seals pt as the object (level, minTXID, maxTXID) and returns the
// ciphertext plus the plaintext size the writer reported.
func sealForTest(t *testing.T, key []byte, level int, minTXID, maxTXID ltx.TXID, pt []byte, frameCode uint8) ([]byte, int64) {
	t.Helper()
	sealer, err := newRawSealingReader(key, level, minTXID, maxTXID, bytes.NewReader(pt), time.Unix(0, 0).UTC(), frameCode)
	if err != nil {
		t.Fatalf("newRawSealingReader: %v", err)
	}
	ct, err := io.ReadAll(sealer)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	return ct, sealer.plaintextSize()
}

// openForTest decrypts the plaintext range [offset, offset+size) of ct (size 0
// meaning "to the end"), going through the same header parse and key derivation the
// decorator uses.
func openForTest(key []byte, level int, minTXID, maxTXID ltx.TXID, ct []byte, offset int64) ([]byte, error) {
	if len(ct) < encHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}
	hdr, err := parseEncHeader(ct)
	if err != nil {
		return nil, err
	}
	aead, err := encObjectKey(key, hdr.salt, level, minTXID, maxTXID)
	if err != nil {
		return nil, err
	}
	// The same helper openAtOffset uses, so a ranged-read test cannot pass against a
	// private copy of the arithmetic.
	idx, skip, ctOffset := encFrameLocation(offset, hdr.frameSize)
	if ctOffset > int64(len(ct)) {
		ctOffset = int64(len(ct))
	}
	rd := newOpeningReader(aead, hdr, bytes.NewReader(ct[ctOffset:]), idx, skip)
	if err := rd.prime(); err != nil {
		return nil, err
	}
	return io.ReadAll(rd)
}

// TestEncryptSizeArithmetic is the property the whole decorator rests on: the
// plaintext size derived from an object's ciphertext size is *exactly* the real one.
// litestream treats a listed size as load-bearing (premature-EOF detection, the
// minimum-size check on restore), so an estimate would not do.
func TestEncryptSizeArithmetic(t *testing.T) {
	// Every length across four frames, against really-sealed bytes.
	t.Run("RealObjectsAtEveryLength", func(t *testing.T) {
		key := testKey(0x11)
		for pt := int64(0); pt <= 3*testFrameSize+7; pt++ {
			plain := make([]byte, pt)
			for i := range plain {
				plain[i] = byte(i)
			}
			ct, reported := sealForTest(t, key, 0, 1, 1, plain, testFrameCode)
			if reported != pt {
				t.Fatalf("pt=%d: writer reported plaintext size %d", pt, reported)
			}
			if got := encCiphertextSize(pt, testFrameSize); got != int64(len(ct)) {
				t.Fatalf("pt=%d: encCiphertextSize=%d, real ciphertext=%d", pt, got, len(ct))
			}
			back, err := encPlaintextSize(int64(len(ct)), testFrameSize)
			if err != nil {
				t.Fatalf("pt=%d: encPlaintextSize(%d): %v", pt, len(ct), err)
			}
			if back != pt {
				t.Fatalf("pt=%d: derived plaintext size %d", pt, back)
			}
		}
	})

	// The same arithmetic at the production frame size, where sealing every length
	// would move gigabytes: check the boundaries plus a sample.
	t.Run("ProductionFrameSize", func(t *testing.T) {
		lengths := []int64{0, 1, 2,
			encFrameSize - 1, encFrameSize, encFrameSize + 1,
			2*encFrameSize - 1, 2 * encFrameSize, 2*encFrameSize + 1,
			3*encFrameSize + 7, 1 << 20}
		for i := 0; i < 64; i++ {
			lengths = append(lengths, rand.Int64N(4*encFrameSize))
		}
		for _, pt := range lengths {
			ct := encCiphertextSize(pt, encFrameSize)
			back, err := encPlaintextSize(ct, encFrameSize)
			if err != nil {
				t.Fatalf("pt=%d ct=%d: %v", pt, ct, err)
			}
			if back != pt {
				t.Fatalf("pt=%d: round-tripped to %d via ct=%d", pt, back, ct)
			}
		}
	})

	// Overhead claim in the docs: a 16-byte tag per 64 KiB frame plus a 40-byte
	// header, i.e. well under 0.03%.
	t.Run("OverheadStaysUnderClaim", func(t *testing.T) {
		const pt = int64(64) << 20 // 64 MiB
		overhead := float64(encCiphertextSize(pt, encFrameSize)-pt) / float64(pt)
		if overhead > 0.0003 {
			t.Fatalf("overhead %.5f%% exceeds the documented 0.024%%", overhead*100)
		}
	})
}

// TestEncryptRoundTrip walks every plaintext length across several frames and back.
func TestEncryptRoundTrip(t *testing.T) {
	key := testKey(0x22)
	for _, pt := range []int64{0, 1, 63, testFrameSize - 1, testFrameSize, testFrameSize + 1,
		2 * testFrameSize, 2*testFrameSize + 1, 3*testFrameSize + 7} {
		plain := make([]byte, pt)
		for i := range plain {
			plain[i] = byte(i * 7)
		}
		ct, _ := sealForTest(t, key, 3, 9, 12, plain, testFrameCode)
		got, err := openForTest(key, 3, 9, 12, ct, 0)
		if err != nil {
			t.Fatalf("pt=%d: open: %v", pt, err)
		}
		if !bytes.Equal(got, plain) {
			t.Fatalf("pt=%d: round trip mismatch (%d bytes back)", pt, len(got))
		}
	}

	// And once at the production frame size, so the 64 KiB path is exercised too.
	t.Run("ProductionFrameSize", func(t *testing.T) {
		plain := make([]byte, 2*encFrameSize+1234)
		for i := range plain {
			plain[i] = byte(i * 31)
		}
		ct, _ := sealForTest(t, key, 0, 1, 4, plain, encFrameSizeCode)
		got, err := openForTest(key, 0, 1, 4, ct, 0)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if !bytes.Equal(got, plain) {
			t.Fatal("round trip mismatch at the production frame size")
		}
	})
}

// TestEncryptRangedReads covers the resume path's arithmetic: an open at an
// arbitrary plaintext offset must yield exactly the plaintext slice from there.
// litestream's ResumableReader reopens at whatever offset it had reached.
func TestEncryptRangedReads(t *testing.T) {
	key := testKey(0x33)
	plain := make([]byte, 3*testFrameSize+11)
	for i := range plain {
		plain[i] = byte(i * 13)
	}
	ct, _ := sealForTest(t, key, 1, 5, 5, plain, testFrameCode)

	offsets := []int64{0, 1, testFrameSize - 1, testFrameSize, testFrameSize + 1,
		2 * testFrameSize, 3 * testFrameSize, 3*testFrameSize + 10, int64(len(plain))}
	for _, off := range offsets {
		got, err := openForTest(key, 1, 5, 5, ct, off)
		if err != nil {
			t.Fatalf("offset=%d: %v", off, err)
		}
		if want := plain[off:]; !bytes.Equal(got, want) {
			t.Fatalf("offset=%d: got %d bytes, want %d", off, len(got), len(want))
		}
	}
}

// TestEncryptTamperIsAlwaysAnError is the security core: no modification of an
// object ever yields *forged* plaintext. Each case mutates a valid object and
// asserts the read fails.
//
// The exact property is what a framed AEAD can promise: a frame is released only
// after it authenticates, so a tampered object yields at most an authentic strict
// prefix and then an error — never modified bytes, never the whole object. That is
// enough for every consumer here, because litestream decodes LTX incrementally and
// restore builds a temp file it renames only on success, so a failed read leaves no
// partially-restored database.
func TestEncryptTamperIsAlwaysAnError(t *testing.T) {
	key := testKey(0x44)
	plain := make([]byte, 2*testFrameSize+5)
	for i := range plain {
		plain[i] = byte(i)
	}
	base, _ := sealForTest(t, key, 2, 7, 9, plain, testFrameCode)

	// A read of the untouched object must succeed, else the cases below prove nothing.
	if _, err := openForTest(key, 2, 7, 9, base, 0); err != nil {
		t.Fatalf("baseline object must decrypt: %v", err)
	}

	stride := testFrameSize + encTagSize
	cases := []struct {
		name string
		mut  func(ct []byte) []byte
	}{
		{"FlippedByteInFirstFrame", func(ct []byte) []byte {
			ct[encHeaderSize+10] ^= 0x01
			return ct
		}},
		{"FlippedByteInLastFrame", func(ct []byte) []byte {
			ct[len(ct)-20] ^= 0x80
			return ct
		}},
		{"FlippedTagByte", func(ct []byte) []byte {
			ct[len(ct)-1] ^= 0xff
			return ct
		}},
		{"FlippedSaltByte", func(ct []byte) []byte {
			ct[10] ^= 0x01 // inside the salt: every derived key changes
			return ct
		}},
		{"FlippedFrameSizeCode", func(ct []byte) []byte {
			ct[5] = testFrameCode + 1
			return ct
		}},
		{"FlippedVersion", func(ct []byte) []byte {
			ct[4] = encVersion + 1
			return ct
		}},
		{"TruncatedFinalFrame", func(ct []byte) []byte {
			return ct[:len(ct)-4]
		}},
		{"DroppedTrailingFrame", func(ct []byte) []byte {
			// Cut cleanly on a frame boundary: the remaining last frame was sealed
			// without the final flag, so it must not authenticate as the last.
			return ct[:encHeaderSize+2*stride]
		}},
		{"DroppedAllButFirstFrame", func(ct []byte) []byte {
			return ct[:encHeaderSize+stride]
		}},
		{"HeaderOnly", func(ct []byte) []byte {
			return ct[:encHeaderSize]
		}},
		{"ReorderedFrames", func(ct []byte) []byte {
			out := append([]byte(nil), ct[:encHeaderSize]...)
			out = append(out, ct[encHeaderSize+stride:encHeaderSize+2*stride]...)
			out = append(out, ct[encHeaderSize:encHeaderSize+stride]...)
			out = append(out, ct[encHeaderSize+2*stride:]...)
			return out
		}},
		{"DuplicatedFirstFrame", func(ct []byte) []byte {
			out := append([]byte(nil), ct[:encHeaderSize+stride]...)
			out = append(out, ct[encHeaderSize:encHeaderSize+stride]...)
			out = append(out, ct[encHeaderSize+stride:]...)
			return out
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ct := tc.mut(append([]byte(nil), base...))
			got, err := openForTest(key, 2, 7, 9, ct, 0)
			if err == nil {
				t.Fatalf("tampered object decrypted to %d bytes", len(got))
			}
			if !bytes.HasPrefix(plain, got) {
				t.Fatalf("tampered read returned %d bytes that are not an authentic prefix", len(got))
			}
			if len(got) == len(plain) {
				t.Fatal("tampered read returned the whole plaintext")
			}
		})
	}
}

// TestEncryptIdentityBinding pins the reason the object's identity is part of the
// key derivation: an adversary with bucket *write* access must not be able to move a
// body from one object name to another. Nothing else in the format authenticates
// which object a body belongs to.
func TestEncryptIdentityBinding(t *testing.T) {
	key := testKey(0x55)
	plain := []byte(strings.Repeat("committed rows\n", 40))

	ct, _ := sealForTest(t, key, 0, 4, 4, plain, testFrameCode)

	// The body decrypts under its own name...
	if _, err := openForTest(key, 0, 4, 4, ct, 0); err != nil {
		t.Fatalf("own name must decrypt: %v", err)
	}
	// ...and under no other.
	for _, other := range []struct {
		name             string
		level            int
		minTXID, maxTXID ltx.TXID
	}{
		{"DifferentLevel", 1, 4, 4},
		{"DifferentMinTXID", 0, 3, 4},
		{"DifferentMaxTXID", 0, 4, 5},
	} {
		t.Run(other.name, func(t *testing.T) {
			_, err := openForTest(key, other.level, other.minTXID, other.maxTXID, ct, 0)
			if !errors.Is(err, ErrKeyMismatch) {
				t.Fatalf("moving a body to %s must fail with ErrKeyMismatch, got %v", other.name, err)
			}
		})
	}
}

// TestEncryptWrongKey pins the typed error a consumer needs: "this bucket is not
// yours" is distinguishable from "you forgot the key" (ErrReplicaEncrypted, raised
// by the restore path) and neither looks like a corrupt database.
func TestEncryptWrongKey(t *testing.T) {
	plain := []byte(strings.Repeat("secret", 100))
	ct, _ := sealForTest(t, testKey(0x66), 0, 2, 2, plain, testFrameCode)

	got, err := openForTest(testKey(0x67), 0, 2, 2, ct, 0)
	if !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("wrong key must give ErrKeyMismatch, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("wrong key returned %d bytes", len(got))
	}
}

// TestEncryptSaltIsPerObject pins that the same bytes written twice to the same
// object name produce different ciphertext under different keystreams. A retried
// upload or a re-run compaction rewrites one name with slightly different bytes, so
// deriving the nonce from identity alone would be keystream reuse.
func TestEncryptSaltIsPerObject(t *testing.T) {
	key := testKey(0x77)
	plain := []byte(strings.Repeat("x", 3*testFrameSize))

	first, _ := sealForTest(t, key, 0, 1, 1, plain, testFrameCode)
	second, _ := sealForTest(t, key, 0, 1, 1, plain, testFrameCode)

	if bytes.Equal(first[:encHeaderSize], second[:encHeaderSize]) {
		t.Fatal("two writes of the same object name reused the salt")
	}
	if bytes.Equal(first[encHeaderSize:], second[encHeaderSize:]) {
		t.Fatal("two writes of the same object name produced identical ciphertext")
	}
	// Both must still decrypt: the salt travels in the object.
	for i, ct := range [][]byte{first, second} {
		got, err := openForTest(key, 0, 1, 1, ct, 0)
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if !bytes.Equal(got, plain) {
			t.Fatalf("write %d: round trip mismatch", i)
		}
	}
}

// TestEncryptCiphertextRevealsNothing is a coarse but load-bearing check: the object
// body must not contain the plaintext, and must look nothing like an LTX file.
func TestEncryptCiphertextRevealsNothing(t *testing.T) {
	key := testKey(0x88)
	plain := []byte(strings.Repeat("alice@example.com|4111111111111111|", 200))
	ct, _ := sealForTest(t, key, 0, 1, 1, plain, testFrameCode)

	if bytes.Contains(ct, []byte("alice@example.com")) {
		t.Fatal("ciphertext contains plaintext")
	}
	if string(ct[:4]) != encMagic {
		t.Fatalf("object magic = %q, want %q", ct[:4], encMagic)
	}
	if bytes.Contains(ct[:encHeaderSize], []byte(ltx.Magic)) {
		t.Fatal("object header looks like an LTX header")
	}
}

// TestParseEncHeader covers the classification the mixed window depends on: our
// magic, litestream's plaintext magic (a distinguishable typed error, not a
// failure), and anything else.
func TestParseEncHeader(t *testing.T) {
	hdr, err := newEncHeader(encFrameSizeCode)
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseEncHeader(hdr.raw)
	if err != nil {
		t.Fatalf("own header must parse: %v", err)
	}
	if got.frameSize != encFrameSize {
		t.Fatalf("frameSize=%d, want %d", got.frameSize, encFrameSize)
	}
	if !bytes.Equal(got.salt, hdr.salt) {
		t.Fatal("salt did not survive the round trip")
	}

	plainLTX := append([]byte(ltx.Magic), make([]byte, 96)...)
	if _, err := parseEncHeader(plainLTX); !errors.Is(err, ErrObjectNotEncrypted) {
		t.Fatalf("a plaintext LTX object must report ErrObjectNotEncrypted, got %v", err)
	}

	for _, tc := range []struct {
		name string
		in   []byte
	}{
		{"TooShort", []byte("S3")},
		{"UnknownMagic", []byte("XXXXsomething")},
		{"TruncatedHeader", append([]byte(encMagic), make([]byte, 4)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseEncHeader(tc.in); err == nil {
				t.Fatal("expected an error")
			} else if errors.Is(err, ErrObjectNotEncrypted) {
				t.Fatalf("must not be classified as plaintext LTX: %v", err)
			}
		})
	}
}

// TestValidateEncryptionKey pins that a wrong-length key fails at Open rather than
// at the first replication attempt.
func TestValidateEncryptionKey(t *testing.T) {
	if err := validateEncryptionKey(nil); err != nil {
		t.Fatalf("no key is valid: %v", err)
	}
	if err := validateEncryptionKey(testKey(1)); err != nil {
		t.Fatalf("a %d-byte key is valid: %v", encKeySize, err)
	}
	for _, n := range []int{1, 16, 31, 33, 64} {
		if err := validateEncryptionKey(make([]byte, n)); err == nil {
			t.Fatalf("a %d-byte key must be rejected", n)
		} else if !strings.Contains(err.Error(), fmt.Sprintf("%d bytes", encKeySize)) {
			t.Fatalf("error should state the required length, got: %v", err)
		}
	}
}

// TestOpaqueOwnerID pins that the id is random and hex — the point being that an
// encrypted instance does not publish its hostname in lock.json.
func TestOpaqueOwnerID(t *testing.T) {
	a, err := opaqueOwnerID()
	if err != nil {
		t.Fatal(err)
	}
	b, err := opaqueOwnerID()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("owner ids must not repeat")
	}
	if len(a) != 32 {
		t.Fatalf("owner id length = %d, want 32 hex chars", len(a))
	}
	if strings.ContainsAny(a, "ghijklmnopqrstuvwxyz-._") {
		t.Fatalf("owner id %q is not plain hex", a)
	}
}

// BenchmarkSeal / BenchmarkOpen guard the streaming path's allocation behaviour. Both
// readers reuse one frame buffer, so bytes/op must stay a small constant rather than
// scaling with the object — sealing a 16 MiB object should not produce 16 MiB of
// garbage. Run with -benchmem; a regression shows up as B/op tracking the payload.
func benchPayload(n int) []byte {
	buf := make([]byte, n)
	rnd := rand.New(rand.NewPCG(42, 43))
	for i := range buf {
		buf[i] = byte(rnd.Uint32())
	}
	return buf
}

func BenchmarkSeal(b *testing.B) {
	const size = 16 << 20
	key := make([]byte, encKeySize)
	plain := benchPayload(size)

	b.SetBytes(size)
	b.ReportAllocs()
	for b.Loop() {
		sealer, err := newRawSealingReader(key, 0, 1, 1, bytes.NewReader(plain),
			time.Unix(0, 0).UTC(), encFrameSizeCode)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, sealer); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpen(b *testing.B) {
	const size = 16 << 20
	key := make([]byte, encKeySize)
	plain := benchPayload(size)

	sealer, err := newRawSealingReader(key, 0, 1, 1, bytes.NewReader(plain),
		time.Unix(0, 0).UTC(), encFrameSizeCode)
	if err != nil {
		b.Fatal(err)
	}
	ct, err := io.ReadAll(sealer)
	if err != nil {
		b.Fatal(err)
	}
	hdr, err := parseEncHeader(ct)
	if err != nil {
		b.Fatal(err)
	}
	aead, err := encObjectKey(key, hdr.salt, 0, 1, 1)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(size)
	b.ReportAllocs()
	for b.Loop() {
		rd := newOpeningReader(aead, hdr, bytes.NewReader(ct[encHeaderSize:]), 0, 0)
		if _, err := io.Copy(io.Discard, rd); err != nil {
			b.Fatal(err)
		}
	}
}
