package s3lite

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/superfly/ltx"
)

// Golden vectors pin the encrypted object format against committed bytes, not just
// round-trips: every other test in this package seals with the same code that opens,
// so a changed derivation, header layout, or nonce/AAD composition would still pass
// them all. See the package comment in encrypt.go for what a failing vector means.

type goldenDerivation struct {
	Key     string `json:"key"`
	Salt    string `json:"salt"`
	Level   int    `json:"level"`
	MinTXID uint64 `json:"min_txid"`
	MaxTXID uint64 `json:"max_txid"`
	Subkey  string `json:"subkey"`
}

type goldenHeader struct {
	FrameCode uint8  `json:"frame_code"`
	Salt      string `json:"salt"`
	Header    string `json:"header"`
}

type goldenObject struct {
	Key        string `json:"key"`
	Level      int    `json:"level"`
	MinTXID    uint64 `json:"min_txid"`
	MaxTXID    uint64 `json:"max_txid"`
	Salt       string `json:"salt"`
	FrameCode  uint8  `json:"frame_code"`
	Plaintext  string `json:"plaintext"`
	Ciphertext string `json:"ciphertext"`
}

type goldenVectors struct {
	FormatVersion int                `json:"format_version"`
	Derivation    []goldenDerivation `json:"derivation"`
	Header        []goldenHeader     `json:"header"`
	Object        []goldenObject     `json:"object"`
}

const goldenPath = "testdata/golden.json"

// sealGoldenObject seals pt under a caller-fixed salt, mirroring sealingReader's
// sealNext loop exactly but against newEncHeaderWithSalt instead of a random salt, so
// the result is reproducible. It shares every crypto primitive (frameSealer, readFrame,
// encObjectKey) with production; only the salt source differs.
func sealGoldenObject(t *testing.T, key, salt []byte, level int, minTXID, maxTXID ltx.TXID, frameCode uint8, pt []byte) []byte {
	t.Helper()
	hdr := newEncHeaderWithSalt(frameCode, salt)
	aead, err := encObjectKey(key, salt, level, minTXID, maxTXID)
	if err != nil {
		t.Fatalf("encObjectKey: %v", err)
	}
	sealer := newFrameSealer(hdr)
	out := append([]byte(nil), hdr.raw...)
	src := bufio.NewReader(bytes.NewReader(pt))
	buf := make([]byte, hdr.frameSize)
	for idx := uint64(0); ; idx++ {
		n, final, err := readFrame(src, buf)
		if err != nil {
			t.Fatalf("readFrame: %v", err)
		}
		sealer.set(idx, final)
		out = aead.Seal(out, sealer.nonce[:], buf[:n], sealer.aad)
		if final {
			break
		}
	}
	return out
}

func repeatByte(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

// patternBytes is deterministic non-degenerate filler: distinct per offset, so a
// framing, nonce, or AAD bug shows up as a content mismatch rather than being masked
// by an all-zero plaintext.
func patternBytes(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i*7 + 13)
	}
	return out
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

// TestGenerateGoldenVectors (re)writes testdata/golden.json from the current
// production code. It only runs when explicitly asked, because regenerating a vector
// to make TestEncryptGoldenVectors pass is never the fix — a failing vector means the
// on-disk format changed, which is exactly what these vectors exist to catch. Run it
// only alongside a deliberate encVersion bump.
func TestGenerateGoldenVectors(t *testing.T) {
	if os.Getenv("S3LITE_GENERATE_GOLDEN") != "1" {
		t.Skip("set S3LITE_GENERATE_GOLDEN=1 to (re)write testdata/golden.json — only for a deliberate encVersion bump")
	}

	vectors := goldenVectors{FormatVersion: encVersion}

	derivCases := []struct {
		key, salt        []byte
		level            int
		minTXID, maxTXID ltx.TXID
	}{
		{repeatByte(0x01, encKeySize), repeatByte(0x02, encSaltSize), 0, 1, 1},
		{repeatByte(0x03, encKeySize), repeatByte(0x04, encSaltSize), 7, 100, 500},
		{repeatByte(0x05, encKeySize), repeatByte(0x06, encSaltSize), 0x01020304, 0x0102030405060708, 0x1112131415161718},
		{repeatByte(0xAA, encKeySize), repeatByte(0x02, encSaltSize), 0, 1, 1},
	}
	for _, c := range derivCases {
		sub, err := encDeriveSubkey(c.key, c.salt, c.level, c.minTXID, c.maxTXID)
		if err != nil {
			t.Fatalf("encDeriveSubkey: %v", err)
		}
		vectors.Derivation = append(vectors.Derivation, goldenDerivation{
			Key: hex.EncodeToString(c.key), Salt: hex.EncodeToString(c.salt),
			Level: c.level, MinTXID: uint64(c.minTXID), MaxTXID: uint64(c.maxTXID),
			Subkey: hex.EncodeToString(sub),
		})
	}

	hdrCases := []struct {
		frameCode uint8
		salt      []byte
	}{
		{10, repeatByte(0x11, encSaltSize)},
		{encFrameSizeCode, repeatByte(0x22, encSaltSize)},
		{30, repeatByte(0x33, encSaltSize)},
	}
	for _, c := range hdrCases {
		hdr := newEncHeaderWithSalt(c.frameCode, c.salt)
		vectors.Header = append(vectors.Header, goldenHeader{
			FrameCode: c.frameCode, Salt: hex.EncodeToString(c.salt),
			Header: hex.EncodeToString(hdr.raw),
		})
	}

	objKey := repeatByte(0x07, encKeySize)
	objSalt := repeatByte(0x08, encSaltSize)
	const objFrameCode = 10
	const objFrameSize = 1 << objFrameCode
	const objLevel = 3
	const objMinTXID, objMaxTXID = ltx.TXID(42), ltx.TXID(99)
	for _, n := range []int{0, 1, objFrameSize - 1, objFrameSize, objFrameSize + 1, 2*objFrameSize + 7} {
		pt := patternBytes(n)
		ct := sealGoldenObject(t, objKey, objSalt, objLevel, objMinTXID, objMaxTXID, objFrameCode, pt)
		vectors.Object = append(vectors.Object, goldenObject{
			Key: hex.EncodeToString(objKey), Level: objLevel,
			MinTXID: uint64(objMinTXID), MaxTXID: uint64(objMaxTXID),
			Salt: hex.EncodeToString(objSalt), FrameCode: objFrameCode,
			Plaintext: hex.EncodeToString(pt), Ciphertext: hex.EncodeToString(ct),
		})
	}

	b, err := json.MarshalIndent(vectors, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(goldenPath, append(b, '\n'), 0o644); err != nil {
		t.Fatalf("write %s: %v", goldenPath, err)
	}
}

// TestEncryptGoldenVectors recomputes each committed vector against the current code
// and asserts the bytes are unchanged. A failure here means the on-disk encrypted
// format changed: see the package comment in encrypt.go.
func TestEncryptGoldenVectors(t *testing.T) {
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read %s: %v", goldenPath, err)
	}
	var vectors goldenVectors
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatalf("unmarshal %s: %v", goldenPath, err)
	}
	if vectors.FormatVersion != encVersion {
		t.Fatalf("%s is format_version %d but this build writes %d — a version bump is a "+
			"deliberate format change: regenerate with S3LITE_GENERATE_GOLDEN=1, never to make "+
			"a stray test pass", goldenPath, vectors.FormatVersion, encVersion)
	}

	for i, c := range vectors.Derivation {
		key, salt := mustHex(t, c.Key), mustHex(t, c.Salt)
		sub, err := encDeriveSubkey(key, salt, c.Level, ltx.TXID(c.MinTXID), ltx.TXID(c.MaxTXID))
		if err != nil {
			t.Fatalf("derivation[%d]: encDeriveSubkey: %v", i, err)
		}
		if got := hex.EncodeToString(sub); got != c.Subkey {
			t.Errorf("derivation[%d]: subkey changed:\n got  %s\n want %s", i, got, c.Subkey)
		}
	}

	for i, c := range vectors.Header {
		salt := mustHex(t, c.Salt)
		hdr := newEncHeaderWithSalt(c.FrameCode, salt)
		if got := hex.EncodeToString(hdr.raw); got != c.Header {
			t.Errorf("header[%d]: header bytes changed:\n got  %s\n want %s", i, got, c.Header)
		}
	}

	for i, c := range vectors.Object {
		key, salt, pt := mustHex(t, c.Key), mustHex(t, c.Salt), mustHex(t, c.Plaintext)
		ct := sealGoldenObject(t, key, salt, c.Level, ltx.TXID(c.MinTXID), ltx.TXID(c.MaxTXID), c.FrameCode, pt)
		if got := hex.EncodeToString(ct); got != c.Ciphertext {
			t.Errorf("object[%d]: ciphertext changed:\n got  %s\n want %s", i, got, c.Ciphertext)
		}
	}
}
