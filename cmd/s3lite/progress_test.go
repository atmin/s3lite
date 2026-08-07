package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestProgressBarEndsAtFull is the cold-open case the bar exists for: a populated
// replica draws, and the last thing it says is 100%.
func TestProgressBarEndsAtFull(t *testing.T) {
	var buf bytes.Buffer
	b := newProgressBar(&buf)
	const total = 4 << 20
	for applied := int64(0); applied <= total; applied += 64 << 10 {
		b.tick(applied, total)
	}
	b.done()

	frames := strings.Split(strings.TrimPrefix(buf.String(), "\r"), "\r")
	if len(frames) < 2 {
		t.Fatalf("the bar did not draw: %q", buf.String())
	}
	last := frames[len(frames)-1]
	if !strings.Contains(last, "100%") || !strings.Contains(last, "4.0 MiB / 4.0 MiB") {
		t.Fatalf("the bar did not finish at full: %q", last)
	}
	if !strings.HasSuffix(buf.String(), "\n") {
		t.Fatal("done did not close the line the bar was redrawing")
	}
}

// TestProgressBarRedrawsOnlyOnWholePercent: the callback runs inline with the
// download, once per read chunk, so the bar must not redraw per chunk.
func TestProgressBarRedrawsOnlyOnWholePercent(t *testing.T) {
	var buf bytes.Buffer
	b := newProgressBar(&buf)
	const total = 10000
	for applied := int64(0); applied <= total; applied++ {
		b.tick(applied, total)
	}
	if frames := strings.Count(buf.String(), "\r"); frames != 101 {
		t.Fatalf("10001 ticks drew %d frames, want 101 (one per whole percent)", frames)
	}
}

// TestProgressBarSilentWhenThereIsNothingToRestore: an empty replica never calls
// the callback, and a plan with no known total must not draw either — in both
// cases the session opens on a clean prompt.
func TestProgressBarSilentWhenThereIsNothingToRestore(t *testing.T) {
	var buf bytes.Buffer
	b := newProgressBar(&buf)
	b.done()
	b.tick(0, 0)
	b.done()
	if buf.Len() != 0 {
		t.Fatalf("the bar wrote %q with nothing to restore", buf.String())
	}
}

func TestHumanBytes(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{3 << 20, "3.0 MiB"},
		{5 << 30, "5.0 GiB"},
	} {
		if got := humanBytes(tc.n); got != tc.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}
