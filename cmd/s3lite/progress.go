package main

import (
	"fmt"
	"io"
	"strings"
)

// progressBar renders Config.OnRestoreProgress as a single redrawn stderr line —
// the reason that callback exists is a human staring at a cold multi-GB open.
//
// The callback runs inline with the download, once per read chunk, so tick must
// stay cheap: it redraws only when the whole percentage changes, which also makes
// its output a function of the byte counts alone rather than of timing.
type progressBar struct {
	w       io.Writer
	lastPct int
	drawn   bool
}

func newProgressBar(w io.Writer) *progressBar {
	return &progressBar{w: w, lastPct: -1}
}

func (b *progressBar) tick(applied, total int64) {
	if total <= 0 {
		return
	}
	pct := int(applied * 100 / total)
	if pct > 100 {
		pct = 100
	}
	if pct == b.lastPct {
		return
	}
	b.lastPct = pct
	b.drawn = true
	const width = 30
	filled := width * pct / 100
	fmt.Fprintf(b.w, "\rrestoring [%s%s] %3d%%  %s / %s",
		strings.Repeat("=", filled), strings.Repeat(" ", width-filled),
		pct, humanBytes(applied), humanBytes(total))
}

// done closes the line the bar has been redrawing. A restore with nothing to
// fetch never called tick, so it prints nothing at all.
func (b *progressBar) done() {
	if !b.drawn {
		return
	}
	fmt.Fprintln(b.w)
	b.drawn = false
	b.lastPct = -1
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
