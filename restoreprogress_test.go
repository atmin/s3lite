package s3lite

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// progressRecorder collects the samples a restore reports. The callback runs on
// whichever goroutine litestream read from, not the caller's, so it locks.
type progressRecorder struct {
	mu      sync.Mutex
	samples [][2]int64
}

func (r *progressRecorder) record(applied, total int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, [2]int64{applied, total})
}

func (r *progressRecorder) snapshot() [][2]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][2]int64(nil), r.samples...)
}

// assertProgressContract checks the guarantees Config.OnRestoreProgress documents
// for a restore that fetched something: it opens at (0, total), never regresses,
// reports one constant total, and lands exactly on it.
func assertProgressContract(t *testing.T, samples [][2]int64) {
	t.Helper()
	if len(samples) < 2 {
		t.Fatalf("got %d samples, want the plan total plus at least one read", len(samples))
	}
	total := samples[0][1]
	if total <= 0 {
		t.Fatalf("first sample reports total %d, want the plan's byte count", total)
	}
	if samples[0][0] != 0 {
		t.Fatalf("first sample = %v, want (0, %d) as soon as the plan is known", samples[0], total)
	}
	for i, s := range samples {
		if s[1] != total {
			t.Fatalf("sample %d reports total %d, want a constant %d", i, s[1], total)
		}
		if i > 0 && s[0] < samples[i-1][0] {
			t.Fatalf("sample %d regressed: %d after %d", i, s[0], samples[i-1][0])
		}
	}
	if last := samples[len(samples)-1]; last[0] != total {
		t.Fatalf("last sample = %v, want (%d, %d) — a completed restore ends at 100%%", last, total, total)
	}
}

func TestInitialOpenRestoreReportsProgress(t *testing.T) {
	// The cold restore inside Open is the one that most needs reporting: it runs
	// before the handle exists, so nothing pollable can observe it, and on a
	// multi-GB database it is the whole of the wait.
	ctx := context.Background()
	replicaURL := seedReplica(t, ctx)

	rec := &progressRecorder{}
	db, err := Open(ctx, Config{
		LocalPath:         filepath.Join(t.TempDir(), "cold.sqlite3"), // absent → Open restores
		RestoreFrom:       replicaURL,
		Logger:            discardLogger(),
		OnRestoreProgress: rec.record,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// It really restored, so the samples describe work rather than a no-op.
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("cold Open restored %d rows, want 1", count)
	}

	assertProgressContract(t, rec.snapshot())
}

func TestEmptyReplicaRestoreReportsNoProgress(t *testing.T) {
	// A first deploy restores from an empty bucket: nothing is fetched, so a
	// consumer must not see a bar appear — least of all one stuck at 0%.
	ctx := context.Background()
	rec := &progressRecorder{}
	db, err := Open(ctx, Config{
		LocalPath:         filepath.Join(t.TempDir(), "fresh.sqlite3"),
		RestoreFrom:       "file://" + t.TempDir(), // an empty replica
		Logger:            discardLogger(),
		OnRestoreProgress: rec.record,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("empty replica reported %d progress samples, want none: %v", len(got), got)
	}
}

func TestNilRestoreProgressRestoresIdentically(t *testing.T) {
	// The callback is opt-in: unset, the restore is the one this package always
	// did — litestream wraps no reader and s3lite installs no hook.
	ctx := context.Background()
	replicaURL := seedReplica(t, ctx)

	dest := filepath.Join(t.TempDir(), "restored.sqlite3")
	if err := restoreDB(ctx, replicaConfig{}, replicaURL, dest, discardLogger(), nil); err != nil {
		t.Fatal(err)
	}
	withCallback := filepath.Join(t.TempDir(), "restored.sqlite3")
	rec := &progressRecorder{}
	if err := restoreDB(ctx, replicaConfig{}, replicaURL, withCallback, discardLogger(), rec.record); err != nil {
		t.Fatal(err)
	}

	assertProgressContract(t, rec.snapshot())
	plain, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	reported, err := os.ReadFile(withCallback)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plain, reported) {
		t.Fatal("restore with a progress callback produced different bytes than without one")
	}
}

func TestTakeoverPromoteReportsRestoreProgress(t *testing.T) {
	// The other restore that blocks a consumer: a takeover promotion re-downloads
	// the whole database, so it reports on the same callback as the cold Open.
	// Mirrors TestTakeoverPromoteLogsRestore.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaURL := "file://" + t.TempDir()
	localPath := filepath.Join(t.TempDir(), "node.sqlite3")

	w1, err := Open(ctx, Config{
		LocalPath: localPath, BackupTo: replicaURL, Role: RoleWriter,
		Owner: "node", LeaseTTL: time.Minute, Migrations: []string{itemsSchema},
		Logger: discardLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('v1')`); err != nil {
		t.Fatal(err)
	}
	if err := w1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	simulateCrash(t, w1) // lease held at generation 1

	rec := &progressRecorder{}
	w2, err := Open(ctx, Config{
		LocalPath: localPath, BackupTo: replicaURL, Role: RoleAuto,
		Owner: "node", LeaseTTL: time.Minute, Logger: discardLogger(),
		OnRestoreProgress: rec.record,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	// Open resumed the local file, so nothing has been reported yet.
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("resuming Open reported %d progress samples, want none: %v", len(got), got)
	}

	lock.steal("other", time.Minute) // a successor took over (generation 2), then lapsed
	lock.expire()
	if promoted, err := w2.tryPromoteOnce(ctx); err != nil || !promoted {
		t.Fatalf("promote: promoted=%v err=%v", promoted, err)
	}

	assertProgressContract(t, rec.snapshot())
}

func TestRestoreProgressReporterAbsorbsConcurrentSamples(t *testing.T) {
	// litestream counts bytes atomically but fires from whichever goroutine did the
	// read, so two samples can arrive out of order. s3lite absorbs that once, here,
	// rather than making a progress bar and a stall watchdog each handle it.
	//
	// The concurrency is exercised directly because litestream v0.5.16 drains a
	// restore plan from a single compactor goroutine — RestoreOptions.Parallelism is
	// no longer wired into the LTX restore path — so no restore this package can
	// drive would reach the interleaving the callback contract has to survive.
	const total = int64(1 << 16)

	var (
		inCallback atomic.Bool
		last       int64 // written only inside the callback, i.e. under the reporter's lock
		n          int
	)
	p := newRestoreProgressReporter(func(applied, reported int64) {
		if !inCallback.CompareAndSwap(false, true) {
			t.Error("callback entered concurrently; calls must be serialized")
		}
		if applied < last {
			t.Errorf("applied regressed: %d after %d", applied, last)
		}
		if reported != total {
			t.Errorf("total = %d, want a constant %d", reported, total)
		}
		last, n = applied, n+1
		inCallback.Store(false)
	})

	// Emulate litestream exactly: an atomic running count, reported by the goroutine
	// that incremented it, half the plan's bytes in flight.
	var applied atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 64 {
				p.sample(applied.Add(64), total)
			}
		}()
	}
	wg.Wait()

	if n == 0 {
		t.Fatal("no samples reached the callback")
	}
	if last > applied.Load() {
		t.Fatalf("reported %d bytes applied, more than the %d counted", last, applied.Load())
	}

	// The restore succeeded, so the consumer sees 100% even though the reads stopped
	// short of the total (a plan whose last bytes arrived out of order, in practice).
	p.finish()
	if last != total {
		t.Fatalf("after finish, last sample applied = %d, want %d", last, total)
	}
	afterFinish := n
	p.finish()
	if n != afterFinish {
		t.Fatalf("finish reported %d extra samples; the completion sample fires once", n-afterFinish)
	}
}

func TestRestoreProgressReporterFinishWithoutSamples(t *testing.T) {
	// The empty-replica path: the restore fails its plan before any byte is
	// counted, so completion must stay silent rather than invent a 0-of-0 bar.
	var fired bool
	p := newRestoreProgressReporter(func(applied, total int64) { fired = true })
	p.finish()
	if fired {
		t.Fatal("a restore that fetched nothing must report no progress")
	}

	// A nil callback yields no reporter at all, and finishing it is still safe.
	newRestoreProgressReporter(nil).finish()
}
