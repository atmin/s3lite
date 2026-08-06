package s3lite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	lss3 "github.com/benbjohnson/litestream/s3"
	"github.com/superfly/ltx"
)

// replicaConfig is the subset of Config the replica-client constructors need: the
// S3 connection settings plus the client-side encryption options. It travels as one
// value because every replica path — replicate, restore, the follower's incremental
// advance, the latest-TXID probe — needs all of it.
type replicaConfig struct {
	S3               S3Config
	EncryptionKey    []byte
	RequireEncrypted bool
}

// replica projects the replica-facing subset out of Config.
func (cfg Config) replica() replicaConfig {
	return replicaConfig{
		S3:               cfg.S3,
		EncryptionKey:    cfg.EncryptionKey,
		RequireEncrypted: cfg.RequireEncrypted,
	}
}

// newReplicaClient is the only constructor of a replica client, which is what makes
// it the single seam for client-side encryption: replication (s3lite.go), restore,
// the follower advance and the latest-TXID probe all come through here, and so does
// remote compaction (it uses the store's client). With Config.EncryptionKey set the
// client is wrapped in the encrypting decorator; with it empty nothing is wrapped
// and the returned client is byte-for-byte the one this package always used.
func newReplicaClient(cfg replicaConfig, rawURL string) (litestream.ReplicaClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("s3lite: invalid replica URL: %w", err)
	}

	var client litestream.ReplicaClient
	switch u.Scheme {
	case "file":
		if client, err = litestream.NewReplicaClientFromURL(rawURL); err != nil {
			return nil, err
		}
	case "s3":
		sc, err := newS3ReplicaClient(cfg.S3, u)
		if err != nil {
			return nil, err
		}
		client = sc
	default:
		return nil, fmt.Errorf("s3lite: unsupported replica scheme %q (supported: file, s3)", u.Scheme)
	}

	if len(cfg.EncryptionKey) == 0 {
		return client, nil
	}
	return newEncryptedClient(client, cfg.EncryptionKey, cfg.RequireEncrypted), nil
}

func newS3ReplicaClient(s3Cfg S3Config, u *url.URL) (*lss3.ReplicaClient, error) {
	bucket := u.Host
	if bucket == "" {
		return nil, fmt.Errorf("s3lite: s3 replica URL requires a bucket (got %q)", u.String())
	}
	client := lss3.NewReplicaClient()
	client.Bucket = bucket
	client.Path = strings.TrimPrefix(u.Path, "/")
	client.Region = s3Cfg.Region
	client.Endpoint = s3Cfg.Endpoint
	client.AccessKeyID = s3Cfg.AccessKeyID
	client.SecretAccessKey = s3Cfg.SecretAccessKey
	// Custom endpoints (MinIO, Scaleway, etc.) need path-style addressing.
	client.ForcePathStyle = s3Cfg.Endpoint != ""
	return client, nil
}

// isEmptyReplica reports whether err means the replica exists but has no data yet —
// the normal state on a first deploy before any backup has run.
func isEmptyReplica(err error) bool {
	return errors.Is(err, litestream.ErrNoSnapshots) || errors.Is(err, litestream.ErrTxNotAvailable)
}

// wireReplica sets the back-reference on client types that require it.
func wireReplica(client litestream.ReplicaClient, replica *litestream.Replica) {
	if fc, ok := unwrapReplicaClient(client).(*file.ReplicaClient); ok {
		fc.Replica = replica
	}
}

// unwrapReplicaClient peels decorators (the encrypting client) off so the few
// callers that need the concrete backend type still find it. It is the alternative
// to wrapping after wiring, and it keeps newReplicaClient the single seam.
func unwrapReplicaClient(client litestream.ReplicaClient) litestream.ReplicaClient {
	for {
		u, ok := client.(interface {
			Unwrap() litestream.ReplicaClient
		})
		if !ok {
			return client
		}
		client = u.Unwrap()
	}
}

// annotateEncryptionError names an encryption cause behind an already-failed read.
//
// It exists because litestream cannot carry ours up: `ltx.Compact` (v0.5.1) reads
// each input's header under a named return and, on failure, does a bare `return` —
// so the error is dropped and the closed output pipe surfaces as
// "decode database: decode header: EOF" no matter what the input actually did. A
// forgotten or wrong key would therefore read exactly like a corrupt database, which
// is the one thing this feature must never look like.
//
// So on failure — only on failure, the happy path pays nothing — re-open the objects
// the restore plan used under the configured settings and report what they say.
func annotateEncryptionError(ctx context.Context, cfg replicaConfig, rawURL string, err error) error {
	// A cancelled context is a shutdown, not a configuration problem — and the probe
	// would fail on it anyway. Skipping it also keeps a repeatedly-failing follower
	// refresh from adding a listing round trip per tick.
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if cause := probeEncryptionCause(ctx, cfg, rawURL); cause != nil {
		// Both are wrapped: the caller needs errors.Is on the typed cause, and the
		// original failure is what actually happened.
		return fmt.Errorf("%w: %w", cause, err)
	}
	return err
}

// probeEncryptionCause returns the typed error the replica's own restore-plan objects
// produce under cfg, or nil when they read fine (so the failure was something else).
// Best-effort by construction: it is a diagnostic, so anything that goes wrong along
// the way just means "cannot tell".
func probeEncryptionCause(ctx context.Context, cfg replicaConfig, rawURL string) error {
	client, err := newReplicaClient(cfg, rawURL)
	if err != nil || client.Init(ctx) != nil {
		return nil
	}
	infos, err := litestream.CalcRestorePlan(ctx, client, 0, time.Time{}, slog.New(slog.DiscardHandler))
	if err != nil || len(infos) == 0 {
		return nil
	}
	// Only the ends of the plan, never all of it: this runs on a failure that may
	// repeat every follower-refresh tick, and sweeping the whole plan would cost a GET
	// per object each time. Encryption is a whole-replica property, so either end
	// answers it — and the two ends are exactly what the mixed window straddles, the
	// oldest object being the one most likely to predate the key.
	for _, info := range []*ltx.FileInfo{infos[0], infos[len(infos)-1]} {
		if cause := probeEncryptionCauseForObject(ctx, cfg, client, info); cause != nil {
			return cause
		}
	}
	return nil
}

// probeEncryptionCauseForObject classifies one object against the configured
// encryption settings.
func probeEncryptionCauseForObject(ctx context.Context, cfg replicaConfig, client litestream.ReplicaClient, info *ltx.FileInfo) error {
	rc, err := client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
	if err != nil {
		switch {
		case errors.Is(err, ErrKeyMismatch):
			return ErrKeyMismatch
		case errors.Is(err, ErrObjectNotEncrypted):
			return ErrObjectNotEncrypted
		}
		return nil
	}
	defer func() { _ = rc.Close() }()

	if len(cfg.EncryptionKey) > 0 {
		return nil // it opened, so its first frame authenticated under our key
	}
	// With no key nothing is wrapped, so the object arrives raw and its magic says
	// whether a key was needed.
	magic := make([]byte, len(encMagic))
	if n, _ := io.ReadFull(rc, magic); n == len(encMagic) && string(magic) == encMagic {
		return ErrReplicaEncrypted
	}
	return nil
}

// replicaLatestTXIDFunc is the "latest replica position" probe used by follower
// refresh. It is a package var so tests can inject failures/positions without a
// real backend; in production it is always replicaLatestTXID.
var replicaLatestTXIDFunc = replicaLatestTXID

// replicaLatestTXID returns the highest transaction id present on the replica
// across all levels, or 0 if the replica is empty. It is the "has anything new
// been committed since I last restored?" probe the follower refresh uses to skip
// no-op restores. It builds a throwaway client each call (like restoreDB), so it
// never shares state with a live writer's replication. It lists every level so a
// transaction that has been compacted upward (out of level 0) is still seen.
func replicaLatestTXID(ctx context.Context, cfg replicaConfig, rawURL string) (ltx.TXID, error) {
	client, err := newReplicaClient(cfg, rawURL)
	if err != nil {
		return 0, err
	}
	if err := client.Init(ctx); err != nil {
		return 0, err
	}
	replica := litestream.NewReplicaWithClient(nil, client)
	var maxTXID ltx.TXID
	for level := 0; level <= litestream.SnapshotLevel; level++ {
		info, err := replica.MaxLTXFileInfo(ctx, level)
		if err != nil {
			return 0, err
		}
		if info.MaxTXID > maxTXID {
			maxTXID = info.MaxTXID
		}
	}
	return maxTXID, nil
}

// restoreDBFunc is the restore entry point used by the follower rebuild path
// (rebuildLocalFromReplica). It is a package var so tests can inject restore
// failures without a real backend, mirroring newLeaserFunc / replicaLatestTXIDFunc;
// in production it is always restoreDB. Open's initial restore deliberately calls
// restoreDB directly, so injecting here isolates the refresh/promote rebuild.
var restoreDBFunc = restoreDB

// restoreDB replaces destPath with the replica's latest committed state, logging the
// operation on the application logger as a lifecycle event beside promote/demote.
// All three paths that pull a whole database down funnel through here — Open's initial
// cold restore, the promote rebuild, the Open-direct fork guard — so one pair of lines
// covers them all. (A follower's incremental refresh advances its private follow file
// instead and logs its own "follower refreshed".)
//
// The logging is not decoration: this is the one lifecycle step that can take seconds
// to minutes (a large database pulled to a machine that has never seen it), and Open's
// initial restore runs before the handle is returned, so an application blocking on
// Open otherwise sees a hang indistinguishable from a stall. litestream's own
// restore-plan log is Debug and s3lite gates litestream to WARN+ (logging.go), so
// nothing else reports it. onProgress (Config.OnRestoreProgress) rides the same
// seam for consumers that need the live byte count rather than the two events.
func restoreDB(ctx context.Context, cfg replicaConfig, rawURL, destPath string, logger *slog.Logger, onProgress func(applied, total int64)) error {
	client, err := newReplicaClient(cfg, rawURL)
	if err != nil {
		return err
	}
	replica := litestream.NewReplicaWithClient(nil, client)
	opt := litestream.NewRestoreOptions()
	opt.OutputPath = destPath
	progress := newRestoreProgressReporter(onProgress)
	if progress != nil {
		opt.OnProgress = progress.sample
	}

	logger.Info("s3lite: restoring from replica", "replica", rawURL, "path", destPath)
	start := time.Now()
	if err := replica.Restore(ctx, opt); err != nil {
		if isEmptyReplica(err) {
			// A fresh bucket: no output file is written and the caller carries on with a
			// clean local database, so say so rather than claim a restore completed.
			logger.Info("s3lite: replica is empty; nothing to restore", "replica", rawURL)
			return nil
		}
		return annotateEncryptionError(ctx, cfg, rawURL, fmt.Errorf("s3lite: restore: %w", err))
	}
	progress.finish()
	// Size comes from the file we just wrote rather than from litestream's restore plan:
	// the plan lives inside Restore and reporting it would mean recalculating it.
	var size int64
	if fi, statErr := os.Stat(destPath); statErr == nil {
		size = fi.Size()
	}
	logger.Info("s3lite: restore complete", "replica", rawURL, "path", destPath,
		"bytes", size, "elapsed", time.Since(start))
	return nil
}

// restoreProgressReporter turns litestream's raw progress samples into the
// contract Config.OnRestoreProgress documents: serialized, never regressing, and
// ending a successful restore at (total, total). litestream fires from whichever
// goroutine performed the read and its samples may arrive out of order, and both
// consumers of this callback — a progress bar and a stall watchdog — would
// otherwise have to absorb that separately. A nil callback yields a nil reporter,
// so nothing is installed on the restore options at all.
type restoreProgressReporter struct {
	fn      func(applied, total int64)
	mu      sync.Mutex
	applied int64
	total   int64
}

func newRestoreProgressReporter(fn func(applied, total int64)) *restoreProgressReporter {
	if fn == nil {
		return nil
	}
	return &restoreProgressReporter{fn: fn}
}

// sample forwards one litestream sample, dropping any that regresses — the sole
// evidence of a stall is the count standing still, so a sample that went
// backwards would read as progress undone rather than as no progress.
func (p *restoreProgressReporter) sample(applied, total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if applied < p.applied {
		return
	}
	p.applied, p.total = applied, total
	p.fn(applied, total)
}

// finish emits the (total, total) a completed restore ends on, so a bar always
// lands at 100% rather than a hair short of it. A restore that fetched nothing
// (an empty replica: Restore fails the plan before any sample) reported no
// progress and gets no completion sample either.
func (p *restoreProgressReporter) finish() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.total == 0 || p.applied >= p.total {
		return
	}
	p.applied = p.total
	p.fn(p.total, p.total)
}

// followCatchupInterval is both how often the managed Restore(Follow) loop polls the
// replica for new LTX and how often we re-read the follow file's TXID sidecar while a
// follower catches up. The follow loop is synchronous and applies every available new
// LTX per tick, so catch-up is typically one tick; this is a latency floor, not a
// busy-spin, and it is deliberately short because this path exists for chatty,
// short-interval followers.
const followCatchupInterval = 50 * time.Millisecond

// advanceFollowFileFunc advances the private follow file toward the replica's latest
// committed state and returns the TXID it reached. It is a package var so tests can
// inject failures / count work without a real backend, mirroring restoreDBFunc; in
// production it is always advanceFollowFile. Follower-only — the writer owns its state
// and must never call it.
var advanceFollowFileFunc = advanceFollowFile

// advanceFollowFile brings followPath — a private database file that no SQLite reader
// ever opens — up to at least target, then returns the TXID actually reached. It does
// not reimplement the LTX apply: it drives litestream's own Restore(Follow), whose
// follow loop does the level-0 apply and the gap-fill from higher compaction levels.
// That apply mutates followPath in place (bypassing SQLite locking), which is safe
// only because the file is private; refreshFollowerOnce publishes a quiesced *copy* to
// readers under the connector gate.
//
// Two non-steady cases are rebuilt from a full restore rather than resumed: a follow
// file with no usable TXID sidecar (an interrupted first restore), and a saved
// position the replica has pruned past (retention). Both are detected up front by
// followNeedsReestablish, so we never depend on litestream's non-sentinel resume-error
// text.
func advanceFollowFile(ctx context.Context, cfg replicaConfig, rawURL, followPath string, target ltx.TXID) (ltx.TXID, error) {
	client, err := newReplicaClient(cfg, rawURL)
	if err != nil {
		return 0, err
	}
	if err := client.Init(ctx); err != nil {
		return 0, err
	}
	replica := litestream.NewReplicaWithClient(nil, client)

	reestablish, err := followNeedsReestablish(ctx, replica, followPath)
	if err != nil {
		return 0, fmt.Errorf("s3lite: follow: validate saved position: %w", err)
	}
	if reestablish {
		if err := removeLocalDBFiles(followPath); err != nil {
			return 0, fmt.Errorf("s3lite: follow: clear stale follow file: %w", err)
		}
	}

	reached, err := runManagedFollow(ctx, replica, followPath, target)
	if err != nil {
		return reached, annotateEncryptionError(ctx, cfg, rawURL, fmt.Errorf("s3lite: follow: %w", err))
	}
	return reached, nil
}

// followNeedsReestablish reports whether the private follow file cannot be resumed
// incrementally and must be rebuilt from a full restore. It mirrors litestream's own
// crash-recovery resume validation so we re-establish exactly when its Restore(Follow)
// resume would reject: the file exists but has no usable TXID sidecar, or the newest
// snapshot begins after the saved position (retention has pruned the intervening
// history). A missing follow file is not a re-establish — Restore(Follow) does the
// initial full restore itself.
func followNeedsReestablish(ctx context.Context, replica *litestream.Replica, followPath string) (bool, error) {
	if _, err := os.Stat(followPath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	saved, err := litestream.ReadTXIDFile(followPath)
	if err != nil || saved == 0 {
		return true, nil // file present but no usable sidecar → rebuild
	}

	itr, err := replica.Client.LTXFiles(ctx, litestream.SnapshotLevel, 0, false)
	if err != nil {
		return false, err
	}
	defer func() { _ = itr.Close() }()
	var newestSnapshot *ltx.FileInfo
	for itr.Next() {
		newestSnapshot = itr.Item()
	}
	if err := itr.Err(); err != nil {
		return false, err
	}
	if newestSnapshot != nil && newestSnapshot.MinTXID > saved {
		return true, nil // saved position pruned by retention → rebuild
	}
	return false, nil
}

// runManagedFollow drives litestream Restore(Follow) against followPath in a
// goroutine, polls the advancing TXID sidecar until it reaches target, then cancels
// the follow and joins the goroutine at a commit boundary. On ctx cancel litestream's
// follow loop syncs, closes, and returns nil, so the join is always bounded and leaves
// followPath quiescent — there is no concurrent writer for the caller's copy to race,
// which is what makes the copy-and-swap consistent. Returns the TXID actually reached
// (>= target on success; whatever was applied so far on error or cancellation).
//
// There is no catch-up timeout: an initial full restore of a large database can take
// a while, so bounding it here would false-positive. The parent ctx (cancelled on
// shutdown) is the only bound, matching the full-restore path; the goroutine *join*
// is always bounded because cancel makes Restore return.
func runManagedFollow(ctx context.Context, replica *litestream.Replica, followPath string, target ltx.TXID) (ltx.TXID, error) {
	followCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		opt := litestream.NewRestoreOptions()
		opt.OutputPath = followPath
		opt.Follow = true
		opt.FollowInterval = followCatchupInterval
		errCh <- replica.Restore(followCtx, opt)
	}()

	ticker := time.NewTicker(followCatchupInterval)
	defer ticker.Stop()

	for {
		// The follow loop advances the sidecar only after each fully-applied LTX file,
		// so a read here always reflects a committed boundary.
		if cur, err := litestream.ReadTXIDFile(followPath); err == nil && cur >= target {
			cancel()
			<-errCh // clean shutdown returns nil; bounded by cancel
			reached, _ := litestream.ReadTXIDFile(followPath)
			return reached, nil
		}

		select {
		case err := <-errCh:
			// Restore returned before we saw target. An empty replica is nothing to do;
			// a nil return means the parent ctx ended (shutdown). Anything else is a real
			// failure the caller turns into a keep-current-state / re-establish decision.
			reached, _ := litestream.ReadTXIDFile(followPath)
			if err == nil || isEmptyReplica(err) {
				return reached, nil
			}
			return reached, err
		case <-ctx.Done():
			cancel()
			<-errCh // bounded: cancel makes Restore return
			reached, _ := litestream.ReadTXIDFile(followPath)
			return reached, ctx.Err()
		case <-ticker.C:
		}
	}
}
