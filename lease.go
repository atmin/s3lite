package s3lite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/benbjohnson/litestream"
	lss3 "github.com/benbjohnson/litestream/s3"
	"github.com/superfly/ltx"
)

// newLeaserFunc builds the leaser for a leased role. It is a package var so
// tests can inject a fake leaser without a conditional-write S3 backend; in
// production it is always newLeaser.
var newLeaserFunc = newLeaser

// errNonS3Backup reports that BackupTo is not an s3:// replica and so cannot be
// leased (leasing needs the object store's atomic conditional write). Open treats
// it as "no lease possible": RoleAuto degrades to the sole writer, while
// RoleWriter/RoleFollower — which demand a lease — surface it.
var errNonS3Backup = errors.New("s3lite: leasing requires an s3:// BackupTo")

// newLeaser builds an s3.Leaser whose lock object lives at "<BackupTo path>/lock.json"
// on the same bucket as the replica, using the same S3 settings as replication.
func newLeaser(ctx context.Context, s3cfg S3Config, backupURL string, ttl time.Duration, owner string, logger *slog.Logger) (litestream.Leaser, error) {
	u, err := url.Parse(backupURL)
	if err != nil {
		return nil, fmt.Errorf("s3lite: invalid BackupTo URL: %w", err)
	}
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("%w (got %q)", errNonS3Backup, backupURL)
	}
	bucket := u.Host
	if bucket == "" {
		return nil, fmt.Errorf("s3lite: s3 BackupTo requires a bucket (got %q)", backupURL)
	}

	client, err := newS3APIClient(ctx, s3cfg)
	if err != nil {
		return nil, err
	}

	leaser := lss3.NewLeaser()
	leaser.Bucket = bucket
	leaser.Path = strings.TrimPrefix(u.Path, "/")
	leaser.TTL = ttl
	if owner != "" {
		leaser.Owner = owner
	}
	leaser.SetClient(client)
	leaser.SetLogger(logger)
	return leaser, nil
}

// newS3APIClient builds an S3 client for the leaser from S3Config, mirroring
// litestream's own endpoint handling so it works against the same S3-compatible
// providers (custom endpoint, path-style, checksum-when-required for MinIO/Scaleway).
func newS3APIClient(ctx context.Context, s3cfg S3Config) (*awss3.Client, error) {
	region := s3cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if s3cfg.AccessKeyID != "" && s3cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(s3cfg.AccessKeyID, s3cfg.SecretAccessKey, ""),
		))
	}
	awscfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("s3lite: load aws config: %w", err)
	}
	return awss3.NewFromConfig(awscfg, func(o *awss3.Options) {
		if s3cfg.Endpoint == "" {
			return
		}
		ep := s3cfg.Endpoint
		if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
			ep = "https://" + ep
		}
		o.BaseEndpoint = aws.String(ep)
		o.UsePathStyle = true
		if strings.HasPrefix(ep, "http://") {
			o.EndpointOptions.DisableHTTPS = true
		}
		// S3-compatible providers (MinIO, Scaleway, etc.) reject the aws-chunked
		// content encoding used by default checksum calculation in aws-sdk-go-v2.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	}), nil
}

// becomeLeaderLocked starts replication and opens a writable handle for a freshly
// acquired lease. On failure it leaves replication stopped so the caller can
// release the lease. The caller must hold db.mu (or be in single-threaded Open).
func (db *DB) becomeLeaderLocked(ctx context.Context, lease *litestream.Lease) error {
	if err := db.startReplicationLocked(ctx); err != nil {
		return err
	}
	// Record the generation this tenure's local writes happen under, so a later
	// self-succession promote (crash + restart on this machine) can prove no other
	// writer took over — see promoteNeedsRestore. Best-effort: without it, a future
	// promote simply falls back to a conservative restore.
	if err := writeLeaseGen(db.leaseGenPath(), lease.Generation); err != nil {
		db.logger.Warn("s3lite: recording lease generation failed; a later self-succession will restore instead of resuming", "error", err)
	}
	// Becoming leader invalidates any clean-shutdown marker: from now until our next
	// clean Close the local file is being written, so a stale marker must never later be
	// mistaken for "cleanly closed and unchanged." A failed removal is still safe — a
	// later restart's replica will have advanced past the stale marker, forcing a restore.
	if err := os.Remove(db.cleanShutdownPath()); err != nil && !os.IsNotExist(err) {
		db.logger.Warn("s3lite: clearing clean-shutdown marker failed", "error", err)
	}
	if err := db.openWriterLocked(ctx); err != nil {
		db.stopReplicationLocked(canceledContext())
		return err
	}
	db.lease = lease
	db.isLeader = true
	return nil
}

// stopReplicationLocked closes the litestream store and clears the replication
// fields. The caller must hold db.mu. Pass a cancelled context to stop without a
// final network sync (used on demotion, where we must cease writes immediately).
func (db *DB) stopReplicationLocked(ctx context.Context) error {
	var err error
	if db.store != nil {
		err = db.store.Close(ctx)
	}
	db.store = nil
	db.lsDB = nil
	return err
}

// startLeaseLoop launches the background renew/promotion goroutine. Called once
// at the end of Open for leased roles.
func (db *DB) startLeaseLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	db.loopCancel = cancel
	db.wg.Add(1)
	go db.leaseLoop(ctx)
}

// leaseLoop is the single goroutine that drives lease state. As leader it renews
// at TTL/3 and demotes on any renew failure (fencing); as follower it polls to
// acquire the lease and promotes on success, and — when FollowerRefreshInterval is
// set — refreshes its read-only snapshot on that separate cadence. Keeping renew,
// promote, and refresh in one goroutine means they never run concurrently within an
// instance; promoteMu additionally guards refresh/promote against TryPromote
// callers. It exits when ctx is cancelled.
func (db *DB) leaseLoop(ctx context.Context) {
	defer db.wg.Done()

	interval := db.leaseTTL / 3
	if interval <= 0 {
		interval = time.Second
	}
	leaseTicker := time.NewTicker(interval)
	defer leaseTicker.Stop()

	// Optional follower-refresh ticker on its own cadence. When refresh is disabled
	// refreshC stays nil, so its select case blocks forever and never fires.
	var refreshC <-chan time.Time
	if db.cfg.FollowerRefreshInterval > 0 {
		refreshTicker := time.NewTicker(db.cfg.FollowerRefreshInterval)
		defer refreshTicker.Stop()
		refreshC = refreshTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-leaseTicker.C:
			db.mu.Lock()
			leader := db.isLeader
			db.mu.Unlock()

			if leader {
				if err := db.tryRenew(ctx); err != nil {
					db.demote(err)
				}
			} else {
				db.followerTick(ctx)
			}
		case <-refreshC:
			if err := db.refreshFollowerOnce(ctx); err != nil && ctx.Err() == nil {
				db.logger.Warn("s3lite: follower refresh failed", "error", err)
			}
		}
	}
}

// tryRenew renews the held lease, bounded by a deadline derived from that lease so
// the renew can never outlive the lease it is protecting. It returns an error when
// the lease is lost OR cannot be confirmed before expiry — both demote the loop and
// stop replication before a successor could acquire, which is exactly what fencing
// requires. A renew interrupted by shutdown (the parent ctx is cancelled) returns
// nil so we do not tear down replication out from under Close.
func (db *DB) tryRenew(ctx context.Context) error {
	db.mu.Lock()
	lease := db.lease
	db.mu.Unlock()
	if lease == nil {
		return litestream.ErrLeaseNotHeld
	}

	// Bound the renew by the lease we currently hold: a hung renew (S3 requests have
	// no overall response timeout) would otherwise stall this single goroutine past
	// ExpiresAt while litestream keeps pushing from its own goroutines, and a
	// successor acquiring at expiry would overlap us. The margin is proportional
	// (leaseTTL/6) so short-TTL tests keep headroom; demote itself is local and fast.
	renewCtx, cancel := context.WithDeadline(ctx, lease.ExpiresAt.Add(-db.leaseTTL/6))
	defer cancel()
	newLease, err := db.leaser.RenewLease(renewCtx, lease)
	if err != nil {
		// Distinguish the two cancellation sources by testing the PARENT ctx, not
		// renewCtx: a parent cancellation is Close shutting us down (return nil, let
		// Close own teardown), whereas a renewCtx deadline with a live parent means
		// the renewal could not be confirmed before expiry and MUST demote. This is
		// the trap — getting it backwards would either demote on every clean Close or
		// never fence a black-holed renew.
		if ctx.Err() != nil {
			return nil // parent cancelled: shutting down; let Close handle teardown
		}
		return err // lease lost, or renew not confirmed before expiry → caller demotes
	}

	db.mu.Lock()
	db.lease = newLease
	db.mu.Unlock()
	return nil
}

// demote steps down from leader: it stops replicating immediately (without a
// final network sync, to guarantee fencing) and fires OnDemote. The loop then
// continues as a follower and may re-acquire the lease later.
func (db *DB) demote(cause error) {
	db.mu.Lock()
	if !db.isLeader {
		db.mu.Unlock()
		return
	}
	db.isLeader = false
	db.lease = nil
	onDemote := db.onDemote
	// Stop with a cancelled context so store.Close does not attempt a final push:
	// a lost lease may already belong to another writer, and pushing now risks a
	// two-writer conflict. Any writes since the last periodic sync are dropped.
	db.stopReplicationLocked(canceledContext())
	db.mu.Unlock()

	// Fence the handle read-only so the demoted instance cannot keep writing to a
	// lease it no longer holds. The stable *sql.DB is untouched; in-flight
	// connections are rejected on next use and re-dialed query_only.
	db.connector.setMode(true)

	// Reset promotion backoff: a fresh demotion should attempt re-promotion promptly,
	// not inherit skip ticks from an earlier failure streak. demote runs on the loop
	// goroutine, so this is loop-confined like the fields it clears.
	db.promoteBackoff = 0
	db.promoteSkip = 0

	db.logger.Warn("s3lite: lease lost, stopped replicating", "error", cause)
	if onDemote != nil {
		onDemote(cause)
	}
}

// followerTick is the background leaseLoop's follower-branch promotion attempt. It
// routes through tryPromoteOnce (the shared, guarded path) and preserves the loop's
// "log and move on" behaviour: a lease still held elsewhere is a silent no-op, and
// errors during shutdown (cancelled ctx) are not logged.
//
// It applies exponential backoff on consecutive real failures (an acquire error, or
// a promote that acquired the lease but could not restore/migrate) so a follower that
// cannot promote — e.g. a broken migration — does not acquire and restore on every
// single tick. A lease merely held elsewhere is a normal no-op and resets the
// backoff. Loop-confined: only leaseLoop calls this.
func (db *DB) followerTick(ctx context.Context) {
	if db.promoteSkip > 0 {
		db.promoteSkip-- // backing off after a recent failure
		return
	}
	promoted, err := db.tryPromoteOnce(ctx)
	switch {
	case promoted:
		db.promoteBackoff = 0
	case err != nil:
		if ctx.Err() == nil {
			db.logger.Warn("s3lite: promotion attempt failed", "error", err)
		}
		db.promoteBackoff++
		db.promoteSkip = promotionSkipTicks(db.promoteBackoff)
	default:
		db.promoteBackoff = 0 // lease held elsewhere — normal, clear any backoff
	}
}

// promotionSkipTicks returns how many loop ticks to skip before the next background
// promotion attempt after `failures` consecutive failures: 1, 2, then capped at 3
// (~one TTL, since the loop ticks at TTL/3). On-demand TryPromote is never throttled.
func promotionSkipTicks(failures int) int {
	const maxSkip = 3
	if failures >= maxSkip {
		return maxSkip
	}
	return 1 << (failures - 1) // failures 1 → 1, 2 → 2
}

// tryPromoteOnce is the single guarded promotion path shared by the background
// leaseLoop and the public TryPromote. It holds promoteMu across the whole
// acquire+restore so concurrent callers cannot both restore and both reach
// becomeLeaderLocked. It returns true when this instance is (or has just become)
// the writer, and false with a nil error when the lease is still held elsewhere.
func (db *DB) tryPromoteOnce(ctx context.Context) (bool, error) {
	if db.IsLeader() { // fast path: no S3 I/O, no lock contention
		return true, nil
	}
	db.promoteMu.Lock()
	defer db.promoteMu.Unlock()
	if db.IsLeader() { // recheck: the loop (or another caller) may have promoted us
		return true, nil
	}
	if db.isClosing() { // do not resurrect an instance that is shutting down
		return false, ErrClosed
	}
	lease, err := db.leaser.AcquireLease(ctx)
	if err != nil {
		var held *litestream.LeaseExistsError
		if errors.As(err, &held) {
			// Still held by a live writer elsewhere (or by us, if the loop just
			// promoted — covered by the recheck above). Normal no-op.
			return db.IsLeader(), nil
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, err
	}
	if err := db.promote(ctx, lease); err != nil {
		db.releaseQuietly(ctx, lease)
		return false, err
	}
	return true, nil
}

// isClosing reports whether Close has begun tearing the instance down.
func (db *DB) isClosing() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.closed
}

// seedRefreshPos records the replica position a follower restored to at Open, so
// the first refresh tick is a no-op unless the replica has since advanced. Called
// once during Open (before the loop starts) when follower refresh is enabled;
// best-effort — a probe failure just means the first tick does one redundant
// restore.
func (db *DB) seedRefreshPos(ctx context.Context) {
	if db.cfg.FollowerRefreshInterval <= 0 || db.leaser == nil {
		return
	}
	pos, err := replicaLatestTXIDFunc(ctx, db.cfg.S3, db.cfg.BackupTo)
	if err != nil {
		db.logger.Warn("s3lite: seeding follower refresh position failed; first refresh may be redundant", "error", err)
		return
	}
	db.lastRefreshPos = pos
}

// refreshFollowerOnce brings a follower up to the replica's latest committed state
// and swaps it in read-only, so the follower serves near-live reads. It is a no-op on
// the writer and when the replica has not advanced since the last refresh. It shares
// promoteMu with promotion so a refresh and a promote can never both rebuild the local
// file at once. Called only from leaseLoop.
//
// The refresh is incremental: it advances a private follow file by applying only the
// LTX committed since the follower's position (litestream's own Restore(Follow)
// resume, driven by advanceFollowFileFunc), then publishes a consistent copy of it
// into the live read path. The advance runs outside the connector gate — readers keep
// serving the current snapshot during the S3 fetch+apply — and only the fast local
// copy+rename runs under the gate. Promote and Open remain full rebuilds by design.
func (db *DB) refreshFollowerOnce(ctx context.Context) error {
	if db.IsLeader() { // writers own the latest state; never self-refresh
		return nil
	}
	db.promoteMu.Lock()
	defer db.promoteMu.Unlock()
	if db.IsLeader() { // recheck: a concurrent promote may have won
		return nil
	}

	pos, err := replicaLatestTXIDFunc(ctx, db.cfg.S3, db.cfg.BackupTo)
	if err != nil {
		return err
	}
	if pos <= db.lastRefreshPos {
		return nil // replica unchanged since our last refresh — no advance, no swap
	}

	// Advance the private follow file (no SQLite reader opens it) to the replica's
	// latest by applying only the delta. A failure here leaves the live read file
	// untouched — the follower keeps serving its current state — so we just return.
	reached, err := advanceFollowFileFunc(ctx, db.cfg.S3, db.cfg.BackupTo, db.followPath(), pos)
	if err != nil {
		return err
	}

	swapErr := db.connector.swapFiles(true, func() error { // stays read-only
		return db.publishFollowFile(ctx, db.followPath())
	})
	if swapErr != nil {
		// The publish is atomic: on failure the live files are untouched, so the
		// follower keeps serving its current state — the swap only bumped the
		// generation, so in-flight connections re-dial against that same state.
		return swapErr
	}
	db.lastRefreshPos = reached

	db.mu.Lock()
	onRefresh := db.onRefresh
	db.mu.Unlock()
	db.logger.Info("s3lite: follower refreshed", "txid", reached)
	if onRefresh != nil {
		onRefresh()
	}
	return nil
}

// promote turns a follower into the writer: it restores the replica's latest
// state (the previous writer's final sync) and starts replicating. The stable
// *sql.DB is not replaced — the connector re-dials the restored file writable
// and drops the superseded read-only connections — so callers keep their handle.
func (db *DB) promote(ctx context.Context, lease *litestream.Lease) error {
	db.mu.Lock()
	if db.isLeader {
		db.mu.Unlock()
		return nil
	}
	db.mu.Unlock()

	// Decide whether to restore or promote in place. Provable self-succession (this
	// machine's crashed writer coming back with no other instance having acquired the
	// lease since) keeps its local committed tail; anything else restores. See
	// promoteNeedsRestore.
	needRestore := db.promoteNeedsRestore(lease)

	// Gate connection creation across the (possible) rebuild, so no query observes a
	// half-restored database, then flip the handle writable. The stable *sql.DB is
	// never replaced: superseded follower connections are dropped from the pool once
	// the generation advances, and any handle a caller cached stays valid.
	swapErr := db.connector.swapFiles(false, func() error {
		if needRestore {
			return db.rebuildLocalFromReplica(ctx)
		}
		// Promote in place: keep the local bytes untouched. becomeLeaderLocked starts
		// litestream on the existing file, which ships the unshipped tail exactly like
		// the unleased crash-restart path.
		return nil
	})
	if swapErr != nil {
		// The rebuild is atomic, so a failed restore left the local files intact:
		// we genuinely remain a read-only follower still serving our current
		// snapshot. Re-assert read-only — the swap set the mode writable in its
		// deferred flip even though fn failed.
		db.connector.setMode(true)
		return swapErr
	}

	db.mu.Lock()
	if db.closed {
		// Close ran while we were restoring; do not bring replication back up.
		db.connector.setMode(true)
		db.mu.Unlock()
		return ErrClosed
	}
	if err := db.becomeLeaderLocked(ctx, lease); err != nil {
		db.connector.setMode(true) // remain a read-only follower
		db.mu.Unlock()
		return err
	}
	onPromote := db.onPromote
	generation := lease.Generation
	db.mu.Unlock()

	db.logger.Info("s3lite: promoted to writer", "generation", generation)
	if onPromote != nil {
		onPromote()
	}
	return nil
}

// rebuildLocalFromReplica atomically replaces the local database with the
// replica's latest committed state. It restores into a sibling temp path and only
// swaps that into place once the restore has fully succeeded, so a restore that
// fails partway (a network drop, an unreachable replica) leaves the existing local
// files untouched — the caller keeps serving its current state. Both the follower
// refresh and promote use it, so the two paths cannot drift.
//
// It runs inside a connector swap (connection creation is gated), so no query ever
// observes a half-built database.
func (db *DB) rebuildLocalFromReplica(ctx context.Context) error {
	path := db.cfg.LocalPath
	tmp := path + ".restoring"

	// A leftover temp from a crashed prior rebuild must not be mistaken for a fresh
	// restore below, so clear it (and its sidecars) first.
	if err := removeLocalDBFiles(tmp); err != nil {
		return fmt.Errorf("s3lite: rebuild: clear stale temp: %w", err)
	}
	if err := restoreDBFunc(ctx, db.cfg.S3, db.cfg.BackupTo, tmp); err != nil {
		// Restore failed without touching the live files: keep serving current state.
		return fmt.Errorf("s3lite: rebuild: restore: %w", err)
	}

	// Empty-replica case: restoreDBFunc returns nil without creating tmp when the
	// replica has no data yet (a fresh bucket). Fall back to a clean local reset so
	// a first promote against an empty replica yields a writable empty DB.
	if _, err := os.Stat(tmp); os.IsNotExist(err) {
		if err := removeLocalDBFiles(path); err != nil {
			return fmt.Errorf("s3lite: rebuild: clear local files: %w", err)
		}
		return precreateWAL(ctx, path)
	} else if err != nil {
		return fmt.Errorf("s3lite: rebuild: stat temp: %w", err)
	}

	// Restore populated tmp. Only now tear down the old files and rename the fresh
	// copy into place. A crash between the remove and the rename leaves no local
	// file, which the next Open restores from the replica — already safe.
	if err := removeLocalDBFiles(path); err != nil {
		return fmt.Errorf("s3lite: rebuild: clear local files: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("s3lite: rebuild: rename temp into place: %w", err)
	}
	return precreateWAL(ctx, path)
}

// followPath is the private follow file the incremental refresh keeps current. No
// SQLite reader ever opens it; refreshFollowerOnce publishes copies of it into the
// live read path. It sits beside LocalPath so it shares the filesystem (the publish
// rename must be same-FS atomic) and is cleaned up by removeLocalDBFiles / Close.
func (db *DB) followPath() string { return db.cfg.LocalPath + ".follow" }

// publishFollowFile atomically swaps a snapshot of the (already quiesced) private
// follow file into the live read path. It mirrors rebuildLocalFromReplica's swap tail
// exactly — restore-into-temp becomes copy-into-temp — so the two paths cannot drift,
// and runs inside the connector gate (via refreshFollowerOnce) so no query observes a
// half-copied database. The follow file itself is left in place as the incremental
// base for the next tick.
func (db *DB) publishFollowFile(ctx context.Context, followPath string) error {
	path := db.cfg.LocalPath
	tmp := path + ".restoring"

	// A leftover temp from a crashed prior publish must not be mistaken for a fresh
	// copy, so clear it (and its sidecars) first.
	if err := removeLocalDBFiles(tmp); err != nil {
		return fmt.Errorf("s3lite: publish: clear stale temp: %w", err)
	}
	if err := copyFile(followPath, tmp); err != nil {
		// Copy failed without touching the live files: keep serving current state.
		return fmt.Errorf("s3lite: publish: copy follow file: %w", err)
	}

	// Copy succeeded. Only now tear down the old files and rename the fresh copy into
	// place. A crash between the remove and the rename leaves no local file, which the
	// next Open restores from the replica — already safe.
	if err := removeLocalDBFiles(path); err != nil {
		return fmt.Errorf("s3lite: publish: clear local files: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("s3lite: publish: rename temp into place: %w", err)
	}
	return precreateWAL(ctx, path)
}

// copyFile copies src to dst (creating/truncating dst) and fsyncs dst, so the
// published snapshot is durable before it is renamed into place. The follow file is a
// standalone SQLite database — litestream's follow apply writes pages into it directly
// and keeps no separate WAL — so copying the single file yields a complete database.
func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

// releaseQuietly releases a lease we acquired but could not promote onto,
// ignoring the "already gone" cases.
func (db *DB) releaseQuietly(ctx context.Context, lease *litestream.Lease) {
	if err := db.leaser.ReleaseLease(ctx, lease); err != nil &&
		!errors.Is(err, litestream.ErrLeaseNotHeld) {
		db.logger.Warn("s3lite: release lease failed", "error", err)
	}
}

// leaseGenPath is the sidecar recording the lease generation the local file's
// committed tail was written under. A self-succeeding writer (crash + restart on the
// same machine) uses it to prove no other instance acquired the lease since — see
// promoteNeedsRestore and INVARIANTS.md.
func (db *DB) leaseGenPath() string { return db.cfg.LocalPath + ".leasegen" }

// writeLeaseGen durably records the held lease generation beside the local file. A
// torn or partial write is harmless: readLeaseGen treats an unparseable value as "no
// record", which forces a conservative restore rather than a wrong promote-in-place.
func writeLeaseGen(path string, gen int64) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "%d\n", gen); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// readLeaseGen returns the recorded lease generation, or 0 if the sidecar is absent
// or unparseable. Either way, 0 means "no proof of self-succession" → restore.
func readLeaseGen(path string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	gen, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || gen < 0 {
		return 0
	}
	return gen
}

// promoteNeedsRestore decides whether a promotion must restore the replica over the
// local file (today's default) or may promote in place and keep the local committed
// tail. It returns false only for provable self-succession: the lease we just acquired
// is exactly one generation past the one our local tail was written under, so no other
// instance acquired the lease (and could have forked the replica) in between. A
// generation gap means a successor took over — restore. A missing/unreadable sidecar
// (a fresh follower's first promotion, or a non-leased-origin file) means no proof —
// restore. See tasks/local-ahead-promote.md and INVARIANTS.md.
func (db *DB) promoteNeedsRestore(lease *litestream.Lease) bool {
	persisted := readLeaseGen(db.leaseGenPath())
	switch {
	case persisted > 0 && lease.Generation == persisted+1:
		db.logger.Info("s3lite: promoting in place (self-succession); keeping local committed tail",
			"generation", lease.Generation, "local_generation", persisted)
		return false
	case persisted > 0:
		db.logger.Warn("s3lite: promote will restore; lease generation advanced past our tenure (takeover)",
			"generation", lease.Generation, "local_generation", persisted)
		return true
	default:
		return true
	}
}

// openDirectNeedsRestore decides whether a leased writer that acquired the lease
// directly at Open (its prior lease had already expired by reopen) must restore the
// replica over its existing local file, or may resume that file in place. It closes the
// sibling gap to promoteNeedsRestore (INVARIANTS.md #9) for the Open path — where the
// lease generation alone is ambiguous, because a clean release resets it to 1 (#8) and
// so cannot tell a clean self-restart from a successor's clean handoff.
//
// Two independent signals prove a resume is safe; both err toward restore when unproven,
// since erring toward restore risks only a wasteful re-download while erring toward
// resume risks shipping a forked lineage (corruption):
//
//   - Self-succession (crashed, no takeover): the lock survived from our tenure and we
//     re-acquired it, bumping exactly one generation past the one our local tail was
//     written under. Identical to promoteNeedsRestore. Generation-only, so it recovers
//     an unshipped WAL tail without any replica read and is immune to the local file's
//     "L0 lags the WAL" skew — a successor cannot share our generation, so a match
//     proves no one wrote since.
//   - Clean restart (cleanly closed, no successor since): a clean Close leaves a marker
//     recording the replica position it finished syncing to. If the marker is present
//     and the replica has not advanced past it, no other writer wrote since our clean
//     close, so the local file still equals the replica — resume for free. This is the
//     common, hot path; without it every clean restart would re-download the database.
//
// Once a returning writer is established (a lease generation was recorded here),
// anything else — a generation gap (a successor took over), a missing/garbage marker,
// an advanced replica, or an unreadable replica — restores, discarding a possibly-forked
// local history in favour of the replica lineage.
//
// A local file with no recorded lease generation was never a leased leader on this
// machine, so it is not a returning writer that could have forked from a successor's
// takeover — it is a fresh or externally-seeded start. That resumes in place, today's
// behaviour: it keeps fresh deploys unchanged (Item 1) and, crucially, lets a first
// writer Open against a momentarily-unreachable replica instead of hanging on a restore.
// Divergence for a brought-in, non-provenance file stays out of scope (the lease, not
// this guard, is the multi-writer boundary).
func (db *DB) openDirectNeedsRestore(ctx context.Context, lease *litestream.Lease) bool {
	persisted := readLeaseGen(db.leaseGenPath())
	if persisted == 0 {
		return false
	}
	if lease.Generation == persisted+1 {
		db.logger.Info("s3lite: open resuming in place (self-succession); keeping local committed tail",
			"generation", lease.Generation, "local_generation", persisted)
		return false
	}
	if marked, ok := readCleanShutdown(db.cleanShutdownPath()); ok {
		latest, err := replicaLatestTXIDFunc(ctx, db.cfg.S3, db.cfg.BackupTo)
		switch {
		case err != nil:
			db.logger.Warn("s3lite: open will restore; clean-shutdown replica probe failed", "error", err)
		case latest == marked:
			db.logger.Info("s3lite: open resuming in place (clean restart); replica unchanged since clean close",
				"txid", uint64(marked))
			return false
		default:
			db.logger.Warn("s3lite: open will restore; replica advanced past our clean shutdown (takeover)",
				"clean_txid", uint64(marked), "replica_txid", uint64(latest))
		}
	} else {
		db.logger.Warn("s3lite: open will restore; no proof the local file matches the replica lineage",
			"generation", lease.Generation, "local_generation", persisted)
	}
	return true
}

// restoreLocalFromReplica replaces the local database with the replica's latest
// committed state for the Open-direct fork guard. Unlike rebuildLocalFromReplica it runs
// before the connector exists (during Open, prior to becomeLeaderLocked), so it restores
// straight into LocalPath as a plain pre-connector file op rather than a swapFiles
// rebuild. restoreDBFunc refuses an existing output path, so the stale local files are
// cleared first; an empty replica (a fresh bucket) leaves a clean WAL database.
func (db *DB) restoreLocalFromReplica(ctx context.Context) error {
	path := db.cfg.LocalPath
	if err := removeLocalDBFiles(path); err != nil {
		return fmt.Errorf("s3lite: open restore: clear local files: %w", err)
	}
	if err := restoreDBFunc(ctx, db.cfg.S3, db.cfg.BackupTo, path); err != nil {
		return fmt.Errorf("s3lite: open restore: %w", err)
	}
	return precreateWAL(ctx, path)
}

// cleanShutdownPath is the sidecar a leader writes on a clean Close, recording the
// replica TXID it finished syncing to. A later Open-direct acquire uses it to prove a
// clean restart (the replica has not advanced since) and resume in place instead of
// re-downloading the whole database. See openDirectNeedsRestore and INVARIANTS.md #9.
func (db *DB) cleanShutdownPath() string { return db.cfg.LocalPath + ".cleanshutdown" }

// writeCleanShutdown durably records the replica TXID a clean Close synced to. A torn or
// partial write is harmless: readCleanShutdown treats an unparseable value as absent,
// which forces a conservative restore on the next open rather than a wrong resume.
func writeCleanShutdown(path string, txid ltx.TXID) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "%d\n", uint64(txid)); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// readCleanShutdown returns the recorded clean-shutdown replica TXID and true, or
// (0, false) when the marker is absent or unparseable. Either way, false means "no proof
// of a clean restart" → restore.
func readCleanShutdown(path string) (ltx.TXID, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, false
	}
	return ltx.TXID(v), true
}

// removeLocalDBFiles deletes the SQLite file and its litestream sidecars so a
// restore can write a fresh copy. Missing files are not an error.
func removeLocalDBFiles(path string) error {
	for _, p := range []string{path, path + "-wal", path + "-shm", path + "-txid"} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// canceledContext returns an already-cancelled context, used to stop the store
// without a final network sync.
func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
