package s3lite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/benbjohnson/litestream"
	lss3 "github.com/benbjohnson/litestream/s3"
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
				db.tryPromote(ctx)
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

	db.logger.Warn("s3lite: lease lost, stopped replicating", "error", cause)
	if onDemote != nil {
		onDemote(cause)
	}
}

// tryPromote is the background leaseLoop's follower-branch promotion attempt. It
// routes through tryPromoteOnce (the shared, guarded path) and preserves the loop's
// "log and move on" behaviour: a lease still held elsewhere is a silent no-op, and
// errors during shutdown (cancelled ctx) are not logged.
func (db *DB) tryPromote(ctx context.Context) {
	if _, err := db.tryPromoteOnce(ctx); err != nil && ctx.Err() == nil {
		db.logger.Warn("s3lite: promotion attempt failed", "error", err)
	}
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

// refreshFollowerOnce restores a follower to the replica's latest committed state
// and swaps it in read-only, so the follower serves near-live reads. It is a no-op
// on the writer and when the replica has not advanced since the last refresh. It
// shares promoteMu with promotion so a refresh and a promote can never both rebuild
// the local file at once. Called only from leaseLoop.
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
		return nil // replica unchanged since our last restore — no swap
	}

	swapErr := db.connector.swapFiles(true, func() error { // stays read-only
		return db.rebuildLocalFromReplica(ctx)
	})
	if swapErr != nil {
		// The rebuild is atomic: on failure the live files are untouched, so the
		// follower keeps serving its current state — the swap only bumped the
		// generation, so in-flight connections re-dial against that same state.
		return swapErr
	}
	db.lastRefreshPos = pos

	db.mu.Lock()
	onRefresh := db.onRefresh
	db.mu.Unlock()
	db.logger.Info("s3lite: follower refreshed", "txid", pos)
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

	// Rebuild the local file from the replica with connection creation gated, so
	// no query observes a half-restored database, then flip the handle writable.
	// The stable *sql.DB is never replaced: superseded follower connections are
	// dropped from the pool automatically once the generation advances, and any
	// handle a caller cached stays valid.
	swapErr := db.connector.swapFiles(false, func() error {
		return db.rebuildLocalFromReplica(ctx)
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

// releaseQuietly releases a lease we acquired but could not promote onto,
// ignoring the "already gone" cases.
func (db *DB) releaseQuietly(ctx context.Context, lease *litestream.Lease) {
	if err := db.leaser.ReleaseLease(ctx, lease); err != nil &&
		!errors.Is(err, litestream.ErrLeaseNotHeld) {
		db.logger.Warn("s3lite: release lease failed", "error", err)
	}
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
