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
// acquire the lease and promotes on success. It exits when ctx is cancelled.
func (db *DB) leaseLoop(ctx context.Context) {
	defer db.wg.Done()

	interval := db.leaseTTL / 3
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

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
	}
}

// tryRenew renews the held lease. It returns an error only when the lease is
// genuinely lost (so the loop demotes); a renew interrupted by shutdown returns
// nil so we do not tear down replication out from under Close.
func (db *DB) tryRenew(ctx context.Context) error {
	db.mu.Lock()
	lease := db.lease
	db.mu.Unlock()
	if lease == nil {
		return litestream.ErrLeaseNotHeld
	}

	newLease, err := db.leaser.RenewLease(ctx, lease)
	if err != nil {
		if ctx.Err() != nil {
			return nil // shutting down; let Close handle teardown
		}
		return err
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

	db.logger.Warn("s3lite: lease lost, stopped replicating", "error", cause)
	if onDemote != nil {
		onDemote(cause)
	}
}

// tryPromote attempts to acquire the lease while a follower. On success it
// promotes to writer; a lease still held by the leader is the normal no-op.
func (db *DB) tryPromote(ctx context.Context) {
	lease, err := db.leaser.AcquireLease(ctx)
	if err != nil {
		var held *litestream.LeaseExistsError
		if errors.As(err, &held) || ctx.Err() != nil {
			return // still held, or shutting down
		}
		db.logger.Warn("s3lite: lease acquire attempt failed", "error", err)
		return
	}
	if err := db.promote(ctx, lease); err != nil {
		db.logger.Warn("s3lite: promotion failed, releasing lease", "error", err)
		db.releaseQuietly(ctx, lease)
	}
}

// promote turns a follower into the writer: it reopens after restoring the
// replica's latest state (the previous writer's final sync) and starts
// replicating. The read-only handle is replaced, so RoleAuto/RoleFollower
// consumers should react via OnPromote rather than caching the embedded *sql.DB.
func (db *DB) promote(ctx context.Context, lease *litestream.Lease) error {
	db.mu.Lock()
	if db.isLeader {
		db.mu.Unlock()
		return nil
	}
	// Close the read-only handle before overwriting the file underneath it.
	if db.DB != nil {
		if err := db.DB.Close(); err != nil {
			db.mu.Unlock()
			return fmt.Errorf("s3lite: promote: close follower handle: %w", err)
		}
		db.DB = nil
	}
	db.mu.Unlock()

	// Remove the stale follower files so litestream can restore fresh — Restore
	// refuses to overwrite an existing output path.
	if err := removeLocalDBFiles(db.cfg.LocalPath); err != nil {
		db.reopenFollowerAfterFailedPromote()
		return fmt.Errorf("s3lite: promote: clear local files: %w", err)
	}

	// Restore the latest committed state, then reopen writable and replicate.
	if err := restoreDB(ctx, db.cfg.S3, db.cfg.BackupTo, db.cfg.LocalPath); err != nil {
		db.reopenFollowerAfterFailedPromote()
		return fmt.Errorf("s3lite: promote: restore: %w", err)
	}
	if err := precreateWAL(ctx, db.cfg.LocalPath); err != nil {
		db.reopenFollowerAfterFailedPromote()
		return err
	}

	db.mu.Lock()
	if err := db.becomeLeaderLocked(ctx, lease); err != nil {
		_ = db.openFollowerLocked(context.Background())
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

// reopenFollowerAfterFailedPromote restores the read-only handle when a promotion
// aborts midway, so the DB remains usable as a follower.
func (db *DB) reopenFollowerAfterFailedPromote() {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.DB == nil {
		// Best effort; if this fails the next promotion attempt will retry.
		_ = db.openFollowerLocked(context.Background())
	}
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
