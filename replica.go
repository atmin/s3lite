package s3lite

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	lss3 "github.com/benbjohnson/litestream/s3"
	"github.com/superfly/ltx"
)

func newReplicaClient(s3Cfg S3Config, rawURL string) (litestream.ReplicaClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("s3lite: invalid replica URL: %w", err)
	}
	switch u.Scheme {
	case "file":
		return litestream.NewReplicaClientFromURL(rawURL)
	case "s3":
		return newS3ReplicaClient(s3Cfg, u)
	default:
		return nil, fmt.Errorf("s3lite: unsupported replica scheme %q (supported: file, s3)", u.Scheme)
	}
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
	if fc, ok := client.(*file.ReplicaClient); ok {
		fc.Replica = replica
	}
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
func replicaLatestTXID(ctx context.Context, s3Cfg S3Config, rawURL string) (ltx.TXID, error) {
	client, err := newReplicaClient(s3Cfg, rawURL)
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

func restoreDB(ctx context.Context, s3Cfg S3Config, rawURL, destPath string) error {
	client, err := newReplicaClient(s3Cfg, rawURL)
	if err != nil {
		return err
	}
	replica := litestream.NewReplicaWithClient(nil, client)
	opt := litestream.NewRestoreOptions()
	opt.OutputPath = destPath
	if err := replica.Restore(ctx, opt); err != nil {
		if isEmptyReplica(err) {
			return nil
		}
		return fmt.Errorf("s3lite: restore: %w", err)
	}
	return nil
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
func advanceFollowFile(ctx context.Context, s3Cfg S3Config, rawURL, followPath string, target ltx.TXID) (ltx.TXID, error) {
	client, err := newReplicaClient(s3Cfg, rawURL)
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
		return reached, fmt.Errorf("s3lite: follow: %w", err)
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
