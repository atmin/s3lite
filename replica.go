package s3lite

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	_ "github.com/benbjohnson/litestream/s3"
)

func newReplicaClient(rawURL string) (litestream.ReplicaClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("s3lite: invalid replica URL: %w", err)
	}
	switch u.Scheme {
	case "file", "s3":
		return litestream.NewReplicaClientFromURL(rawURL)
	default:
		return nil, fmt.Errorf("s3lite: unsupported replica scheme %q (supported: file, s3)", u.Scheme)
	}
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

func restoreDB(ctx context.Context, rawURL, destPath string) error {
	client, err := newReplicaClient(rawURL)
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
