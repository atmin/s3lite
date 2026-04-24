package s3lite

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	lss3 "github.com/benbjohnson/litestream/s3"
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
