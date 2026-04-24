package s3lite

import (
	"fmt"
	"net/url"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

func newReplicaClient(rawURL string) (litestream.ReplicaClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("s3lite: invalid replica URL: %w", err)
	}
	switch u.Scheme {
	case "file":
		return litestream.NewReplicaClientFromURL(rawURL)
	default:
		return nil, fmt.Errorf("s3lite: unsupported replica scheme %q (supported: file)", u.Scheme)
	}
}

// wireReplica sets the back-reference on client types that require it.
func wireReplica(client litestream.ReplicaClient, replica *litestream.Replica) {
	if fc, ok := client.(*file.ReplicaClient); ok {
		fc.Replica = replica
	}
}
