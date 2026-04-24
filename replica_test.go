package s3lite

import (
	"strings"
	"testing"

	lss3 "github.com/benbjohnson/litestream/s3"
)

func TestNewReplicaClientS3(t *testing.T) {
	client, err := newReplicaClient("s3://my-bucket/some/path")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	sc, ok := client.(*lss3.ReplicaClient)
	if !ok {
		t.Fatalf("expected *s3.ReplicaClient, got %T", client)
	}
	if sc.Bucket != "my-bucket" {
		t.Errorf("expected bucket my-bucket, got %q", sc.Bucket)
	}
	if sc.Path != "some/path" {
		t.Errorf("expected path some/path, got %q", sc.Path)
	}
}

func TestNewReplicaClientUnknownScheme(t *testing.T) {
	_, err := newReplicaClient("ftp://host/path")
	if err == nil {
		t.Fatal("expected error for unknown scheme")
	}
	if !strings.Contains(err.Error(), "file") || !strings.Contains(err.Error(), "s3") {
		t.Errorf("error should mention supported schemes, got: %v", err)
	}
}
