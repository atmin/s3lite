package s3lite

import (
	"strings"
	"testing"

	lss3 "github.com/benbjohnson/litestream/s3"
)

func TestNewReplicaClientS3(t *testing.T) {
	client, err := newReplicaClient(S3Config{Region: "us-east-1"}, "s3://my-bucket/some/path")
	if err != nil {
		t.Fatal(err)
	}
	sc, ok := client.(*lss3.ReplicaClient)
	if !ok {
		t.Fatalf("expected *s3.ReplicaClient, got %T", client)
	}
	if sc.Bucket != "my-bucket" {
		t.Errorf("bucket: got %q, want my-bucket", sc.Bucket)
	}
	if sc.Path != "some/path" {
		t.Errorf("path: got %q, want some/path", sc.Path)
	}
	if sc.Region != "us-east-1" {
		t.Errorf("region: got %q, want us-east-1", sc.Region)
	}
	if sc.Endpoint != "" {
		t.Errorf("endpoint should be empty, got %q", sc.Endpoint)
	}
	if sc.ForcePathStyle {
		t.Error("ForcePathStyle should be false when Endpoint is empty")
	}
}

func TestNewReplicaClientS3CustomEndpoint(t *testing.T) {
	client, err := newReplicaClient(S3Config{
		Region:          "us-east-1",
		Endpoint:        "http://localhost:9000",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
	}, "s3://test/smokedb")
	if err != nil {
		t.Fatal(err)
	}
	sc := client.(*lss3.ReplicaClient)
	if sc.Endpoint != "http://localhost:9000" {
		t.Errorf("endpoint: got %q", sc.Endpoint)
	}
	if !sc.ForcePathStyle {
		t.Error("ForcePathStyle should be true when Endpoint is set")
	}
	if sc.AccessKeyID != "minioadmin" || sc.SecretAccessKey != "minioadmin" {
		t.Error("credentials not propagated to client")
	}
}

func TestNewReplicaClientS3RequiresBucket(t *testing.T) {
	_, err := newReplicaClient(S3Config{}, "s3:///just/a/path")
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
}

func TestNewReplicaClientUnknownScheme(t *testing.T) {
	_, err := newReplicaClient(S3Config{}, "ftp://host/path")
	if err == nil {
		t.Fatal("expected error for unknown scheme")
	}
	if !strings.Contains(err.Error(), "file") || !strings.Contains(err.Error(), "s3") {
		t.Errorf("error should mention supported schemes, got: %v", err)
	}
}
