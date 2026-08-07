package s3lite

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/benbjohnson/litestream"
	lss3 "github.com/benbjohnson/litestream/s3"
	"github.com/superfly/ltx"
)

func TestNewReplicaClientS3(t *testing.T) {
	client, err := newReplicaClient(replicaConfig{S3: S3Config{Region: "us-east-1"}}, "s3://my-bucket/some/path")
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
	client, err := newReplicaClient(replicaConfig{S3: S3Config{
		Region:          "us-east-1",
		Endpoint:        "http://localhost:9000",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
	}}, "s3://test/smokedb")
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
	_, err := newReplicaClient(replicaConfig{}, "s3:///just/a/path")
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
}

func TestNewReplicaClientUnknownScheme(t *testing.T) {
	_, err := newReplicaClient(replicaConfig{}, "ftp://host/path")
	if err == nil {
		t.Fatal("expected error for unknown scheme")
	}
	if !strings.Contains(err.Error(), "file") || !strings.Contains(err.Error(), "s3") {
		t.Errorf("error should mention supported schemes, got: %v", err)
	}
}

// probeReplicaTXID runs the replica-position probe against a replica URL, building a
// one-off client the way a caller outside a *DB has to.
func probeReplicaTXID(ctx context.Context, rawURL string) (ltx.TXID, error) {
	client, err := newReplicaClient(replicaConfig{}, rawURL)
	if err != nil {
		return 0, err
	}
	if err := client.Init(ctx); err != nil {
		return 0, err
	}
	return replicaLatestTXID(ctx, client)
}

// levelClient is a replica client holding one LTX file per configured level and
// counting the listings each level receives. It is how the probe's coverage and its
// cost are asserted together without a backend: the levels a real replica cannot
// produce (a snapshot ahead of level 0, a level 0 pruned out from under level 1) are
// set directly, and every listing is tallied.
type levelClient struct {
	litestream.ReplicaClient // unimplemented methods panic if the probe ever grows one
	mu                       sync.Mutex
	files                    map[int][]*ltx.FileInfo
	lists                    map[int]int
	inits                    int
}

func newLevelClient(maxima map[int]ltx.TXID) *levelClient {
	c := &levelClient{files: map[int][]*ltx.FileInfo{}, lists: map[int]int{}}
	for level, maxTXID := range maxima {
		c.files[level] = []*ltx.FileInfo{{Level: level, MinTXID: 1, MaxTXID: maxTXID}}
	}
	return c
}

func (c *levelClient) Init(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inits++
	return nil
}

func (c *levelClient) LTXFiles(_ context.Context, level int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lists[level]++
	return ltx.NewFileInfoSliceIterator(c.files[level]), nil
}

// listings reports the total number of level listings and the count for one level.
func (c *levelClient) listings() (total int, perLevel map[int]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	perLevel = map[int]int{}
	for level, n := range c.lists {
		perLevel[level] = n
		total += n
	}
	return total, perLevel
}

// probeCases pin both halves of the probe's contract at once — the TXID it reports
// (it must equal what a walk of every level would report) and what that cost.
var probeCases = []struct {
	name      string
	levels    map[int]ltx.TXID
	want      ltx.TXID
	wantLists int
}{
	{"empty replica", nil, 0, 10},
	{"level 0 only", map[int]ltx.TXID{0: 7}, 7, 2},
	{"level 0 dominates a compacted level 1", map[int]ltx.TXID{0: 9, 1: 4}, 9, 2},
	{"level 0 dominates every compacted level", map[int]ltx.TXID{0: 40, 1: 30, 2: 20, 9: 20}, 40, 2},
	// A snapshot is stamped from the writer's local position, so it can outrun the
	// level-0 objects on the replica; a writer that dies in that window leaves it as
	// the only record of the tail. This is why the snapshot level is always read.
	{"snapshot ahead of level 0", map[int]ltx.TXID{0: 9, 9: 12}, 12, 2},
	// Level 0 emptied under data above it: litestream's own retention never does this
	// (it keeps the newest file in a level), but an external bucket lifecycle policy
	// could — so the walk survives as the fallback and the transaction is still seen.
	{"compacted out of an emptied level 0", map[int]ltx.TXID{1: 5}, 5, 10},
	{"only a snapshot survives", map[int]ltx.TXID{9: 30}, 30, 10},
}

func TestReplicaLatestTXIDSeesEveryLevel(t *testing.T) {
	// The cheap probe must report exactly what a walk of all ten levels would: a
	// transaction that has been compacted upward out of level 0 — the case the walk
	// existed for — is still seen.
	for _, tc := range probeCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := replicaLatestTXID(context.Background(), newLevelClient(tc.levels))
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("latest txid: got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestReplicaLatestTXIDListingBudget(t *testing.T) {
	// The probe runs once per follower tick and once per statement for a consumer
	// forcing freshness, so its cost is part of its contract: two listings whenever
	// level 0 holds anything, and the full walk only when it does not.
	for _, tc := range probeCases {
		t.Run(tc.name, func(t *testing.T) {
			client := newLevelClient(tc.levels)
			if _, err := replicaLatestTXID(context.Background(), client); err != nil {
				t.Fatal(err)
			}
			total, perLevel := client.listings()
			if total != tc.wantLists {
				t.Fatalf("listings: got %d, want %d (per level: %v)", total, tc.wantLists, perLevel)
			}
			for level, n := range perLevel {
				if n != 1 {
					t.Fatalf("level %d listed %d times; each level must be listed at most once", level, n)
				}
			}
			if _, ok := perLevel[litestream.SnapshotLevel]; !ok {
				t.Fatal("the snapshot level must always be read")
			}
		})
	}
}

func TestProbeClientIsReusedAcrossProbes(t *testing.T) {
	// The probe used to build and Init a client per call — for S3 a region lookup,
	// credential resolution and a fresh HTTP transport each time. One client is built
	// per instance and every later probe rides it.
	ctx := context.Background()
	db := &DB{cfg: Config{BackupTo: "file://" + t.TempDir()}}

	first, err := db.probeClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.probeClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("probeClient built a second client instead of reusing the first")
	}

	// Swap in a counting client to prove the probe path itself never constructs one.
	counter := newLevelClient(map[int]ltx.TXID{0: 3})
	db.probeMu.Lock()
	db.probe = counter
	db.probeMu.Unlock()
	for i := 0; i < 3; i++ {
		if _, err := db.latestReplicaTXID(ctx); err != nil {
			t.Fatal(err)
		}
	}
	total, _ := counter.listings()
	if total != 6 {
		t.Fatalf("three probes listed %d times, want 6 (2 each on the shared client)", total)
	}
	if counter.inits != 0 {
		t.Fatalf("an already-built client must not be re-Init'd; got %d", counter.inits)
	}
}

func TestProbeClientInitFailureIsNotCached(t *testing.T) {
	// A transient Init failure (credentials, region lookup) must not poison the
	// instance: nothing is cached, so the next probe tries again.
	db := &DB{cfg: Config{BackupTo: "s3://bucket/path"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.probeClient(ctx); err == nil {
		t.Skip("Init succeeded without a backend; nothing to assert")
	}
	db.probeMu.Lock()
	cached := db.probe
	db.probeMu.Unlock()
	if cached != nil {
		t.Fatal("a client whose Init failed must not be cached")
	}
}
