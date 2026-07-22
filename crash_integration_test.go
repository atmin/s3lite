//go:build integration

package s3lite_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/atmin/s3lite"
)

// TestCrashReacquireResumedTenureSurvivesRestoreS3 is the full-fidelity repro of
// tasks/crash-reacquire-rewind-repro.md: a leased writer over real S3 semantics
// (MinIO) is SIGKILLed with a dirty WAL and a lingering lease; the same machine
// restarts it against the same LocalPath, where it retries Open until the TTL
// expires and the direct acquire succeeds (INVARIANTS.md #9's Open-direct
// re-entry); the successor tenure commits, syncs, and closes cleanly; a fresh
// node's restore must then see both tenures in full. Everything the in-process
// TestOpenDirectCrashSelfSuccessionResumesTail fakes is real here: the SIGKILL
// (the WAL stays un-checkpointed), the lease CAS, and the S3 replica.
func TestCrashReacquireResumedTenureSurvivesRestoreS3(t *testing.T) {
	if testing.Short() {
		t.Skip("crash harness re-execs the test binary; skipped under -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	env := startMinIO(ctx, t, "reacquire")

	const ttl = 3 * time.Second
	root := t.TempDir()
	out := runCrashReacquireScenario(t,
		filepath.Join(root, "node.sqlite3"), "s3://reacquire/db",
		env.cfg, leaseChildEnv(ttl, "node", env.cfg))

	// The guard's decision must be the generation-proved self-succession resume —
	// the kill left the lock object in place, so the successor's acquire is exactly
	// one generation past the recorded tenure. A restore here would silently drop
	// the crashed tenure's unshipped tail (the data assertions would catch it only
	// when the kill left one).
	if !strings.Contains(out, "open resuming in place (self-succession)") {
		t.Fatalf("successor did not resume via self-succession; child output:\n%s", out)
	}
}

// leaseChildEnv carries the leased-writer settings into a re-exec'd crash child:
// RoleWriter with the given TTL and owner, against the MinIO endpoint under test.
func leaseChildEnv(ttl time.Duration, owner string, cfg s3lite.S3Config) []string {
	return []string{
		crashEnvRole + "=writer",
		crashEnvTTL + "=" + ttl.String(),
		crashEnvOwner + "=" + owner,
		crashEnvS3Endpoint + "=" + cfg.Endpoint,
		crashEnvS3Region + "=" + cfg.Region,
		crashEnvS3Key + "=" + cfg.AccessKeyID,
		crashEnvS3Secret + "=" + cfg.SecretAccessKey,
	}
}
