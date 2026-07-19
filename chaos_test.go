package s3lite

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// chaosSlot holds the live instance occupying one of the K logical slots. A slot's
// instance is replaced when the driver cleanly closes and reopens it; gen bumps on
// each replacement so a slot's reader resets its monotonic baseline.
type chaosSlot struct {
	mu   sync.Mutex
	db   *DB
	gen  int
	name string
}

func (s *chaosSlot) current() (*DB, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db, s.gen
}

// TestChaosSingleWriterDurability is the Jepsen-shaped soak: K RoleAuto instances
// over one lock and one file:// replica, hit with a seeded stream of writes, clean
// close+reopens, lock steals (simulated expiry), and TryPromote storms. It pins the
// library's core promise under adversity:
//
//  1. after each settle, at most one live instance reports IsLeader();
//  2. no instance's read of durable (acked+synced) rows ever regresses;
//  3. every acked+synced row survives to a fresh restore, which passes
//     integrity_check;
//  4. the whole run is -race clean.
//
// Writes go only to a settled, stable leader, so a recorded row is genuinely on the
// replica (Sync is a no-op on a demoted instance, so racing a write against a steal
// could otherwise record a row that never synced). Rows a steal drops mid-flight are
// simply never recorded — "allowed lost". The seed is fixed for reproducibility and
// printed on every failure.
func TestChaosSingleWriterDurability(t *testing.T) {
	const (
		seed  = 1
		k     = 4
		ttl   = 120 * time.Millisecond
		batch = 4
	)
	settle := 5 * ttl
	rounds := 4
	if !testing.Short() {
		rounds = 10
	}

	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaURL := "file://" + t.TempDir()
	rng := rand.New(rand.NewSource(seed))
	var nextID atomic.Int64

	// model: the ids that were acked AND confirmed synced — the rows that must
	// survive. Guarded: the driver adds, readers intersect against it.
	var modelMu sync.Mutex
	model := map[int64]bool{}
	addModel := func(ids []int64) {
		modelMu.Lock()
		defer modelMu.Unlock()
		for _, id := range ids {
			model[id] = true
		}
	}
	countDurable := func(ids []int64) int {
		modelMu.Lock()
		defer modelMu.Unlock()
		c := 0
		for _, id := range ids {
			if model[id] {
				c++
			}
		}
		return c
	}

	refreshFor := func(i int) time.Duration {
		if i%2 == 0 { // give half the instances near-live follower refresh
			return 40 * time.Millisecond
		}
		return 0
	}
	openInstance := func(name string, refresh time.Duration) *DB {
		db, err := Open(ctx, Config{
			LocalPath:               filepath.Join(t.TempDir(), name+".sqlite3"),
			BackupTo:                replicaURL,
			Role:                    RoleAuto,
			Owner:                   name,
			LeaseTTL:                ttl,
			FollowerRefreshInterval: refresh,
			Migrations:              []string{itemsSchema},
		})
		if err != nil {
			t.Fatalf("chaos open %s (seed %d): %v", name, seed, err)
		}
		return db
	}

	slots := make([]*chaosSlot, k)
	for i := range slots {
		name := fmt.Sprintf("inst-%d-a", i)
		slots[i] = &chaosSlot{db: openInstance(name, refreshFor(i)), name: name}
	}

	// Continuous readers: one per slot. The count of durable rows a slot serves must
	// never go backwards — durable rows live on the replica and are present in every
	// live instance's local file, so demote/promote/refresh swaps can only add them.
	// A read that errors at a swap boundary is tolerated; only complete reads compare.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range slots {
		wg.Add(1)
		go func(s *chaosSlot) {
			defer wg.Done()
			var lastDurable, lastGen int
			for {
				select {
				case <-stop:
					return
				default:
				}
				db, gen := s.current()
				if gen != lastGen {
					lastGen, lastDurable = gen, 0 // fresh instance in this slot
				}
				rows, err := db.QueryContext(ctx, `SELECT id FROM items`)
				if err != nil {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				var ids []int64
				for rows.Next() {
					var id int64
					if rows.Scan(&id) == nil {
						ids = append(ids, id)
					}
				}
				errRows := rows.Err()
				rows.Close()
				if errRows != nil {
					time.Sleep(2 * time.Millisecond)
					continue // partial read at a swap boundary — do not compare
				}
				if durable := countDurable(ids); durable < lastDurable {
					t.Errorf("slot %s: durable rows regressed %d -> %d (seed %d)", s.name, lastDurable, durable, seed)
					return
				} else {
					lastDurable = durable
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(slots[i])
	}

	leaderIndex := func() int {
		for i, s := range slots {
			if db, _ := s.current(); db.IsLeader() {
				return i
			}
		}
		return -1
	}

	for r := 0; r < rounds; r++ {
		time.Sleep(settle) // let the previous round's churn propagate

		// Invariant 1: at most one leader once the system has settled.
		leaders := 0
		for _, s := range slots {
			if db, _ := s.current(); db.IsLeader() {
				leaders++
			}
		}
		if leaders > 1 {
			t.Fatalf("round %d: %d instances report IsLeader after settle (seed %d)", r, leaders, seed)
		}

		// Write phase: ensure a stable leader, write a batch, Sync, record durable ids.
		li := leaderIndex()
		for attempts := 0; li < 0 && attempts < k; attempts++ {
			db, _ := slots[rng.Intn(k)].current()
			_, _ = db.TryPromote(ctx)
			li = leaderIndex()
		}
		if li >= 0 {
			db, _ := slots[li].current()
			ids := make([]int64, 0, batch)
			ok := true
			for b := 0; b < batch; b++ {
				id := nextID.Add(1)
				if _, err := db.ExecContext(ctx, `INSERT INTO items (id, name) VALUES (?, ?)`, id, "v"); err != nil {
					ok = false
					break
				}
				ids = append(ids, id)
			}
			if ok && db.Sync(ctx) == nil { // synced → genuinely durable
				addModel(ids)
			}
		}

		// Churn phase: one random disruptive op.
		switch rng.Intn(3) {
		case 0: // simulated expiry: another owner steals the lock
			lock.steal("chaos-thief", ttl)
		case 1: // TryPromote storm on random instances
			for j := 0; j < 3; j++ {
				db, _ := slots[rng.Intn(k)].current()
				_, _ = db.TryPromote(ctx)
			}
		case 2: // clean close + reopen a random slot as a fresh instance
			idx := rng.Intn(k)
			s := slots[idx]
			name := fmt.Sprintf("inst-%d-r%d", idx, r)
			nd := openInstance(name, refreshFor(idx))
			s.mu.Lock()
			old := s.db
			s.db, s.gen, s.name = nd, s.gen+1, name
			s.mu.Unlock()
			if err := old.Close(); err != nil {
				t.Logf("round %d: close slot %d: %v (seed %d)", r, idx, err, seed)
			}
		}
	}

	// Stop readers, then close every live instance cleanly.
	close(stop)
	wg.Wait()
	for _, s := range slots {
		db, _ := s.current()
		if err := db.Close(); err != nil {
			t.Logf("final close %s: %v (seed %d)", s.name, err, seed)
		}
	}

	modelMu.Lock()
	durableCount := len(model)
	modelMu.Unlock()
	if durableCount == 0 {
		t.Fatalf("chaos recorded no durable writes — churn starved the writer (seed %d)", seed)
	}

	// A fresh instance restored from the replica must be intact and hold every
	// durable row.
	final, err := Open(ctx, Config{
		LocalPath:   filepath.Join(t.TempDir(), "final.sqlite3"),
		RestoreFrom: replicaURL,
	})
	if err != nil {
		t.Fatalf("final restore (seed %d): %v", seed, err)
	}
	defer final.Close()

	var ig string
	if err := final.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&ig); err != nil {
		t.Fatalf("integrity_check query (seed %d): %v", seed, err)
	}
	if ig != "ok" {
		t.Fatalf("restored replica failed integrity_check: %q (seed %d)", ig, seed)
	}

	present := map[int64]bool{}
	rows, err := final.QueryContext(ctx, `SELECT id FROM items`)
	if err != nil {
		t.Fatalf("read restored rows (seed %d): %v", seed, err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan restored row (seed %d): %v", seed, err)
		}
		present[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate restored rows (seed %d): %v", seed, err)
	}
	rows.Close()

	modelMu.Lock()
	defer modelMu.Unlock()
	for id := range model {
		if !present[id] {
			t.Fatalf("durable row %d missing from the restored replica (seed %d)", id, seed)
		}
	}
	t.Logf("chaos: %d durable rows all survived to the restore (seed %d, %d rounds)", durableCount, seed, rounds)
}
