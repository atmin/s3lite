package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// schemaFixture is the database the dot-command goldens run against: two tables
// (one with a NULL and one with a blob), an index and a view, created in this
// order — sqlite_schema returns them in it, and .schema and .dump print them in it.
var schemaFixture = []string{
	`CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)`,
	`INSERT INTO t VALUES (1, 'a@b.c')`,
	`INSERT INTO t VALUES (2, NULL)`,
	`CREATE INDEX t_email ON t (email)`,
	`CREATE VIEW v AS SELECT id FROM t`,
	`CREATE TABLE b (v BLOB)`,
	`INSERT INTO b VALUES (x'0102')`,
	// The replication engine's own table, which lands in every replicated
	// database. No golden below mentions it: the shell shows the user's schema.
	`CREATE TABLE _litestream_lock (id INTEGER)`,
}

func withSchema(t *testing.T) *fakeDB {
	t.Helper()
	f := newFake(t, filepath.Join(t.TempDir(), "db.sqlite3"), "")
	for _, stmt := range schemaFixture {
		mustExec(t, f.DB, stmt)
	}
	return f
}

// golden runs a script through the shell and compares everything it wrote to
// stdout. Dot-command output is a user-facing contract, so it is pinned whole.
func golden(t *testing.T, script, want string) {
	t.Helper()
	out, errOut, err := runShell(t, withSchema(t), script, nil)
	if err != nil {
		t.Fatalf("run: %v\nstderr:\n%s", err, errOut)
	}
	if out != want {
		t.Fatalf("output mismatch\n--- got ---\n%s\n--- want ---\n%s", out, want)
	}
}

func TestDotTables(t *testing.T) {
	golden(t, ".tables\n", "b\nt\nv\n")
}

func TestDotTablesPattern(t *testing.T) {
	golden(t, ".tables t%\n", "t\n")
}

func TestDotSchema(t *testing.T) {
	golden(t, ".schema\n", `CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT);
CREATE INDEX t_email ON t (email);
CREATE VIEW v AS SELECT id FROM t;
CREATE TABLE b (v BLOB);
`)
}

// TestDotSchemaOneObject: naming a table narrows the output to it and the indexes
// and triggers attached to it.
func TestDotSchemaOneObject(t *testing.T) {
	golden(t, ".schema t\n", `CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT);
CREATE INDEX t_email ON t (email);
`)
}

// TestDotDump pins the replayable SQL: tables with their rows first, then the
// indexes and views that depend on them, with NULLs and blobs as SQL literals.
func TestDotDump(t *testing.T) {
	golden(t, ".dump\n", `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT);
INSERT INTO "t" VALUES(1,'a@b.c');
INSERT INTO "t" VALUES(2,NULL);
CREATE TABLE b (v BLOB);
INSERT INTO "b" VALUES(X'0102');
CREATE INDEX t_email ON t (email);
CREATE VIEW v AS SELECT id FROM t;
COMMIT;
`)
}

// TestDotDumpRoundTrips is the point of a dump: replaying it rebuilds the database.
func TestDotDumpRoundTrips(t *testing.T) {
	out, _, err := runShell(t, withSchema(t), ".dump\n", nil)
	if err != nil {
		t.Fatalf("dump: %v", err)
	}
	replayed := newFake(t, filepath.Join(t.TempDir(), "replay.sqlite3"), "")
	if _, _, err := runShell(t, replayed, out, nil); err != nil {
		t.Fatalf("replay the dump: %v", err)
	}
	again, _, err := runShell(t, replayed, ".dump\n", nil)
	if err != nil {
		t.Fatalf("dump the replay: %v", err)
	}
	if again != out {
		t.Fatalf("replaying the dump did not reproduce it\n--- first ---\n%s\n--- second ---\n%s", out, again)
	}
}

func TestDotImport(t *testing.T) {
	csvPath := filepath.Join(t.TempDir(), "rows.csv")
	if err := os.WriteFile(csvPath, []byte("3,c@d.e\n4,\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	golden(t, ".import "+csvPath+" t\nSELECT count(*) FROM t;\n", "4\n")
}

func TestDotImportRefusedInsideTransaction(t *testing.T) {
	_, errOut, err := runShell(t, withSchema(t), "BEGIN;\n.import /nonexistent t\n", nil)
	if err == nil {
		t.Fatal("an .import inside a transaction was accepted")
	}
	if !strings.Contains(errOut, "COMMIT or ROLLBACK first") {
		t.Fatalf("unhelpful refusal:\n%s", errOut)
	}
}

func TestDotModeAndHeaders(t *testing.T) {
	golden(t, ".mode\n.headers\n.mode csv\n.headers on\n.mode\n.headers\n", "list\noff\ncsv\non\n")
}

func TestDotModeRejectsUnknownMode(t *testing.T) {
	_, errOut, err := runShell(t, withSchema(t), ".mode fancy\n", nil)
	if err == nil {
		t.Fatal("an unknown mode was accepted")
	}
	if !strings.Contains(errOut, "valid: list, table, csv, json, line") {
		t.Fatalf("the error does not name the valid modes:\n%s", errOut)
	}
}

func TestUnknownDotCommand(t *testing.T) {
	_, errOut, err := runShell(t, withSchema(t), ".nope\n", nil)
	if err == nil {
		t.Fatal("an unknown dot-command was accepted")
	}
	if !strings.Contains(errOut, `.help`) {
		t.Fatalf("the error does not point at .help:\n%s", errOut)
	}
}

// TestDotHelpStatesTheContracts: .help is where the two rules that make this shell
// different from sqlite3 are written down, so they are a contract and not a
// surprise.
func TestDotHelpStatesTheContracts(t *testing.T) {
	out, _, err := runShell(t, withSchema(t), ".help\n", nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, want := range []string{
		"familiar, not sqlite3-compatible",
		"suppressed between BEGIN and COMMIT",
		"pulls once, before the",
		"--role=follower never takes it",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("`.help` does not state %q:\n%s", want, out)
		}
	}
}
