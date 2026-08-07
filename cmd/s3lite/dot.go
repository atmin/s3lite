package main

import (
	"context"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const helpText = `s3lite shell — familiar, not sqlite3-compatible. It implements the dot-commands
people actually type and nothing beyond them; a script that needs the real shell
should keep using sqlite3 against the local file.

.tables ?PATTERN?    list tables and views, optionally matching a LIKE pattern
.schema ?NAME?       show the CREATE statements, optionally for one object
.mode ?MODE?         output mode: list (default), table, csv, json, line
.headers ?on|off?    column names in list and csv output (table, json and line always name them)
.dump                dump the database as SQL
.import FILE TABLE   insert the rows of a CSV file into TABLE
.help                this text
.quit                exit through a clean, durable Close — so do Ctrl-D and Ctrl-C at an idle prompt

Freshness. At an interactive prompt every statement is preceded by a pull from
the replica, so what you read is what the replica held when you pressed Enter.
Piped input is logically one read of one version: it pulls once, before the
first statement, and never again.

Transactions. The pull is suppressed between BEGIN and COMMIT/ROLLBACK, because
publishing new state swaps the database file underneath the session. A
transaction therefore sees one consistent snapshot throughout, and the pull
resumes on the first statement after it ends.

Writing. --role=writer and --role=auto take the write lease on the first
statement and hand it back after --idle-yield of silence at the prompt; the next
statement takes it back. --role=follower never takes it, and rejects writes.
`

// dot runs a dot-command. It returns errQuit to end the session; any other error
// is reported like a statement error (and stops a piped script).
func (r *repl) dot(ctx context.Context, line string) error {
	fields := strings.Fields(line)
	args := fields[1:]
	switch fields[0] {
	case ".quit", ".exit":
		return errQuit
	case ".help":
		_, err := io.WriteString(r.out, helpText)
		return err
	case ".mode":
		if len(args) == 0 {
			fmt.Fprintln(r.out, r.mode)
			return nil
		}
		if !validMode(args[0]) {
			return fmt.Errorf("unknown mode %q (valid: %s)", args[0], strings.Join(renderModes, ", "))
		}
		r.mode = args[0]
		return nil
	case ".headers":
		if len(args) == 0 {
			fmt.Fprintln(r.out, onOff(r.headers))
			return nil
		}
		switch args[0] {
		case "on":
			r.headers = true
		case "off":
			r.headers = false
		default:
			return fmt.Errorf(`unknown .headers argument %q (valid: on, off)`, args[0])
		}
		return nil
	case ".tables":
		r.beforeStatement(ctx)
		return r.dotTables(ctx, args)
	case ".schema":
		r.beforeStatement(ctx)
		return r.dotSchema(ctx, args)
	case ".dump":
		r.beforeStatement(ctx)
		return r.dotDump(ctx)
	case ".import":
		if len(args) != 2 {
			return fmt.Errorf(".import needs a file and a table: .import FILE TABLE")
		}
		if r.txDepth > 0 {
			return fmt.Errorf(".import runs in its own transaction; COMMIT or ROLLBACK first")
		}
		r.beforeStatement(ctx)
		return r.dotImport(ctx, args[0], args[1])
	default:
		return fmt.Errorf(`unknown command %q — enter ".help" for the command list`, fields[0])
	}
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// userObjects excludes SQLite's own bookkeeping and litestream's alike: the lock
// and sequence tables belong to the replication engine, and a shell that listed
// them — or offered to replay them out of a .dump — would be handing the user
// someone else's state.
const userObjects = `name NOT LIKE 'sqlite\_%' ESCAPE '\' AND name NOT LIKE '\_litestream\_%' ESCAPE '\'`

func (r *repl) dotTables(ctx context.Context, args []string) error {
	q := `SELECT name FROM sqlite_schema WHERE type IN ('table','view') AND ` + userObjects
	var params []any
	if len(args) > 0 {
		q += ` AND name LIKE ?`
		params = append(params, args[0])
	}
	rows, err := r.db.QueryContext(ctx, q+` ORDER BY name`, params...)
	if err != nil {
		return r.sqlError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		fmt.Fprintln(r.out, name)
	}
	return rows.Err()
}

func (r *repl) dotSchema(ctx context.Context, args []string) error {
	var name string
	if len(args) > 0 {
		name = args[0]
	}
	objs, err := r.schemaObjects(ctx, name)
	if err != nil {
		return err
	}
	for _, o := range objs {
		fmt.Fprintf(r.out, "%s;\n", o.sql)
	}
	return nil
}

// schemaObject is one row of sqlite_schema: what .schema prints and what .dump
// walks.
type schemaObject struct {
	typ  string
	name string
	sql  string
}

// schemaObjects reads the user objects out of sqlite_schema, in creation order,
// optionally narrowed to one table (with its indexes and triggers).
func (r *repl) schemaObjects(ctx context.Context, name string) ([]schemaObject, error) {
	q := `SELECT type, name, sql FROM sqlite_schema WHERE sql IS NOT NULL AND ` + userObjects
	var params []any
	if name != "" {
		q += ` AND tbl_name LIKE ?`
		params = append(params, name)
	}
	rows, err := r.db.QueryContext(ctx, q, params...)
	if err != nil {
		return nil, r.sqlError(err)
	}
	defer rows.Close()
	var objs []schemaObject
	for rows.Next() {
		var o schemaObject
		if err := rows.Scan(&o.typ, &o.name, &o.sql); err != nil {
			return nil, err
		}
		objs = append(objs, o)
	}
	return objs, rows.Err()
}

func (r *repl) dotDump(ctx context.Context) error {
	objs, err := r.schemaObjects(ctx, "")
	if err != nil {
		return err
	}
	fmt.Fprintln(r.out, "PRAGMA foreign_keys=OFF;")
	fmt.Fprintln(r.out, "BEGIN TRANSACTION;")
	// Tables (with their rows) first, so the indexes, views and triggers that
	// follow apply to a populated schema.
	for _, o := range objs {
		if o.typ != "table" {
			continue
		}
		fmt.Fprintf(r.out, "%s;\n", o.sql)
		if err := r.dumpRows(ctx, o.name); err != nil {
			return err
		}
	}
	for _, o := range objs {
		if o.typ == "table" {
			continue
		}
		fmt.Fprintf(r.out, "%s;\n", o.sql)
	}
	fmt.Fprintln(r.out, "COMMIT;")
	return nil
}

func (r *repl) dumpRows(ctx context.Context, table string) error {
	rows, err := r.db.QueryContext(ctx, `SELECT * FROM `+quoteIdent(table))
	if err != nil {
		return r.sqlError(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	literals := make([]string, len(cols))
	if err := eachRow(rows, len(cols), func(vals []any) error {
		for i, v := range vals {
			literals[i] = sqlLiteral(v)
		}
		_, err := fmt.Fprintf(r.out, "INSERT INTO %s VALUES(%s);\n", quoteIdent(table), strings.Join(literals, ","))
		return err
	}); err != nil {
		return err
	}
	return rows.Err()
}

func (r *repl) dotImport(ctx context.Context, path, table string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if len(records) == 0 {
		fmt.Fprintf(r.errw, "s3lite: %s is empty, nothing imported\n", path)
		return nil
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(records[0])), ",")
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return r.sqlError(err)
	}
	defer tx.Rollback() // a no-op once committed; the guard is for the error paths below
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES(%s)", quoteIdent(table), placeholders))
	if err != nil {
		return r.sqlError(err)
	}
	defer stmt.Close()
	for i, rec := range records {
		args := make([]any, len(rec))
		for j, v := range rec {
			args[j] = v
		}
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("%s line %d: %w", path, i+1, r.sqlError(err))
		}
	}
	if err := tx.Commit(); err != nil {
		return r.sqlError(err)
	}
	fmt.Fprintf(r.errw, "s3lite: imported %d rows into %s\n", len(records), table)
	return nil
}

// sqlLiteral renders a scanned value as the SQL literal .dump replays.
func sqlLiteral(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case bool:
		if t {
			return "1"
		}
		return "0"
	case []byte:
		return "X'" + hex.EncodeToString(t) + "'"
	default:
		return "'" + strings.ReplaceAll(displayValue(v), "'", "''") + "'"
	}
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
