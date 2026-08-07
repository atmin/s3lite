package main

import (
	"strings"
	"testing"
)

// The rendering goldens run against the same fixture as the dot-commands: two
// rows, one of them with a NULL, so every mode has to say what it does with one.
const selectRows = "SELECT id, email FROM t ORDER BY id;\n"

func TestRenderList(t *testing.T) {
	golden(t, selectRows, "1|a@b.c\n2|\n")
}

func TestRenderListWithHeaders(t *testing.T) {
	golden(t, ".headers on\n"+selectRows, "id|email\n1|a@b.c\n2|\n")
}

func TestRenderTable(t *testing.T) {
	golden(t, ".mode table\n"+selectRows, `+----+-------+
| id | email |
+----+-------+
| 1  | a@b.c |
| 2  |       |
+----+-------+
`)
}

func TestRenderCSV(t *testing.T) {
	golden(t, ".mode csv\n.headers on\n"+selectRows, "id,email\n1,a@b.c\n2,\n")
}

// TestRenderJSON keeps SQLite's types in JSON's — a NULL is null, an integer is a
// number — which is the whole reason to pipe a shell into jq.
func TestRenderJSON(t *testing.T) {
	golden(t, ".mode json\n"+selectRows, "[{\"id\":1,\"email\":\"a@b.c\"},\n{\"id\":2,\"email\":null}]\n")
}

func TestRenderLine(t *testing.T) {
	golden(t, ".mode line\n"+selectRows, "   id = 1\nemail = a@b.c\n\n   id = 2\nemail = \n")
}

// TestRenderNoRows: an empty result set prints nothing, in every mode — including
// the two that would otherwise emit a lone header or an empty document.
func TestRenderNoRows(t *testing.T) {
	for _, mode := range renderModes {
		t.Run(mode, func(t *testing.T) {
			golden(t, ".mode "+mode+"\n.headers on\nSELECT id FROM t WHERE 0;\n", "")
		})
	}
}

// TestRenderStatementWithoutResultSet: DML comes back with zero columns and must
// print nothing, while an INSERT … RETURNING on the same path still prints rows.
func TestRenderStatementWithoutResultSet(t *testing.T) {
	golden(t, "INSERT INTO t VALUES (3, 'c@d.e');\n", "")
	golden(t, "INSERT INTO t VALUES (3, 'c@d.e') RETURNING id;\n", "3\n")
}

// TestRenderWidthIsCountedInRunes guards the box drawing against multi-byte text.
func TestRenderWidthIsCountedInRunes(t *testing.T) {
	out, _, err := runShell(t, withSchema(t), ".mode table\nSELECT 'ünïcodé' AS c;\n", nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected a five-line box, got:\n%s", out)
	}
	if lines[0] != lines[4] || len([]rune(lines[3])) != len([]rune(lines[0])) {
		t.Fatalf("the box does not line up:\n%s", out)
	}
}
