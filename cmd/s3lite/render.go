package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

// renderModes are the output modes .mode and --mode accept.
var renderModes = []string{"list", "table", "csv", "json", "line"}

func validMode(mode string) bool {
	for _, m := range renderModes {
		if m == mode {
			return true
		}
	}
	return false
}

// renderRows writes a result set in the current mode. A statement with no result
// set (every DML statement) arrives here with zero columns and prints nothing.
//
// list and csv follow the headers setting, as sqlite3's do; table, json and line
// always name their columns, because without the names those shapes are unreadable.
func renderRows(w io.Writer, rows *sql.Rows, mode string, headers bool) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	switch mode {
	case "table":
		return renderTable(w, rows, cols)
	case "csv":
		return renderCSV(w, rows, cols, headers)
	case "json":
		return renderJSON(w, rows, cols)
	case "line":
		return renderLine(w, rows, cols)
	default:
		return renderList(w, rows, cols, headers)
	}
}

// eachRow scans every row into a reused value slice and hands it to fn.
func eachRow(rows *sql.Rows, n int, fn func(vals []any) error) error {
	vals := make([]any, n)
	ptrs := make([]any, n)
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		if err := fn(vals); err != nil {
			return err
		}
	}
	return nil
}

func renderList(w io.Writer, rows *sql.Rows, cols []string, headers bool) error {
	cells := make([]string, len(cols))
	first := true
	return eachRow(rows, len(cols), func(vals []any) error {
		// The header goes out with the first row, so an empty result set prints
		// nothing at all rather than a lone header.
		if first && headers {
			fmt.Fprintln(w, strings.Join(cols, "|"))
		}
		first = false
		for i, v := range vals {
			cells[i] = displayValue(v)
		}
		_, err := fmt.Fprintln(w, strings.Join(cells, "|"))
		return err
	})
}

func renderCSV(w io.Writer, rows *sql.Rows, cols []string, headers bool) error {
	cw := csv.NewWriter(w)
	cells := make([]string, len(cols))
	first := true
	if err := eachRow(rows, len(cols), func(vals []any) error {
		if first && headers {
			if err := cw.Write(cols); err != nil {
				return err
			}
		}
		first = false
		for i, v := range vals {
			cells[i] = displayValue(v)
		}
		return cw.Write(cells)
	}); err != nil {
		return err
	}
	cw.Flush()
	return cw.Error()
}

// renderTable buffers the result set: box widths cannot be known before the last
// row. The other modes stream.
func renderTable(w io.Writer, rows *sql.Rows, cols []string) error {
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = utf8.RuneCountInString(c)
	}
	var buffered [][]string
	if err := eachRow(rows, len(cols), func(vals []any) error {
		row := make([]string, len(cols))
		for i, v := range vals {
			row[i] = displayValue(v)
			if n := utf8.RuneCountInString(row[i]); n > widths[i] {
				widths[i] = n
			}
		}
		buffered = append(buffered, row)
		return nil
	}); err != nil {
		return err
	}
	if len(buffered) == 0 {
		return nil
	}
	rule := tableRule(widths)
	fmt.Fprintln(w, rule)
	fmt.Fprintln(w, tableRow(cols, widths))
	fmt.Fprintln(w, rule)
	for _, row := range buffered {
		fmt.Fprintln(w, tableRow(row, widths))
	}
	fmt.Fprintln(w, rule)
	return nil
}

func tableRule(widths []int) string {
	var b strings.Builder
	b.WriteByte('+')
	for _, n := range widths {
		b.WriteString(strings.Repeat("-", n+2))
		b.WriteByte('+')
	}
	return b.String()
}

func tableRow(cells []string, widths []int) string {
	var b strings.Builder
	b.WriteByte('|')
	for i, c := range cells {
		b.WriteByte(' ')
		b.WriteString(c)
		b.WriteString(strings.Repeat(" ", widths[i]-utf8.RuneCountInString(c)))
		b.WriteString(" |")
	}
	return b.String()
}

func renderJSON(w io.Writer, rows *sql.Rows, cols []string) error {
	first := true
	if err := eachRow(rows, len(cols), func(vals []any) error {
		var b strings.Builder
		if first {
			b.WriteByte('[')
			first = false
		} else {
			b.WriteString(",\n")
		}
		b.WriteByte('{')
		for i, v := range vals {
			if i > 0 {
				b.WriteByte(',')
			}
			name, err := json.Marshal(cols[i])
			if err != nil {
				return err
			}
			value, err := jsonValue(v)
			if err != nil {
				return err
			}
			b.Write(name)
			b.WriteByte(':')
			b.Write(value)
		}
		b.WriteByte('}')
		_, err := io.WriteString(w, b.String())
		return err
	}); err != nil {
		return err
	}
	if first {
		return nil // no rows, no document
	}
	_, err := io.WriteString(w, "]\n")
	return err
}

func renderLine(w io.Writer, rows *sql.Rows, cols []string) error {
	width := 0
	for _, c := range cols {
		if n := utf8.RuneCountInString(c); n > width {
			width = n
		}
	}
	first := true
	return eachRow(rows, len(cols), func(vals []any) error {
		if !first {
			fmt.Fprintln(w)
		}
		first = false
		for i, v := range vals {
			pad := strings.Repeat(" ", width-utf8.RuneCountInString(cols[i]))
			if _, err := fmt.Fprintf(w, "%s%s = %s\n", pad, cols[i], displayValue(v)); err != nil {
				return err
			}
		}
		return nil
	})
}

// displayValue renders one column value for the text modes. NULL is the empty
// string, as it is in sqlite3's list and table output.
func displayValue(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	case time.Time:
		return t.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(v)
	}
}

// jsonValue keeps SQLite's types in JSON's: NULL is null, integers and reals are
// numbers, text and blobs are strings.
func jsonValue(v any) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return json.Marshal(string(b))
	}
	return json.Marshal(v)
}
