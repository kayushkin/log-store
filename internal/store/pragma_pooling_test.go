package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

// connectionsHeldOpen is deliberately larger than one. The bug this file pins
// is invisible at one connection: a pragma set by a single db.Exec lands on
// whichever connection the pool hands out, and that connection answers
// correctly forever. Only holding several open at once exposes the rest.
const connectionsHeldOpen = 8

// readPragmaFromEveryPooledConnection opens connectionsHeldOpen connections,
// holds them all open at the same time, and returns what each one answers for
// the named pragma. Holding them is load-bearing: released connections are
// reused, so a sequential loop would keep asking the same one.
func readPragmaFromEveryPooledConnection(t *testing.T, db *sql.DB, pragma string) []int {
	t.Helper()
	ctx := context.Background()
	db.SetMaxOpenConns(connectionsHeldOpen)

	// Released only after every value is collected: the connections must be
	// held simultaneously for the read to mean anything, but holding them past
	// the return would exhaust the pool for the next call on the same *sql.DB.
	connections := make([]*sql.Conn, 0, connectionsHeldOpen)
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()

	values := make([]int, 0, connectionsHeldOpen)
	for i := 0; i < connectionsHeldOpen; i++ {
		connection, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("open pooled connection %d: %v", i, err)
		}
		connections = append(connections, connection)

		var value int
		if err := connection.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&value); err != nil {
			t.Fatalf("read PRAGMA %s on connection %d: %v", pragma, i, err)
		}
		values = append(values, value)
	}
	return values
}

func TestBothPoolsCarryBusyTimeout(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "logs.db"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	// The writer is capped at one connection, so this asks the only one there
	// is rather than sweeping a pool.
	var writerBusyTimeoutMilliseconds int
	if err := s.writer.QueryRow("PRAGMA busy_timeout").Scan(&writerBusyTimeoutMilliseconds); err != nil {
		t.Fatalf("read writer busy_timeout: %v", err)
	}
	if writerBusyTimeoutMilliseconds != busyTimeoutMillisecondsWanted {
		t.Errorf("writer reports busy_timeout %d, want %d",
			writerBusyTimeoutMilliseconds, busyTimeoutMillisecondsWanted)
	}

	// The reader really is a pool, so every connection has to carry it.
	for connectionIndex, value := range readPragmaFromEveryPooledConnection(t, s.reader, "busy_timeout") {
		if value != busyTimeoutMillisecondsWanted {
			t.Errorf("reader connection %d reports busy_timeout %d, want %d",
				connectionIndex, value, busyTimeoutMillisecondsWanted)
		}
	}
}

// TestTheOneShotPragmaMissesMostOfAPool is the control, and it is the reason
// the test above means anything. It reproduces the form the writer used to be
// configured with and asserts the instrument can still report the broken
// state. Without it, a reader cannot tell "every connection is configured"
// from "the check silently stopped looking".
//
// The writer being capped at one connection is what made the old form work
// here; this shows it was a property of the cap, not of the pragma.
func TestTheOneShotPragmaMissesMostOfAPool(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;"); err != nil {
		t.Fatalf("one-shot pragmas: %v", err)
	}

	values := readPragmaFromEveryPooledConnection(t, db, "busy_timeout")
	connectionsMissingTheSetting := 0
	for _, value := range values {
		if value != busyTimeoutMillisecondsWanted {
			connectionsMissingTheSetting++
		}
	}
	if connectionsMissingTheSetting == 0 {
		t.Fatalf("control did not reproduce the bug: all %d pooled connections report "+
			"busy_timeout %d from a one-shot db.Exec, so this instrument cannot "+
			"distinguish a configured pool from an unconfigured one",
			len(values), busyTimeoutMillisecondsWanted)
	}
	t.Logf("control reproduced the bug: %d of %d pooled connections never saw the one-shot pragma",
		connectionsMissingTheSetting, len(values))
}
