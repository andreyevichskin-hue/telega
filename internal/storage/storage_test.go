package storage

import (
	"database/sql"
	"testing"

	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"
)

func TestLastRead(t *testing.T) {
	testDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	testDB.SetMaxOpenConns(1)
	err = goose.SetDialect("sqlite")
	if err != nil {
		t.Fatalf("failed to set dialect: %v", err)
	}
	err = goose.Up(testDB, "../../migrations")
	if err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}
	_, err = testDB.Exec("INSERT INTO rooms_reads (user_id, room, last_message_id) VALUES (?, ?, ?)", 1, "test", 55)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}
	store := New(testDB)

	cases := []struct {
		name       string
		UserID     int64
		Room       string
		WantLastID int64
		WantFound  bool
		WantErr    bool
	}{
		{"found", 1, "test", 55, true, false},
		{"not found", 999, "nonexistent", 0, false, false},
		{"db error", 1, "test", 0, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			isErr := false
			if c.WantErr {
				testDB.Close()
			}
			lastID, found, err := store.LastRead(c.UserID, c.Room)
			if err != nil {
				isErr = true
			}
			if c.WantErr != isErr {
				t.Fatalf("err: expected %v, get %v", c.WantErr, isErr)
			}
			if found != c.WantFound {
				t.Fatalf("found: expected %v, get %v", c.WantFound, found)
			}
			if lastID != c.WantLastID {
				t.Fatalf("lastID: expected %v, get %v", c.WantLastID, lastID)
			}
		})
	}
}
