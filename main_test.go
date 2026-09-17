package main

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/pressly/goose/v3"
)

func TestGenerateToken(t *testing.T) {
	cases := []struct {
		name         string
		Reader       io.Reader
		wantErr      bool
		wantTokenLen int64
	}{
		{"truly", rand.Reader, false, 64},
		{"empty", iotest.ErrReader(errors.New("unexpected error")), true, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			isErr := false
			tokenReader = c.Reader
			token, err := generateToken()
			if err != nil {
				isErr = true
			}
			if c.wantErr != isErr {
				t.Fatalf("expected %v, get %v", c.wantErr, isErr)
			}
			if len(token) != int(c.wantTokenLen) {
				t.Fatalf("expected %v, get %v", c.wantTokenLen, len(token))
			}
		})
	}
}

func TestGetRoom(t *testing.T) {
	test := roomsRelation{
		rooms: make(map[string]*Hub),
	}
	var nameTest string
	var HubTest Hub
	_, ok := test.GetRoom(nameTest)
	if !ok {
		test.rooms[nameTest] = &HubTest
	} else {
		t.Fatalf("expected %v, get %v", false, true)
	}
	hub, ok := test.GetRoom(nameTest)
	if !ok || hub != &HubTest {
		t.Fatalf("expected %v, get %v", true, false)
	}
}

func TestGetLastRead(t *testing.T) {
	testDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	testDB.SetMaxOpenConns(1)
	db = testDB
	err = goose.Up(db, "migrations")
	if err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}
	_, err = db.Exec("INSERT INTO rooms_reads (user_id, room, last_message_id) VALUES (?, ?, ?)", 1, "test", 55)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

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
			lastID, found, err := getLastRead(c.UserID, c.Room)
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
