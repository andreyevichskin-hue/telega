package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"

	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"

	"telega/internal/auth"
	"telega/internal/chat"
	"telega/internal/httpapi"
	"telega/internal/storage"
)

func main() {
	err := os.MkdirAll("data", 0o755)
	must(err)
	db, err := sql.Open("sqlite", "file:data/telega.db?_foreign_keys=on")
	must(err)
	err = db.Ping()
	must(err)
	err = goose.SetDialect("sqlite")
	must(err)
	err = goose.Up(db, "migrations")
	must(err)

	store := storage.New(db)
	authService := auth.NewService(store)
	rooms := chat.NewRegistry(store)
	server := httpapi.NewServer(store, authService, rooms)

	err = http.ListenAndServe(":8082", server.Routes())
	must(err)
}

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
