package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	srv := &http.Server{
		Addr:              ":8082",
		Handler:           server.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rowsNum, err := store.DeleteExpiredSessions()
				if err != nil {
					log.Printf("session cleanup: %v", err)
					continue
				}
				log.Printf("session cleanup: removed %d expired sessions", rowsNum)
			case <-ctx.Done():
				return
			}
		}
	}()
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) && err != nil {
			errCh <- err
		}
	}()
	select {
	case <-ctx.Done():
		log.Printf("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := srv.Shutdown(shutdownCtx)
		if errors.Is(err, context.DeadlineExceeded) {
			log.Printf("timeout: %v", err)
		} else if err != nil {
			log.Printf("shutdown err: %v", err)
		}
		log.Printf("stopped")
	case err := <-errCh:
		log.Printf("server err: %v", err)
		defer os.Exit(1)
	}
	db.Close()
}

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
