package httpapi

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"

	"telega/internal/auth"
	"telega/internal/chat"
	"telega/internal/storage"
)

func TestRegisterHandlers(t *testing.T) {
	testDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer testDB.Close()
	testDB.SetMaxOpenConns(1)
	err = goose.SetDialect("sqlite")
	if err != nil {
		t.Fatalf("failed to set dialect: %v", err)
	}
	err = goose.Up(testDB, "../../migrations")
	if err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}
	store := storage.New(testDB)
	authService := auth.NewService(store)
	rooms := chat.NewRegistry(store)
	server := NewServer(store, authService, rooms)
	h := server.Routes()

	// step 1: /me without cookie
	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no cookie: expected %d, get %d", http.StatusUnauthorized, rec.Code)
	}

	// step 2: register
	body := strings.NewReader(`{"username":"andrew","password":"123"}`)
	reqReg := httptest.NewRequest(http.MethodPost, "/register", body)
	recReg := httptest.NewRecorder()
	h.ServeHTTP(recReg, reqReg)
	if recReg.Code != http.StatusOK {
		t.Fatalf("register: expected %d, get %d", http.StatusOK, recReg.Code)
	}
	cookies := recReg.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("register: no cookie in response")
	}

	// step 3: /me with cookie
	reqC := httptest.NewRequest(http.MethodGet, "/me", nil)
	reqC.AddCookie(cookies[0])
	recC := httptest.NewRecorder()
	h.ServeHTTP(recC, reqC)
	if recC.Code != http.StatusOK {
		t.Fatalf("me: expected %d, get %d", http.StatusOK, recC.Code)
	}
	if !strings.Contains(recC.Body.String(), "andrew") {
		t.Fatalf("me: expected username andrew, get %s", recC.Body.String())
	}
}
