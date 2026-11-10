package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"runtime/debug"

	"github.com/gorilla/websocket"

	"telega/internal/auth"
	"telega/internal/chat"
	"telega/internal/storage"
)

type Server struct {
	store    *storage.Store
	auth     *auth.Service
	rooms    *chat.Registry
	upgrader websocket.Upgrader
}

func NewServer(store *storage.Store, authService *auth.Service, rooms *chat.Registry) *Server {
	return &Server{store: store, auth: authService, rooms: rooms}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.requireAuth(s.wsHandler))
	mux.HandleFunc("GET /rooms", s.requireAuth(s.roomsHandler))
	mux.HandleFunc("POST /rooms", s.requireAuth(s.createRoomHandler))
	mux.HandleFunc("/register", s.registerHandler)
	mux.HandleFunc("/login", s.loginHandler)
	mux.HandleFunc("/me", s.requireAuth(s.meHandler))
	mux.HandleFunc("/", serveFile("index.html"))
	mux.HandleFunc("/style.css", serveFile("style.css"))
	mux.HandleFunc("/script.js", serveFile("script.js"))
	return recoverMiddleware(mux)
}

func serveFile(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, path)
	}
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				log.Printf("panic in %s %s: %v\n%s", r.Method, r.URL.Path, v, debug.Stack())
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type userIDKey struct{}

func withUserID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, userIDKey{}, id)
}

func userIDFrom(ctx context.Context) (int64, bool) {
	id, ok := ctx.Value(userIDKey{}).(int64)
	return id, ok
}

func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("session")
		if err != nil {
			http.Error(w, "Cookie not found", http.StatusUnauthorized)
			return
		}
		userID, err := s.auth.UserID(cookie.Value)
		if errors.Is(err, auth.ErrInvalidCredentials) {
			http.Error(w, "Cookie not found", http.StatusUnauthorized)
			return
		}
		if err != nil {
			log.Printf("require auth: %v", err)
			http.Error(w, "unexpected error", http.StatusInternalServerError)
			return
		}
		r = r.WithContext(withUserID(r.Context(), userID))
		next(w, r)
	}
}

type user struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func decodeUser(r *http.Request) (user, error) {
	var u user
	err := json.NewDecoder(r.Body).Decode(&u)
	return u, err
}

func setSessionCookie(w http.ResponseWriter, token string) {
	cookie := http.Cookie{
		Name:     "session",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		// Secure: true,
	}
	http.SetCookie(w, &cookie)
}
