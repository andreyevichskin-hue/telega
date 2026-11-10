package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/gorilla/websocket"

	"telega/internal/auth"
	"telega/internal/storage"
)

func (s *Server) wsHandler(w http.ResponseWriter, r *http.Request) { // 1 запуск на 1 клиента
	userID, ok := userIDFrom(r.Context())
	if !ok {
		http.Error(w, "unexpected error", 500)
		return
	}
	nickname, err := s.store.Username(userID)
	if err != nil {
		log.Printf("error %v", err)
		http.Error(w, "unexpected error", 500)
		return
	}
	roomName := r.URL.Query().Get("room")
	exists, err := s.store.RoomExists(roomName)
	if err != nil {
		log.Printf("error %v", err)
		http.Error(w, "unexpected error", 500)
		return
	}
	if !exists {
		http.Error(w, "room not found", http.StatusNotFound)
		return
	}
	lastID, found, err := s.store.LastRead(userID, roomName)
	if err != nil {
		log.Printf("ws: %v", err)
		http.Error(w, "unexpected error", http.StatusInternalServerError)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("error %v", err)
		return
	}
	log.Println("client connected")
	hub := s.rooms.GetOrStartHub(roomName)
	var history []storage.Message
	if found {
		history, err = s.store.MessagesSince(roomName, lastID)
	} else {
		history, err = s.store.History(roomName, 10)
	}
	if err != nil {
		log.Printf("error %v", err)
	}
	for _, msg := range history {
		msg, err := json.Marshal(msg)
		if err != nil {
			log.Printf("error %v", err)
			continue
		}
		if err = conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			log.Printf("error %v", err)
		}
	}
	newLastID := lastID
	if len(history) > 0 {
		newLastID = history[len(history)-1].ID
	}
	if err := s.store.SaveLastRead(userID, roomName, newLastID); err != nil {
		log.Printf("ws: save bookmark: %v", err)
	}

	hub.Join(conn, nickname, userID)
}

func (s *Server) roomsHandler(w http.ResponseWriter, r *http.Request) {
	names, err := s.store.RoomNames()
	if err != nil {
		log.Printf("error %v", err)
		http.Error(w, "unexpected error", 500)
		return
	}

	result := make(map[string][]string)
	for _, name := range names {
		hub, ok := s.rooms.GetRoom(name)
		if !ok {
			result[name] = []string{}
			continue
		}
		result[name] = hub.Members()
	}

	response, err := json.Marshal(result)
	if err != nil {
		log.Printf("error %v", err)
		http.Error(w, "unexpected error", 500)
		return
	}
	w.Write(response)
}

type roomRequest struct {
	Name string `json:"name"`
}

func (s *Server) createRoomHandler(w http.ResponseWriter, r *http.Request) {
	var req roomRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "wrong JSON format", http.StatusBadRequest)
		log.Printf("error %v", err)
		return
	}
	if err = s.store.CreateRoom(req.Name); err != nil {
		http.Error(w, "failed to create room", http.StatusInternalServerError)
		log.Printf("error %v", err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("room created"))
}

func (s *Server) registerHandler(w http.ResponseWriter, r *http.Request) {
	u, err := decodeUser(r)
	if err != nil {
		http.Error(w, "wrong JSON format", http.StatusBadRequest)
		log.Printf("error %v", err)
		return
	}
	token, err := s.auth.Register(u.Username, u.Password)
	if errors.Is(err, storage.ErrUsernameTaken) {
		http.Error(w, "try another username", http.StatusInternalServerError)
		log.Printf("error %v", err)
		return
	}
	if err != nil {
		http.Error(w, "unexpected error", 500)
		log.Printf("error %v", err)
		return
	}
	setSessionCookie(w, token)

	w.Write([]byte("successful registration"))
}

func (s *Server) loginHandler(w http.ResponseWriter, r *http.Request) {
	u, err := decodeUser(r)
	if err != nil {
		http.Error(w, "wrong JSON format", http.StatusBadRequest)
		log.Printf("error %v", err)
		return
	}
	token, err := s.auth.Login(u.Username, u.Password)
	if errors.Is(err, auth.ErrInvalidCredentials) {
		http.Error(w, "Incorrect login or password", 401)
		return
	}
	if err != nil {
		http.Error(w, "unexpected error", 500)
		log.Printf("error %v", err)
		return
	}
	setSessionCookie(w, token)

	w.Write([]byte("logged in"))
}

func (s *Server) meHandler(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFrom(r.Context())
	if !ok {
		http.Error(w, "unexpected error", 500)
		return
	}
	nickname, err := s.store.Username(userID)
	if err != nil {
		log.Printf("error %v", err)
		http.Error(w, "unexpected error", 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"username": nickname})
}
