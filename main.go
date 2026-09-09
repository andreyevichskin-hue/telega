package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"slices"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/pressly/goose/v3"
	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

type Hub struct { // один на сервер, хранит список клиентов
	name       string
	clients    map[*Client]bool
	broadcast  chan []byte // сообщение
	register   chan *Client
	unregister chan *Client
	list       chan chan []string
}

type roomsRelation struct { // справочник активных комнат
	mu    sync.Mutex
	rooms map[string]*Hub
}

type Client struct { // один на подключение, держит conn, send, ссылку на hub
	hub      *Hub
	conn     *websocket.Conn
	send     chan []byte
	nickname string
}

type Message struct {
	ID       int64  `json:"id"`
	Nickname string `json:"nickname"`
	Text     string `json:"text"`
	ReplyTo  *int64 `json:"reply_to,omitempty"`
}

type IncomingMessage struct {
	Text    string `json:"text"`
	ReplyTo *int64 `json:"reply_to,omitempty"`
}

type User struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

var registry = &roomsRelation{
	rooms: make(map[string]*Hub),
}

var db *sql.DB

var upgrader websocket.Upgrader

func (h *Hub) Run() { // слушает 3 канала из Hub через select
	for {
		select {
		case clnt := <-h.register:
			h.clients[clnt] = true
		case clnt := <-h.unregister:
			delete(h.clients, clnt)
			close(clnt.send)
			if len(h.clients) == 0 {
				registry.RemoveRoom(h.name)
				return
			}
		case msg := <-h.broadcast:
			for clnt := range h.clients {
				clnt.send <- msg
			}
		case reply := <-h.list:
			var names []string
			for clnt := range h.clients {
				names = append(names, clnt.nickname)
			}
			reply <- names
		}
	}
}

func (rooms *roomsRelation) GetOrStartHub(name string) *Hub {
	rooms.mu.Lock()
	defer rooms.mu.Unlock()
	h, ok := rooms.rooms[name]
	if !ok {
		h = &Hub{
			name:       name,
			clients:    make(map[*Client]bool),
			broadcast:  make(chan []byte),
			register:   make(chan *Client),
			unregister: make(chan *Client),
			list:       make(chan chan []string),
		}
		go h.Run()
		rooms.rooms[name] = h
	}
	return h
}

func (rooms *roomsRelation) GetRoom(name string) (*Hub, bool) {
	rooms.mu.Lock()
	defer rooms.mu.Unlock()
	hub, ok := rooms.rooms[name]
	return hub, ok
}

func (rooms *roomsRelation) RemoveRoom(name string) {
	rooms.mu.Lock()
	defer rooms.mu.Unlock()
	delete(rooms.rooms, name)
}

func (clnt *Client) readPump() { // читает из client, передает в Hub
	defer clnt.conn.Close()
	for {
		_, msg, err := clnt.conn.ReadMessage()
		if err != nil {
			logDisconnect(err)
			clnt.hub.unregister <- clnt
			break
		}
		var incoming IncomingMessage
		err = json.Unmarshal(msg, &incoming)
		if err != nil {
			log.Printf("error %v", err)
			continue
		}
		result, err := db.Exec("INSERT INTO messages (room, nickname, mes, reply_to_id) VALUES (?, ?, ?, ?)", clnt.hub.name, clnt.nickname, incoming.Text, incoming.ReplyTo)
		if err != nil {
			log.Printf("error %v", err)
			continue
		}
		id, err := result.LastInsertId()
		if err != nil {
			log.Printf("error %v", err)
		}
		data, err := json.Marshal(Message{ID: id, Nickname: clnt.nickname, Text: incoming.Text, ReplyTo: incoming.ReplyTo})
		if err != nil {
			log.Printf("error %v", err)
			clnt.hub.unregister <- clnt
			break
		}
		clnt.hub.broadcast <- data
	}
}

func (clnt *Client) writePump() { // пишет в client, передает из Hub
	defer clnt.conn.Close()
	for msg := range clnt.send {
		if err := clnt.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			log.Printf("error %v", err)
			break
		}
	}
}

func getLastRead(userID int64, room string) (int64, bool, error) {
	var lastMsgID int64
	err := db.QueryRow("SELECT last_message_id FROM rooms_reads WHERE user_id = ? AND room = ?", userID, room).Scan(&lastMsgID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		} else {
			log.Printf("error %v", err)
			return 0, false, err
		}
	}
	return lastMsgID, true, nil
}

func getMsgsSince(room string, afterID int64) ([]Message, error) {
	rows, err := db.Query("SELECT id, nickname, mes, reply_to_id FROM messages WHERE room = ? AND id > ? ORDER BY id ASC", room, afterID)
	if err != nil {
		log.Printf("error %v", err)
		return nil, err
	}
	defer rows.Close()
	var history []Message
	for rows.Next() {
		var id int64
		var nickname, mes string
		var replyTo *int64
		if err := rows.Scan(&id, &nickname, &mes, &replyTo); err != nil {
			log.Printf("error %v", err)
			return history, err
		}
		history = append(history, Message{ID: id, Nickname: nickname, Text: mes, ReplyTo: replyTo})
	}
	if err := rows.Err(); err != nil {
		return history, err
	}
	return history, err
}

func getHistory(room string, limit int) ([]Message, error) {
	rows, err := db.Query("SELECT id, nickname, mes, reply_to_id FROM messages WHERE room = ? ORDER BY id DESC LIMIT ?", room, limit)
	if err != nil {
		log.Printf("error %v", err)
		return nil, err
	}
	defer rows.Close()
	var history []Message
	for rows.Next() {
		var id int64
		var nickname, mes string
		var replyTo *int64
		if err := rows.Scan(&id, &nickname, &mes, &replyTo); err != nil {
			log.Printf("error %v", err)
			slices.Reverse(history)
			return history, err
		}
		history = append(history, Message{ID: id, Nickname: nickname, Text: mes, ReplyTo: replyTo})
	}
	if err := rows.Err(); err != nil {
		slices.Reverse(history)
		return history, err
	}
	slices.Reverse(history)
	return history, err
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", requireAuth(wsHandler))
	mux.HandleFunc("GET /rooms", requireAuth(roomsHandler))
	mux.HandleFunc("POST /rooms", requireAuth(createRoomHandler))
	mux.HandleFunc("/register", registerHandler)
	mux.HandleFunc("/login", loginHandler)
	mux.HandleFunc("/me", requireAuth(meHandler))
	mux.HandleFunc("/", serveIndex)
	mux.HandleFunc("/style.css", serveStyle)
	mux.HandleFunc("/script.js", serveScript)
	var err error
	db, err = sql.Open("sqlite", "file:telega.db?_foreign_keys=on")
	must(err)
	err = db.Ping()
	must(err)
	err = goose.SetDialect("sqlite")
	must(err)
	err = goose.Up(db, "migrations")
	must(err)
	err = http.ListenAndServe(":8082", mux)
	must(err)
}

func must(err error) {
	if err != nil {
		log.Fatal("fatal", err)
	}
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}
func serveStyle(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "style.css")
}
func serveScript(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "script.js")
}

type contextKey string

const IDKey contextKey = "userID"

func requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("session")
		if err != nil {
			http.Error(w, "Cookie not found", 401)
			log.Printf("error %v", err)
			return
		}
		var userID int64
		err = db.QueryRow("SELECT user_id FROM sessions WHERE token = ?", cookie.Value).Scan(&userID)
		if err != nil {
			http.Error(w, "Cookie not found", 401)
			log.Printf("error %v", err)
			return
		} else {
			ctx := context.WithValue(r.Context(), IDKey, userID)
			r = r.WithContext(ctx)
			next(w, r)
		}
	}
}

func wsHandler(w http.ResponseWriter, r *http.Request) { // 1 запуск на 1 клиента
	userID, ok := r.Context().Value(IDKey).(int64)
	if !ok {
		http.Error(w, "unexpected error", 500)
		return
	}
	var nickname string
	err := db.QueryRow("SELECT username FROM users WHERE id = ?", userID).Scan(&nickname)
	if err != nil {
		log.Printf("error %v", err)
		http.Error(w, "unexpected error", 500)
		return
	}
	roomName := r.URL.Query().Get("room")
	var exists bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM rooms WHERE name = ?)", roomName).Scan(&exists)
	if err != nil {
		log.Printf("error %v", err)
		http.Error(w, "unexpected error", 500)
		return
	}
	if !exists {
		http.Error(w, "room not found", http.StatusNotFound)
		return
	}
	lastID, found, err := getLastRead(userID, roomName)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("error %v", err)
		return
	}
	log.Println("client connected")
	hub := registry.GetOrStartHub(roomName) // уже локальный
	clnt := &Client{
		hub:      hub,
		conn:     conn,
		send:     make(chan []byte, 256),
		nickname: nickname,
	}
	var history []Message
	if found {
		history, err = getMsgsSince(roomName, lastID)
		if err != nil {
			log.Printf("error %v", err)
		}
	} else {
		history, err = getHistory(roomName, 10)
		if err != nil {
			log.Printf("error %v", err)
		}
	}
	for _, msg := range history {
		msg, err := json.Marshal(msg)
		if err != nil {
			log.Printf("error %v", err)
			continue
		}
		if err = clnt.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			log.Printf("error %v", err)
		}
	}
	newLastID := lastID
	if len(history) > 0 {
		newLastID = history[len(history)-1].ID
	}
	if found {
		_, err = db.Exec("UPDATE rooms_reads SET last_message_id = ? WHERE user_id = ? AND room = ?", newLastID, userID, roomName)
	} else {
		_, err = db.Exec("INSERT INTO rooms_reads (user_id, room, last_message_id) VALUES (?, ?, ?)", userID, roomName, newLastID)
	}
	if err != nil {
		log.Printf("error %v", err)
	}

	hub.register <- clnt
	go clnt.readPump()
	go clnt.writePump()
}

func roomsHandler(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT name FROM rooms")
	if err != nil {
		log.Printf("error %v", err)
		http.Error(w, "unexpected error", 500)
		return
	}
	defer rows.Close()

	result := make(map[string][]string)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Printf("error %v", err)
			http.Error(w, "unexpected error", 500)
			return
		}
		hub, ok := registry.GetRoom(name)
		if !ok {
			result[name] = []string{}
			continue
		}
		reply := make(chan []string)
		hub.list <- reply
		result[name] = <-reply
	}
	if err := rows.Err(); err != nil {
		log.Printf("error %v", err)
		http.Error(w, "unexpected error", 500)
		return
	}

	response, err := json.Marshal(result)
	if err != nil {
		log.Printf("error %v", err)
		http.Error(w, "unexpected error", 500)
		return
	}
	w.Write(response)
}

type RoomRequest struct {
	Name string `json:"name"`
}

func createRoomHandler(w http.ResponseWriter, r *http.Request) {
	var req RoomRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "wrong JSON format", http.StatusBadRequest)
		log.Printf("error %v", err)
		return
	}
	_, err = db.Exec("INSERT OR IGNORE INTO rooms (name) VALUES (?)", req.Name)
	if err != nil {
		http.Error(w, "failed to create room", http.StatusInternalServerError)
		log.Printf("error %v", err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("room created"))
}

func registerHandler(w http.ResponseWriter, r *http.Request) {
	user, err := decodeUser(r)
	if err != nil {
		http.Error(w, "wrong JSON format", http.StatusBadRequest)
		log.Printf("error %v", err)
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		log.Printf("error %v", err)
		return
	}

	result, err := db.Exec("INSERT INTO users (username, password_hash) VALUES (?, ?)", user.Username, hash)
	if err != nil {
		http.Error(w, "try another username", http.StatusInternalServerError)
		log.Printf("error %v", err)
		return
	}
	userID, err := result.LastInsertId()
	if err != nil {
		http.Error(w, "unexpected error", 500)
		log.Printf("error %v", err)
		return
	}
	err = sessRelation(w, userID)
	if err != nil {
		http.Error(w, "unexpected error", 500)
		log.Printf("error %v", err)
		return
	}

	w.Write([]byte("successful registration"))
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	var hash []byte
	user, err := decodeUser(r)
	if err != nil {
		http.Error(w, "wrong JSON format", http.StatusBadRequest)
		log.Printf("error %v", err)
		return
	}
	var userID int64
	err = db.QueryRow("SELECT password_hash, id FROM users WHERE username = ?", user.Username).Scan(&hash, &userID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Incorrect login or password", 401)
			return
		} else {
			http.Error(w, "unexpected error", 500)
			log.Printf("error %v", err)
			return
		}
	}
	if err = bcrypt.CompareHashAndPassword(hash, []byte(user.Password)); err != nil {
		http.Error(w, "Incorrect login or password", 401)
		return
	}
	err = sessRelation(w, userID)
	if err != nil {
		http.Error(w, "unexpected error", 500)
		log.Printf("error %v", err)
		return
	}

	w.Write([]byte("logged in"))
}

func generateToken() (string, error) {
	buf := make([]byte, 32)
	_, err := rand.Read(buf)
	if err != nil {
		log.Printf("error %v", err)
		return "", err
	}
	token := hex.EncodeToString(buf)
	return token, nil
}

func meHandler(w http.ResponseWriter, r *http.Request) {
	userID, ok := r.Context().Value(IDKey).(int64)
	if !ok {
		http.Error(w, "unexpected error", 500)
		return
	}
	var nickname string
	err := db.QueryRow("SELECT username FROM users WHERE id = ?", userID).Scan(&nickname)
	if err != nil {
		log.Printf("error %v", err)
		http.Error(w, "unexpected error", 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"username": nickname})
}

func logDisconnect(err error) {
	if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		log.Printf("error %v", err)
	} else {
		log.Println("client disconnected")
	}
}

func decodeUser(r *http.Request) (User, error) {
	var user User
	err := json.NewDecoder(r.Body).Decode(&user)
	return user, err
}

func sessRelation(w http.ResponseWriter, userID int64) error {
	token, err := generateToken()
	if err != nil {
		log.Printf("error %v", err)
		return err
	}
	_, err = db.Exec("INSERT INTO sessions (token, user_id) VALUES (?, ?)", token, userID)
	if err != nil {
		log.Printf("error %v", err)
		return err
	}
	cookie := http.Cookie{
		Name:     "session",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		// Secure: true,
	}
	http.SetCookie(w, &cookie)
	return nil
}
