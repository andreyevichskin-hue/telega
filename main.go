package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pressly/goose/v3"
	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

type Hub struct { // один на сервер, хранит список клиентов
	name       string
	clients    map[*Client]bool
	broadcast  chan MessageEvent // сообщение вместе с id
	register   chan *Client
	unregister chan *Client
	list       chan chan []string
	typing     chan []byte
}

type MessageEvent struct {
	data []byte
	id   int64
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
	userID   int64 // ! чтобы знать чью закладку двигать
}

type Message struct {
	ID       int64  `json:"id"`
	Nickname string `json:"nickname"`
	Text     string `json:"text"`
	ReplyTo  *int64 `json:"reply_to,omitempty"`
}

type TypingSignal struct {
	Nickname string `json:"nickname"`
	Type     string `json:"type"`
}

type IncomingMessage struct {
	Text    string `json:"text"`
	ReplyTo *int64 `json:"reply_to,omitempty"`
	Type    string `json:"type"`
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

func (h *Hub) Run() { // слушает каналы Hub через select
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in hub %q: %v\n%s", h.name, r, debug.Stack())
			registry.RemoveRoom(h.name)
		}
	}()
	lastMsgByUser := make(map[int64]int64)
	lastMsgTransfer := make(chan map[int64]int64, 2)
	defer close(lastMsgTransfer)
	go writeMarkers(h.name, lastMsgTransfer)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case clnt := <-h.register:
			h.clients[clnt] = true
		case clnt := <-h.unregister:
			delete(h.clients, clnt)
			close(clnt.send)
			if len(h.clients) == 0 {
				if len(lastMsgByUser) > 0 {
					lastMsgTransfer <- lastMsgByUser
				}
				registry.RemoveRoom(h.name)
				return
			}
		case msg := <-h.broadcast:
			for clnt := range h.clients {
				if msg.id > lastMsgByUser[clnt.userID] {
					lastMsgByUser[clnt.userID] = msg.id
				}
				clnt.send <- msg.data
			}
		case <-ticker.C:
			if len(lastMsgByUser) == 0 {
				continue
			}
			snapshot := lastMsgByUser
			lastMsgTransfer <- snapshot
			lastMsgByUser = make(map[int64]int64)
		case signal := <-h.typing:
			for clnt := range h.clients {
				clnt.send <- signal
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

func writeMarkers(room string, markers <-chan map[int64]int64) { // читает срезы закладок, пока канал не закрыт
	defer recoverGoroutine("writeMarkers")
	for snapshot := range markers {
		if err := updateMarkers(room, snapshot); err != nil {
			log.Printf("error %v", err)
		}
	}
	log.Printf("room %s: writer stopped", room)
}

func updateMarkers(room string, snapshot map[int64]int64) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for userID, id := range snapshot {
		_, err = tx.Exec("UPDATE rooms_reads SET last_message_id = ? WHERE user_id = ? AND room = ? AND last_message_id < ?", id, userID, room, id)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (rooms *roomsRelation) GetOrStartHub(name string) *Hub {
	rooms.mu.Lock()
	defer rooms.mu.Unlock()
	h, ok := rooms.rooms[name]
	if !ok {
		h = &Hub{
			name:       name,
			clients:    make(map[*Client]bool),
			broadcast:  make(chan MessageEvent),
			register:   make(chan *Client),
			unregister: make(chan *Client),
			list:       make(chan chan []string),
			typing:     make(chan []byte),
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
	defer func() { clnt.hub.unregister <- clnt }()
	defer recoverGoroutine("readPump")
	clnt.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	clnt.conn.SetPongHandler((func(appData string) error {
		err := clnt.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			log.Printf("error %v", err)
			return err
		}
		return nil
	}))
	for {
		_, msg, err := clnt.conn.ReadMessage()
		if err != nil {
			logDisconnect(err)
			break
		}
		var incoming IncomingMessage
		err = json.Unmarshal(msg, &incoming)
		if err != nil {
			log.Printf("error %v", err)
			continue
		}
		if incoming.Type == "typing" {
			typingSignal := TypingSignal{Nickname: clnt.nickname, Type: "typing"}
			signal, err := json.Marshal(typingSignal)
			if err != nil {
				log.Printf("error %v", err)
				continue
			}
			clnt.hub.typing <- signal
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
			continue
		}
		data, err := json.Marshal(Message{ID: id, Nickname: clnt.nickname, Text: incoming.Text, ReplyTo: incoming.ReplyTo})
		if err != nil {
			log.Printf("error %v", err)
			break
		}
		clnt.hub.broadcast <- MessageEvent{data: data, id: id}
	}
}

func (clnt *Client) writePump() { // пишет в client, передает из Hub
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	defer clnt.conn.Close()
	defer recoverGoroutine("writePump")
	for {
		select {
		case msg, ok := <-clnt.send:
			if !ok {
				return
			}
			if err := clnt.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("error %v", err)
				return
			}
		case <-ticker.C:
			if err := clnt.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				log.Printf("error %v", err)
				return
			}
			clnt.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		}
	}
}

func getLastRead(userID int64, room string) (int64, bool, error) {
	var lastMsgID int64
	err := db.QueryRow("SELECT last_message_id FROM rooms_reads WHERE user_id = ? AND room = ?", userID, room).Scan(&lastMsgID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("get last read user %d room %q: %w", userID, room, err)
	}
	return lastMsgID, true, nil
}

func getMsgsSince(room string, afterID int64) ([]Message, error) {
	rows, err := db.Query("SELECT id, nickname, mes, reply_to_id FROM messages WHERE room = ? AND id > ? ORDER BY id ASC", room, afterID)
	if err != nil {
		return nil, fmt.Errorf("get messages since %d room %q: %w", afterID, room, err)
	}
	defer rows.Close()
	var history []Message
	for rows.Next() {
		var id int64
		var nickname, mes string
		var replyTo *int64
		if err := rows.Scan(&id, &nickname, &mes, &replyTo); err != nil {
			return history, fmt.Errorf("get messages since %d room %q: scan: %w", afterID, room, err)
		}
		history = append(history, Message{ID: id, Nickname: nickname, Text: mes, ReplyTo: replyTo})
	}
	if err := rows.Err(); err != nil {
		return history, fmt.Errorf("get messages since %d room %q: %w", afterID, room, err)
	}
	return history, nil
}

func getHistory(room string, limit int) ([]Message, error) {
	rows, err := db.Query("SELECT id, nickname, mes, reply_to_id FROM messages WHERE room = ? ORDER BY id DESC LIMIT ?", room, limit)
	if err != nil {
		return nil, fmt.Errorf("get history room %q: %w", room, err)
	}
	defer rows.Close()
	var history []Message
	for rows.Next() {
		var id int64
		var nickname, mes string
		var replyTo *int64
		if err := rows.Scan(&id, &nickname, &mes, &replyTo); err != nil {
			slices.Reverse(history)
			return history, fmt.Errorf("get history room %q: scan: %w", room, err)
		}
		history = append(history, Message{ID: id, Nickname: nickname, Text: mes, ReplyTo: replyTo})
	}
	if err := rows.Err(); err != nil {
		slices.Reverse(history)
		return history, fmt.Errorf("get history room %q: %w", room, err)
	}
	slices.Reverse(history)
	return history, nil
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
	db, err = sql.Open("sqlite", "file:data/telega.db?_foreign_keys=on")
	must(err)
	err = db.Ping()
	must(err)
	err = goose.SetDialect("sqlite")
	must(err)
	err = goose.Up(db, "migrations")
	must(err)
	err = http.ListenAndServe(":8082", recoverMiddleware(mux))
	must(err)
}

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func recoverGoroutine(name string) {
	if r := recover(); r != nil {
		log.Printf("panic in %s: %v\n%s", name, r, debug.Stack())
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

func serveIndex(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}
func serveStyle(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "style.css")
}
func serveScript(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "script.js")
}

type userIDKey struct{}

func withUserID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, userIDKey{}, id)
}

func userIDFrom(ctx context.Context) (int64, bool) {
	id, ok := ctx.Value(userIDKey{}).(int64)
	return id, ok
}

func requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("session")
		if err != nil {
			http.Error(w, "Cookie not found", http.StatusUnauthorized)
			return
		}
		var userID int64
		err = db.QueryRow("SELECT user_id FROM sessions WHERE token = ?", cookie.Value).Scan(&userID)
		if errors.Is(err, sql.ErrNoRows) {
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

func wsHandler(w http.ResponseWriter, r *http.Request) { // 1 запуск на 1 клиента
	userID, ok := userIDFrom(r.Context())
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
	if err != nil {
		log.Printf("ws: %v", err)
		http.Error(w, "unexpected error", http.StatusInternalServerError)
		return
	}

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
		userID:   userID,
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
	_, err = db.Exec(`INSERT INTO rooms_reads (user_id, room, last_message_id) VALUES (?, ?, ?)
		ON CONFLICT (user_id, room) DO UPDATE SET last_message_id = MAX(last_message_id, excluded.last_message_id)`,
		userID, roomName, newLastID)
	if err != nil {
		log.Printf("ws: save bookmark: %v", err)
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

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "unexpected error", 500)
		log.Printf("error %v", err)
		return
	}
	defer tx.Rollback()

	result, err := tx.Exec("INSERT INTO users (username, password_hash) VALUES (?, ?)", user.Username, hash)
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
	token, err := createSession(tx, userID)
	if err != nil {
		http.Error(w, "unexpected error", 500)
		log.Printf("error %v", err)
		return
	}
	if err = tx.Commit(); err != nil {
		http.Error(w, "unexpected error", 500)
		log.Printf("error %v", err)
		return
	}
	setSessionCookie(w, token)

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
	token, err := createSession(db, userID)
	if err != nil {
		http.Error(w, "unexpected error", 500)
		log.Printf("error %v", err)
		return
	}
	setSessionCookie(w, token)

	w.Write([]byte("logged in"))
}

var tokenReader io.Reader = rand.Reader

func generateToken() (string, error) {
	buf := make([]byte, 32)
	_, err := io.ReadFull(tokenReader, buf)
	if err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	token := hex.EncodeToString(buf)
	return token, nil
}

func meHandler(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFrom(r.Context())
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

type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func createSession(e execer, userID int64) (string, error) {
	token, err := generateToken()
	if err != nil {
		return "", fmt.Errorf("create session: %w", err)
	}
	_, err = e.Exec("INSERT INTO sessions (token, user_id) VALUES (?, ?)", token, userID)
	if err != nil {
		return "", fmt.Errorf("create session user %d: %w", userID, err)
	}
	return token, nil
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
