package chat

import (
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Store is the part of storage the chat needs. Declared here, next to the
// code that uses it, so tests can pass a fake instead of a real DB.
type Store interface {
	SaveMessage(room, nickname, text string, replyTo *int64) (int64, error)
	UpdateMarkers(room string, markers map[int64]int64) error
}

type Hub struct { // один на комнату, хранит список клиентов
	name       string
	store      Store
	registry   *Registry
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

func (h *Hub) Run() { // слушает каналы Hub через select
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in hub %q: %v\n%s", h.name, r, debug.Stack())
			h.registry.RemoveRoom(h.name)
		}
	}()
	lastMsgByUser := make(map[int64]int64)
	lastMsgTransfer := make(chan map[int64]int64, 2)
	defer close(lastMsgTransfer)
	go writeMarkers(h.store, h.name, lastMsgTransfer)
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
				h.registry.RemoveRoom(h.name)
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

// Members returns nicknames of connected clients. The Hub goroutine builds
// the list, so the clients map is never read from another goroutine.
func (h *Hub) Members() []string {
	reply := make(chan []string)
	h.list <- reply
	return <-reply
}

// Join registers a connection in the hub and starts its read and write pumps.
func (h *Hub) Join(conn *websocket.Conn, nickname string, userID int64) {
	clnt := &Client{
		hub:      h,
		conn:     conn,
		send:     make(chan []byte, 256),
		nickname: nickname,
		userID:   userID,
	}
	h.register <- clnt
	go clnt.readPump()
	go clnt.writePump()
}

func writeMarkers(store Store, room string, markers <-chan map[int64]int64) { // читает срезы закладок, пока канал не закрыт
	defer recoverGoroutine("writeMarkers")
	for snapshot := range markers {
		if err := store.UpdateMarkers(room, snapshot); err != nil {
			log.Printf("error %v", err)
		}
	}
	log.Printf("room %s: writer stopped", room)
}

type Registry struct { // справочник активных комнат
	store Store
	mu    sync.Mutex
	rooms map[string]*Hub
}

func NewRegistry(store Store) *Registry {
	return &Registry{
		store: store,
		rooms: make(map[string]*Hub),
	}
}

func (rooms *Registry) GetOrStartHub(name string) *Hub {
	rooms.mu.Lock()
	defer rooms.mu.Unlock()
	h, ok := rooms.rooms[name]
	if !ok {
		h = &Hub{
			name:       name,
			store:      rooms.store,
			registry:   rooms,
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

func (rooms *Registry) GetRoom(name string) (*Hub, bool) {
	rooms.mu.Lock()
	defer rooms.mu.Unlock()
	hub, ok := rooms.rooms[name]
	return hub, ok
}

func (rooms *Registry) RemoveRoom(name string) {
	rooms.mu.Lock()
	defer rooms.mu.Unlock()
	delete(rooms.rooms, name)
}

func recoverGoroutine(name string) {
	if r := recover(); r != nil {
		log.Printf("panic in %s: %v\n%s", name, r, debug.Stack())
	}
}
