package chat

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"

	"telega/internal/storage"
)

type Client struct { // один на подключение, держит conn, send, ссылку на hub
	hub      *Hub
	conn     *websocket.Conn
	send     chan []byte
	nickname string
	userID   int64 // ! чтобы знать чью закладку двигать
}

type typingSignal struct {
	Nickname string `json:"nickname"`
	Type     string `json:"type"`
}

type incomingMessage struct {
	Text    string `json:"text"`
	ReplyTo *int64 `json:"reply_to,omitempty"`
	Type    string `json:"type"`
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
		var incoming incomingMessage
		err = json.Unmarshal(msg, &incoming)
		if err != nil {
			log.Printf("error %v", err)
			continue
		}
		if incoming.Type == "typing" {
			signal, err := json.Marshal(typingSignal{Nickname: clnt.nickname, Type: "typing"})
			if err != nil {
				log.Printf("error %v", err)
				continue
			}
			clnt.hub.typing <- signal
			continue
		}
		id, err := clnt.hub.store.SaveMessage(clnt.hub.name, clnt.nickname, incoming.Text, incoming.ReplyTo)
		if err != nil {
			log.Printf("error %v", err)
			continue
		}
		data, err := json.Marshal(storage.Message{ID: id, Nickname: clnt.nickname, Text: incoming.Text, ReplyTo: incoming.ReplyTo})
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

func logDisconnect(err error) {
	if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		log.Printf("error %v", err)
	} else {
		log.Println("client disconnected")
	}
}
