package chat

import (
	"testing"
	"time"
)

func TestGetRoom(t *testing.T) {
	test := NewRegistry(nil)
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

type fakeStore struct{}

func (fS *fakeStore) SaveMessage(room, nicknames, text string, replyTo *int64) (int64, error) {
	return 0, nil
}
func (fS *fakeStore) UpdateMarkers(room string, markers map[int64]int64) error { return nil }

func TestHubBroadcast(t *testing.T) {
	hub := NewRegistry(&fakeStore{}).GetOrStartHub("go")

	clnt1 := &Client{hub: hub, send: make(chan []byte, 16), nickname: "Andrew", userID: 1}
	clnt2 := &Client{hub: hub, send: make(chan []byte, 16), nickname: "LittleAndrew", userID: 2}
	select {
	case hub.register <- clnt1:
	case <-time.After(time.Second):
		t.Fatal("hub did not accept register")
	}
	hub.register <- clnt2
	hub.broadcast <- MessageEvent{data: []byte("hello")}
	for _, c := range []*Client{clnt1, clnt2} {
		select {
		case msg := <-c.send:
			if string(msg) != "hello" {
				t.Fatalf("expected %v, get %v", "hello", string(msg))
			}
		case <-time.After(time.Second * 2):
			t.Fatalf("message waiting timeout")
		}
	}
}
