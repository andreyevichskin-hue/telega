package chat

import "testing"

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
