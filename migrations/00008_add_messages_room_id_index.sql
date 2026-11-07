-- +goose Up
CREATE INDEX idx_messages_room_id ON messages (room, id);

-- +goose Down
DROP INDEX idx_messages_room_id;
