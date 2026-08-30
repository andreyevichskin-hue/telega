-- +goose Up
ALTER TABLE messages ADD COLUMN reply_to_id INTEGER REFERENCES messages(id);

-- +goose Down
ALTER TABLE messages DROP COLUMN reply_to_id;