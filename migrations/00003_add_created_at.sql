-- +goose Up
ALTER TABLE messages ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP;

-- +goose Down
ALTER TABLE messages DROP COLUMN created_at;