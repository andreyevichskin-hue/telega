-- +goose Up
ALTER TABLE sessions ADD COLUMN last_seen_at DATETIME;
UPDATE sessions SET last_seen_at = created_at;

-- +goose Down
ALTER TABLE sessions DROP COLUMN last_seen_at;
