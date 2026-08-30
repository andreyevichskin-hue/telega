-- +goose Up
CREATE TABLE messages (
    id INTEGER PRIMARY KEY,
    room TEXT NOT NULL,
    nickname TEXT NOT NULL,
    mes TEXT NOT NULL
);

-- +goose Down
DROP TABLE messages;