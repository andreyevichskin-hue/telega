-- +goose Up
CREATE TABLE rooms_reads (
    user_id INTEGER NOT NULL REFERENCES users(id),
    room TEXT NOT NULL REFERENCES rooms(name),
    last_message_id INTEGER NOT NULL,
    PRIMARY KEY (user_id, room)
);

-- +goose Down
DROP TABLE rooms_reads;