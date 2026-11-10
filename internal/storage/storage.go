package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
)

var ErrUsernameTaken = errors.New("username taken")

type Message struct {
	ID       int64  `json:"id"`
	Nickname string `json:"nickname"`
	Text     string `json:"text"`
	ReplyTo  *int64 `json:"reply_to,omitempty"`
}

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) SaveMessage(room, nickname, text string, replyTo *int64) (int64, error) {
	result, err := s.db.Exec("INSERT INTO messages (room, nickname, mes, reply_to_id) VALUES (?, ?, ?, ?)", room, nickname, text, replyTo)
	if err != nil {
		return 0, fmt.Errorf("save message room %q: %w", room, err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("save message room %q: %w", room, err)
	}
	return id, nil
}

func (s *Store) MessagesSince(room string, afterID int64) ([]Message, error) {
	rows, err := s.db.Query("SELECT id, nickname, mes, reply_to_id FROM messages WHERE room = ? AND id > ? ORDER BY id ASC", room, afterID)
	if err != nil {
		return nil, fmt.Errorf("get messages since %d room %q: %w", afterID, room, err)
	}
	defer rows.Close()
	history, err := scanMessages(rows)
	if err != nil {
		return history, fmt.Errorf("get messages since %d room %q: %w", afterID, room, err)
	}
	return history, nil
}

func (s *Store) History(room string, limit int) ([]Message, error) {
	rows, err := s.db.Query("SELECT id, nickname, mes, reply_to_id FROM messages WHERE room = ? ORDER BY id DESC LIMIT ?", room, limit)
	if err != nil {
		return nil, fmt.Errorf("get history room %q: %w", room, err)
	}
	defer rows.Close()
	history, err := scanMessages(rows)
	slices.Reverse(history)
	if err != nil {
		return history, fmt.Errorf("get history room %q: %w", room, err)
	}
	return history, nil
}

func scanMessages(rows *sql.Rows) ([]Message, error) {
	var msgs []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Nickname, &m.Text, &m.ReplyTo); err != nil {
			return msgs, fmt.Errorf("scan: %w", err)
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

func (s *Store) LastRead(userID int64, room string) (int64, bool, error) {
	var lastMsgID int64
	err := s.db.QueryRow("SELECT last_message_id FROM rooms_reads WHERE user_id = ? AND room = ?", userID, room).Scan(&lastMsgID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("get last read user %d room %q: %w", userID, room, err)
	}
	return lastMsgID, true, nil
}

func (s *Store) SaveLastRead(userID int64, room string, lastID int64) error {
	_, err := s.db.Exec(`INSERT INTO rooms_reads (user_id, room, last_message_id) VALUES (?, ?, ?)
		ON CONFLICT (user_id, room) DO UPDATE SET last_message_id = MAX(last_message_id, excluded.last_message_id)`,
		userID, room, lastID)
	if err != nil {
		return fmt.Errorf("save last read user %d room %q: %w", userID, room, err)
	}
	return nil
}

func (s *Store) UpdateMarkers(room string, markers map[int64]int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for userID, id := range markers {
		_, err = tx.Exec("UPDATE rooms_reads SET last_message_id = ? WHERE user_id = ? AND room = ? AND last_message_id < ?", id, userID, room, id)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) RoomExists(name string) (bool, error) {
	var exists bool
	err := s.db.QueryRow("SELECT EXISTS(SELECT 1 FROM rooms WHERE name = ?)", name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("room exists %q: %w", name, err)
	}
	return exists, nil
}

func (s *Store) RoomNames() ([]string, error) {
	rows, err := s.db.Query("SELECT name FROM rooms")
	if err != nil {
		return nil, fmt.Errorf("list rooms: %w", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("list rooms: scan: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list rooms: %w", err)
	}
	return names, nil
}

func (s *Store) CreateRoom(name string) error {
	_, err := s.db.Exec("INSERT OR IGNORE INTO rooms (name) VALUES (?)", name)
	if err != nil {
		return fmt.Errorf("create room %q: %w", name, err)
	}
	return nil
}

func (s *Store) Username(userID int64) (string, error) {
	var name string
	err := s.db.QueryRow("SELECT username FROM users WHERE id = ?", userID).Scan(&name)
	if err != nil {
		return "", fmt.Errorf("get username %d: %w", userID, err)
	}
	return name, nil
}

// UserByName returns sql.ErrNoRows (wrapped) when the user does not exist.
func (s *Store) UserByName(username string) (id int64, passwordHash []byte, err error) {
	err = s.db.QueryRow("SELECT id, password_hash FROM users WHERE username = ?", username).Scan(&id, &passwordHash)
	if err != nil {
		return 0, nil, fmt.Errorf("get user %q: %w", username, err)
	}
	return id, passwordHash, nil
}

// CreateUserWithSession inserts the user and their first session in one
// transaction, so a failed session never leaves a user without a way to log in.
func (s *Store) CreateUserWithSession(username string, passwordHash []byte, token string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("create user %q: %w", username, err)
	}
	defer tx.Rollback()
	result, err := tx.Exec("INSERT INTO users (username, password_hash) VALUES (?, ?)", username, passwordHash)
	if err != nil {
		return fmt.Errorf("create user %q: %w: %v", username, ErrUsernameTaken, err)
	}
	userID, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("create user %q: %w", username, err)
	}
	if _, err = tx.Exec("INSERT INTO sessions (token, user_id) VALUES (?, ?)", token, userID); err != nil {
		return fmt.Errorf("create session user %d: %w", userID, err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("create user %q: %w", username, err)
	}
	return nil
}

func (s *Store) CreateSession(token string, userID int64) error {
	_, err := s.db.Exec("INSERT INTO sessions (token, user_id) VALUES (?, ?)", token, userID)
	if err != nil {
		return fmt.Errorf("create session user %d: %w", userID, err)
	}
	return nil
}

// UserIDBySession returns sql.ErrNoRows (wrapped) for an unknown token.
func (s *Store) UserIDBySession(token string) (int64, error) {
	var userID int64
	err := s.db.QueryRow("SELECT user_id FROM sessions WHERE token = ?", token).Scan(&userID)
	if err != nil {
		return 0, fmt.Errorf("get session: %w", err)
	}
	return userID, nil
}
