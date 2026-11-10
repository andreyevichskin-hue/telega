package auth

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/bcrypt"

	"telega/internal/storage"
)

var ErrInvalidCredentials = errors.New("invalid credentials")

type Service struct {
	store  *storage.Store
	random io.Reader
}

func NewService(store *storage.Store) *Service {
	return &Service{store: store, random: rand.Reader}
}

// Register creates the user and returns a token for their first session.
func (s *Service) Register(username, password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("register %q: hash password: %w", username, err)
	}
	token, err := generateToken(s.random)
	if err != nil {
		return "", fmt.Errorf("register %q: %w", username, err)
	}
	if err := s.store.CreateUserWithSession(username, hash, token); err != nil {
		return "", fmt.Errorf("register: %w", err)
	}
	return token, nil
}

// Login checks the password and returns a token for a new session.
func (s *Service) Login(username, password string) (string, error) {
	userID, hash, err := s.store.UserByName(username)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrInvalidCredentials
	}
	if err != nil {
		return "", fmt.Errorf("login: %w", err)
	}
	if err := bcrypt.CompareHashAndPassword(hash, []byte(password)); err != nil {
		return "", ErrInvalidCredentials
	}
	token, err := generateToken(s.random)
	if err != nil {
		return "", fmt.Errorf("login: %w", err)
	}
	if err := s.store.CreateSession(token, userID); err != nil {
		return "", fmt.Errorf("login: %w", err)
	}
	return token, nil
}

// UserID resolves a session token. Unknown tokens give ErrInvalidCredentials.
func (s *Service) UserID(token string) (int64, error) {
	userID, err := s.store.UserIDBySession(token)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrInvalidCredentials
	}
	if err != nil {
		return 0, fmt.Errorf("auth: %w", err)
	}
	return userID, nil
}

func generateToken(r io.Reader) (string, error) {
	buf := make([]byte, 32)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
