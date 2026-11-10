package auth

import (
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"testing/iotest"
)

func TestGenerateToken(t *testing.T) {
	cases := []struct {
		name         string
		Reader       io.Reader
		wantErr      bool
		wantTokenLen int64
	}{
		{"truly", rand.Reader, false, 64},
		{"empty", iotest.ErrReader(errors.New("unexpected error")), true, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			isErr := false
			token, err := generateToken(c.Reader)
			if err != nil {
				isErr = true
			}
			if c.wantErr != isErr {
				t.Fatalf("expected %v, get %v", c.wantErr, isErr)
			}
			if len(token) != int(c.wantTokenLen) {
				t.Fatalf("expected %v, get %v", c.wantTokenLen, len(token))
			}
		})
	}
}
