package core

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
)

// cursorState is what an opaque cursor binds: the operation, the team and
// the position after the last returned item.
type cursorState struct {
	Op    string `json:"o"`
	Team  int64  `json:"t"`
	At    int64  `json:"c,omitempty"`
	ID    int64  `json:"i"`
	KeyID int64  `json:"k"`
}

func (s *Service) signCursor(c cursorState) string {
	key := s.cfg.CursorKeys[0]
	c.KeyID = key.ID
	payload := base64.RawURLEncoding.EncodeToString(mustJSON(c))
	mac := hmac.New(sha256.New, key.Secret)
	mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// openCursor verifies a cursor for op and team.
func (s *Service) openCursor(raw, op string, team int64) (cursorState, bool) {
	var c cursorState
	payload, sig, ok := strings.Cut(raw, ".")
	if !ok || len(raw) > 512 {
		return c, false
	}
	body, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return c, false
	}
	mac, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return c, false
	}
	if err := json.Unmarshal(body, &c); err != nil {
		return c, false
	}
	for _, k := range s.cfg.CursorKeys {
		if k.ID != c.KeyID {
			continue
		}
		h := hmac.New(sha256.New, k.Secret)
		h.Write([]byte(payload))
		if !hmac.Equal(h.Sum(nil), mac) {
			return c, false
		}
		return c, c.Op == op && c.Team == team && c.ID >= 0
	}
	return c, false
}
