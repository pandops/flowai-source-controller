// Package config loads source-controller's YAML configuration. Every key the
// service reads is listed in svc/source-controller/config.example.yaml.
package config

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultSchema is the PostgreSQL schema source-controller owns.
const DefaultSchema = "source_controller_schema"

// Config is the parsed configuration.
type Config struct {
	HTTP struct {
		Listen string `yaml:"listen"`
	} `yaml:"http"`
	DB struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Database string `yaml:"database"`
		Schema   string `yaml:"schema"`
		SSLMode  string `yaml:"sslmode"`
		CAFile   string `yaml:"ca_file"`
	} `yaml:"db"`
	NATS struct {
		URL             string        `yaml:"url"`
		CAFile          string        `yaml:"ca_file"`
		CredentialsFile string        `yaml:"credentials_file"`
		InboxPrefix     string        `yaml:"inbox_prefix"`
		RequestTimeout  time.Duration `yaml:"request_timeout"`
	} `yaml:"nats"`
	PublicBaseURL string `yaml:"public_base_url"`
	Poll          struct {
		AllowedOrigins []string `yaml:"allowed_origins"`
		SigningKeys    []struct {
			ID   string `yaml:"id"`
			Seed string `yaml:"seed"`
		} `yaml:"signing_keys"`
		CAFile string `yaml:"ca_file"`
	} `yaml:"poll"`
	CursorKeys []struct {
		ID     int64  `yaml:"id"`
		KeyHex string `yaml:"key_hex"`
	} `yaml:"cursor_keys"`
}

// SigningKey is one decoded poll-signing key.
type SigningKey struct {
	ID      string
	Private ed25519.PrivateKey
}

// CursorKey is one decoded cursor HMAC key.
type CursorKey struct {
	ID     int64
	Secret []byte
}

// Load reads and validates the configuration file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(raw)
}

// Parse parses and validates configuration bytes. Unknown keys are refused.
func Parse(raw []byte) (*Config, error) {
	var c Config
	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if c.HTTP.Listen == "" {
		c.HTTP.Listen = ":8080"
	}
	if c.DB.Schema == "" {
		c.DB.Schema = DefaultSchema
	}
	if c.DB.Port == 0 {
		c.DB.Port = 5432
	}
	if c.DB.Database == "" {
		c.DB.Database = "flowai"
	}
	if c.DB.SSLMode == "" {
		c.DB.SSLMode = "verify-full"
	}
	if c.NATS.InboxPrefix == "" {
		c.NATS.InboxPrefix = "_INBOX.source-controller"
	}
	if c.NATS.RequestTimeout == 0 {
		c.NATS.RequestTimeout = 5 * time.Second
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) validate() error {
	var problems []string
	need := func(ok bool, key string) {
		if !ok {
			problems = append(problems, key)
		}
	}
	need(c.DB.Host != "", "db.host")
	need(c.DB.User != "", "db.user")
	need(c.DB.Password != "", "db.password")
	need(validIdentifier(c.DB.Schema), "db.schema")
	need(c.DB.SSLMode == "verify-full", "db.sslmode")
	need(c.DB.CAFile != "", "db.ca_file")
	need(strings.HasPrefix(c.NATS.URL, "tls://"), "nats.url")
	need(c.NATS.CAFile != "", "nats.ca_file")
	need(c.NATS.CredentialsFile != "", "nats.credentials_file")
	need(c.NATS.RequestTimeout > 0, "nats.request_timeout")
	base, err := url.Parse(c.PublicBaseURL)
	need(err == nil && (base.Scheme == "https" || base.Scheme == "http") && base.Host != "" &&
		base.User == nil && base.RawQuery == "" && base.Fragment == "" && strings.TrimRight(base.Path, "/") == "",
		"public_base_url")
	for _, origin := range c.Poll.AllowedOrigins {
		u, err := url.Parse(origin)
		need(err == nil && u.Scheme == "https" && u.Host != "" && u.Path == "" && u.User == nil &&
			u.RawQuery == "" && u.Fragment == "", "poll.allowed_origins")
	}
	need(len(c.Poll.SigningKeys) > 0, "poll.signing_keys")
	if _, err := c.SigningKeys(); err != nil {
		problems = append(problems, "poll.signing_keys")
	}
	need(len(c.CursorKeys) > 0, "cursor_keys")
	if _, err := c.DecodedCursorKeys(); err != nil {
		problems = append(problems, "cursor_keys")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(problems, ", "))
	}
	return nil
}

// CheckFiles verifies that every referenced trust and credential file is
// readable. Startup fails closed without production trust.
func (c *Config) CheckFiles() error {
	files := map[string]string{"db.ca_file": c.DB.CAFile, "nats.ca_file": c.NATS.CAFile, "nats.credentials_file": c.NATS.CredentialsFile}
	if c.Poll.CAFile != "" {
		files["poll.ca_file"] = c.Poll.CAFile
	}
	var problems []string
	for key, path := range files {
		if raw, err := os.ReadFile(path); err != nil || len(raw) == 0 {
			problems = append(problems, key)
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("unreadable config files: %s", strings.Join(problems, ", "))
	}
	return nil
}

func validIdentifier(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		lower, digit := r == '_' || r >= 'a' && r <= 'z', i > 0 && r >= '0' && r <= '9'
		if !lower && !digit {
			return false
		}
	}
	return true
}

// SigningKeys decodes the poll-signing seeds (Ed25519 seed, base64url).
func (c *Config) SigningKeys() ([]SigningKey, error) {
	keys := make([]SigningKey, 0, len(c.Poll.SigningKeys))
	seen := map[string]bool{}
	for _, k := range c.Poll.SigningKeys {
		seed, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(k.Seed, "="))
		if err != nil || len(seed) != ed25519.SeedSize || k.ID == "" || seen[k.ID] {
			return nil, errors.New("invalid poll signing key")
		}
		seen[k.ID] = true
		keys = append(keys, SigningKey{ID: k.ID, Private: ed25519.NewKeyFromSeed(seed)})
	}
	return keys, nil
}

// DecodedCursorKeys decodes the cursor HMAC keys; key IDs are positive.
func (c *Config) DecodedCursorKeys() ([]CursorKey, error) {
	keys := make([]CursorKey, 0, len(c.CursorKeys))
	seen := map[int64]bool{}
	for _, k := range c.CursorKeys {
		secret, err := hex.DecodeString(k.KeyHex)
		if err != nil || len(secret) != 32 || k.ID < 1 || seen[k.ID] {
			return nil, errors.New("invalid cursor key")
		}
		seen[k.ID] = true
		keys = append(keys, CursorKey{ID: k.ID, Secret: secret})
	}
	return keys, nil
}

// DSN returns the pgx connection string. search_path is pinned to the
// configured schema only.
func (c *Config) DSN() string {
	q := url.Values{}
	q.Set("sslmode", c.DB.SSLMode)
	q.Set("sslrootcert", c.DB.CAFile)
	q.Set("search_path", c.DB.Schema)
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.DB.User, c.DB.Password),
		Host:     fmt.Sprintf("%s:%d", c.DB.Host, c.DB.Port),
		Path:     "/" + c.DB.Database,
		RawQuery: q.Encode(),
	}
	return u.String()
}
