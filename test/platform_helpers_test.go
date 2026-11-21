//go:build !integration

package test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// writeCreds writes a syntactically valid NATS user .creds file.
func writeCreds(t *testing.T, dir string) string {
	t.Helper()
	account, _ := nkeys.CreateAccount()
	user, _ := nkeys.CreateUser()
	pub, _ := user.PublicKey()
	claims := jwt.NewUserClaims(pub)
	claims.Name = "source-controller"
	token, err := claims.Encode(account)
	if err != nil {
		t.Fatal(err)
	}
	seed, _ := user.Seed()
	creds, err := jwt.FormatUserConfig(token, seed)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "user.creds")
	if err := os.WriteFile(path, creds, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// writeCA writes a self-signed CA certificate.
func writeCA(t *testing.T, dir string) string {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "source-controller test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// configYAML returns a complete valid configuration whose files exist.
func configYAML(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	ca, creds := writeCA(t, dir), writeCreds(t, dir)
	y := `http:
  listen: ":8080"
db:
  host: postgres.flowai.svc
  port: 5432
  user: source_controller
  password: secret-password
  schema: source_controller_schema
  sslmode: verify-full
  ca_file: ` + ca + `
nats:
  url: tls://nats:4222
  ca_file: ` + ca + `
  credentials_file: ` + creds + `
  inbox_prefix: _INBOX.source-controller
  request_timeout: 5s
public_base_url: https://flowai.example
poll:
  allowed_origins: ["https://poller.example"]
  signing_keys:
    - id: k1
      seed: AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
  ca_file: ` + ca + `
cursor_keys:
  - id: 1
    key_hex: ` + strings.Repeat("ab", 32) + `
`
	return y, dir
}

func replaceLine(t *testing.T, y, prefix, line string) string {
	t.Helper()
	lines := strings.Split(y, "\n")
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), prefix) {
			indent := l[:len(l)-len(strings.TrimLeft(l, " "))]
			lines[i] = indent + line
			return strings.Join(lines, "\n")
		}
	}
	t.Fatalf("no line %s", prefix)
	return ""
}
