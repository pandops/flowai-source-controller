package core

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

// apiError is a client-visible error: status, stable code and a safe message.
type apiError struct {
	status int
	code   string
	msg    string
	fields []string
}

func (e *apiError) Error() string { return e.code }

func errf(status int, code, msg string) *apiError {
	return &apiError{status: status, code: code, msg: msg}
}

var (
	errNotFound      = errf(404, "not_found", "not found")
	errInvalid       = errf(400, "invalid_request", "invalid request")
	errInternal      = errf(500, "internal_error", "internal error")
	errStale         = errf(412, "stale_revision", "the revision is stale")
	errUnavailable   = errf(409, "source_unavailable", "the Source is unavailable")
	errBrokerDown    = errf(503, "broker_unavailable", "the message broker is unavailable")
	errOwnerDown     = errf(503, "service_unavailable", "a required service is unavailable")
	errNotBlocked    = errf(409, "polling_not_blocked", "polling is not blocked")
	errNotPoller     = errf(409, "not_poller_source", "the Source is not a poller")
	errPollBusy      = errf(409, "poll_in_progress", "a poll is in progress")
	errNameConflict  = errf(409, "source_name_conflict", "a Source with this name exists")
	errRevConflict   = errf(409, "source_revision_conflict", "a Source revision does not match")
	errInvalidKey    = errf(401, "invalid_source_key", "invalid Source key")
	errTooLarge      = errf(413, "request_too_large", "the request is too large")
	errReadOnlyTx    = errors.New("read-only transaction")
	idPattern        = regexp.MustCompile(`^[1-9][0-9]{0,18}$`)
	sourceNameRegexp = regexp.MustCompile(`^[A-Za-z0-9._~-]{1,63}$`)
)

func invalidFields(fields ...string) *apiError {
	return &apiError{status: 400, code: "invalid_request", msg: "invalid request", fields: fields}
}

// asAPI converts any error to an apiError; unknown errors are internal.
func asAPI(err error) *apiError {
	var a *apiError
	if errors.As(err, &a) {
		return a
	}
	return errInternal
}

// parseID parses a decimal positive int64 in canonical form.
func parseID(s string) (int64, bool) {
	if !idPattern.MatchString(s) {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, err == nil
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

// ts formats a time as RFC 3339 UTC with milliseconds.
func ts(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

func tsOrNil(t *time.Time) any {
	if t == nil {
		return nil
	}
	return ts(*t)
}

func strOrNil(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

func intOrNil(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}

func ptr[T any](v T) *T { return &v }

// requestID returns a random positive identifier for HTTP edge errors.
func randomID() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	v := int64(binary.BigEndian.Uint64(b[:]) >> 1)
	if v == 0 {
		return 1
	}
	return v
}

// newKey returns a fresh Source key and its digest.
func newKey() (string, []byte) {
	var b [32]byte
	_, _ = rand.Read(b[:])
	key := "tsk_v1_" + base64.RawURLEncoding.EncodeToString(b[:])
	d := sha256.Sum256([]byte(key))
	return key, d[:]
}

func keyDigest(key string) []byte {
	if len(key) != 50 || !strings.HasPrefix(key, "tsk_v1_") {
		return nil
	}
	if _, err := base64.RawURLEncoding.DecodeString(key[7:]); err != nil {
		return nil
	}
	d := sha256.Sum256([]byte(key))
	return d[:]
}

func etag(rev int) string { return `"rev-` + strconv.Itoa(rev) + `"` }

var ifMatchPattern = regexp.MustCompile(`^"rev-[1-9][0-9]{0,9}"$`)

// validIfMatch reports whether an If-Match header is one canonical "rev-N".
func validIfMatch(h string) bool { return ifMatchPattern.MatchString(h) }

var errAmbiguousJSON = errors.New("invalid UTF-8 or duplicate object key")

// checkJSON refuses invalid UTF-8 and objects that repeat a key, which
// encoding/json would otherwise accept (replacing bytes, last key wins).
func checkJSON(raw []byte) error {
	if !utf8.Valid(raw) {
		return errAmbiguousJSON
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	return walkJSON(dec)
}

func walkJSON(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch tok {
	case json.Delim('{'):
		seen := map[string]bool{}
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			name, _ := key.(string)
			if seen[name] {
				return errAmbiguousJSON
			}
			seen[name] = true
			if err := walkJSON(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
		return err
	case json.Delim('['):
		for dec.More() {
			if err := walkJSON(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
		return err
	}
	return nil
}

// decodeStrict decodes JSON with numbers kept exact; invalid UTF-8 and
// duplicate object keys are errors.
func decodeStrict(raw []byte, v any) error {
	if err := checkJSON(raw); err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data")
	}
	return nil
}

func mustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

// read runs fn in a transaction that always rolls back.
func (s *Service) read(ctx context.Context, fn func(Tx) error) error {
	err := s.store.Tx(ctx, func(tx Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		return errReadOnlyTx
	})
	if errors.Is(err, errReadOnlyTx) {
		return nil
	}
	return err
}

// errorBody is the rule 9 error body.
func errorBody(e *apiError, requestID int64) []byte {
	body := map[string]any{"code": e.code, "message": e.msg}
	// No valid request_id header: the field is left out (D12).
	if requestID > 0 {
		body["request_id"] = requestID
	}
	if len(e.fields) > 0 {
		f := e.fields
		if len(f) > 100 {
			f = f[:100]
		}
		body["fields"] = f
	}
	return mustJSON(body)
}

func replyOK(status int, body any, header broker.Header) broker.Reply {
	h := broker.Header{"status": strconv.Itoa(status)}
	for k, v := range header {
		h[k] = v
	}
	var data []byte
	if body != nil {
		data = mustJSON(body)
	}
	return broker.Reply{Header: h, Data: data}
}

// replyErr answers an error; requestID 0 (no valid header) omits the field.
func replyErr(e *apiError, requestID int64) broker.Reply {
	return broker.Reply{Header: broker.Header{"status": strconv.Itoa(e.status)}, Data: errorBody(e, requestID)}
}

func unixMicro(v int64) time.Time { return time.UnixMicro(v).UTC() }

func minutes(n int) time.Duration { return time.Duration(n) * time.Minute }
