// Package postgres implements core.Store on PostgreSQL in the service's own
// schema. Every statement is unqualified; the connection search_path is the
// service schema only.
package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/pandops/flowai-source-controller/internal/core"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

var schemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

const (
	qNextSeq        = `SELECT nextval('publisher_seq')`
	qNextCounter    = `INSERT INTO team_counters (team_id, kind, value) VALUES ($1, $2, $3) ON CONFLICT (team_id, kind) DO UPDATE SET value = team_counters.value + EXCLUDED.value RETURNING value`
	qTeam           = `SELECT team_id, team_name, archived_at, last_seq FROM teams WHERE team_id = $1`
	qTeamByName     = `SELECT team_id, team_name, archived_at, last_seq FROM teams WHERE lower(team_name) = lower($1) ORDER BY team_id LIMIT 1`
	qUpsertTeam     = `INSERT INTO teams (team_id, team_name, archived_at, last_seq) VALUES ($1, $2, $3, $4) ON CONFLICT (team_id) DO UPDATE SET team_name = EXCLUDED.team_name, archived_at = EXCLUDED.archived_at, last_seq = EXCLUDED.last_seq`
	qMarkApplied    = `INSERT INTO applied_messages (publisher, publisher_seq) VALUES ($1, $2) ON CONFLICT DO NOTHING`
	qSource         = `SELECT doc, key_digest FROM sources WHERE team_id = $1 AND source_id = $2`
	qSourceDigest   = `SELECT doc, key_digest FROM sources WHERE key_digest = $1`
	qInsertSource   = `INSERT INTO sources (team_id, source_id, name, key_digest, mode, enabled, settings_status, unfinished_poll_id, polling_blocked_at, next_poll_at, created_at, doc) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`
	qUpdateSource   = `UPDATE sources SET name = $3, key_digest = $4, mode = $5, enabled = $6, settings_status = $7, unfinished_poll_id = $8, polling_blocked_at = $9, next_poll_at = $10, created_at = $11, doc = $12 WHERE team_id = $1 AND source_id = $2`
	qListFirst      = `SELECT doc, key_digest FROM sources WHERE team_id = $1 ORDER BY created_at, source_id LIMIT $2`
	qListAfter      = `SELECT doc, key_digest FROM sources WHERE team_id = $1 AND (created_at, source_id) > ($2, $3) ORDER BY created_at, source_id LIMIT $4`
	qSchemaRevision = `INSERT INTO source_schema_revisions (team_id, source_id, schema_revision, implementation_version, fields) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`
	qSourceRevision = `INSERT INTO source_revisions (team_id, source_id, revision, snapshot) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`
	qDue            = `SELECT s.team_id, s.source_id FROM sources s JOIN teams t ON t.team_id = s.team_id WHERE t.archived_at IS NULL AND s.mode = 'poller' AND s.enabled AND s.settings_status = 'complete' AND s.polling_blocked_at IS NULL AND s.unfinished_poll_id IS NULL AND s.next_poll_at <= $1 ORDER BY s.team_id, s.source_id LIMIT $2`
	qInsertPoll     = `INSERT INTO polls (team_id, poll_id, source_id, expires_at, finished_at, doc) VALUES ($1, $2, $3, $4, $5, $6)`
	qPoll           = `SELECT doc FROM polls WHERE team_id = $1 AND poll_id = $2`
	qUpdatePoll     = `UPDATE polls SET expires_at = $3, finished_at = $4, doc = $5 WHERE team_id = $1 AND poll_id = $2`
	qExpired        = `SELECT p.team_id, p.poll_id FROM polls p JOIN sources s ON s.team_id = p.team_id AND s.source_id = p.source_id WHERE p.finished_at IS NULL AND p.expires_at <= $1 ORDER BY p.team_id, p.poll_id LIMIT $2 FOR UPDATE OF p, s SKIP LOCKED`
	qEnqueue        = `INSERT INTO outbox (publisher_seq, subject, header, data) VALUES ($1, $2, $3, $4)`
	qOutbox         = `SELECT publisher_seq, subject, header, data FROM outbox ORDER BY publisher_seq LIMIT $1 FOR UPDATE SKIP LOCKED`
	qDeleteOutbox   = `DELETE FROM outbox WHERE publisher_seq = $1`
	qSchemaExists   = `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`
)

// Statements returns every SQL statement the store issues.
func Statements() []string {
	return []string{qNextSeq, qNextCounter, qTeam, qTeamByName, qUpsertTeam, qMarkApplied, qSource, qSourceDigest, qInsertSource,
		qUpdateSource, qListFirst, qListAfter, qSchemaRevision, qSourceRevision, qDue, qInsertPoll, qPoll, qUpdatePoll, qExpired,
		qEnqueue, qOutbox, qDeleteOutbox, qSchemaExists}
}

// Store is core.Store over one PostgreSQL schema.
type Store struct {
	pool   *pgxpool.Pool
	schema string
	mu     sync.Mutex
	now    func() time.Time
}

var _ core.Store = (*Store)(nil)

// Open connects to dsn with schema as the only search path.
func Open(ctx context.Context, dsn, schema string) (*Store, error) {
	if !schemaPattern.MatchString(schema) {
		return nil, errors.New("invalid schema name")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid database settings")
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("database pool: %w", err)
	}
	return &Store{pool: pool, schema: schema, now: time.Now}, nil
}

// SetClock sets the clock Tx.Now reports (tests only change it).
func (s *Store) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

func (s *Store) clock() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now().UTC().Truncate(time.Microsecond)
}

// Close closes the pool.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Migrate creates the schema when it is missing and applies the migrations.
// CREATE SCHEMA runs only when pg_namespace has no such schema, so a role
// without CREATE on the database works with a pre-created schema.
func (s *Store) Migrate(ctx context.Context) error {
	var exists bool
	if err := s.pool.QueryRow(ctx, qSchemaExists, s.schema).Scan(&exists); err != nil {
		return fmt.Errorf("check schema: %w", err)
	}
	if !exists {
		if _, err := s.pool.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{s.schema}.Sanitize()); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}
	scoped, err := fs.Sub(migrationFS, "migrations")
	if err != nil {
		return err
	}
	db := stdlib.OpenDBFromPool(s.pool)
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, scoped,
		goose.WithDisableGlobalRegistry(true), goose.WithTableName(s.schema+".goose_db_version"))
	if err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// Ping implements core.Store.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// NextPublisherSeq implements core.Store (autocommit).
func (s *Store) NextPublisherSeq(ctx context.Context) (int64, error) {
	var v int64
	err := s.pool.QueryRow(ctx, qNextSeq).Scan(&v)
	return v, err
}

// NextIntakeIDs implements core.Store (autocommit).
func (s *Store) NextIntakeIDs(ctx context.Context, teamID int64, n int) ([]int64, error) {
	var last int64
	if err := s.pool.QueryRow(ctx, qNextCounter, teamID, "intake", n).Scan(&last); err != nil {
		return nil, err
	}
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = last - int64(n) + 1 + int64(i)
	}
	return ids, nil
}

// Tx implements core.Store.
func (s *Store) Tx(ctx context.Context, fn func(core.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	if err := fn(&pgTx{ctx: ctx, tx: tx, store: s}); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return err
	}
	return tx.Commit(ctx)
}

type pgTx struct {
	ctx   context.Context
	tx    pgx.Tx
	store *Store
}

// Now returns the transaction time at the microsecond precision that
// timestamptz stores, so replies and cursors match later reads.
func (t *pgTx) Now() (time.Time, error) { return t.store.clock().Truncate(time.Microsecond), nil }

func (t *pgTx) NextPublisherSeq() (int64, error) {
	var v int64
	err := t.tx.QueryRow(t.ctx, qNextSeq).Scan(&v)
	return v, err
}

func (t *pgTx) NextCounter(teamID int64, kind string) (int64, error) {
	var v int64
	err := t.tx.QueryRow(t.ctx, qNextCounter, teamID, kind, 1).Scan(&v)
	return v, err
}

func scanTeam(row pgx.Row) (*core.Team, error) {
	var team core.Team
	err := row.Scan(&team.TeamID, &team.TeamName, &team.ArchivedAt, &team.LastSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &team, nil
}

func (t *pgTx) Team(teamID int64) (*core.Team, error) {
	return scanTeam(t.tx.QueryRow(t.ctx, qTeam, teamID))
}

func (t *pgTx) TeamByName(name string) (*core.Team, error) {
	return scanTeam(t.tx.QueryRow(t.ctx, qTeamByName, name))
}

func (t *pgTx) UpsertTeam(team core.Team) error {
	_, err := t.tx.Exec(t.ctx, qUpsertTeam, team.TeamID, team.TeamName, team.ArchivedAt, team.LastSeq)
	return err
}

func (t *pgTx) MarkApplied(publisher string, seq int64) (bool, error) {
	tag, err := t.tx.Exec(t.ctx, qMarkApplied, publisher, seq)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 0, nil
}

func lockSuffix(lock bool) string {
	if lock {
		return " FOR UPDATE"
	}
	return ""
}

func decodeSource(doc, digest []byte) (*core.Source, error) {
	var src core.Source
	if err := json.Unmarshal(doc, &src); err != nil {
		return nil, err
	}
	src.KeyDigest = digest
	return &src, nil
}

func (t *pgTx) oneSource(q string, args ...any) (*core.Source, error) {
	var doc, digest []byte
	err := t.tx.QueryRow(t.ctx, q, args...).Scan(&doc, &digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeSource(doc, digest)
}

func (t *pgTx) Source(teamID, sourceID int64, lock bool) (*core.Source, error) {
	return t.oneSource(qSource+lockSuffix(lock), teamID, sourceID)
}

func (t *pgTx) SourceByDigest(digest []byte, lock bool) (*core.Source, error) {
	if len(digest) == 0 {
		return nil, nil
	}
	return t.oneSource(qSourceDigest+lockSuffix(lock), digest)
}

// sourceArgs returns the column values of a Source; the document never
// holds the key digest.
func sourceArgs(s *core.Source) ([]any, error) {
	doc := *s
	doc.KeyDigest = nil
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	var digest any
	if len(s.KeyDigest) > 0 {
		digest = s.KeyDigest
	}
	return []any{s.TeamID, s.ID, s.Name, digest, s.Mode, s.Enabled, s.SettingsStatus, s.UnfinishedPollID, s.PollingBlockedAt,
		s.NextPollAt, s.CreatedAt, raw}, nil
}

func (t *pgTx) InsertSource(s *core.Source) error {
	args, err := sourceArgs(s)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(t.ctx, qInsertSource, args...)
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "23505" && pg.ConstraintName == "sources_team_lower_name" {
		return core.ErrNameConflict
	}
	return err
}

func (t *pgTx) UpdateSource(s *core.Source) error {
	args, err := sourceArgs(s)
	if err != nil {
		return err
	}
	tag, err := t.tx.Exec(t.ctx, qUpdateSource, args...)
	if err == nil && tag.RowsAffected() != 1 {
		return errors.New("unknown source")
	}
	return err
}

func (t *pgTx) ListSources(teamID int64, after *core.Position, limit int) ([]core.Source, error) {
	var rows pgx.Rows
	var err error
	if after == nil {
		rows, err = t.tx.Query(t.ctx, qListFirst, teamID, limit)
	} else {
		rows, err = t.tx.Query(t.ctx, qListAfter, teamID, after.CreatedAt, after.ID, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.Source
	for rows.Next() {
		var doc, digest []byte
		if err := rows.Scan(&doc, &digest); err != nil {
			return nil, err
		}
		src, err := decodeSource(doc, digest)
		if err != nil {
			return nil, err
		}
		out = append(out, *src)
	}
	return out, rows.Err()
}

func (t *pgTx) AppendSchemaRevision(teamID, sourceID int64, revision int, version string, schema []core.Field) error {
	raw, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(t.ctx, qSchemaRevision, teamID, sourceID, revision, version, raw)
	return err
}

func (t *pgTx) AppendSourceRevision(teamID, sourceID int64, revision int, snapshot json.RawMessage) error {
	_, err := t.tx.Exec(t.ctx, qSourceRevision, teamID, sourceID, revision, []byte(snapshot))
	return err
}

func (t *pgTx) DueSources(now time.Time, limit int) ([]core.SourceRef, error) {
	rows, err := t.tx.Query(t.ctx, qDue, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.SourceRef
	for rows.Next() {
		var ref core.SourceRef
		if err := rows.Scan(&ref.TeamID, &ref.SourceID); err != nil {
			return nil, err
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

func (t *pgTx) pollArgs(p *core.Poll) ([]any, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var finished *time.Time
	if p.Finished {
		now := t.store.clock()
		finished = &now
	}
	return []any{p.TeamID, p.ID, p.ExpiresAt, finished, raw}, nil
}

func (t *pgTx) InsertPoll(p *core.Poll) error {
	args, err := t.pollArgs(p)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(t.ctx, qInsertPoll, args[0], args[1], p.SourceID, args[2], args[3], args[4])
	return err
}

func (t *pgTx) Poll(teamID, pollID int64, lock bool) (*core.Poll, error) {
	var doc []byte
	err := t.tx.QueryRow(t.ctx, qPoll+lockSuffix(lock), teamID, pollID).Scan(&doc)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p core.Poll
	if err := json.Unmarshal(doc, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (t *pgTx) UpdatePoll(p *core.Poll) error {
	args, err := t.pollArgs(p)
	if err != nil {
		return err
	}
	tag, err := t.tx.Exec(t.ctx, qUpdatePoll, args...)
	if err == nil && tag.RowsAffected() != 1 {
		return errors.New("unknown poll")
	}
	return err
}

func (t *pgTx) ExpiredPolls(now time.Time, limit int) ([]core.PollRef, error) {
	rows, err := t.tx.Query(t.ctx, qExpired, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.PollRef
	for rows.Next() {
		var ref core.PollRef
		if err := rows.Scan(&ref.TeamID, &ref.PollID); err != nil {
			return nil, err
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

func (t *pgTx) Enqueue(m core.OutboxMessage) error {
	header, err := json.Marshal(m.Header)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(t.ctx, qEnqueue, m.Seq, m.Subject, header, m.Data)
	return err
}

func (t *pgTx) Outbox(limit int) ([]core.OutboxMessage, error) {
	rows, err := t.tx.Query(t.ctx, qOutbox, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.OutboxMessage
	for rows.Next() {
		var m core.OutboxMessage
		var header []byte
		if err := rows.Scan(&m.Seq, &m.Subject, &header, &m.Data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(header, &m.Header); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (t *pgTx) DeleteOutbox(seq int64) error {
	_, err := t.tx.Exec(t.ctx, qDeleteOutbox, seq)
	return err
}
