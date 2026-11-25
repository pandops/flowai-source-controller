// Package mocks holds in-process test fakes of source-controller's
// dependencies: an in-memory Store and a fake broker. They are test fixtures,
// never production code paths.
package mocks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pandops/flowai-source-controller/internal/core"
)

// Clock is a controllable clock: real time plus an offset.
type Clock struct {
	mu     sync.Mutex
	offset time.Duration
	fixed  *time.Time
}

// Now returns the current fake time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fixed != nil {
		return *c.fixed
	}
	return time.Now().Add(c.offset).UTC()
}

// Advance moves the clock forward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fixed != nil {
		t := c.fixed.Add(d)
		c.fixed = &t
		return
	}
	c.offset += d
}

type state struct {
	counters   map[string]int64
	teams      map[int64]core.Team
	applied    map[string]bool
	sources    map[core.SourceRef]core.Source
	schemaRevs map[core.SourceRef][]int
	sourceRevs map[core.SourceRef][]int
	polls      map[core.PollRef]core.Poll
	outbox     map[int64]core.OutboxMessage
}

func (s *state) clone() *state {
	c := &state{counters: map[string]int64{}, teams: map[int64]core.Team{},
		applied: map[string]bool{}, sources: map[core.SourceRef]core.Source{}, schemaRevs: map[core.SourceRef][]int{},
		sourceRevs: map[core.SourceRef][]int{}, polls: map[core.PollRef]core.Poll{}, outbox: map[int64]core.OutboxMessage{}}
	for k, v := range s.counters {
		c.counters[k] = v
	}
	for k, v := range s.teams {
		c.teams[k] = v
	}
	for k, v := range s.applied {
		c.applied[k] = v
	}
	for k, v := range s.sources {
		c.sources[k] = v
	}
	for k, v := range s.schemaRevs {
		c.schemaRevs[k] = append([]int(nil), v...)
	}
	for k, v := range s.sourceRevs {
		c.sourceRevs[k] = append([]int(nil), v...)
	}
	for k, v := range s.polls {
		c.polls[k] = v
	}
	for k, v := range s.outbox {
		c.outbox[k] = v
	}
	return c
}

// MemStore is an in-memory core.Store. One global mutex serializes
// transactions; a failed transaction restores the snapshot taken at Begin.
type MemStore struct {
	mu    sync.Mutex
	st    *state
	clock *Clock
	// seq is the publisher sequence; it is not transactional.
	seq int64

	// Down makes every call fail like an unreachable database.
	Down bool
	// FailCommit, when set, makes the next commit fail (the transaction is
	// rolled back) and is then cleared.
	FailCommit bool
	// Commits counts committed transactions.
	Commits int
	// Accesses counts every transaction (committed, failed or read-only)
	// and every sequence allocation: a request that reads and writes
	// nothing leaves it unchanged.
	Accesses int
	// Enqueued records every outbox message ever committed.
	Enqueued []core.OutboxMessage
	// AfterCommit runs after every committed transaction (outside the lock).
	AfterCommit func()
}

// ErrDown is returned while the fake database is down.
var ErrDown = errors.New("memstore: database unavailable")

// NewMemStore returns an empty store using clock for Tx.Now.
func NewMemStore(clock *Clock) *MemStore {
	if clock == nil {
		clock = &Clock{}
	}
	return &MemStore{st: (&state{}).clone(), clock: clock}
}

// Tx runs fn in one serialized transaction.
func (m *MemStore) Tx(ctx context.Context, fn func(core.Tx) error) error {
	m.mu.Lock()
	m.Accesses++
	if m.Down {
		m.mu.Unlock()
		return ErrDown
	}
	work := m.st.clone()
	tx := &memTx{store: m, st: work, clock: m.clock}
	err := fn(tx)
	if err == nil && m.FailCommit {
		m.FailCommit = false
		err = errors.New("memstore: commit failed")
	}
	if err != nil {
		m.mu.Unlock()
		return err
	}
	m.st = work
	m.Commits++
	m.Enqueued = append(m.Enqueued, tx.enqueued...)
	hook := m.AfterCommit
	m.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

// NextPublisherSeq allocates one publisher_seq outside a transaction.
func (m *MemStore) NextPublisherSeq(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Accesses++
	if m.Down {
		return 0, ErrDown
	}
	m.seq++
	return m.seq, nil
}

// NextIntakeIDs allocates n per-team intake IDs outside a transaction.
func (m *MemStore) NextIntakeIDs(ctx context.Context, teamID int64, n int) ([]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Accesses++
	if m.Down {
		return nil, ErrDown
	}
	key := counterKey(teamID, "intake")
	ids := make([]int64, n)
	for i := range ids {
		m.st.counters[key]++
		ids[i] = m.st.counters[key]
	}
	return ids, nil
}

// Ping reports whether the fake database is up.
func (m *MemStore) Ping(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Down {
		return ErrDown
	}
	return nil
}

// AccessCount returns Accesses under the store lock.
func (m *MemStore) AccessCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Accesses
}

// LastPublisherSeq returns the last allocated publisher_seq, committed or
// not.
func (m *MemStore) LastPublisherSeq() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.seq
}

// SetPublisherSeq sets the publisher sequence (a restart keeps it).
func (m *MemStore) SetPublisherSeq(v int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq = v
}

// SetCounter sets a per-team counter's last value.
func (m *MemStore) SetCounter(teamID int64, kind string, v int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.st.counters[counterKey(teamID, kind)] = v
}

// Source returns a stored Source (nil when absent).
func (m *MemStore) Source(teamID, sourceID int64) *core.Source {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.st.sources[core.SourceRef{TeamID: teamID, SourceID: sourceID}]
	if !ok {
		return nil
	}
	c := cloneSource(s)
	return &c
}

// PutSource overwrites a stored Source (test setup only).
func (m *MemStore) PutSource(s core.Source) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.st.sources[core.SourceRef{TeamID: s.TeamID, SourceID: s.ID}] = cloneSource(s)
}

// Poll returns a stored poll (nil when absent).
func (m *MemStore) Poll(teamID, pollID int64) *core.Poll {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.st.polls[core.PollRef{TeamID: teamID, PollID: pollID}]
	if !ok {
		return nil
	}
	return &p
}

// Team returns a projected team (nil when absent).
func (m *MemStore) Team(teamID int64) *core.Team {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.st.teams[teamID]
	if !ok {
		return nil
	}
	return &t
}

// PutTeam stores a team projection row (test setup only).
func (m *MemStore) PutTeam(t core.Team) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.st.teams[t.TeamID] = t
}

// PendingOutbox returns the messages still in the outbox, in seq order.
func (m *MemStore) PendingOutbox() []core.OutboxMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]core.OutboxMessage, 0, len(m.st.outbox))
	for _, v := range m.st.outbox {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// SchemaRevisions returns the appended schema revision numbers of a Source.
func (m *MemStore) SchemaRevisions(teamID, sourceID int64) []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int(nil), m.st.schemaRevs[core.SourceRef{TeamID: teamID, SourceID: sourceID}]...)
}

// SourceRevisions returns the appended settings revision numbers of a Source.
func (m *MemStore) SourceRevisions(teamID, sourceID int64) []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int(nil), m.st.sourceRevs[core.SourceRef{TeamID: teamID, SourceID: sourceID}]...)
}

func counterKey(teamID int64, kind string) string {
	return kind + "/" + itoa(teamID)
}

func itoa(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

type memTx struct {
	store    *MemStore
	st       *state
	clock    *Clock
	enqueued []core.OutboxMessage
}

// Now truncates to microseconds, like PostgreSQL timestamptz.
func (t *memTx) Now() (time.Time, error) { return t.clock.Now().Truncate(time.Microsecond), nil }

func (t *memTx) NextPublisherSeq() (int64, error) {
	// Like a PostgreSQL sequence, an allocation survives a rollback.
	t.store.seq++
	return t.store.seq, nil
}

func (t *memTx) NextCounter(teamID int64, kind string) (int64, error) {
	if kind != "source" && kind != "poll" && kind != "intake" {
		return 0, errors.New("unknown counter")
	}
	key := counterKey(teamID, kind)
	t.st.counters[key]++
	return t.st.counters[key], nil
}

func (t *memTx) Team(teamID int64) (*core.Team, error) {
	v, ok := t.st.teams[teamID]
	if !ok {
		return nil, nil
	}
	return &v, nil
}

func (t *memTx) TeamByName(name string) (*core.Team, error) {
	for _, v := range t.st.teams {
		if strings.EqualFold(v.TeamName, name) {
			c := v
			return &c, nil
		}
	}
	return nil, nil
}

func (t *memTx) UpsertTeam(team core.Team) error {
	t.st.teams[team.TeamID] = team
	return nil
}

func (t *memTx) MarkApplied(publisher string, seq int64) (bool, error) {
	key := publisher + "/" + itoa(seq)
	if t.st.applied[key] {
		return true, nil
	}
	t.st.applied[key] = true
	return false, nil
}

func (t *memTx) Source(teamID, sourceID int64, lock bool) (*core.Source, error) {
	v, ok := t.st.sources[core.SourceRef{TeamID: teamID, SourceID: sourceID}]
	if !ok {
		return nil, nil
	}
	c := cloneSource(v)
	return &c, nil
}

func (t *memTx) SourceByDigest(digest []byte, lock bool) (*core.Source, error) {
	if len(digest) == 0 {
		return nil, nil
	}
	for _, v := range t.st.sources {
		if bytes.Equal(v.KeyDigest, digest) {
			c := cloneSource(v)
			return &c, nil
		}
	}
	return nil, nil
}

func (t *memTx) InsertSource(s *core.Source) error {
	ref := core.SourceRef{TeamID: s.TeamID, SourceID: s.ID}
	if _, ok := t.st.sources[ref]; ok {
		return errors.New("duplicate source id")
	}
	for _, v := range t.st.sources {
		if v.TeamID == s.TeamID && strings.EqualFold(v.Name, s.Name) {
			return core.ErrNameConflict
		}
		if len(s.KeyDigest) > 0 && bytes.Equal(v.KeyDigest, s.KeyDigest) {
			return errors.New("duplicate key digest")
		}
	}
	t.st.sources[ref] = cloneSource(*s)
	return nil
}

func (t *memTx) UpdateSource(s *core.Source) error {
	ref := core.SourceRef{TeamID: s.TeamID, SourceID: s.ID}
	if _, ok := t.st.sources[ref]; !ok {
		return errors.New("unknown source")
	}
	t.st.sources[ref] = cloneSource(*s)
	return nil
}

func (t *memTx) ListSources(teamID int64, after *core.Position, limit int) ([]core.Source, error) {
	var out []core.Source
	for _, v := range t.st.sources {
		if v.TeamID != teamID {
			continue
		}
		if after != nil && (v.CreatedAt.Before(after.CreatedAt) || v.CreatedAt.Equal(after.CreatedAt) && v.ID <= after.ID) {
			continue
		}
		out = append(out, cloneSource(v))
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (t *memTx) AppendSchemaRevision(teamID, sourceID int64, revision int, version string, schema []core.Field) error {
	ref := core.SourceRef{TeamID: teamID, SourceID: sourceID}
	t.st.schemaRevs[ref] = append(t.st.schemaRevs[ref], revision)
	return nil
}

func (t *memTx) AppendSourceRevision(teamID, sourceID int64, revision int, snapshot json.RawMessage) error {
	ref := core.SourceRef{TeamID: teamID, SourceID: sourceID}
	t.st.sourceRevs[ref] = append(t.st.sourceRevs[ref], revision)
	return nil
}

func (t *memTx) DueSources(now time.Time, limit int) ([]core.SourceRef, error) {
	var out []core.SourceRef
	for ref, v := range t.st.sources {
		team, ok := t.st.teams[v.TeamID]
		if !ok || team.ArchivedAt != nil {
			continue
		}
		if v.Mode == "poller" && v.Enabled && v.SettingsStatus == "complete" && v.PollingBlockedAt == nil &&
			v.UnfinishedPollID == nil && v.NextPollAt != nil && !v.NextPollAt.After(now) {
			out = append(out, ref)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TeamID != out[j].TeamID {
			return out[i].TeamID < out[j].TeamID
		}
		return out[i].SourceID < out[j].SourceID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (t *memTx) InsertPoll(p *core.Poll) error {
	ref := core.PollRef{TeamID: p.TeamID, PollID: p.ID}
	if _, ok := t.st.polls[ref]; ok {
		return errors.New("duplicate poll")
	}
	t.st.polls[ref] = *p
	return nil
}

func (t *memTx) Poll(teamID, pollID int64, lock bool) (*core.Poll, error) {
	v, ok := t.st.polls[core.PollRef{TeamID: teamID, PollID: pollID}]
	if !ok {
		return nil, nil
	}
	return &v, nil
}

func (t *memTx) UpdatePoll(p *core.Poll) error {
	ref := core.PollRef{TeamID: p.TeamID, PollID: p.ID}
	if _, ok := t.st.polls[ref]; !ok {
		return errors.New("unknown poll")
	}
	t.st.polls[ref] = *p
	return nil
}

func (t *memTx) ExpiredPolls(now time.Time, limit int) ([]core.PollRef, error) {
	var out []core.PollRef
	for ref, p := range t.st.polls {
		if !p.Finished && !p.ExpiresAt.After(now) {
			out = append(out, ref)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TeamID != out[j].TeamID {
			return out[i].TeamID < out[j].TeamID
		}
		return out[i].PollID < out[j].PollID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (t *memTx) Enqueue(m core.OutboxMessage) error {
	if _, ok := t.st.outbox[m.Seq]; ok {
		return errors.New("duplicate outbox seq")
	}
	t.st.outbox[m.Seq] = m
	t.enqueued = append(t.enqueued, m)
	return nil
}

func (t *memTx) Outbox(limit int) ([]core.OutboxMessage, error) {
	out := make([]core.OutboxMessage, 0, len(t.st.outbox))
	for _, v := range t.st.outbox {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (t *memTx) DeleteOutbox(seq int64) error {
	delete(t.st.outbox, seq)
	return nil
}

// cloneSource deep-copies a Source so stored rows are never aliased.
func cloneSource(s core.Source) core.Source {
	c := s
	c.Description = clonePtr(s.Description)
	c.KeyDigest = append([]byte(nil), s.KeyDigest...)
	if s.KeyDigest == nil {
		c.KeyDigest = nil
	}
	c.Schema = cloneFields(s.Schema)
	c.PreviousSchema = cloneFields(s.PreviousSchema)
	c.SchemaRevision = clonePtr(s.SchemaRevision)
	if s.Values != nil {
		c.Values = make(map[string]json.RawMessage, len(s.Values))
		for k, v := range s.Values {
			c.Values[k] = append(json.RawMessage(nil), v...)
		}
	}
	c.PollURL = clonePtr(s.PollURL)
	c.PollInterval = clonePtr(s.PollInterval)
	c.Cursor = clonePtr(s.Cursor)
	c.UnfinishedPollID = clonePtr(s.UnfinishedPollID)
	c.LastLeasedAt = clonePtr(s.LastLeasedAt)
	c.NextPollAt = clonePtr(s.NextPollAt)
	c.PollingBlockedAt = clonePtr(s.PollingBlockedAt)
	if s.LastPoll != nil {
		lp := *s.LastPoll
		lp.Outcome = clonePtr(s.LastPoll.Outcome)
		lp.ErrorCode = clonePtr(s.LastPoll.ErrorCode)
		c.LastPoll = &lp
	}
	c.LastSeenAt = clonePtr(s.LastSeenAt)
	c.Activity = clonePtr(s.Activity)
	c.ErrorClass = clonePtr(s.ErrorClass)
	if s.ObservedCounters != nil {
		c.ObservedCounters = append(json.RawMessage(nil), s.ObservedCounters...)
	}
	return c
}

func cloneFields(fields []core.Field) []core.Field {
	if fields == nil {
		return nil
	}
	out := make([]core.Field, len(fields))
	for i, f := range fields {
		out[i] = f
		out[i].Description = clonePtr(f.Description)
		out[i].Required = clonePtr(f.Required)
		out[i].Default = append(json.RawMessage(nil), f.Default...)
	}
	return out
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
