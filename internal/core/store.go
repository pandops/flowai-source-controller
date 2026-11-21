package core

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ErrNameConflict is returned by Tx.InsertSource for a duplicate
// (team_id, lower(name)).
var ErrNameConflict = errors.New("source name conflict")

// Team is source-controller's projection of one team-registry team.
type Team struct {
	TeamID     int64
	TeamName   string
	ArchivedAt *time.Time
	// LastSeq is the team-registry publisher_seq that last changed the row;
	// 0 when the row came from team-registry.service.get.
	LastSeq int64
}

// Field is one registered settings field.
type Field struct {
	Name        string          `json:"name"`
	Label       string          `json:"label"`
	Description *string         `json:"description,omitempty"`
	Type        string          `json:"type"`
	Required    *bool           `json:"required"`
	Default     json.RawMessage `json:"default"`
}

// IntakeCounts are the per-Source intake counts.
type IntakeCounts struct {
	Published    int64 `json:"published"`
	Rejected     int64 `json:"rejected"`
	Accepted     int64 `json:"accepted"`
	Deduplicated int64 `json:"deduplicated"`
	NoTarget     int64 `json:"no_target"`
	Failed       int64 `json:"failed"`
}

// LastPoll is the safe summary of the latest finalized poll.
type LastPoll struct {
	PollID         int64     `json:"poll_id"`
	StartedAt      time.Time `json:"started_at"`
	Outcome        *string   `json:"outcome"`
	Status         string    `json:"status"`
	PagesRead      int       `json:"pages_read"`
	ReturnedTasks  int       `json:"returned_tasks"`
	PublishedTasks int       `json:"published_tasks"`
	RejectedTasks  int       `json:"rejected_tasks"`
	ErrorCode      *string   `json:"error_code"`
	FinishedAt     time.Time `json:"finished_at"`
}

// Source is one stored Source with everything source-controller keeps for it.
type Source struct {
	TeamID                int64
	ID                    int64
	Name                  string
	Mode                  string
	ContractVersion       string
	ImplementationVersion string
	Description           *string
	FilterSummary         string
	Enabled               bool
	KeyDigest             []byte
	Revision              int
	SettingsStatus        string
	CreatedAt             time.Time
	Schema                []Field
	PreviousSchema        []Field
	SchemaRevision        *int
	Values                map[string]json.RawMessage
	PollURL               *string
	PollInterval          *int
	Generation            int64
	Cursor                *string
	UnfinishedPollID      *int64
	LastLeasedAt          *time.Time
	NextPollAt            *time.Time
	ConsecutiveFailures   int
	PollingBlockedAt      *time.Time
	LastPoll              *LastPoll
	LastSeenAt            *time.Time
	Activity              *string
	ErrorClass            *string
	ObservedCounters      json.RawMessage
	Counts                IntakeCounts
}

// Poll is one recorded poll with its frozen inputs.
type Poll struct {
	TeamID                int64
	ID                    int64
	SourceID              int64
	URL                   string
	Values                json.RawMessage
	SettingsRevision      int
	Generation            int64
	Cursor                *string
	ImplementationVersion string
	ScheduledAt           time.Time
	LeasedAt              time.Time
	StartBy               time.Time
	CallDeadline          time.Time
	ExpiresAt             time.Time
	PagesRead             int
	Returned              int
	Published             int
	Rejected              int
	ResumeCursor          *string
	Finished              bool
}

// SourceRef names one Source.
type SourceRef struct{ TeamID, SourceID int64 }

// PollRef names one poll.
type PollRef struct{ TeamID, PollID int64 }

// Position is a (created_at, id) list position.
type Position struct {
	CreatedAt time.Time
	ID        int64
}

// OutboxMessage is one stream message waiting for its JetStream ack.
type OutboxMessage struct {
	Seq     int64
	Subject string
	Header  map[string]string
	Data    []byte
}

// Store is source-controller's persistence. Methods on Store run outside a
// transaction (autocommit); Tx runs fn in one transaction and rolls it back
// when fn returns an error.
type Store interface {
	Tx(ctx context.Context, fn func(Tx) error) error
	// NextPublisherSeq allocates one publisher_seq in its own statement.
	NextPublisherSeq(ctx context.Context) (int64, error)
	// NextIntakeIDs allocates n consecutive per-team intake IDs in its own
	// statement.
	NextIntakeIDs(ctx context.Context, teamID int64, n int) ([]int64, error)
	Ping(ctx context.Context) error
}

// Tx is one database transaction. Lock arguments take the row lock
// (SELECT ... FOR UPDATE) that serializes writers of that Source.
type Tx interface {
	Now() (time.Time, error)
	NextPublisherSeq() (int64, error)
	// NextCounter allocates the next per-team number of kind "source" or "poll".
	NextCounter(teamID int64, kind string) (int64, error)
	Team(teamID int64) (*Team, error)
	TeamByName(name string) (*Team, error)
	UpsertTeam(team Team) error
	// MarkApplied records a consumed message; it reports true when the
	// (publisher, publisher_seq) pair was already applied.
	MarkApplied(publisher string, seq int64) (bool, error)
	Source(teamID, sourceID int64, lock bool) (*Source, error)
	SourceByDigest(digest []byte, lock bool) (*Source, error)
	InsertSource(s *Source) error
	UpdateSource(s *Source) error
	ListSources(teamID int64, after *Position, limit int) ([]Source, error)
	AppendSchemaRevision(teamID, sourceID int64, revision int, version string, schema []Field) error
	AppendSourceRevision(teamID, sourceID int64, revision int, snapshot json.RawMessage) error
	DueSources(now time.Time, limit int) ([]SourceRef, error)
	InsertPoll(p *Poll) error
	Poll(teamID, pollID int64, lock bool) (*Poll, error)
	UpdatePoll(p *Poll) error
	ExpiredPolls(now time.Time, limit int) ([]PollRef, error)
	Enqueue(m OutboxMessage) error
	Outbox(limit int) ([]OutboxMessage, error)
	DeleteOutbox(seq int64) error
}
