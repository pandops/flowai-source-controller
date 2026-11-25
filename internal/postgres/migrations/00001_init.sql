-- +goose Up
CREATE SEQUENCE publisher_seq AS bigint START 1;

CREATE TABLE teams (
    team_id     bigint PRIMARY KEY CHECK (team_id > 0),
    team_name   text NOT NULL,
    archived_at timestamptz,
    last_seq    bigint NOT NULL DEFAULT 0
);
CREATE INDEX teams_lower_name ON teams (lower(team_name));

CREATE TABLE team_counters (
    team_id bigint NOT NULL CHECK (team_id > 0),
    kind    text   NOT NULL CHECK (kind IN ('source', 'poll', 'intake')),
    value   bigint NOT NULL,
    PRIMARY KEY (team_id, kind)
);

CREATE TABLE applied_messages (
    publisher     text   NOT NULL,
    publisher_seq bigint NOT NULL,
    applied_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publisher, publisher_seq)
);

CREATE TABLE sources (
    team_id            bigint      NOT NULL CHECK (team_id > 0),
    source_id          bigint      NOT NULL CHECK (source_id > 0),
    name               text        NOT NULL,
    key_digest         bytea,
    mode               text        NOT NULL CHECK (mode IN ('poller', 'webhook')),
    enabled            boolean     NOT NULL,
    settings_status    text        NOT NULL,
    unfinished_poll_id bigint,
    polling_blocked_at timestamptz,
    next_poll_at       timestamptz,
    created_at         timestamptz NOT NULL,
    doc                jsonb       NOT NULL,
    PRIMARY KEY (team_id, source_id)
);
CREATE UNIQUE INDEX sources_team_lower_name ON sources (team_id, lower(name));
CREATE UNIQUE INDEX sources_key_digest ON sources (key_digest) WHERE key_digest IS NOT NULL;
CREATE INDEX sources_list ON sources (team_id, created_at, source_id);
CREATE INDEX sources_due ON sources (next_poll_at)
    WHERE mode = 'poller' AND enabled AND settings_status = 'complete' AND polling_blocked_at IS NULL AND unfinished_poll_id IS NULL;

CREATE TABLE source_schema_revisions (
    team_id                bigint      NOT NULL,
    source_id              bigint      NOT NULL,
    schema_revision        integer     NOT NULL,
    implementation_version text        NOT NULL,
    fields                 jsonb       NOT NULL,
    created_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (team_id, source_id, schema_revision),
    FOREIGN KEY (team_id, source_id) REFERENCES sources (team_id, source_id)
);

CREATE TABLE source_revisions (
    team_id    bigint      NOT NULL,
    source_id  bigint      NOT NULL,
    revision   integer     NOT NULL,
    snapshot   jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (team_id, source_id, revision),
    FOREIGN KEY (team_id, source_id) REFERENCES sources (team_id, source_id)
);

CREATE TABLE polls (
    team_id     bigint      NOT NULL,
    poll_id     bigint      NOT NULL CHECK (poll_id > 0),
    source_id   bigint      NOT NULL,
    expires_at  timestamptz NOT NULL,
    finished_at timestamptz,
    doc         jsonb       NOT NULL,
    PRIMARY KEY (team_id, poll_id),
    FOREIGN KEY (team_id, source_id) REFERENCES sources (team_id, source_id)
);
CREATE INDEX polls_unfinished ON polls (expires_at) WHERE finished_at IS NULL;

CREATE TABLE outbox (
    publisher_seq bigint PRIMARY KEY CHECK (publisher_seq > 0),
    subject       text   NOT NULL,
    header        jsonb  NOT NULL,
    data          bytea  NOT NULL
);

-- +goose Down
DROP TABLE outbox;
DROP TABLE polls;
DROP TABLE source_revisions;
DROP TABLE source_schema_revisions;
DROP TABLE sources;
DROP TABLE applied_messages;
DROP TABLE team_counters;
DROP TABLE teams;
DROP SEQUENCE publisher_seq;
