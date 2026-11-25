# source-controller

Owner of task Sources: poller and webhook Sources with their immutable
per-team integer `source_id`, URL-safe name (unique per team, ignoring case),
API key digest, settings schema and revisions, settings overrides, poll
schedule and failure block, health, and per-Source intake counts. The behavior
is specified under `openspec/specs/source-controller/` in FlowAI's Gitea
coordination repository.

## Interfaces

### NATS requests

source-controller answers requests in the queue group `source-controller` on
`source-controller.ui.<op>.sub` (API Gateway, headers `team`, `operator_id`,
`request_id`) and `source-controller.service.<op>.sub` (other services,
headers `team`, `request_id`). Callers publish to the matching `.pub` subject.

| Op                                 | Backs                                                 |
| ---------------------------------- | ----------------------------------------------------- |
| `ui.sources-create`                | create a Source; the key is shown once                |
| `ui.sources-list`                  | paginated list with an opaque cursor                  |
| `ui.sources-get`                   | one Source with health and counts                     |
| `ui.sources-settings-get`          | settings, schema and `ETag`                           |
| `ui.sources-settings-put`          | save settings with `If-Match`                         |
| `ui.sources-update-implementation` | replace the poll URL or interval                      |
| `ui.sources-rotate-key`            | issue a new key; the old one stops working            |
| `ui.sources-disable`               | disable a Source                                      |
| `ui.sources-unblock-polling`       | clear the five-failure poll block                     |
| `service.sources-lookup`           | Source facts for other services                       |
| `service.binding-guard`            | check expected revisions before a Flow binding change |

The request body is `{"params":{...},"body":{...}}`. The reply has the
header `status` and the REST body, or `{code, message, request_id, fields?}`.
`If-Match` is a request header; `ETag` and `Cache-Control` are reply headers.

### HTTP (one listener, `:8080`)

- `POST /webhook/{team_name}/{source_name}/task` (one task) and
  `.../tasks` (a batch): webhook intake, authenticated by the Source key. The
  reply is 202 only after the broker acknowledges the intake message.
- `GET /v1/sources/self`, `GET /v1/sources/self/settings`,
  `PUT /v1/sources/self/settings-schema`, `PUT /v1/sources/self/status`:
  the Source system's own routes, authenticated by its key.
- `GET /v1/sources/poll-signing-keys`: the public keys that verify signed
  poll calls.
- `/v1/livez` and `/v1/readyz`.

### Streams

Every change is written to the outbox in the same transaction and published
after commit, in `publisher_seq` order, with
`Nats-Msg-Id: source-controller-<publisher_seq>` and the header `team`:

- `source-controller.task.<team_id>.pub` carries one message per webhook
  request and one per polled task.
- `source-controller.event.*.pub` carries `schema_registered`,
  `source_status_reported`, `webhook_accepted`, `webhook_rejected`,
  `poll_finalized` and `polling_blocked`.
- `source-controller.audit.*.pub` carries operator actions on Sources.

The durable consumer `source-controller` on `ALL_EVENT` applies team events
from team-registry and `intake_processed` from task-manager. Each message is
applied once, deduplicated by its message ID. The JSON Schemas of all
requests, replies and messages are in `schemas/`, and the consumer is
declared in `schemas/consumers.json`.

## Runtime

- Image: `Containerfile`. Entrypoint:
  `/source-controller --config /etc/flowai/config.yaml`.
- Configuration: `config.example.yaml` lists every key.
  - NATS must use `tls://`, with the user credentials at
    `/run/flowai/nats/user.creds`.
  - PostgreSQL uses `sslmode=verify-full`.
  - Poll calls trust only `poll.ca_file`, go only to `poll.allowed_origins`,
    and do not follow redirects.
- PostgreSQL: the service uses only the schema `db.schema` (default
  `source_controller_schema`). The migration runs `CREATE SCHEMA` only when
  the schema is missing.
- Readiness: `/v1/readyz` reports ready once the schema is migrated, the
  database and broker are reachable, and the durable consumer exists.

## Tests

```sh
FLOWAI_CONTRACT_FIXTURE_DIR=/absolute/path/to/contract-fixtures go test -mod=readonly ./...
go test -mod=readonly -tags=integration ./...
```

Unit tests run the service over an in-memory store and a fake broker.
The contract tests read these files from the supplied directory.

- `team-registry/event.team_updated.schema.json`
- `task-manager/event.intake_processed.schema.json`
- `task-manager/defs.schema.json`
- `shared-test-inputs.json` (FlowAI's schema-to-consumer declaration map)

Use the selected publisher revisions and coordination declaration. The fixture
directory is supplied by the caller and stays outside this repository.
Integration tests use a disposable PostgreSQL container via Docker or Podman.
