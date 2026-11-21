package core

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
)

const (
	maxPages        = 10
	maxPageTasks    = 100
	maxPageBytes    = 1 << 20
	maxCursorLength = 1024
	dueBatch        = 100
	pollWorkers     = 8
	failuresToBlock = 5
)

var (
	// errPollGone stops a poll whose record was already finalized elsewhere.
	errPollGone = errors.New("poll already finalized")
	// errSettingsChanged stops a poll whose Source settings changed after
	// the poll started; its cursor is no longer valid.
	errSettingsChanged = errors.New("settings changed during the poll")
)

// pollResult is what one poll execution observed.
type pollResult struct {
	outcome     *string
	errorCode   *string
	pages       int
	returned    int
	published   int
	rejected    int
	resume      *string
	cursorValid bool
}

func (r *pollResult) fail(code string) {
	r.outcome, r.errorCode = &code, &code
}

// StartDuePolls records a poll for every due Source.
func (s *Service) StartDuePolls(ctx context.Context) ([]Poll, error) {
	var due []SourceRef
	if err := s.read(ctx, func(tx Tx) error {
		now, err := tx.Now()
		if err != nil {
			return err
		}
		due, err = tx.DueSources(now, dueBatch)
		return err
	}); err != nil {
		return nil, err
	}
	var polls []Poll
	var firstErr error
	for _, ref := range due {
		var started *Poll
		err := s.store.Tx(ctx, func(tx Tx) error {
			started = nil
			src, err := tx.Source(ref.TeamID, ref.SourceID, true)
			if err != nil || src == nil {
				return err
			}
			team, err := tx.Team(ref.TeamID)
			if err != nil {
				return err
			}
			now, err := tx.Now()
			if err != nil {
				return err
			}
			if team == nil || team.ArchivedAt != nil || src.Mode != "poller" || !src.Enabled || src.SettingsStatus != "complete" ||
				src.PollingBlockedAt != nil || src.UnfinishedPollID != nil || src.NextPollAt == nil || src.NextPollAt.After(now) ||
				src.PollURL == nil || src.PollInterval == nil {
				return nil
			}
			id, err := tx.NextCounter(ref.TeamID, "poll")
			if err != nil {
				return err
			}
			start := now.Add(s.cfg.PollStartWindow)
			deadline := now.Add(s.cfg.PollCallWindow)
			p := &Poll{TeamID: ref.TeamID, ID: id, SourceID: src.ID, URL: *src.PollURL, Values: mustJSON(effectiveValues(src)),
				SettingsRevision: src.Revision, Generation: src.Generation, Cursor: src.Cursor, ImplementationVersion: src.ImplementationVersion,
				ScheduledAt: *src.NextPollAt, LeasedAt: now, StartBy: start, CallDeadline: deadline, ExpiresAt: deadline.Add(s.cfg.PollExpiryGrace)}
			if err := tx.InsertPoll(p); err != nil {
				return err
			}
			src.UnfinishedPollID, src.LastLeasedAt = &id, &now
			src.NextPollAt = ptr(now.Add(minutes(*src.PollInterval)))
			if err := tx.UpdateSource(src); err != nil {
				return err
			}
			started = p
			return nil
		})
		if err != nil {
			s.log.Error("poll not started", "team_id", ref.TeamID, "source_id", ref.SourceID)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if started != nil {
			polls = append(polls, *started)
		}
	}
	return polls, firstErr
}

// pollClient is the configured client that never follows a redirect.
func (s *Service) pollClient() *http.Client {
	c := *s.cfg.PollClient
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &c
}

type pageBody struct {
	tasks []sourceTask
	next  *string
}

// callPage makes one signed callback and validates the page.
func (s *Service) callPage(ctx context.Context, client *http.Client, p Poll, page int, cursor *string) (pageBody, string) {
	body := mustJSON(map[string]any{"poll_id": p.ID, "page": page, "cursor": strOrNil(cursor), "source_id": p.SourceID,
		"scheduled_at": ts(p.ScheduledAt), "settings_revision": p.SettingsRevision, "settings": p.Values, "implementation_version": p.ImplementationVersion})
	stamp := strconv.FormatInt(s.cfg.Now().Unix(), 10)
	key := s.cfg.PollSigningKeys[0]
	sig := ed25519.Sign(key.Private, append([]byte(stamp+"."+p.URL+"."), body...))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, bytes.NewReader(body))
	if err != nil {
		return pageBody{}, "url_rejected"
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-FlowAI-Poll-Timestamp", stamp)
	req.Header.Set("X-FlowAI-Poll-Signature", "keyid="+key.ID+",sig="+base64.RawURLEncoding.EncodeToString(sig))
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return pageBody{}, "timeout"
		}
		return pageBody{}, "http_error"
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return pageBody{}, "http_error"
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxPageBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return pageBody{}, "timeout"
		}
		return pageBody{}, "http_error"
	}
	if len(raw) > maxPageBytes {
		return pageBody{}, "invalid_response"
	}
	var obj map[string]json.RawMessage
	if decodeStrict(raw, &obj) != nil || obj == nil || len(obj) != 2 {
		return pageBody{}, "invalid_response"
	}
	rawTasks, ok1 := obj["tasks"]
	rawNext, ok2 := obj["next_cursor"]
	if !ok1 || !ok2 {
		return pageBody{}, "invalid_response"
	}
	tasks, bad := parseTaskList(rawTasks, "/tasks", 0, maxPageTasks)
	if len(bad) > 0 {
		return pageBody{}, "invalid_response"
	}
	out := pageBody{tasks: tasks}
	if !isNull(rawNext) {
		var next string
		if json.Unmarshal(rawNext, &next) != nil || next == "" || len(next) > maxCursorLength {
			return pageBody{}, "invalid_response"
		}
		out.next = &next
	}
	return out, ""
}

// publishPage stores a page's tasks as outbox rows (one intake message per
// task) with the poll's progress, then publishes them.
func (s *Service) publishPage(ctx context.Context, p Poll, page int, body pageBody, res *pollResult) error {
	archived, changed := false, false
	err := s.store.Tx(ctx, func(tx Tx) error {
		archived, changed = false, false
		rec, err := tx.Poll(p.TeamID, p.ID, true)
		if err != nil {
			return err
		}
		if rec == nil || rec.Finished {
			return errPollGone
		}
		team, err := tx.Team(p.TeamID)
		if err != nil {
			return err
		}
		if team == nil || team.ArchivedAt != nil {
			archived = true
			return nil
		}
		src, err := tx.Source(p.TeamID, p.SourceID, false)
		if err != nil {
			return err
		}
		changed = src == nil || src.Generation != p.Generation
		now, err := tx.Now()
		if err != nil {
			return err
		}
		for _, t := range body.tasks {
			seq, err := tx.NextPublisherSeq()
			if err != nil {
				return err
			}
			intakeID, err := tx.NextCounter(p.TeamID, "intake")
			if err != nil {
				return err
			}
			msg := map[string]any{"publisher": Publisher, "publisher_seq": seq, "team_id": p.TeamID, "occurred_at": ts(now), "source_id": p.SourceID,
				"origin": "poll", "poll_id": p.ID, "page": page, "implementation_version": p.ImplementationVersion, "accepted_at": ts(now),
				"tasks": []any{intakeEntry(t, intakeID)}}
			if err := enqueue(tx, seq, p.TeamID, "source-controller.task."+itoa(p.TeamID)+".pub", msg, 0); err != nil {
				return err
			}
		}
		rec.PagesRead = res.pages + 1
		rec.Returned = res.returned + len(body.tasks)
		rec.Published = res.published + len(body.tasks)
		rec.ResumeCursor = body.next
		return tx.UpdatePoll(rec)
	})
	if err != nil {
		return err
	}
	if archived {
		return errors.New("team archived")
	}
	if len(body.tasks) > 0 {
		s.notify()
		if _, err := s.FlushOutbox(ctx); err != nil {
			return err
		}
	}
	if changed {
		return errSettingsChanged
	}
	return nil
}

// ExecutePoll calls the poller for one started poll and finalizes it.
func (s *Service) ExecutePoll(ctx context.Context, p Poll) error {
	res := &pollResult{cursorValid: true}
	switch {
	case !s.allowedPollURL(p.URL):
		res.fail("url_rejected")
	case s.cfg.Now().After(p.StartBy):
		res.fail("not_started")
	default:
		s.runPages(ctx, p, res)
	}
	if res.errorCode == nil && res.outcome == nil {
		res.outcome = ptr("succeeded")
	}
	err := s.finalize(context.WithoutCancel(ctx), p.TeamID, p.ID, res)
	if errors.Is(err, errPollGone) {
		return nil
	}
	return err
}

func (s *Service) runPages(ctx context.Context, p Poll, res *pollResult) {
	cctx, cancel := context.WithTimeout(ctx, p.CallDeadline.Sub(s.cfg.Now()))
	defer cancel()
	client := s.pollClient()
	cursor := p.Cursor
	res.resume = p.Cursor
	for page := 1; page <= maxPages; page++ {
		if cctx.Err() != nil {
			res.fail("timeout")
			return
		}
		body, code := s.callPage(cctx, client, p, page, cursor)
		if code != "" {
			// The cursor of the last good page stays the resume cursor (D27).
			res.fail(code)
			return
		}
		if err := s.publishPage(ctx, p, page, body, res); err != nil {
			if errors.Is(err, errSettingsChanged) {
				res.pages++
				res.returned += len(body.tasks)
				res.published += len(body.tasks)
				res.cursorValid = false
				return
			}
			if errors.Is(err, errPollGone) {
				res.cursorValid = false
				res.errorCode = ptr("lease_expired")
				return
			}
			res.cursorValid = false
			res.rejected += len(body.tasks)
			res.errorCode = ptr("intake_rejected")
			return
		}
		res.pages++
		res.returned += len(body.tasks)
		res.published += len(body.tasks)
		res.resume = body.next
		cursor = body.next
		if body.next == nil {
			return
		}
	}
}

// finalize records a poll outcome exactly once.
func (s *Service) finalize(ctx context.Context, teamID, pollID int64, res *pollResult) error {
	return s.store.Tx(ctx, func(tx Tx) error {
		p, err := tx.Poll(teamID, pollID, true)
		if err != nil {
			return err
		}
		if p == nil || p.Finished {
			return errPollGone
		}
		now, err := tx.Now()
		if err != nil {
			return err
		}
		p.Finished = true
		p.PagesRead, p.Returned, p.Published, p.Rejected = res.pages, res.returned, res.published, res.rejected
		if err := tx.UpdatePoll(p); err != nil {
			return err
		}
		status := "succeeded"
		if res.errorCode != nil {
			status = "failed"
		}
		src, err := tx.Source(teamID, p.SourceID, true)
		if err != nil {
			return err
		}
		if src != nil {
			if src.UnfinishedPollID != nil && *src.UnfinishedPollID == p.ID {
				src.UnfinishedPollID = nil
			}
			if res.pages > 0 && res.cursorValid && p.Generation == src.Generation {
				src.Cursor = res.resume
			}
			if status == "succeeded" {
				src.ConsecutiveFailures = 0
			} else if src.ConsecutiveFailures < failuresToBlock {
				src.ConsecutiveFailures++
			}
			src.LastPoll = &LastPoll{PollID: p.ID, StartedAt: p.LeasedAt, Outcome: res.outcome, Status: status, PagesRead: res.pages,
				ReturnedTasks: res.returned, PublishedTasks: res.published, RejectedTasks: res.rejected, ErrorCode: res.errorCode, FinishedAt: now}
			blocked := status == "failed" && src.ConsecutiveFailures >= failuresToBlock && src.PollingBlockedAt == nil
			if blocked {
				src.PollingBlockedAt = &now
				src.Revision++
				if err := tx.AppendSourceRevision(teamID, src.ID, src.Revision, snapshot(src)); err != nil {
					return err
				}
			}
			if err := tx.UpdateSource(src); err != nil {
				return err
			}
			if blocked {
				if err := s.event(tx, teamID, src.ID, "polling_blocked", map[string]any{"poll_id": p.ID, "consecutive_failures": failuresToBlock,
					"polling_blocked_at": ts(now), "revision": src.Revision}); err != nil {
					return err
				}
			}
		}
		return s.event(tx, teamID, p.SourceID, "poll_finalized", map[string]any{"poll_id": p.ID, "outcome": strOrNil(res.outcome), "status": status,
			"pages_read": res.pages, "published_count": res.published, "rejected_count": res.rejected, "error_code": strOrNil(res.errorCode)})
	})
}

// FinalizeExpiredPolls finalizes every poll past expires_at as lease_expired.
func (s *Service) FinalizeExpiredPolls(ctx context.Context) (int, error) {
	var expired []PollRef
	if err := s.read(ctx, func(tx Tx) error {
		now, err := tx.Now()
		if err != nil {
			return err
		}
		expired, err = tx.ExpiredPolls(now, dueBatch)
		return err
	}); err != nil {
		return 0, err
	}
	n := 0
	for _, ref := range expired {
		var rec *Poll
		if err := s.read(ctx, func(tx Tx) error {
			var err error
			rec, err = tx.Poll(ref.TeamID, ref.PollID, false)
			return err
		}); err != nil {
			return n, err
		}
		if rec == nil || rec.Finished {
			continue
		}
		res := &pollResult{errorCode: ptr("lease_expired"), pages: rec.PagesRead, returned: rec.Returned, published: rec.Published,
			resume: rec.ResumeCursor, cursorValid: rec.PagesRead > 0}
		err := s.finalize(ctx, ref.TeamID, ref.PollID, res)
		if errors.Is(err, errPollGone) {
			continue
		}
		if err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// RunPollCycle finalizes expired polls, starts due polls and executes them.
func (s *Service) RunPollCycle(ctx context.Context) error {
	if _, err := s.FinalizeExpiredPolls(ctx); err != nil {
		return err
	}
	polls, err := s.StartDuePolls(ctx)
	var wg sync.WaitGroup
	sem := make(chan struct{}, pollWorkers)
	for _, p := range polls {
		wg.Add(1)
		sem <- struct{}{}
		go func(p Poll) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.ExecutePoll(ctx, p); err != nil {
				s.log.Error("poll not finalized", "team_id", p.TeamID, "poll_id", p.ID)
			}
		}(p)
	}
	wg.Wait()
	return err
}
