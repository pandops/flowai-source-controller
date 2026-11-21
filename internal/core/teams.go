package core

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

// team returns the projected team, fetching it from team-registry when the
// projection has not seen it yet.
func (s *Service) team(ctx context.Context, teamID, requestID int64) (*Team, error) {
	var team *Team
	if err := s.read(ctx, func(tx Tx) error {
		var err error
		team, err = tx.Team(teamID)
		return err
	}); err != nil {
		return nil, err
	}
	if team != nil {
		return team, nil
	}
	rctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	rep, err := s.broker.Request(rctx, broker.Request{
		Subject: "team-registry.service.get.pub",
		Header:  broker.Header{"team": itoa(teamID), "request_id": itoa(requestID)},
		Data:    mustJSON(map[string]any{"params": map[string]any{"team_id": teamID}}),
	})
	switch {
	case errors.Is(err, broker.ErrDisconnected):
		return nil, errBrokerDown
	case err != nil:
		return nil, errOwnerDown
	}
	switch rep.Header["status"] {
	case "200":
	case "404":
		return nil, errNotFound
	default:
		return nil, errOwnerDown
	}
	var body struct {
		TeamID     int64   `json:"team_id"`
		TeamName   string  `json:"team_name"`
		ArchivedAt *string `json:"archived_at"`
	}
	if err := json.Unmarshal(rep.Data, &body); err != nil || body.TeamID != teamID || body.TeamName == "" {
		return nil, errOwnerDown
	}
	fetched := Team{TeamID: teamID, TeamName: body.TeamName}
	if body.ArchivedAt != nil {
		at, err := time.Parse(time.RFC3339Nano, *body.ArchivedAt)
		if err != nil {
			return nil, errOwnerDown
		}
		fetched.ArchivedAt = &at
	}
	err = s.store.Tx(ctx, func(tx Tx) error {
		existing, err := tx.Team(teamID)
		if err != nil || existing != nil {
			if existing != nil {
				fetched = *existing
			}
			return err
		}
		return tx.UpsertTeam(fetched)
	})
	if err != nil {
		return nil, err
	}
	return &fetched, nil
}

// HandleEvent applies one delivery of the source-controller durable consumer.
func (s *Service) HandleEvent(ctx context.Context, d broker.Delivery) broker.Disposition {
	var env struct {
		Publisher    string  `json:"publisher"`
		PublisherSeq int64   `json:"publisher_seq"`
		TeamID       int64   `json:"team_id"`
		Kind         string  `json:"kind"`
		TeamName     *string `json:"team_name"`
		ArchivedAt   *string `json:"archived_at"`
		SourceID     int64   `json:"source_id"`
		Outcome      string  `json:"outcome"`
	}
	if err := json.Unmarshal(d.Data, &env); err != nil || env.Publisher == "" || env.PublisherSeq < 1 || env.TeamID < 1 {
		s.log.Warn("malformed delivery terminated", "subject", d.Subject)
		return broker.Term
	}
	teamEvent := env.Publisher == "team-registry" && (env.Kind == "team_created" || env.Kind == "team_updated" || env.Kind == "team_archived")
	// team-registry's team events carry `team: admin`, the header of the
	// administrator request that caused them; the projection is keyed by the
	// payload team_id. Every other delivery's decimal header must match it.
	if hdr := d.Header["team"]; !teamEvent || hdr != "admin" {
		if team, ok := parseID(hdr); !ok || team != env.TeamID {
			s.log.Warn("delivery team header does not match its payload", "subject", d.Subject)
			return broker.Term
		}
	}
	var apply func(tx Tx) error
	switch {
	case teamEvent:
		if env.TeamName == nil || *env.TeamName == "" {
			return broker.Term
		}
		team := Team{TeamID: env.TeamID, TeamName: *env.TeamName, LastSeq: env.PublisherSeq}
		if env.ArchivedAt != nil {
			at, err := time.Parse(time.RFC3339Nano, *env.ArchivedAt)
			if err != nil {
				return broker.Term
			}
			team.ArchivedAt = &at
		}
		apply = func(tx Tx) error {
			existing, err := tx.Team(team.TeamID)
			if err != nil {
				return err
			}
			if existing != nil && existing.LastSeq > team.LastSeq {
				return nil
			}
			return tx.UpsertTeam(team)
		}
	case env.Publisher == "task-manager" && env.Kind == "intake_processed":
		if env.SourceID < 1 {
			return broker.Term
		}
		apply = func(tx Tx) error {
			src, err := tx.Source(env.TeamID, env.SourceID, true)
			if err != nil || src == nil {
				return err
			}
			switch env.Outcome {
			case "accepted":
				src.Counts.Accepted++
			case "deduplicated":
				src.Counts.Deduplicated++
			case "no_target":
				src.Counts.NoTarget++
			case "rejected", "failed":
				src.Counts.Failed++
			default:
				return nil
			}
			return tx.UpdateSource(src)
		}
		switch env.Outcome {
		case "accepted", "deduplicated", "no_target", "rejected", "failed":
		default:
			return broker.Term
		}
	default:
		s.log.Warn("unexpected delivery terminated", "subject", d.Subject)
		return broker.Term
	}
	err := s.store.Tx(ctx, func(tx Tx) error {
		seen, err := tx.MarkApplied(env.Publisher, env.PublisherSeq)
		if err != nil || seen {
			return err
		}
		return apply(tx)
	})
	if err != nil {
		s.log.Error("delivery not applied; redelivery requested", "subject", d.Subject)
		return broker.Nak
	}
	return broker.Ack
}
