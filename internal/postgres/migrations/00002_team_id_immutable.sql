-- Every team-owned row keeps the team_id it was stored with: the database
-- refuses any update that changes it, whether or not the row has children.

-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION team_id_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.team_id IS DISTINCT FROM OLD.team_id THEN
        RAISE EXCEPTION 'team_id is immutable' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
CREATE TRIGGER sources_team_id_immutable BEFORE UPDATE OF team_id ON sources
    FOR EACH ROW EXECUTE FUNCTION team_id_immutable();
CREATE TRIGGER source_revisions_team_id_immutable BEFORE UPDATE OF team_id ON source_revisions
    FOR EACH ROW EXECUTE FUNCTION team_id_immutable();
CREATE TRIGGER source_schema_revisions_team_id_immutable BEFORE UPDATE OF team_id ON source_schema_revisions
    FOR EACH ROW EXECUTE FUNCTION team_id_immutable();
CREATE TRIGGER polls_team_id_immutable BEFORE UPDATE OF team_id ON polls
    FOR EACH ROW EXECUTE FUNCTION team_id_immutable();

-- +goose Down
DROP TRIGGER polls_team_id_immutable ON polls;
DROP TRIGGER source_schema_revisions_team_id_immutable ON source_schema_revisions;
DROP TRIGGER source_revisions_team_id_immutable ON source_revisions;
DROP TRIGGER sources_team_id_immutable ON sources;
DROP FUNCTION team_id_immutable();
