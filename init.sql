CREATE TABLE campaign(
  id uuid PRIMARY KEY,
  approved_ad_set_count int NOT NULL DEFAULT 0,
  pending_ad_set_count int NOT NULL DEFAULT 0
);

CREATE TABLE ad_set(
  id uuid PRIMARY KEY,
  campaign_id uuid NOT NULL REFERENCES campaign(id),
  review_status text NOT NULL,
  start_date_time timestamp without time zone NOT NULL,
  end_date_time timestamp without time zone
);

CREATE FUNCTION update_campaign_ad_set_counts()
  RETURNS TRIGGER
  AS $$
DECLARE
  v_campaign_id_old uuid;
  v_campaign_id_new uuid;
BEGIN
  -- Determine which campaign(s) are affected
  IF (TG_OP = 'DELETE') THEN
    -- On DELETE, only the OLD campaign is affected
    v_campaign_id_old := OLD.campaign_id;
  ELSIF (TG_OP = 'INSERT') THEN
    -- On INSERT, only the NEW campaign is affected
    v_campaign_id_new := NEW.campaign_id;
  ELSIF (TG_OP = 'UPDATE') THEN
    -- On UPDATE, check if the status or campaign link changed
    IF OLD.review_status = NEW.review_status AND OLD.campaign_id = NEW.campaign_id THEN
      -- Nothing we care about changed, so exit
      RETURN NEW;
    END IF;
    -- The status or campaign_id changed.
    -- Both OLD and NEW campaigns might be affected
    v_campaign_id_old := OLD.campaign_id;
    v_campaign_id_new := NEW.campaign_id;
  END IF;
  -- Recalculate counts for the OLD campaign_id
  -- (This runs on DELETE or if campaign_id changed on UPDATE)
  IF v_campaign_id_old IS NOT NULL THEN
    UPDATE
      campaign
    SET
      approved_ad_set_count =(
        SELECT
          COUNT(*)
        FROM
          ad_set
        WHERE
          campaign_id = v_campaign_id_old
          AND review_status = 'APPROVED'),
      pending_ad_set_count =(
        SELECT
          COUNT(*)
        FROM
          ad_set
        WHERE
          campaign_id = v_campaign_id_old
          AND review_status = 'PENDING')
    WHERE
      id = v_campaign_id_old;
  END IF;
  -- Recalculate counts for the NEW campaign_id
  -- (This runs on INSERT or if campaign_id changed on UPDATE)
  -- We check if it's different from the old one to avoid updating the same row twice
  IF v_campaign_id_new IS NOT NULL AND (v_campaign_id_old IS NULL OR v_campaign_id_new <> v_campaign_id_old) THEN
    UPDATE
      campaign
    SET
      approved_ad_set_count =(
        SELECT
          COUNT(*)
        FROM
          ad_set
        WHERE
          campaign_id = v_campaign_id_new
          AND review_status = 'APPROVED'),
      pending_ad_set_count =(
        SELECT
          COUNT(*)
        FROM
          ad_set
        WHERE
          campaign_id = v_campaign_id_new
          AND review_status = 'PENDING')
    WHERE
      id = v_campaign_id_new;
  END IF;
  -- Return the appropriate record
  IF (TG_OP = 'DELETE') THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER trg_ad_set_count_update
  AFTER INSERT OR UPDATE OR DELETE ON ad_set
  FOR EACH ROW
  EXECUTE FUNCTION update_campaign_ad_set_counts();

-- Insert a sample campaign
INSERT INTO campaign(id)
  VALUES ('550e8400-e29b-41d4-a716-446655440000');

-- Insert 3 ad sets for the campaign
-- Insert 3 ad sets for the campaign
INSERT INTO ad_set(id, campaign_id, review_status, start_date_time, end_date_time)
VALUES
  ('650e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655440000', 'APPROVED', '2024-01-01 00:00:00', '2024-12-31 23:59:59'),
('650e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655440000', 'PENDING', '2024-01-15 00:00:00', NULL),
('650e8400-e29b-41d4-a716-446655440003', '550e8400-e29b-41d4-a716-446655440000', 'APPROVED', '2024-02-01 00:00:00', '2024-11-30 23:59:59');

SELECT
  *
FROM
  campaign;

