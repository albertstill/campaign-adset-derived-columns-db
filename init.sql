CREATE TABLE budget_history(
  id uuid PRIMARY KEY,
  budget_type text NOT NULL
);

CREATE TABLE campaign(
  id uuid PRIMARY KEY,
  approved_ad_set_count int NOT NULL DEFAULT 0,
  pending_ad_set_count int NOT NULL DEFAULT 0,
  active_ad_set_start_end_date_times jsonb NOT NULL DEFAULT '[]'::jsonb,
  all_ad_set_end_date_times jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE ad_set(
  id uuid PRIMARY KEY,
  campaign_id uuid NOT NULL REFERENCES campaign(id),
  review_status text NOT NULL,
  start_date_time timestamp without time zone NOT NULL,
  end_date_time timestamp without time zone,
  current_budget_history_id uuid REFERENCES budget_history(id)
);

CREATE FUNCTION update_campaign_ad_set_counts()
  RETURNS TRIGGER
  AS $$
DECLARE
  v_campaign_id_old uuid;
  v_campaign_id_new uuid;
  v_campaign_id uuid;
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
    IF OLD.review_status = NEW.review_status AND OLD.campaign_id = NEW.campaign_id AND OLD.current_budget_history_id = NEW.current_budget_history_id THEN
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
  -- Collect affected campaign IDs
  FOR v_campaign_id IN SELECT DISTINCT
    campaign_id_val
  FROM (
    SELECT
      v_campaign_id_old AS campaign_id_val
    WHERE
      v_campaign_id_old IS NOT NULL
    UNION
    SELECT
      v_campaign_id_new AS campaign_id_val
    WHERE
      v_campaign_id_new IS NOT NULL) AS affected_campaigns LOOP
    UPDATE
      campaign
    SET
      approved_ad_set_count =(
        SELECT
          COUNT(*)
        FROM
          ad_set
        WHERE
          campaign_id = v_campaign_id
          AND review_status = 'APPROVED'),
      pending_ad_set_count =(
        SELECT
          COUNT(*)
        FROM
          ad_set
        WHERE
          campaign_id = v_campaign_id
          AND review_status = 'PENDING'),
      active_ad_set_start_end_date_times =(
        SELECT
          COALESCE(jsonb_agg(jsonb_build_array(start_date_time, end_date_time)), '[]'::jsonb)
        FROM
          ad_set
        WHERE
          campaign_id = v_campaign_id
          AND review_status IN ('READY', 'APPROVED')),
      all_ad_set_end_date_times =(
        SELECT
          COALESCE(jsonb_agg(end_date_time), '[]'::jsonb)
        FROM
          ad_set
        WHERE
          campaign_id = v_campaign_id
          AND review_status <> 'ARCHIVED')
    WHERE
      id = v_campaign_id;
  END LOOP;
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

-- Insert a sample budget history record
INSERT INTO budget_history(id, budget_type)
  VALUES ('750e8400-e29b-41d4-a716-446655440000', 'DAILY');

-- Insert 3 ad sets for the campaign
INSERT INTO ad_set(id, campaign_id, review_status, start_date_time, end_date_time, current_budget_history_id)
VALUES
  ('650e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655440000', 'APPROVED', '2024-01-01 00:00:00', '2024-12-31 23:59:59', NULL),
('650e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655440000', 'PENDING', '2024-01-15 00:00:00', NULL, '750e8400-e29b-41d4-a716-446655440000'),
('650e8400-e29b-41d4-a716-446655440003', '550e8400-e29b-41d4-a716-446655440000', 'APPROVED', '2024-02-01 00:00:00', '2024-11-30 23:59:59', NULL);

SELECT
  *
FROM
  campaign;

