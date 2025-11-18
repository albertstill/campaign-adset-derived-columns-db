CREATE TABLE budget_history(
  id uuid PRIMARY KEY,
  budget_type text NOT NULL
);

CREATE TABLE campaign(
  id uuid PRIMARY KEY,
  name text NOT NULL,
  -- the aggregated ad set columns
  ad_sets_total_count int NOT NULL DEFAULT 0,
  ad_sets_approved_count int NOT NULL DEFAULT 0,
  ad_sets_pending_approval_count int NOT NULL DEFAULT 0,
  ad_sets_completed_count int NOT NULL DEFAULT 0,
  ad_sets_active_start_end_ranges tsrange[] NOT NULL DEFAULT ARRAY[]::tsrange[],
  ad_sets_all_end_date_times timestamp without time zone[] NOT NULL DEFAULT ARRAY[]::timestamp without time zone[],
  ad_sets_is_paused_count int NOT NULL DEFAULT 0
);

CREATE TABLE ad_set(
  id uuid PRIMARY KEY,
  campaign_id uuid NOT NULL REFERENCES campaign(id),
  review_status text NOT NULL,
  start_date_time timestamp without time zone NOT NULL,
  end_date_time timestamp without time zone,
  current_budget_history_id uuid REFERENCES budget_history(id),
  is_paused boolean NOT NULL DEFAULT FALSE
);

CREATE FUNCTION update_campaign_ad_set_columns()
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
      campaign c
    SET
      ad_sets_total_count = agg.total_count,
      ad_sets_approved_count = agg.approved_count,
      ad_sets_pending_approval_count = agg.pending_approval_count,
      ad_sets_completed_count = agg.completed_count,
      ad_sets_active_start_end_ranges = agg.active_start_end_date_times,
      ad_sets_all_end_date_times = agg.all_end_date_times,
      ad_sets_is_paused_count = agg.is_paused_count
    FROM (
      SELECT
        a.campaign_id,
        COUNT(*) AS total_count,
        COUNT(*) FILTER (WHERE a.review_status = 'APPROVED') AS approved_count,
        COUNT(*) FILTER (WHERE a.review_status = 'PENDING_APPROVAL') AS pending_approval_count,
        COUNT(*) FILTER (WHERE a.review_status = 'COMPLETED') AS completed_count,
        -- used for ACTIVE status calculation
        COALESCE(array_agg(tsrange(a.start_date_time, a.end_date_time, '()')) FILTER (WHERE a.start_date_time IS NOT NULL
            AND ((a.end_date_time IS NOT NULL)
          OR (a.end_date_time IS NULL
          AND bh.budget_type = 'DAILY'))
      AND a.review_status IN ('READY', 'APPROVED')), ARRAY[]::tsrange[]) AS active_start_end_date_times,
        -- used for COMPLETED status calculation
        COALESCE(array_agg(a.end_date_time) FILTER (WHERE a.end_date_time IS NOT NULL
            AND a.review_status <> 'ARCHIVED'), ARRAY[]::timestamp WITHOUT time zone[]) AS all_end_date_times,
        COUNT(*) FILTER (WHERE a.is_paused = TRUE) AS is_paused_count
      FROM
        ad_set a
      LEFT JOIN budget_history bh ON a.current_budget_history_id = bh.id
    WHERE
      a.campaign_id = v_campaign_id
    GROUP BY
      a.campaign_id) agg
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
  EXECUTE FUNCTION update_campaign_ad_set_columns();

-- Insert a sample campaign
INSERT INTO campaign(name, id)
VALUES
  ('Active Campaign - Live dates', '550e8400-e29b-41d4-a716-446655440003'),
('Active Campaign - Budget History', '550e8400-e29b-41d4-a716-446655440000'),
('Pending Approval Campaign', '550e8400-e29b-41d4-a716-446655440009'),
('Completed Campaign', '750e8400-e29b-41d4-a716-446655440001');

-- Insert a sample budget history record
INSERT INTO budget_history(id, budget_type)
  VALUES ('750e8400-e29b-41d4-a716-446655440000', 'DAILY');

-- Insert 3 ad sets for the campaign
INSERT INTO ad_set(id, campaign_id, review_status, start_date_time, end_date_time, current_budget_history_id, is_paused)
VALUES
  --
('650e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655440003', 'APPROVED', '2025-01-01 00:00:00', '2025-12-31 23:59:59', NULL, FALSE),
('650e8400-e29b-41d4-a716-446655440003', '550e8400-e29b-41d4-a716-446655440003', 'APPROVED', '2025-02-01 00:00:00', '2025-12-30 23:59:59', NULL, TRUE),
  --
('650e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655440000', 'APPROVED', '2024-01-15 00:00:00', NULL, '750e8400-e29b-41d4-a716-446655440000', FALSE),
  --
('650e8400-e29b-41d4-a716-446655440004', '550e8400-e29b-41d4-a716-446655440009', 'PENDING_APPROVAL', '2024-03-01 00:00:00', '2025-12-30 23:59:59', NULL, FALSE),
  --
('650e8400-e29b-41d4-a716-446655440005', '750e8400-e29b-41d4-a716-446655440001', 'COMPLETED', '2024-03-01 00:00:00', '2024-05-01 00:00:00', NULL, FALSE);

-- Helper function to count how many ad sets are currently active
CREATE FUNCTION count_active_ad_sets(ad_sets_active_start_end_ranges tsrange[])
  RETURNS integer
  AS $$
DECLARE
  date_range tsrange;
  current_timestamp_val timestamp without time zone;
  active_count integer := 0;
BEGIN
  current_timestamp_val := now();
  -- Iterate through each tsrange in the array
  FOREACH date_range IN ARRAY ad_sets_active_start_end_ranges LOOP
    -- Check if current time is within the range using the @> operator
    IF date_range @> current_timestamp_val THEN
      active_count := active_count + 1;
    END IF;
  END LOOP;
  RETURN active_count;
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION all_end_dates_before_now(ad_sets_all_end_date_times timestamp without time zone[])
  RETURNS boolean
  AS $$
DECLARE
  end_date_time timestamp without time zone;
  current_timestamp_val timestamp without time zone;
BEGIN
  current_timestamp_val := now();
  -- If the array is empty, return false
  IF array_length(ad_sets_all_end_date_times, 1) IS NULL THEN
    RETURN FALSE;
  END IF;
  -- Iterate through each end date in the array
  FOREACH end_date_time IN ARRAY ad_sets_all_end_date_times LOOP
    -- Check if the end date is before now
    IF end_date_time >= current_timestamp_val THEN
      RETURN FALSE;
    END IF;
  END LOOP;
  -- All end dates are before now
  RETURN TRUE;
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION has_one_distinct_ad_set_status(p_ad_sets_approved_count int, p_ad_sets_pending_approval_count int, p_ad_sets_completed_count int)
  RETURNS boolean
  AS $$
BEGIN
  RETURN(
    CASE WHEN p_ad_sets_approved_count > 0 THEN
      1
    ELSE
      0
    END + CASE WHEN p_ad_sets_pending_approval_count > 0 THEN
      1
    ELSE
      0
    END + CASE WHEN p_ad_sets_completed_count > 0 THEN
      1
    ELSE
      0
    END) = 1;
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION get_campaign_status(p_ad_sets_total_count int, p_ad_sets_approved_count int, p_ad_sets_pending_approval_count int, p_ad_sets_completed_count int, p_ad_sets_active_start_end_ranges tsrange[], p_ad_sets_all_end_date_times timestamp without time zone[])
  RETURNS text
  AS $$
BEGIN
  RETURN CASE WHEN p_ad_sets_total_count = 0 THEN
    'PENDING_APPROVAL'
  WHEN count_active_ad_sets(p_ad_sets_active_start_end_ranges) > 0 THEN
    'ACTIVE'
  WHEN has_one_distinct_ad_set_status(p_ad_sets_approved_count, p_ad_sets_pending_approval_count, p_ad_sets_completed_count) = TRUE
    AND all_end_dates_before_now(p_ad_sets_all_end_date_times) THEN
    'COMPLETED'
  WHEN p_ad_sets_pending_approval_count > 0 THEN
    'PENDING_APPROVAL'
  ELSE
    'UNKNOWN'
  END;
END;
$$
LANGUAGE plpgsql;

SELECT
  *,
  count_active_ad_sets(ad_sets_active_start_end_ranges) AS active_ad_sets_count,
  get_campaign_status(ad_sets_total_count, ad_sets_approved_count, ad_sets_pending_approval_count, ad_sets_completed_count, ad_sets_active_start_end_ranges, ad_sets_all_end_date_times) AS derived_status
FROM
  campaign;

