-- Operational signals (Event Hubs -> Azure Function -> here). Context only: nothing in this file is read by the
-- fulfillment-risk calculation. Rerun-safe. Applied together with schema.sql.
--
--   fr_signal_writer : the Function's identity - INSERT/UPDATE the three signal tables, nothing else

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fr_signal_writer') THEN CREATE ROLE fr_signal_writer NOLOGIN; END IF;
END $$;

CREATE TABLE IF NOT EXISTS fr.signal_event (
  event_id          uuid        PRIMARY KEY,               -- idempotency key
  event_type        text        NOT NULL,
  entity_key        text        NOT NULL,                  -- order:<id> | sku:<sku>@<location>
  customer_id       text,
  order_id          text,
  product_sku       text,
  location          text,
  source            text        NOT NULL,
  occurred_at       timestamptz NOT NULL,                  -- at the producer
  enqueued_at       timestamptz,                           -- in Event Hubs
  received_at       timestamptz NOT NULL DEFAULT now(),    -- stored by the Function
  partition_id      text,
  sequence_number   bigint,
  is_late           boolean     NOT NULL DEFAULT false,    -- older than the entity's latest event when it arrived
  duplicate_count   int         NOT NULL DEFAULT 0,        -- extra deliveries of the same event_id
  last_duplicate_at timestamptz,
  payload           jsonb       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS signal_event_received_idx ON fr.signal_event (received_at DESC);
CREATE INDEX IF NOT EXISTS signal_event_entity_idx ON fr.signal_event (entity_key, occurred_at DESC);

CREATE TABLE IF NOT EXISTS fr.signal_rejected (
  id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id        text,
  reason          text        NOT NULL,
  detail          text,
  raw             text,                                    -- first 4 KB of the body
  partition_id    text,
  sequence_number bigint,
  enqueued_at     timestamptz,
  received_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS signal_rejected_received_idx ON fr.signal_rejected (received_at DESC);

-- latest event per entity (by occurred_at, then event_id) - drives out-of-order detection
CREATE TABLE IF NOT EXISTS fr.signal_entity_state (
  entity_key   text PRIMARY KEY,
  event_id     uuid        NOT NULL,
  event_type   text        NOT NULL,
  occurred_at  timestamptz NOT NULL,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- ---------- read surface ----------
CREATE OR REPLACE VIEW fr.v_signal_recent AS
WITH order_state AS (
  SELECT order_id,
         CASE max(CASE status WHEN 'DATA_ISSUE' THEN 4 WHEN 'UNFULFILLABLE' THEN 3 WHEN 'PARTIAL' THEN 2 ELSE 1 END)
           WHEN 4 THEN 'DATA_ISSUE' WHEN 3 THEN 'UNFULFILLABLE' WHEN 2 THEN 'PARTIAL' ELSE 'FULFILLABLE' END AS order_status,
         sum(short_value) AS order_revenue_at_risk
  FROM fr.v_fulfillment_risk_lines_current GROUP BY order_id)
SELECT e.event_id, e.event_type, e.entity_key, e.source, e.occurred_at, e.enqueued_at, e.received_at,
       e.is_late, e.duplicate_count, e.payload,
       coalesce(e.customer_id, l.customer_id)                 AS customer_id,
       coalesce(c.customer_name, l.customer_name)             AS customer_name,
       e.order_id, e.product_sku, e.location,
       os.order_status                                        AS snapshot_order_status,
       os.order_revenue_at_risk                               AS snapshot_order_revenue_at_risk,
       c.status                                               AS snapshot_customer_status,
       k.short_units                                          AS snapshot_sku_short_units,
       extract(epoch FROM e.received_at - e.enqueued_at)::numeric(10,3) AS pipeline_lag_s,
       extract(epoch FROM e.received_at - e.occurred_at)::numeric(12,3) AS end_to_end_s,
       e.received_at > (SELECT snapshot_at FROM fr.v_snapshot_status) AS after_snapshot
FROM fr.signal_event e
LEFT JOIN order_state os ON os.order_id = e.order_id
LEFT JOIN LATERAL (SELECT customer_id, customer_name FROM fr.v_fulfillment_risk_lines_current x
                   WHERE x.order_id = e.order_id LIMIT 1) l ON true
LEFT JOIN fr.v_fulfillment_risk_current c ON c.customer_id = coalesce(e.customer_id, l.customer_id)
LEFT JOIN fr.v_sku_risk_current k ON k.product_sku = e.product_sku
WHERE e.received_at > now() - interval '7 days';

CREATE OR REPLACE VIEW fr.v_signal_summary AS
SELECT
  (SELECT count(*) FROM fr.signal_event   WHERE received_at > now() - interval '1 hour')   AS accepted_1h,
  (SELECT count(*) FROM fr.signal_event   WHERE received_at > now() - interval '24 hours') AS accepted_24h,
  (SELECT count(*) FROM fr.signal_rejected WHERE received_at > now() - interval '1 hour')  AS rejected_1h,
  (SELECT count(*) FROM fr.signal_rejected WHERE received_at > now() - interval '24 hours') AS rejected_24h,
  (SELECT coalesce(sum(duplicate_count), 0) FROM fr.signal_event
     WHERE last_duplicate_at > now() - interval '1 hour')                                  AS duplicates_1h,
  (SELECT coalesce(sum(duplicate_count), 0) FROM fr.signal_event
     WHERE last_duplicate_at > now() - interval '24 hours')                                AS duplicates_24h,
  (SELECT count(*) FROM fr.signal_event WHERE is_late AND received_at > now() - interval '1 hour')   AS late_1h,
  (SELECT count(*) FROM fr.signal_event WHERE is_late AND received_at > now() - interval '24 hours') AS late_24h,
  (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - enqueued_at))
     FROM fr.signal_event WHERE received_at > now() - interval '1 hour' AND enqueued_at IS NOT NULL)  AS lag_p50_s_1h,
  (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - enqueued_at))
     FROM fr.signal_event WHERE received_at > now() - interval '1 hour' AND enqueued_at IS NOT NULL)  AS lag_p95_s_1h,
  (SELECT max(received_at) FROM fr.signal_event)                                           AS last_accepted_at,
  (SELECT max(received_at) FROM fr.signal_rejected)                                        AS last_rejected_at,
  (SELECT coalesce(jsonb_object_agg(event_type, n), '{}'::jsonb) FROM
     (SELECT event_type, count(*) n FROM fr.signal_event WHERE received_at > now() - interval '24 hours'
      GROUP BY 1) t)                                                                       AS by_type_24h;

CREATE OR REPLACE VIEW fr.v_signal_rejected_recent AS
SELECT id, event_id, reason, detail, left(raw, 300) AS raw_preview, partition_id, sequence_number, enqueued_at,
       received_at
FROM fr.signal_rejected WHERE received_at > now() - interval '7 days';

-- ---------- privileges ----------
REVOKE ALL ON fr.signal_event, fr.signal_rejected, fr.signal_entity_state FROM PUBLIC;
GRANT USAGE ON SCHEMA fr TO fr_signal_writer;
GRANT SELECT, INSERT, UPDATE ON fr.signal_event, fr.signal_entity_state TO fr_signal_writer;
GRANT SELECT, INSERT ON fr.signal_rejected TO fr_signal_writer;
GRANT SELECT ON fr.v_signal_recent, fr.v_signal_summary, fr.v_signal_rejected_recent TO fr_reader;
