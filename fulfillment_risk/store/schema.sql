-- Fulfillment-risk operational snapshot. Rerun-safe (CREATE ... IF NOT EXISTS / OR REPLACE).
-- Only the fulfillment-risk OUTCOME is stored here; Salesforce, Databricks and Snowflake stay the systems of record.
--
-- Roles (NOLOGIN groups; grant them to login users or Entra principals):
--   fr_writer : the refresh job - inserts runs/results and publishes
--   fr_reader : consumers (the agent skill) - SELECT on the fr.v_* views only, never the base tables

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fr_writer') THEN CREATE ROLE fr_writer NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fr_reader') THEN CREATE ROLE fr_reader NOLOGIN; END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS fr;

CREATE TABLE IF NOT EXISTS fr.refresh_run (
  run_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  status        text        NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'abandoned')),
  trigger       text        NOT NULL DEFAULT 'manual',          -- manual | schedule | test
  code_version  text        NOT NULL,
  started_at    timestamptz NOT NULL DEFAULT now(),
  finished_at   timestamptz,
  sources       jsonb       NOT NULL DEFAULT '{}'::jsonb,       -- {salesforce: {adapter, as_of, rows}, ...}
  summary       jsonb,                                          -- engine summary (counts, values)
  error         text
);
CREATE INDEX IF NOT EXISTS refresh_run_status_idx ON fr.refresh_run (status, run_id DESC);

CREATE TABLE IF NOT EXISTS fr.customer_risk (
  run_id              bigint  NOT NULL REFERENCES fr.refresh_run ON DELETE CASCADE,
  customer_id         text    NOT NULL,                  -- Salesforce Account Id
  customer_name       text,
  tier                text,
  status              text    NOT NULL,
  open_orders         int     NOT NULL,
  open_lines          int     NOT NULL,
  at_risk_orders      int     NOT NULL,
  short_units         int     NOT NULL,
  open_value          numeric(14,2) NOT NULL,
  short_value         numeric(14,2) NOT NULL,
  blocked_order_value numeric(14,2) NOT NULL,
  worst_order_id      text,
  PRIMARY KEY (run_id, customer_id)
);

CREATE TABLE IF NOT EXISTS fr.order_line_risk (
  run_id              bigint  NOT NULL REFERENCES fr.refresh_run ON DELETE CASCADE,
  order_id            text    NOT NULL,
  line_number         int     NOT NULL,
  customer_id         text    NOT NULL,
  customer_name       text,
  product_sku         text    NOT NULL,
  order_date          date    NOT NULL,
  quantity            int     NOT NULL,
  allocated_qty       int     NOT NULL,
  short_qty           int     NOT NULL,
  unit_net_value      numeric(14,2) NOT NULL,
  line_value          numeric(14,2) NOT NULL,
  short_value         numeric(14,2) NOT NULL,
  allocation_rank     int,
  sku_available_before int,
  status              text    NOT NULL,
  reason              text    NOT NULL,
  PRIMARY KEY (run_id, order_id, line_number)
);
CREATE INDEX IF NOT EXISTS order_line_risk_cust_idx ON fr.order_line_risk (run_id, customer_id);

CREATE TABLE IF NOT EXISTS fr.sku_risk (
  run_id       bigint NOT NULL REFERENCES fr.refresh_run ON DELETE CASCADE,
  product_sku  text   NOT NULL,
  available    int    NOT NULL,
  demanded     int    NOT NULL,
  allocated    int    NOT NULL,
  short_units  int    NOT NULL,
  PRIMARY KEY (run_id, product_sku)
);

-- Exactly one row: the run everyone reads. Moved only inside a successful publish transaction.
CREATE TABLE IF NOT EXISTS fr.published_snapshot (
  singleton     boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  run_id        bigint  NOT NULL REFERENCES fr.refresh_run,
  published_at  timestamptz NOT NULL DEFAULT now()
);

-- ---------- curated read surface (the only objects fr_reader can see) ----------
CREATE OR REPLACE VIEW fr.v_snapshot_status AS
SELECT p.run_id,
       r.finished_at                                         AS snapshot_at,
       p.published_at,
       (r.sources -> 'salesforce' ->> 'as_of')::timestamptz  AS salesforce_as_of,
       (r.sources -> 'databricks' ->> 'as_of')::timestamptz  AS databricks_as_of,
       (r.sources -> 'snowflake'  ->> 'as_of')::timestamptz  AS snowflake_as_of,
       r.summary,
       (SELECT max(finished_at) FROM fr.refresh_run WHERE status = 'failed' AND run_id > p.run_id) AS last_failed_after_snapshot,
       (SELECT count(*) FROM fr.refresh_run WHERE status = 'failed' AND run_id > p.run_id)        AS failures_since_snapshot
FROM fr.published_snapshot p JOIN fr.refresh_run r USING (run_id);

CREATE OR REPLACE VIEW fr.v_fulfillment_risk_current AS
SELECT c.customer_id, c.customer_name, c.tier, c.status, c.open_orders, c.open_lines, c.at_risk_orders,
       c.short_units, c.open_value, c.short_value, c.blocked_order_value, c.worst_order_id,
       s.run_id, s.snapshot_at, s.salesforce_as_of, s.databricks_as_of, s.snowflake_as_of
FROM fr.customer_risk c JOIN fr.v_snapshot_status s ON c.run_id = s.run_id;

CREATE OR REPLACE VIEW fr.v_fulfillment_risk_lines_current AS
SELECT l.order_id, l.line_number, l.customer_id, l.customer_name, l.product_sku, l.order_date, l.quantity,
       l.allocated_qty, l.short_qty, l.unit_net_value, l.line_value, l.short_value, l.allocation_rank,
       l.sku_available_before, l.status, l.reason,
       s.run_id, s.snapshot_at, s.salesforce_as_of, s.databricks_as_of, s.snowflake_as_of
FROM fr.order_line_risk l JOIN fr.v_snapshot_status s ON l.run_id = s.run_id;

CREATE OR REPLACE VIEW fr.v_sku_risk_current AS
SELECT k.product_sku, k.available, k.demanded, k.allocated, k.short_units, s.run_id, s.snapshot_at, s.snowflake_as_of
FROM fr.sku_risk k JOIN fr.v_snapshot_status s ON k.run_id = s.run_id;

-- ---------- privileges ----------
REVOKE ALL ON SCHEMA fr FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA fr FROM PUBLIC;
GRANT USAGE ON SCHEMA fr TO fr_writer, fr_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON fr.refresh_run, fr.customer_risk, fr.order_line_risk, fr.sku_risk,
      fr.published_snapshot TO fr_writer;
GRANT SELECT ON fr.v_snapshot_status, fr.v_fulfillment_risk_current, fr.v_fulfillment_risk_lines_current,
      fr.v_sku_risk_current TO fr_reader, fr_writer;
