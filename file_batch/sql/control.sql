CREATE TABLE IF NOT EXISTS settlement_run (
    run_id uuid PRIMARY KEY,
    run_key text NOT NULL UNIQUE,
    settlement_date date NOT NULL,
    bundle_id text NOT NULL,
    manifest_hash text NOT NULL,
    code_version text NOT NULL,
    expected_revision bigint NOT NULL CHECK(expected_revision>=0),
    status text NOT NULL CHECK(status IN ('READY','RUNNING','VALIDATED','FAILED','PUBLISHED')),
    owner_token uuid,
    lease_until timestamptz,
    attempt integer NOT NULL DEFAULT 0,
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS settlement_attempt (
    run_id uuid REFERENCES settlement_run(run_id),
    attempt integer NOT NULL,
    owner_token uuid NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    error text,
    PRIMARY KEY(run_id,attempt)
);
CREATE TABLE IF NOT EXISTS settlement_publication (
    settlement_date date PRIMARY KEY,
    revision bigint NOT NULL DEFAULT 0,
    run_id uuid REFERENCES settlement_run(run_id),
    finalized boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS settlement_publication_audit (
    id bigserial PRIMARY KEY, settlement_date date NOT NULL,
    revision bigint NOT NULL, action text NOT NULL, reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS settlement_candidate (
    run_id uuid REFERENCES settlement_run(run_id), owner_token uuid NOT NULL,
    settlement_date date NOT NULL, partner_id integer NOT NULL, brand smallint NOT NULL,
    method smallint NOT NULL, gross bigint NOT NULL, cancel bigint NOT NULL,
    net bigint NOT NULL, fee bigint NOT NULL, payout bigint NOT NULL, txn_count bigint NOT NULL,
    PRIMARY KEY(run_id,owner_token,settlement_date,partner_id,brand,method),
    CHECK(net=gross-cancel), CHECK(payout=net-fee),
    CHECK(gross>=0 AND cancel>=0 AND fee>=0 AND txn_count>0)
);
CREATE TABLE IF NOT EXISTS settlement_agg (
    settlement_date date NOT NULL, partner_id integer NOT NULL, brand smallint NOT NULL,
    method smallint NOT NULL, gross bigint NOT NULL, cancel bigint NOT NULL,
    net bigint NOT NULL, fee bigint NOT NULL, payout bigint NOT NULL, txn_count bigint NOT NULL,
    PRIMARY KEY(settlement_date,partner_id,brand,method)
);
