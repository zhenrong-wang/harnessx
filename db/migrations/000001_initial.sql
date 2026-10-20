CREATE SCHEMA IF NOT EXISTS harness_core;
CREATE SCHEMA IF NOT EXISTS event_core;
CREATE SCHEMA IF NOT EXISTS volume_core;

CREATE TABLE IF NOT EXISTS harness_core.runs (
    id                  text PRIMARY KEY,
    tenant_id           text NOT NULL,
    status              integer NOT NULL,
    model               text NOT NULL DEFAULT '',
    output_json         jsonb NOT NULL DEFAULT '[]'::jsonb,
    usage_json          jsonb,
    error_json          jsonb,
    policy_snapshot     jsonb NOT NULL,
    metadata_json       jsonb NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key     text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    completed_at        timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS runs_tenant_idempotency_key
    ON harness_core.runs (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';
CREATE INDEX IF NOT EXISTS runs_tenant_created_at
    ON harness_core.runs (tenant_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS harness_core.steps (
    id                  text PRIMARY KEY,
    tenant_id           text NOT NULL,
    run_id              text NOT NULL REFERENCES harness_core.runs(id) ON DELETE CASCADE,
    sequence            integer NOT NULL,
    kind                text NOT NULL,
    status              text NOT NULL,
    input_json          jsonb,
    output_json         jsonb,
    error_json          jsonb,
    started_at          timestamptz NOT NULL DEFAULT now(),
    completed_at        timestamptz,
    UNIQUE (run_id, sequence)
);

CREATE TABLE IF NOT EXISTS harness_core.event_outbox (
    id                  text PRIMARY KEY,
    tenant_id           text NOT NULL,
    run_id              text NOT NULL REFERENCES harness_core.runs(id) ON DELETE CASCADE,
    step_id             text NOT NULL DEFAULT '',
    type                text NOT NULL,
    payload_json        jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata_json       jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at         timestamptz NOT NULL,
    delivered_at        timestamptz,
    attempts            integer NOT NULL DEFAULT 0,
    last_error          text
);
CREATE INDEX IF NOT EXISTS event_outbox_pending
    ON harness_core.event_outbox (occurred_at, id)
    WHERE delivered_at IS NULL;

CREATE TABLE IF NOT EXISTS event_core.events (
    id                  text PRIMARY KEY,
    tenant_id           text NOT NULL,
    run_id              text NOT NULL,
    step_id             text NOT NULL DEFAULT '',
    sequence            bigint NOT NULL,
    type                text NOT NULL,
    payload_json        jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata_json       jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at         timestamptz NOT NULL,
    UNIQUE (tenant_id, run_id, sequence)
);
CREATE INDEX IF NOT EXISTS events_tenant_run_sequence
    ON event_core.events (tenant_id, run_id, sequence);

CREATE TABLE IF NOT EXISTS volume_core.volumes (
    id                  text PRIMARY KEY,
    tenant_id           text NOT NULL,
    name                text NOT NULL,
    backend             text NOT NULL,
    metadata_json       jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);
CREATE INDEX IF NOT EXISTS volumes_tenant_created_at
    ON volume_core.volumes (tenant_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS volume_core.objects (
    volume_id           text NOT NULL REFERENCES volume_core.volumes(id) ON DELETE CASCADE,
    tenant_id           text NOT NULL,
    path                text NOT NULL,
    directory           boolean NOT NULL DEFAULT false,
    size_bytes          bigint NOT NULL DEFAULT 0,
    digest              text NOT NULL DEFAULT '',
    mime_type           text NOT NULL DEFAULT 'application/octet-stream',
    modified_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (volume_id, path)
);
CREATE INDEX IF NOT EXISTS objects_tenant_volume_path
    ON volume_core.objects (tenant_id, volume_id, path);
