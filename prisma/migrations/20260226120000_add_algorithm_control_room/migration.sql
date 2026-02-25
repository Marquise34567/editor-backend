CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS editor_config_versions (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by_user_id TEXT NULL,
  preset_name TEXT NULL,
  params JSONB NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT FALSE,
  note TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_editor_config_versions_created_at
  ON editor_config_versions (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_editor_config_versions_active_created_at
  ON editor_config_versions (is_active, created_at DESC);

CREATE TABLE IF NOT EXISTS render_quality_metrics (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  job_id TEXT NOT NULL,
  user_id TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  config_version_id TEXT NOT NULL,
  score_total NUMERIC NOT NULL,
  score_hook NUMERIC NOT NULL,
  score_pacing NUMERIC NOT NULL,
  score_emotion NUMERIC NOT NULL,
  score_visual NUMERIC NOT NULL,
  score_story NUMERIC NOT NULL,
  score_jank NUMERIC NOT NULL,
  features JSONB NOT NULL,
  flags JSONB NOT NULL DEFAULT '{}'::jsonb
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'render_quality_metrics_job_id_fkey'
  ) THEN
    ALTER TABLE render_quality_metrics
      ADD CONSTRAINT render_quality_metrics_job_id_fkey
      FOREIGN KEY (job_id) REFERENCES jobs(id)
      ON DELETE CASCADE;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'render_quality_metrics_config_version_id_fkey'
  ) THEN
    ALTER TABLE render_quality_metrics
      ADD CONSTRAINT render_quality_metrics_config_version_id_fkey
      FOREIGN KEY (config_version_id) REFERENCES editor_config_versions(id)
      ON DELETE CASCADE;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_render_quality_metrics_created_at
  ON render_quality_metrics (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_render_quality_metrics_config_version_id
  ON render_quality_metrics (config_version_id);

CREATE INDEX IF NOT EXISTS idx_render_quality_metrics_config_version_created_at
  ON render_quality_metrics (config_version_id, created_at DESC);

CREATE TABLE IF NOT EXISTS algorithm_experiments (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by_user_id TEXT NULL,
  name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('draft', 'running', 'stopped')),
  arms JSONB NOT NULL,
  allocation JSONB NOT NULL,
  reward_metric TEXT NOT NULL DEFAULT 'score_total',
  start_at TIMESTAMPTZ NULL,
  end_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_algorithm_experiments_status_created_at
  ON algorithm_experiments (status, created_at DESC);

CREATE TABLE IF NOT EXISTS security_events (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  type TEXT NOT NULL,
  meta JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_security_events_created_at
  ON security_events (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_security_events_type
  ON security_events (type);

ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS config_version_id TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'jobs_config_version_id_fkey'
  ) THEN
    ALTER TABLE jobs
      ADD CONSTRAINT jobs_config_version_id_fkey
      FOREIGN KEY (config_version_id) REFERENCES editor_config_versions(id)
      ON DELETE SET NULL;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_jobs_config_version_id
  ON jobs (config_version_id);
