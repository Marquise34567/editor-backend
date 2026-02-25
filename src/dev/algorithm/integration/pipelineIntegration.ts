import { prisma } from '../../../db/prisma'
import { getActiveConfigVersion, getConfigVersionById } from '../config/configService'
import { selectConfigVersionForNewJob } from '../experiments/experimentService'
import { evaluateRetentionScoring } from '../scoring/retentionScoring'
import { RenderQualityMetric } from '../types'

const canRunRawSql = () =>
  typeof (prisma as any)?.$executeRawUnsafe === 'function' &&
  typeof (prisma as any)?.$queryRawUnsafe === 'function'

let infraEnsured = false
const memoryMetrics: RenderQualityMetric[] = []

const ensureInfra = async () => {
  if (!canRunRawSql() || infraEnsured) return
  await (prisma as any).$executeRawUnsafe(`
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
    )
  `)
  await (prisma as any).$executeRawUnsafe(
    `
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
    `
  )
  await (prisma as any).$executeRawUnsafe(
    `
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
    `
  )
  await (prisma as any).$executeRawUnsafe(
    'CREATE INDEX IF NOT EXISTS idx_render_quality_metrics_created_at ON render_quality_metrics (created_at DESC)'
  )
  await (prisma as any).$executeRawUnsafe(
    'CREATE INDEX IF NOT EXISTS idx_render_quality_metrics_config_version_id ON render_quality_metrics (config_version_id)'
  )
  await (prisma as any).$executeRawUnsafe(
    'CREATE INDEX IF NOT EXISTS idx_render_quality_metrics_config_version_created_at ON render_quality_metrics (config_version_id, created_at DESC)'
  )
  infraEnsured = true
}

const normalizeMetric = (row: any): RenderQualityMetric => ({
  id: String(row?.id || ''),
  job_id: String(row?.job_id || ''),
  user_id: row?.user_id ? String(row.user_id) : null,
  created_at: row?.created_at ? new Date(row.created_at).toISOString() : new Date().toISOString(),
  config_version_id: String(row?.config_version_id || ''),
  score_total: Number(row?.score_total || 0),
  score_hook: Number(row?.score_hook || 0),
  score_pacing: Number(row?.score_pacing || 0),
  score_emotion: Number(row?.score_emotion || 0),
  score_visual: Number(row?.score_visual || 0),
  score_story: Number(row?.score_story || 0),
  score_jank: Number(row?.score_jank || 0),
  features: (row?.features || {}) as any,
  flags: (row?.flags || {}) as Record<string, unknown>
})

const parseRangeToMs = (range: string) => {
  const normalized = String(range || '7d').trim().toLowerCase()
  if (/^\d+h$/.test(normalized)) return Number(normalized.replace('h', '')) * 60 * 60 * 1000
  if (/^\d+d$/.test(normalized)) return Number(normalized.replace('d', '')) * 24 * 60 * 60 * 1000
  if (/^\d+w$/.test(normalized)) return Number(normalized.replace('w', '')) * 7 * 24 * 60 * 60 * 1000
  return 7 * 24 * 60 * 60 * 1000
}

const resolveConfigVersionIdForJob = async (job: any): Promise<string> => {
  const fromJob = String(job?.configVersionId || job?.config_version_id || '').trim()
  if (fromJob) return fromJob
  const fromRenderSettings = String((job?.renderSettings as any)?.algorithm_config_version_id || '').trim()
  if (fromRenderSettings) return fromRenderSettings
  const fromAnalysis = String((job?.analysis as any)?.algorithm_config_version_id || '').trim()
  if (fromAnalysis) return fromAnalysis
  const active = await getActiveConfigVersion()
  return active.id
}

const resolveTranscriptPayload = (analysis: any) => {
  if (!analysis || typeof analysis !== 'object') return null
  const raw =
    analysis.transcript ||
    analysis.transcript_cues ||
    analysis.captions ||
    analysis.subtitle_cues ||
    analysis.editPlan?.transcriptSignals ||
    null
  return raw
}

const resolveCutListPayload = (analysis: any) => {
  if (!analysis || typeof analysis !== 'object') return []
  if (Array.isArray(analysis?.editPlan?.segments)) return analysis.editPlan.segments
  if (Array.isArray(analysis?.metadata_summary?.segments)) return analysis.metadata_summary.segments
  return []
}

export const chooseConfigForJobCreation = async () => {
  const selected = await selectConfigVersionForNewJob()
  const config = await getConfigVersionById(selected.config_version_id)
  if (!config) {
    const fallback = await getActiveConfigVersion()
    return {
      config_version_id: fallback.id,
      experiment_id: null,
      source: 'active' as const
    }
  }
  return selected
}

export const computeAndStoreRenderQualityMetric = async ({
  job
}: {
  job: any
}): Promise<RenderQualityMetric | null> => {
  if (!job?.id) return null

  const analysis = (job.analysis as any) || {}
  const transcriptPayload = resolveTranscriptPayload(analysis)
  const cutListPayload = resolveCutListPayload(analysis)
  const configVersionId = await resolveConfigVersionIdForJob(job)
  const configVersion = await getConfigVersionById(configVersionId)
  if (!configVersion) return null

  const scoring = evaluateRetentionScoring(analysis, transcriptPayload, cutListPayload, configVersion.params)

  const metricPayload: RenderQualityMetric = {
    id: `metric_${job.id}_${Date.now()}`,
    job_id: String(job.id),
    user_id: job.userId ? String(job.userId) : null,
    created_at: new Date().toISOString(),
    config_version_id: configVersion.id,
    score_total: Number(scoring.score_total.toFixed(4)),
    score_hook: Number(scoring.subscores.H.toFixed(4)),
    score_pacing: Number(scoring.subscores.P.toFixed(4)),
    score_emotion: Number(scoring.subscores.E.toFixed(4)),
    score_visual: Number(scoring.subscores.V.toFixed(4)),
    score_story: Number(scoring.subscores.S.toFixed(4)),
    score_jank: Number(scoring.subscores.J.toFixed(4)),
    features: scoring.features,
    flags: {
      ...scoring.flags,
      missing_signals: scoring.features.missing_signals
    }
  }

  if (!canRunRawSql()) {
    memoryMetrics.unshift(metricPayload)
    if (memoryMetrics.length > 5_000) memoryMetrics.length = 5_000
    return metricPayload
  }

  try {
    await ensureInfra()
    const rows = await (prisma as any).$queryRawUnsafe(
      `
        INSERT INTO render_quality_metrics (
          id,
          job_id,
          user_id,
          config_version_id,
          score_total,
          score_hook,
          score_pacing,
          score_emotion,
          score_visual,
          score_story,
          score_jank,
          features,
          flags
        )
        VALUES (
          gen_random_uuid()::text,
          $1,
          $2,
          $3,
          $4,
          $5,
          $6,
          $7,
          $8,
          $9,
          $10,
          $11::jsonb,
          $12::jsonb
        )
        RETURNING id, job_id, user_id, created_at, config_version_id, score_total, score_hook, score_pacing, score_emotion, score_visual, score_story, score_jank, features, flags
      `,
      metricPayload.job_id,
      metricPayload.user_id,
      metricPayload.config_version_id,
      metricPayload.score_total,
      metricPayload.score_hook,
      metricPayload.score_pacing,
      metricPayload.score_emotion,
      metricPayload.score_visual,
      metricPayload.score_story,
      metricPayload.score_jank,
      JSON.stringify(metricPayload.features),
      JSON.stringify(metricPayload.flags)
    )
    const inserted = Array.isArray(rows) && rows[0] ? normalizeMetric(rows[0]) : metricPayload
    memoryMetrics.unshift(inserted)
    if (memoryMetrics.length > 5_000) memoryMetrics.length = 5_000
    return inserted
  } catch (error) {
    console.error('[algorithm-metrics] insert failed', error)
    memoryMetrics.unshift(metricPayload)
    if (memoryMetrics.length > 5_000) memoryMetrics.length = 5_000
    return metricPayload
  }
}

export const listRecentRenderMetrics = async (limit = 50): Promise<RenderQualityMetric[]> => {
  const safeLimit = Math.max(1, Math.min(200, Math.round(Number(limit || 50))))
  if (!canRunRawSql()) return memoryMetrics.slice(0, safeLimit)

  await ensureInfra()
  const rows = await (prisma as any).$queryRawUnsafe(
    `
      SELECT id, job_id, user_id, created_at, config_version_id, score_total, score_hook, score_pacing, score_emotion, score_visual, score_story, score_jank, features, flags
      FROM render_quality_metrics
      ORDER BY created_at DESC
      LIMIT $1
    `,
    safeLimit
  )

  const result = Array.isArray(rows) ? rows.map(normalizeMetric) : []
  for (const metric of result.reverse()) {
    const exists = memoryMetrics.find((entry) => entry.id === metric.id)
    if (!exists) memoryMetrics.unshift(metric)
  }
  if (memoryMetrics.length > 5_000) memoryMetrics.length = 5_000
  return result
}

export const listMetricsByRange = async ({
  range,
  limit
}: {
  range: string
  limit: number
}): Promise<RenderQualityMetric[]> => {
  const safeLimit = Math.max(1, Math.min(5_000, Math.round(Number(limit || 1000))))
  const fromMs = Date.now() - parseRangeToMs(range)

  if (!canRunRawSql()) {
    return memoryMetrics
      .filter((metric) => new Date(metric.created_at).getTime() >= fromMs)
      .slice(0, safeLimit)
  }

  await ensureInfra()
  const fromDate = new Date(fromMs)
  const rows = await (prisma as any).$queryRawUnsafe(
    `
      SELECT id, job_id, user_id, created_at, config_version_id, score_total, score_hook, score_pacing, score_emotion, score_visual, score_story, score_jank, features, flags
      FROM render_quality_metrics
      WHERE created_at >= $1::timestamptz
      ORDER BY created_at DESC
      LIMIT $2
    `,
    fromDate,
    safeLimit
  )

  return Array.isArray(rows) ? rows.map(normalizeMetric) : []
}

export const listLastMetrics = async (limit = 1_000) => {
  const safeLimit = Math.max(1, Math.min(5_000, Math.round(Number(limit || 1_000))))
  if (!canRunRawSql()) return memoryMetrics.slice(0, safeLimit)

  await ensureInfra()
  const rows = await (prisma as any).$queryRawUnsafe(
    `
      SELECT id, job_id, user_id, created_at, config_version_id, score_total, score_hook, score_pacing, score_emotion, score_visual, score_story, score_jank, features, flags
      FROM render_quality_metrics
      ORDER BY created_at DESC
      LIMIT $1
    `,
    safeLimit
  )
  return Array.isArray(rows) ? rows.map(normalizeMetric) : []
}
