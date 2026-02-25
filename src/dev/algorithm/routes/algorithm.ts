import express from 'express'
import { z, ZodTypeAny } from 'zod'
import { prisma } from '../../../db/prisma'
import { getPresetByKey } from '../presets'
import {
  activateConfigVersion,
  createConfigVersion,
  getActiveConfigVersion,
  getConfigVersionById,
  listAlgorithmPresets,
  listConfigVersions,
  parseConfigParams,
  rollbackConfigVersion
} from '../config/configService'
import { getExperimentStatus, startExperiment, stopRunningExperiment } from '../experiments/experimentService'
import {
  chooseConfigForJobCreation,
  listMetricsByRange,
  listRecentRenderMetrics
} from '../integration/pipelineIntegration'
import { evaluateRetentionScoring } from '../scoring/retentionScoring'
import { requireAlgorithmDevAccess } from '../security/requireAlgorithmDevAccess'
import { analyzeRenderImprovements } from '../suggestions/suggestionEngine'
import {
  analyzeRendersRequestSchema,
  algorithmConfigParamsSchema,
  applyPresetRequestSchema,
  createConfigRequestSchema,
  experimentStartRequestSchema,
  sampleFootageTestRequestSchema
} from '../types'

const router = express.Router()

const parseWith = <T>(schema: z.ZodType<T>, input: unknown, res: express.Response): T | null => {
  const parsed = schema.safeParse(input)
  if (!parsed.success) {
    res.status(400).json({
      error: 'invalid_payload',
      issues: parsed.error.issues.map((issue) => ({
        path: issue.path.join('.'),
        message: issue.message
      }))
    })
    return null
  }
  return parsed.data
}

const sendValidated = <T>(res: express.Response, schema: ZodTypeAny, payload: T, status = 200) => {
  const parsed = schema.parse(payload)
  return res.status(status).json(parsed)
}

const parseLimit = (raw: unknown, fallback: number, min: number, max: number) => {
  const value = Number(raw)
  if (!Number.isFinite(value)) return fallback
  return Math.max(min, Math.min(max, Math.round(value)))
}

const canRunRawSql = () =>
  typeof (prisma as any)?.$queryRawUnsafe === 'function' &&
  typeof (prisma as any)?.$executeRawUnsafe === 'function'

const configVersionResponseSchema = z
  .object({
    id: z.string(),
    created_at: z.string(),
    created_by_user_id: z.string().nullable(),
    preset_name: z.string().nullable(),
    params: algorithmConfigParamsSchema,
    is_active: z.boolean(),
    note: z.string().nullable()
  })
  .strict()

const metricResponseSchema = z
  .object({
    id: z.string(),
    job_id: z.string(),
    user_id: z.string().nullable(),
    created_at: z.string(),
    config_version_id: z.string(),
    score_total: z.number(),
    score_hook: z.number(),
    score_pacing: z.number(),
    score_emotion: z.number(),
    score_visual: z.number(),
    score_story: z.number(),
    score_jank: z.number(),
    features: z.record(z.string(), z.unknown()),
    flags: z.record(z.string(), z.unknown())
  })
  .strict()

const experimentArmSchema = z
  .object({
    config_version_id: z.string(),
    weight: z.number()
  })
  .strict()

const experimentSchema = z
  .object({
    id: z.string(),
    created_at: z.string(),
    created_by_user_id: z.string().nullable(),
    name: z.string(),
    status: z.enum(['draft', 'running', 'stopped']),
    arms: z.array(experimentArmSchema),
    allocation: z.record(z.string(), z.number()),
    reward_metric: z.string(),
    start_at: z.string().nullable(),
    end_at: z.string().nullable()
  })
  .strict()

const experimentResultSchema = z
  .object({
    config_version_id: z.string(),
    avg_score: z.number(),
    std_dev: z.number(),
    sample_size: z.number(),
    confidence: z.number()
  })
  .strict()

const experimentStatusSchema = z
  .object({
    experiment: experimentSchema.nullable(),
    results: z.array(experimentResultSchema),
    winner_suggestion: z
      .object({
        config_version_id: z.string().nullable(),
        rationale: z.string()
      })
      .strict()
  })
  .strict()

const suggestionSchema = z
  .object({
    title: z.string(),
    why: z.string(),
    change: z.record(z.string(), z.number()),
    predicted_delta_score: z.number(),
    confidence: z.number(),
    risk: z.string()
  })
  .strict()

const analyzeSummarySchema = z
  .object({
    avg_score_total: z.number(),
    avg_hook: z.number(),
    avg_pacing: z.number(),
    avg_emotion: z.number(),
    avg_visual: z.number(),
    avg_story: z.number(),
    avg_jank: z.number(),
    score_std: z.number(),
    sample_size: z.number(),
    failure_counts: z
      .object({
        low_hook: z.number(),
        low_pacing: z.number(),
        high_jank: z.number(),
        low_story: z.number()
      })
      .strict()
  })
  .strict()

const analyzeGroupSchema = z
  .object({
    config_version_id: z.string(),
    preset_name: z.string().nullable(),
    sample_size: z.number(),
    avg_score_total: z.number(),
    avg_hook: z.number(),
    avg_pacing: z.number(),
    avg_emotion: z.number(),
    avg_visual: z.number(),
    avg_story: z.number(),
    avg_jank: z.number()
  })
  .strict()

const analyzeResponseSchema = z
  .object({
    summary: analyzeSummarySchema,
    correlations: z.record(z.string(), z.number()),
    groups: z.array(analyzeGroupSchema),
    suggestions: z.array(suggestionSchema)
  })
  .strict()

const sampleFootageSchema = z
  .object({
    job_id: z.string(),
    user_id: z.string().nullable(),
    created_at: z.string(),
    retention_score: z.number(),
    config_version_id: z.string().nullable(),
    duration_sec: z.number(),
    hook_score: z.number()
  })
  .strict()

const retentionSubscoresSchema = z
  .object({
    H: z.number(),
    P: z.number(),
    E: z.number(),
    V: z.number(),
    S: z.number(),
    F: z.number(),
    J: z.number()
  })
  .strict()

const retentionScoringResultSchema = z
  .object({
    score_total: z.number(),
    subscores: retentionSubscoresSchema,
    features: z.record(z.string(), z.unknown()),
    flags: z.record(z.string(), z.unknown())
  })
  .strict()

router.use(requireAlgorithmDevAccess)

router.get('/config', async (_req, res) => {
  const active = await getActiveConfigVersion()
  return sendValidated(
    res,
    z.object({ config: configVersionResponseSchema }),
    {
      config: {
        ...active,
        params: active.params
      }
    }
  )
})

router.get('/config/versions', async (req, res) => {
  const limit = parseLimit(req.query.limit, 40, 1, 200)
  const versions = await listConfigVersions(limit)
  return sendValidated(
    res,
    z.object({ versions: z.array(configVersionResponseSchema) }),
    {
      versions: versions.map((version) => ({
        ...version,
        params: version.params
      }))
    }
  )
})

router.post('/config', async (req: any, res) => {
  const payload = parseWith(createConfigRequestSchema, req.body, res)
  if (!payload) return

  const created = await createConfigVersion({
    createdByUserId: req.user?.id || null,
    presetName: payload.preset_name || null,
    params: payload.params,
    activate: Boolean(payload.activate),
    note: payload.note || null
  })

  return sendValidated(
    res,
    z.object({ config: configVersionResponseSchema }),
    {
      config: {
        ...created,
        params: created.params
      }
    },
    201
  )
})

router.post('/config/activate', async (req, res) => {
  const payload = parseWith(
    z
      .object({
        config_version_id: z.string().trim().min(1)
      })
      .strict(),
    req.body,
    res
  )
  if (!payload) return
  const activated = await activateConfigVersion(payload.config_version_id)
  if (!activated) return res.status(404).json({ error: 'config_not_found' })
  return sendValidated(
    res,
    z.object({ config: configVersionResponseSchema }),
    {
      config: {
        ...activated,
        params: activated.params
      }
    }
  )
})

router.post('/config/rollback', async (_req, res) => {
  const rolledBack = await rollbackConfigVersion()
  if (!rolledBack) return res.status(404).json({ error: 'rollback_unavailable' })
  return sendValidated(
    res,
    z.object({ config: configVersionResponseSchema }),
    {
      config: {
        ...rolledBack,
        params: rolledBack.params
      }
    }
  )
})

router.post('/preset/apply', async (req: any, res) => {
  const payload = parseWith(applyPresetRequestSchema, req.body, res)
  if (!payload) return

  const preset = getPresetByKey(payload.preset_key)
  if (!preset) return res.status(404).json({ error: 'preset_not_found' })

  const created = await createConfigVersion({
    createdByUserId: req.user?.id || null,
    presetName: preset.name,
    params: preset.params,
    activate: true,
    note: payload.note || `Applied preset ${preset.name}`
  })

  return sendValidated(
    res,
    z.object({
      preset: z.object({ key: z.string(), name: z.string() }),
      config: configVersionResponseSchema
    }),
    {
      preset: { key: preset.key, name: preset.name },
      config: {
        ...created,
        params: created.params
      }
    }
  )
})

router.get('/presets', (_req, res) => {
  const presets = listAlgorithmPresets().map((preset) => ({
    key: preset.key,
    name: preset.name,
    description: preset.description,
    params: preset.params
  }))
  return sendValidated(
    res,
    z.object({
      presets: z.array(
        z.object({
          key: z.string(),
          name: z.string(),
          description: z.string(),
          params: algorithmConfigParamsSchema
        })
      )
    }),
    { presets }
  )
})

router.get('/metrics/recent', async (req, res) => {
  const limit = parseLimit(req.query.limit, 50, 1, 200)
  const metrics = await listRecentRenderMetrics(limit)
  return sendValidated(
    res,
    z.object({ metrics: z.array(metricResponseSchema) }),
    { metrics }
  )
})

router.get('/scorecards', async (req, res) => {
  const range = String(req.query.range || '7d')
  const limit = parseLimit(req.query.limit, 600, 50, 2_000)
  const metrics = await listMetricsByRange({ range, limit })

  const series = metrics
    .slice()
    .reverse()
    .map((metric) => ({
      t: metric.created_at,
      score_total: metric.score_total,
      hook: metric.score_hook,
      pacing: metric.score_pacing,
      emotion: metric.score_emotion,
      visual: metric.score_visual,
      story: metric.score_story,
      jank: metric.score_jank,
      cut_rate_per_min: Number((metric.features as any)?.cut_rate_per_min || 0)
    }))

  return sendValidated(
    res,
    z.object({
      series: z.array(
        z.object({
          t: z.string(),
          score_total: z.number(),
          hook: z.number(),
          pacing: z.number(),
          emotion: z.number(),
          visual: z.number(),
          story: z.number(),
          jank: z.number(),
          cut_rate_per_min: z.number()
        })
      )
    }),
    { series }
  )
})

router.get('/suggestions', async (req, res) => {
  const range = String(req.query.range || '7d')
  const report = await analyzeRenderImprovements({ limit: 400, range })
  return sendValidated(
    res,
    z
      .object({
        suggestions: z.array(suggestionSchema)
      })
      .strict(),
    { suggestions: report.suggestions }
  )
})

router.post('/analyze-renders', async (req, res) => {
  const payload = parseWith(analyzeRendersRequestSchema, req.body || {}, res)
  if (!payload) return

  const limit = payload.limit || 1_000
  const report = await analyzeRenderImprovements({
    limit,
    range: payload.range
  })

  return sendValidated(res, analyzeResponseSchema, report)
})

router.post('/experiment/start', async (req: any, res) => {
  const payload = parseWith(experimentStartRequestSchema, req.body, res)
  if (!payload) return

  const experiment = await startExperiment({
    name: payload.name,
    createdByUserId: req.user?.id || null,
    arms: payload.arms,
    allocation: payload.allocation,
    rewardMetric: payload.reward_metric,
    startAt: payload.start_at || null,
    endAt: payload.end_at || null
  })

  return sendValidated(
    res,
    z
      .object({
        experiment: experimentSchema
      })
      .strict(),
    { experiment }
  )
})

router.post('/experiment/stop', async (_req, res) => {
  const stopped = await stopRunningExperiment()
  return sendValidated(
    res,
    z
      .object({
        experiment: experimentSchema.nullable()
      })
      .strict(),
    { experiment: stopped }
  )
})

router.get('/experiment/status', async (_req, res) => {
  const status = await getExperimentStatus()
  return sendValidated(res, experimentStatusSchema, status)
})

router.get('/sample-footage', async (req, res) => {
  const limit = parseLimit(req.query.limit, 20, 1, 100)

  if (!canRunRawSql()) {
    return sendValidated(
      res,
      z
        .object({
          samples: z.array(sampleFootageSchema)
        })
        .strict(),
      { samples: [] }
    )
  }

  const rows = await (prisma as any).$queryRawUnsafe(
    `
      SELECT
        id,
        "userId" AS user_id,
        created_at,
        retention_score,
        config_version_id,
        analysis
      FROM jobs
      WHERE status = 'completed'
      ORDER BY created_at DESC
      LIMIT $1
    `,
    limit
  )

  const samples = Array.isArray(rows)
    ? rows.map((row) => ({
        job_id: String((row as any)?.id || ''),
        user_id: (row as any)?.user_id ? String((row as any).user_id) : null,
        created_at: (row as any)?.created_at ? new Date((row as any).created_at).toISOString() : new Date().toISOString(),
        retention_score: Number((row as any)?.retention_score || 0),
        config_version_id: (row as any)?.config_version_id ? String((row as any).config_version_id) : null,
        duration_sec: Number(((row as any)?.analysis as any)?.duration || 0),
        hook_score: Number(((row as any)?.analysis as any)?.hook_score || 0)
      }))
    : []

  return sendValidated(
    res,
    z
      .object({
        samples: z.array(sampleFootageSchema)
      })
      .strict(),
    { samples }
  )
})

router.post('/sample-footage/test', async (req, res) => {
  const payload = parseWith(sampleFootageTestRequestSchema, req.body, res)
  if (!payload) return

  const job = await prisma.job.findUnique({ where: { id: payload.job_id } })
  if (!job) return res.status(404).json({ error: 'job_not_found' })

  let resolvedParams = payload.params ? parseConfigParams(payload.params) : null
  if (!resolvedParams) {
    const fallbackConfigId = String((job as any)?.configVersionId || (job as any)?.config_version_id || '').trim()
    const fallbackConfig = fallbackConfigId ? await getConfigVersionById(fallbackConfigId) : null
    resolvedParams = fallbackConfig?.params || (await getActiveConfigVersion()).params
  }

  const analysis = (job.analysis as any) || {}
  const transcript =
    analysis.transcript ||
    analysis.transcript_cues ||
    analysis.captions ||
    analysis.subtitle_cues ||
    analysis.editPlan?.transcriptSignals ||
    null
  const cutList =
    analysis.editPlan?.segments ||
    analysis.metadata_summary?.segments ||
    []

  const scoring = evaluateRetentionScoring(analysis, transcript, cutList, resolvedParams)
  return sendValidated(res, retentionScoringResultSchema, scoring)
})

router.get('/config-selector', async (_req, res) => {
  const selection = await chooseConfigForJobCreation()
  return sendValidated(
    res,
    z.object({
      config_version_id: z.string(),
      experiment_id: z.string().nullable(),
      source: z.string()
    }),
    selection
  )
})

export default router
