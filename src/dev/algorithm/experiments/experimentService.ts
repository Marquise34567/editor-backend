import crypto from 'crypto'
import { prisma } from '../../../db/prisma'
import { getConfigVersionById, getActiveConfigVersion } from '../config/configService'
import { AlgorithmExperiment, ExperimentArm } from '../types'

const canRunRawSql = () =>
  typeof (prisma as any)?.$executeRawUnsafe === 'function' &&
  typeof (prisma as any)?.$queryRawUnsafe === 'function'

let loaded = false
let infraEnsured = false
let cache: AlgorithmExperiment[] = []

type StartExperimentInput = {
  name: string
  createdByUserId?: string | null
  arms: ExperimentArm[]
  allocation: Record<string, number>
  rewardMetric?: string
  startAt?: string | null
  endAt?: string | null
}

type ArmResult = {
  config_version_id: string
  avg_score: number
  std_dev: number
  sample_size: number
  confidence: number
}

export type ExperimentStatusResult = {
  experiment: AlgorithmExperiment | null
  results: ArmResult[]
  winner_suggestion: {
    config_version_id: string | null
    rationale: string
  }
}

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const normalizeExperimentRow = (row: any): AlgorithmExperiment => {
  const armsRaw = Array.isArray(row?.arms) ? row.arms : []
  const arms = armsRaw
    .map((arm) => {
      if (!arm || typeof arm !== 'object') return null
      const payload = arm as Record<string, unknown>
      const configVersionId = String(payload.config_version_id || '').trim()
      if (!configVersionId) return null
      const weight = clamp(Number(payload.weight || 0), 0, 1)
      return {
        config_version_id: configVersionId,
        weight
      }
    })
    .filter((arm): arm is ExperimentArm => Boolean(arm))

  const allocationRaw = row?.allocation && typeof row.allocation === 'object' ? row.allocation : {}
  const allocation = Object.entries(allocationRaw as Record<string, unknown>).reduce((acc, [key, value]) => {
    const id = String(key || '').trim()
    const numeric = Number(value)
    if (!id || !Number.isFinite(numeric) || numeric < 0) return acc
    acc[id] = numeric
    return acc
  }, {} as Record<string, number>)

  const statusRaw = String(row?.status || '').toLowerCase()
  const status: 'draft' | 'running' | 'stopped' =
    statusRaw === 'running' || statusRaw === 'stopped' ? statusRaw : 'draft'

  return {
    id: String(row?.id || ''),
    created_at: row?.created_at ? new Date(row.created_at).toISOString() : new Date().toISOString(),
    created_by_user_id: row?.created_by_user_id ? String(row.created_by_user_id) : null,
    name: String(row?.name || 'untitled_experiment').slice(0, 120),
    status,
    arms,
    allocation,
    reward_metric: String(row?.reward_metric || 'score_total'),
    start_at: row?.start_at ? new Date(row.start_at).toISOString() : null,
    end_at: row?.end_at ? new Date(row.end_at).toISOString() : null
  }
}

const ensureInfra = async () => {
  if (!canRunRawSql() || infraEnsured) return
  await (prisma as any).$executeRawUnsafe(`
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
    )
  `)
  await (prisma as any).$executeRawUnsafe(
    'CREATE INDEX IF NOT EXISTS idx_algorithm_experiments_status_created_at ON algorithm_experiments (status, created_at DESC)'
  )
  infraEnsured = true
}

const loadExperiments = async () => {
  if (!canRunRawSql()) {
    loaded = true
    return
  }
  await ensureInfra()
  const rows = await (prisma as any).$queryRawUnsafe(`
    SELECT id, created_at, created_by_user_id, name, status, arms, allocation, reward_metric, start_at, end_at
    FROM algorithm_experiments
    ORDER BY created_at DESC
    LIMIT 80
  `)
  cache = Array.isArray(rows) ? rows.map(normalizeExperimentRow) : []
  loaded = true
}

const ensureLoaded = async () => {
  if (!loaded) await loadExperiments()
}

const normalizeAllocation = (arms: ExperimentArm[], allocationRaw: Record<string, number>) => {
  const values = arms.map((arm) => ({
    config_version_id: arm.config_version_id,
    value: Math.max(0, Number(allocationRaw[arm.config_version_id] || arm.weight * 100 || 0))
  }))
  const total = values.reduce((sum, row) => sum + row.value, 0)
  if (total <= 0) {
    const equal = 100 / Math.max(1, values.length)
    return values.reduce((acc, row) => {
      acc[row.config_version_id] = Number(equal.toFixed(4))
      return acc
    }, {} as Record<string, number>)
  }
  return values.reduce((acc, row) => {
    acc[row.config_version_id] = Number(((row.value / total) * 100).toFixed(4))
    return acc
  }, {} as Record<string, number>)
}

const runningNow = (experiment: AlgorithmExperiment) => {
  if (experiment.status !== 'running') return false
  const now = Date.now()
  if (experiment.start_at && new Date(experiment.start_at).getTime() > now) return false
  if (experiment.end_at && new Date(experiment.end_at).getTime() < now) return false
  return true
}

const chooseByAllocation = (allocation: Record<string, number>) => {
  const entries = Object.entries(allocation)
    .map(([configVersionId, pct]) => ({ configVersionId, pct: Math.max(0, Number(pct || 0)) }))
    .filter((entry) => entry.pct > 0)
  if (!entries.length) return null
  const total = entries.reduce((sum, entry) => sum + entry.pct, 0)
  if (total <= 0) return entries[0].configVersionId
  let cursor = Math.random() * total
  for (const entry of entries) {
    cursor -= entry.pct
    if (cursor <= 0) return entry.configVersionId
  }
  return entries[entries.length - 1].configVersionId
}

const validateArms = async (arms: ExperimentArm[]) => {
  const normalized: ExperimentArm[] = []
  for (const arm of arms) {
    const id = String(arm.config_version_id || '').trim()
    if (!id) continue
    const config = await getConfigVersionById(id)
    if (!config) throw new Error(`invalid_config_version:${id}`)
    normalized.push({
      config_version_id: config.id,
      weight: clamp(Number(arm.weight || 0), 0, 1)
    })
  }
  if (normalized.length < 2 || normalized.length > 4) {
    throw new Error('experiment_requires_2_to_4_valid_arms')
  }
  return normalized
}

export const getRunningExperiment = async (): Promise<AlgorithmExperiment | null> => {
  await ensureLoaded()
  const candidate = cache.find((experiment) => runningNow(experiment))
  if (candidate) return candidate

  if (!canRunRawSql()) return null

  await ensureInfra()
  const rows = await (prisma as any).$queryRawUnsafe(`
    SELECT id, created_at, created_by_user_id, name, status, arms, allocation, reward_metric, start_at, end_at
    FROM algorithm_experiments
    WHERE status = 'running'
    ORDER BY created_at DESC
    LIMIT 3
  `)
  const found = Array.isArray(rows) ? rows.map(normalizeExperimentRow).find(runningNow) : null
  if (found) {
    cache = [found, ...cache.filter((entry) => entry.id !== found.id)]
  }
  return found || null
}

export const startExperiment = async (input: StartExperimentInput): Promise<AlgorithmExperiment> => {
  await ensureLoaded()
  const validatedArms = await validateArms(input.arms)
  const allocation = normalizeAllocation(validatedArms, input.allocation)

  if (!canRunRawSql()) {
    cache = cache.map((entry) => ({ ...entry, status: 'stopped' as const, end_at: new Date().toISOString() }))
    const row: AlgorithmExperiment = {
      id: crypto.randomUUID(),
      created_at: new Date().toISOString(),
      created_by_user_id: input.createdByUserId || null,
      name: input.name.trim().slice(0, 120),
      status: 'running',
      arms: validatedArms,
      allocation,
      reward_metric: String(input.rewardMetric || 'score_total').slice(0, 120),
      start_at: input.startAt || new Date().toISOString(),
      end_at: input.endAt || null
    }
    cache = [row, ...cache]
    return row
  }

  await ensureInfra()
  await (prisma as any).$executeRawUnsafe(
    `UPDATE algorithm_experiments SET status = 'stopped', end_at = COALESCE(end_at, NOW()) WHERE status = 'running'`
  )
  const rows = await (prisma as any).$queryRawUnsafe(
    `
      INSERT INTO algorithm_experiments (
        id,
        created_by_user_id,
        name,
        status,
        arms,
        allocation,
        reward_metric,
        start_at,
        end_at
      )
      VALUES (gen_random_uuid()::text, $1, $2, 'running', $3::jsonb, $4::jsonb, $5, $6, $7)
      RETURNING id, created_at, created_by_user_id, name, status, arms, allocation, reward_metric, start_at, end_at
    `,
    input.createdByUserId || null,
    input.name.trim().slice(0, 120),
    JSON.stringify(validatedArms),
    JSON.stringify(allocation),
    String(input.rewardMetric || 'score_total').slice(0, 120),
    input.startAt || new Date().toISOString(),
    input.endAt || null
  )
  const inserted = Array.isArray(rows) && rows[0] ? normalizeExperimentRow(rows[0]) : null
  if (!inserted) throw new Error('experiment_start_failed')
  cache = [
    inserted,
    ...cache
      .filter((entry) => entry.id !== inserted.id)
      .map((entry) => ({ ...entry, status: 'stopped' as const }))
  ]
  return inserted
}

export const stopRunningExperiment = async (): Promise<AlgorithmExperiment | null> => {
  await ensureLoaded()
  const running = await getRunningExperiment()
  if (!running) return null

  if (!canRunRawSql()) {
    cache = cache.map((entry) =>
      entry.id === running.id
        ? { ...entry, status: 'stopped' as const, end_at: new Date().toISOString() }
        : entry
    )
    return cache.find((entry) => entry.id === running.id) || null
  }

  const rows = await (prisma as any).$queryRawUnsafe(
    `
      UPDATE algorithm_experiments
      SET status = 'stopped', end_at = COALESCE(end_at, NOW())
      WHERE id = $1
      RETURNING id, created_at, created_by_user_id, name, status, arms, allocation, reward_metric, start_at, end_at
    `,
    running.id
  )
  const updated = Array.isArray(rows) && rows[0] ? normalizeExperimentRow(rows[0]) : null
  if (!updated) return null
  cache = [updated, ...cache.filter((entry) => entry.id !== updated.id)]
  return updated
}

export const selectConfigVersionForNewJob = async () => {
  const running = await getRunningExperiment()
  if (running) {
    const selected = chooseByAllocation(running.allocation)
    if (selected) {
      return {
        config_version_id: selected,
        experiment_id: running.id,
        source: 'experiment' as const
      }
    }
  }

  const active = await getActiveConfigVersion()
  return {
    config_version_id: active.id,
    experiment_id: null,
    source: 'active' as const
  }
}

const calculateConfidence = ({ sampleSize, stdDev }: { sampleSize: number; stdDev: number }) => {
  const sampleSignal = clamp(Math.log10(sampleSize + 1) / 2.4, 0, 1)
  const spreadPenalty = clamp(1 - stdDev / 24, 0, 1)
  return Number((0.35 + 0.65 * sampleSignal * spreadPenalty).toFixed(4))
}

const queryResultsForExperiment = async (experiment: AlgorithmExperiment): Promise<ArmResult[]> => {
  if (!canRunRawSql()) {
    return experiment.arms.map((arm) => ({
      config_version_id: arm.config_version_id,
      avg_score: 0,
      std_dev: 0,
      sample_size: 0,
      confidence: 0
    }))
  }

  const startAt = experiment.start_at || experiment.created_at
  const endAt = experiment.end_at || new Date().toISOString()
  const armIds = experiment.arms.map((arm) => arm.config_version_id)
  if (!armIds.length) return []

  const rows = await (prisma as any).$queryRawUnsafe(
    `
      SELECT
        config_version_id,
        AVG(score_total)::float AS avg_score,
        COALESCE(STDDEV_POP(score_total), 0)::float AS std_dev,
        COUNT(*)::int AS sample_size
      FROM render_quality_metrics
      WHERE created_at >= $1
        AND created_at <= $2
        AND config_version_id = ANY($3::text[])
      GROUP BY config_version_id
    `,
    startAt,
    endAt,
    armIds
  )

  const byConfig = new Map<string, { avg: number; std: number; n: number }>()
  if (Array.isArray(rows)) {
    for (const row of rows) {
      const configVersionId = String((row as any)?.config_version_id || '')
      if (!configVersionId) continue
      byConfig.set(configVersionId, {
        avg: Number((row as any)?.avg_score || 0),
        std: Number((row as any)?.std_dev || 0),
        n: Number((row as any)?.sample_size || 0)
      })
    }
  }

  return experiment.arms.map((arm) => {
    const sample = byConfig.get(arm.config_version_id) || { avg: 0, std: 0, n: 0 }
    return {
      config_version_id: arm.config_version_id,
      avg_score: Number(sample.avg.toFixed(4)),
      std_dev: Number(sample.std.toFixed(4)),
      sample_size: Math.max(0, Math.round(sample.n)),
      confidence: calculateConfidence({ sampleSize: sample.n, stdDev: sample.std })
    }
  })
}

export const getExperimentStatus = async (): Promise<ExperimentStatusResult> => {
  const experiment = await getRunningExperiment()
  if (!experiment) {
    return {
      experiment: null,
      results: [],
      winner_suggestion: {
        config_version_id: null,
        rationale: 'No running experiment.'
      }
    }
  }

  const results = await queryResultsForExperiment(experiment)
  const ranked = results
    .slice()
    .sort((a, b) => b.avg_score - a.avg_score || b.confidence - a.confidence || b.sample_size - a.sample_size)
  const winner = ranked[0] || null

  if (!winner || winner.sample_size < 5) {
    return {
      experiment,
      results,
      winner_suggestion: {
        config_version_id: winner?.config_version_id || null,
        rationale: 'Insufficient sample size. Keep experiment running until each arm has at least 5 samples.'
      }
    }
  }

  return {
    experiment,
    results,
    winner_suggestion: {
      config_version_id: winner.config_version_id,
      rationale: `Highest average score with confidence ${winner.confidence.toFixed(2)} from ${winner.sample_size} samples.`
    }
  }
}
