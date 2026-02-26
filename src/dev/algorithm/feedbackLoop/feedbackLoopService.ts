import { prisma } from '../../../db/prisma'
import {
  createConfigVersion,
  getActiveConfigVersion,
  parseConfigParams
} from '../config/configService'
import { AlgorithmConfigParams, AlgorithmConfigVersion } from '../types'

type FeedbackLoopTrigger =
  | 'manual_run'
  | 'feedback_submission'
  | 'platform_feedback_submission'
  | 'creator_feedback_submission'

export type FeedbackLoopSettings = {
  enabled: boolean
  auto_apply: boolean
  min_feedback_samples: number
  lookback_limit: number
  cooldown_minutes: number
  min_confidence: number
  min_delta_score: number
}

export type FeedbackLoopRuntime = {
  last_run_at: string | null
  last_run_reason: string | null
  last_trigger: FeedbackLoopTrigger | null
  last_applied_at: string | null
  last_applied_note: string | null
  last_applied_config_version_id: string | null
  last_apply_confidence: number | null
  last_apply_delta_score: number | null
}

type FeedbackLoopState = {
  settings: FeedbackLoopSettings
  runtime: FeedbackLoopRuntime
  updated_at: string
}

type FeedbackSignalEvent = {
  job_id: string
  created_at: string
  source_type: 'platform' | 'internal'
  signal_outcome: number
  watch_percent: number | null
  hook_hold_percent: number | null
  completion_percent: number | null
  rewatch_rate: number | null
  manual_score: number | null
  first30_retention: number | null
  click_through_rate: number | null
  shares_per_view: number | null
  likes_per_view: number | null
  comments_per_view: number | null
  editor_mode: string | null
  strategy_profile: string | null
  target_platform: string | null
  hook_selection_mode: string | null
  model_hook_score: number | null
  model_pacing_score: number | null
  model_jank_score: number | null
  model_retention_score: number | null
}

type PerformanceRow = {
  key: string
  count: number
  avg_outcome: number
}

type ProposedDeltaMap = Partial<Record<Exclude<keyof AlgorithmConfigParams, 'subtitle_style_mode'>, number>>

export type FeedbackBrainSnapshot = {
  generated_at: string
  sample_size: number
  platform_feedback_share: number
  avg_outcome: number
  avg_hook_hold: number | null
  avg_completion: number | null
  avg_model_hook: number | null
  avg_model_pacing: number | null
  avg_model_jank: number | null
  confidence: number
  predicted_delta_score: number
  recommended_editor_mode: string | null
  recommended_strategy_profile: string | null
  recommended_target_platform: string | null
  rationale: string[]
  proposed_param_deltas: Record<string, number>
  mode_performance: PerformanceRow[]
  strategy_performance: PerformanceRow[]
  platform_performance: PerformanceRow[]
  recent_signals: FeedbackSignalEvent[]
}

export type FeedbackLoopStatus = {
  settings: FeedbackLoopSettings
  runtime: FeedbackLoopRuntime
  brain_snapshot: FeedbackBrainSnapshot
}

export type FeedbackLoopRunResult = {
  applied: boolean
  reason: string
  config: AlgorithmConfigVersion | null
  status: FeedbackLoopStatus
}

const STATE_ROW_ID = 'global'
const MAX_RECENT_SIGNALS = 24

const DEFAULT_SETTINGS: FeedbackLoopSettings = {
  enabled: true,
  auto_apply: true,
  min_feedback_samples: 8,
  lookback_limit: 220,
  cooldown_minutes: 30,
  min_confidence: 0.58,
  min_delta_score: 0.012
}

const DEFAULT_RUNTIME: FeedbackLoopRuntime = {
  last_run_at: null,
  last_run_reason: null,
  last_trigger: null,
  last_applied_at: null,
  last_applied_note: null,
  last_applied_config_version_id: null,
  last_apply_confidence: null,
  last_apply_delta_score: null
}

type NumericParamKey = Exclude<keyof AlgorithmConfigParams, 'subtitle_style_mode'>
const NUMERIC_PARAM_LIMITS: Record<NumericParamKey, { min: number; max: number; integer?: boolean }> = {
  cut_aggression: { min: 0, max: 100 },
  min_clip_len_ms: { min: 120, max: 30_000, integer: true },
  max_clip_len_ms: { min: 300, max: 120_000, integer: true },
  silence_db_threshold: { min: -80, max: -5 },
  silence_min_ms: { min: 80, max: 8_000, integer: true },
  filler_word_weight: { min: 0, max: 4 },
  redundancy_weight: { min: 0, max: 4 },
  energy_floor: { min: 0, max: 1 },
  spike_boost: { min: 0, max: 3 },
  pattern_interrupt_every_sec: { min: 2, max: 60 },
  hook_priority_weight: { min: 0, max: 3 },
  story_coherence_guard: { min: 0, max: 100 },
  jank_guard: { min: 0, max: 100 },
  pacing_multiplier: { min: 0.3, max: 3 }
}

const canRunRawSql = () =>
  typeof (prisma as any)?.$queryRawUnsafe === 'function' &&
  typeof (prisma as any)?.$executeRawUnsafe === 'function'

let infraEnsured = false
let stateLoaded = false
let stateCache: FeedbackLoopState = {
  settings: { ...DEFAULT_SETTINGS },
  runtime: { ...DEFAULT_RUNTIME },
  updated_at: new Date(0).toISOString()
}

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))
const clamp01 = (value: number) => clamp(value, 0, 1)
const nowIso = () => new Date().toISOString()

const roundTo = (value: number, decimals = 4) => {
  const multiplier = 10 ** Math.max(0, Math.round(decimals))
  return Math.round(value * multiplier) / multiplier
}

const asObject = (value: unknown): Record<string, any> => {
  if (!value) return {}
  if (typeof value === 'object') return value as Record<string, any>
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value)
      return parsed && typeof parsed === 'object' ? (parsed as Record<string, any>) : {}
    } catch {
      return {}
    }
  }
  return {}
}

const toIsoOrNow = (value: unknown) => {
  const date = new Date(String(value || ''))
  if (Number.isNaN(date.getTime())) return nowIso()
  return date.toISOString()
}

const normalizePercentMetric = (value: unknown): number | null => {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return null
  if (numeric <= 1) return roundTo(clamp01(numeric))
  return roundTo(clamp01(numeric / 100))
}

const normalizeModelScore = (value: unknown): number | null => {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return null
  if (numeric <= 1) return roundTo(clamp01(numeric))
  return roundTo(clamp01(numeric / 100))
}

const normalizeEditorMode = (value: unknown): string | null => {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
  if (!normalized) return null
  return normalized.length > 80 ? normalized.slice(0, 80) : normalized
}

const normalizeStrategyProfile = (value: unknown): string | null => {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
  if (!normalized) return null
  return normalized.length > 80 ? normalized.slice(0, 80) : normalized
}

const normalizeTargetPlatform = (value: unknown): string | null => {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
  if (!normalized) return null
  return normalized.length > 80 ? normalized.slice(0, 80) : normalized
}

const normalizeHookSelectionMode = (value: unknown): string | null => {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
  if (!normalized) return null
  return normalized.length > 80 ? normalized.slice(0, 80) : normalized
}

const parseSettings = (value: unknown): FeedbackLoopSettings => {
  const payload = asObject(value)
  const enabled = payload.enabled
  const autoApply = payload.auto_apply
  const minFeedbackSamples = Number(payload.min_feedback_samples)
  const lookbackLimit = Number(payload.lookback_limit)
  const cooldownMinutes = Number(payload.cooldown_minutes)
  const minConfidence = Number(payload.min_confidence)
  const minDeltaScore = Number(payload.min_delta_score)
  return {
    enabled: typeof enabled === 'boolean' ? enabled : DEFAULT_SETTINGS.enabled,
    auto_apply: typeof autoApply === 'boolean' ? autoApply : DEFAULT_SETTINGS.auto_apply,
    min_feedback_samples: Number.isFinite(minFeedbackSamples) ? Math.round(clamp(minFeedbackSamples, 3, 80)) : DEFAULT_SETTINGS.min_feedback_samples,
    lookback_limit: Number.isFinite(lookbackLimit) ? Math.round(clamp(lookbackLimit, 30, 800)) : DEFAULT_SETTINGS.lookback_limit,
    cooldown_minutes: Number.isFinite(cooldownMinutes) ? Math.round(clamp(cooldownMinutes, 2, 1_440)) : DEFAULT_SETTINGS.cooldown_minutes,
    min_confidence: Number.isFinite(minConfidence) ? roundTo(clamp(minConfidence, 0.2, 0.99), 4) : DEFAULT_SETTINGS.min_confidence,
    min_delta_score: Number.isFinite(minDeltaScore) ? roundTo(clamp(minDeltaScore, 0.001, 0.2), 4) : DEFAULT_SETTINGS.min_delta_score
  }
}

const parseRuntime = (value: unknown): FeedbackLoopRuntime => {
  const payload = asObject(value)
  return {
    last_run_at: typeof payload.last_run_at === 'string' ? payload.last_run_at : null,
    last_run_reason: typeof payload.last_run_reason === 'string' ? payload.last_run_reason : null,
    last_trigger: typeof payload.last_trigger === 'string' ? (payload.last_trigger as FeedbackLoopTrigger) : null,
    last_applied_at: typeof payload.last_applied_at === 'string' ? payload.last_applied_at : null,
    last_applied_note: typeof payload.last_applied_note === 'string' ? payload.last_applied_note : null,
    last_applied_config_version_id: typeof payload.last_applied_config_version_id === 'string'
      ? payload.last_applied_config_version_id
      : null,
    last_apply_confidence: Number.isFinite(Number(payload.last_apply_confidence))
      ? roundTo(Number(payload.last_apply_confidence), 4)
      : null,
    last_apply_delta_score: Number.isFinite(Number(payload.last_apply_delta_score))
      ? roundTo(Number(payload.last_apply_delta_score), 4)
      : null
  }
}

const normalizeStateRow = (row: any): FeedbackLoopState => ({
  settings: parseSettings((row as any)?.settings),
  runtime: parseRuntime((row as any)?.runtime),
  updated_at: (row as any)?.updated_at ? new Date((row as any).updated_at).toISOString() : nowIso()
})

const ensureInfra = async () => {
  if (infraEnsured || !canRunRawSql()) return
  await (prisma as any).$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS algorithm_feedback_loop_state (
      id TEXT PRIMARY KEY,
      settings JSONB NOT NULL DEFAULT '{}'::jsonb,
      runtime JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `)
  infraEnsured = true
}

const loadStateFromDb = async () => {
  if (!canRunRawSql()) {
    stateLoaded = true
    return
  }

  await ensureInfra()
  const rows = await (prisma as any).$queryRawUnsafe(
    `
      SELECT settings, runtime, updated_at
      FROM algorithm_feedback_loop_state
      WHERE id = $1
      LIMIT 1
    `,
    STATE_ROW_ID
  )

  if (Array.isArray(rows) && rows[0]) {
    stateCache = normalizeStateRow(rows[0])
  }

  stateLoaded = true
}

const ensureStateLoaded = async () => {
  if (stateLoaded) return
  await loadStateFromDb()
}

const persistState = async (state: FeedbackLoopState) => {
  stateCache = state
  if (!canRunRawSql()) return

  await ensureInfra()
  await (prisma as any).$executeRawUnsafe(
    `
      INSERT INTO algorithm_feedback_loop_state (id, settings, runtime, updated_at)
      VALUES ($1, $2::jsonb, $3::jsonb, $4::timestamptz)
      ON CONFLICT (id) DO UPDATE
      SET settings = EXCLUDED.settings,
          runtime = EXCLUDED.runtime,
          updated_at = EXCLUDED.updated_at
    `,
    STATE_ROW_ID,
    JSON.stringify(state.settings),
    JSON.stringify(state.runtime),
    state.updated_at
  )
}

const averageOf = (values: Array<number | null>) => {
  const valid = values.filter((value): value is number => value !== null && Number.isFinite(value))
  if (!valid.length) return null
  return roundTo(valid.reduce((sum, value) => sum + value, 0) / valid.length, 4)
}

const computeFeedbackOutcome = ({
  watchPercent,
  hookHoldPercent,
  completionPercent,
  rewatchRate,
  manualScore,
  first30Retention,
  clickThroughRate,
  sharesPerView,
  likesPerView,
  commentsPerView,
  modelRetentionScore
}: {
  watchPercent: number | null
  hookHoldPercent: number | null
  completionPercent: number | null
  rewatchRate: number | null
  manualScore: number | null
  first30Retention: number | null
  clickThroughRate: number | null
  sharesPerView: number | null
  likesPerView: number | null
  commentsPerView: number | null
  modelRetentionScore: number | null
}) => {
  const platformBoostValues = [
    clickThroughRate,
    sharesPerView,
    likesPerView,
    commentsPerView
  ].filter((value): value is number => value !== null && Number.isFinite(value))

  const platformBoost = platformBoostValues.length
    ? clamp01(platformBoostValues.reduce((sum, value) => sum + value, 0) / platformBoostValues.length)
    : null

  const weightedSignals = [
    { value: watchPercent, weight: 0.28 },
    { value: hookHoldPercent, weight: 0.21 },
    { value: completionPercent, weight: 0.12 },
    { value: first30Retention, weight: 0.14 },
    { value: manualScore, weight: 0.08 },
    { value: rewatchRate, weight: 0.05 },
    { value: modelRetentionScore, weight: 0.08 },
    { value: platformBoost, weight: 0.04 }
  ].filter((entry) => entry.value !== null && Number.isFinite(entry.value))

  if (!weightedSignals.length) return null
  const totalWeight = weightedSignals.reduce((sum, entry) => sum + entry.weight, 0)
  if (totalWeight <= 0) return null
  return roundTo(
    clamp01(weightedSignals.reduce((sum, entry) => sum + Number(entry.value) * entry.weight, 0) / totalWeight),
    4
  )
}

const pullFeedbackSignals = (analysis: Record<string, any>) => {
  const feedback = asObject(analysis.retention_feedback)
  const watchPercent = normalizePercentMetric(feedback.watchPercent ?? feedback.watch_percent)
  const hookHoldPercent = normalizePercentMetric(
    feedback.hookHoldPercent ?? feedback.hook_hold_percent ?? feedback.first8sRetention
  )
  const completionPercent = normalizePercentMetric(feedback.completionPercent ?? feedback.completion_percent)
  const rewatchRate = normalizePercentMetric(feedback.rewatchRate ?? feedback.rewatch_rate)
  const manualScore = normalizePercentMetric(feedback.manualScore ?? feedback.manual_score)
  const first30Retention = normalizePercentMetric(feedback.first30Retention ?? feedback.first30_retention)
  const clickThroughRate = normalizePercentMetric(feedback.clickThroughRate ?? feedback.click_through_rate ?? feedback.ctr)
  const sharesPerView = normalizePercentMetric(feedback.sharesPerView ?? feedback.shares_per_view)
  const likesPerView = normalizePercentMetric(feedback.likesPerView ?? feedback.likes_per_view)
  const commentsPerView = normalizePercentMetric(feedback.commentsPerView ?? feedback.comments_per_view)
  const sourceType: 'platform' | 'internal' = String(feedback.sourceType || feedback.source_type || '')
    .toLowerCase()
    .trim() === 'platform'
    ? 'platform'
    : 'internal'

  return {
    hasFeedback: watchPercent !== null
      || hookHoldPercent !== null
      || completionPercent !== null
      || rewatchRate !== null
      || manualScore !== null
      || first30Retention !== null
      || clickThroughRate !== null
      || sharesPerView !== null
      || likesPerView !== null
      || commentsPerView !== null,
    sourceType,
    watchPercent,
    hookHoldPercent,
    completionPercent,
    rewatchRate,
    manualScore,
    first30Retention,
    clickThroughRate,
    sharesPerView,
    likesPerView,
    commentsPerView
  }
}

const selectRecentFeedbackSignals = async (limit: number): Promise<FeedbackSignalEvent[]> => {
  if (!canRunRawSql()) return []
  await ensureInfra()
  const safeLimit = Math.max(12, Math.min(800, Math.round(Number(limit || DEFAULT_SETTINGS.lookback_limit))))
  const rows = await (prisma as any).$queryRawUnsafe(
    `
      SELECT
        id,
        created_at,
        updated_at,
        analysis,
        render_settings,
        retention_score
      FROM jobs
      WHERE status = 'completed'
      ORDER BY updated_at DESC
      LIMIT $1
    `,
    safeLimit
  )

  const events: FeedbackSignalEvent[] = []
  const inputRows = Array.isArray(rows) ? rows : []
  for (const row of inputRows) {
    const analysis = asObject((row as any)?.analysis)
    const renderSettings = asObject((row as any)?.render_settings)
    const feedback = pullFeedbackSignals(analysis)
    if (!feedback.hasFeedback) continue

    const modelRetentionScore = normalizeModelScore(
      analysis?.retention_score ??
      analysis?.retentionScore ??
      (row as any)?.retention_score
    )
    const judge = asObject(analysis?.retention_judge)
    const modelHookScore = normalizeModelScore(judge?.hook_strength ?? judge?.hookScore)
    const modelPacingScore = normalizeModelScore(judge?.pacing_score ?? judge?.pacingScore)
    const modelJankScore = normalizeModelScore(
      judge?.jank_score ??
      judge?.jankScore ??
      judge?.jank_risk
    )

    const outcome = computeFeedbackOutcome({
      watchPercent: feedback.watchPercent,
      hookHoldPercent: feedback.hookHoldPercent,
      completionPercent: feedback.completionPercent,
      rewatchRate: feedback.rewatchRate,
      manualScore: feedback.manualScore,
      first30Retention: feedback.first30Retention,
      clickThroughRate: feedback.clickThroughRate,
      sharesPerView: feedback.sharesPerView,
      likesPerView: feedback.likesPerView,
      commentsPerView: feedback.commentsPerView,
      modelRetentionScore
    })
    if (outcome === null) continue

    events.push({
      job_id: String((row as any)?.id || ''),
      created_at: toIsoOrNow((row as any)?.updated_at || (row as any)?.created_at),
      source_type: feedback.sourceType,
      signal_outcome: outcome,
      watch_percent: feedback.watchPercent,
      hook_hold_percent: feedback.hookHoldPercent,
      completion_percent: feedback.completionPercent,
      rewatch_rate: feedback.rewatchRate,
      manual_score: feedback.manualScore,
      first30_retention: feedback.first30Retention,
      click_through_rate: feedback.clickThroughRate,
      shares_per_view: feedback.sharesPerView,
      likes_per_view: feedback.likesPerView,
      comments_per_view: feedback.commentsPerView,
      editor_mode: normalizeEditorMode(
        analysis?.editorMode ??
        analysis?.editor_mode ??
        analysis?.contentMode ??
        renderSettings?.editorMode ??
        renderSettings?.editor_mode ??
        renderSettings?.contentMode
      ),
      strategy_profile: normalizeStrategyProfile(
        analysis?.retentionStrategyProfile ??
        analysis?.retention_strategy_profile ??
        analysis?.retentionProfile ??
        renderSettings?.retentionStrategyProfile ??
        renderSettings?.retention_strategy_profile ??
        renderSettings?.retentionProfile
      ),
      target_platform: normalizeTargetPlatform(
        analysis?.retentionTargetPlatform ??
        analysis?.retention_target_platform ??
        analysis?.targetPlatform ??
        renderSettings?.retentionTargetPlatform ??
        renderSettings?.retention_target_platform ??
        renderSettings?.targetPlatform
      ),
      hook_selection_mode: normalizeHookSelectionMode(
        analysis?.hookSelectionMode ??
        analysis?.hook_selection_mode ??
        renderSettings?.hookSelectionMode ??
        renderSettings?.hook_selection_mode
      ),
      model_hook_score: modelHookScore,
      model_pacing_score: modelPacingScore,
      model_jank_score: modelJankScore,
      model_retention_score: modelRetentionScore
    })
  }

  return events
}

const aggregatePerformance = (
  events: FeedbackSignalEvent[],
  keyFn: (event: FeedbackSignalEvent) => string | null
) => {
  const map = new Map<string, { count: number; outcomeTotal: number }>()
  for (const event of events) {
    const key = keyFn(event)
    if (!key) continue
    const current = map.get(key) || { count: 0, outcomeTotal: 0 }
    current.count += 1
    current.outcomeTotal += event.signal_outcome
    map.set(key, current)
  }
  const rows: PerformanceRow[] = Array.from(map.entries())
    .map(([key, entry]) => ({
      key,
      count: entry.count,
      avg_outcome: roundTo(entry.outcomeTotal / Math.max(1, entry.count), 4)
    }))
    .sort((a, b) => (
      b.avg_outcome - a.avg_outcome ||
      b.count - a.count ||
      a.key.localeCompare(b.key)
    ))
  return rows
}

const chooseTopProfile = (
  rows: PerformanceRow[],
  baseline: number
): { key: string | null; margin: number } => {
  if (!rows.length) return { key: null, margin: 0 }
  const top = rows[0]
  const second = rows[1]
  const comparison = second ? second.avg_outcome : baseline
  const margin = roundTo(top.avg_outcome - comparison, 4)
  const minCount = Math.max(2, Math.round(rows.reduce((sum, row) => sum + row.count, 0) * 0.08))
  if (top.count < minCount) return { key: null, margin: 0 }
  return { key: top.key, margin }
}

const computeDeltaMap = ({
  avgOutcome,
  avgHookHold,
  avgCompletion,
  avgModelJank
}: {
  avgOutcome: number
  avgHookHold: number | null
  avgCompletion: number | null
  avgModelJank: number | null
}): ProposedDeltaMap => {
  const outcomeDeficit = clamp01((0.72 - avgOutcome) / 0.34)
  const hookDeficit = avgHookHold === null ? outcomeDeficit : clamp01((0.66 - avgHookHold) / 0.34)
  const completionDeficit = avgCompletion === null ? outcomeDeficit : clamp01((0.62 - avgCompletion) / 0.34)
  const jankDeficit = avgModelJank === null ? Math.max(0, outcomeDeficit - 0.1) : clamp01((0.6 - avgModelJank) / 0.35)

  const deltas: ProposedDeltaMap = {
    cut_aggression: roundTo(8 * outcomeDeficit + 4 * hookDeficit - 6 * jankDeficit, 3),
    pacing_multiplier: roundTo(0.16 * outcomeDeficit + 0.08 * completionDeficit - 0.11 * jankDeficit, 4),
    hook_priority_weight: roundTo(0.22 * hookDeficit + 0.06 * outcomeDeficit, 4),
    pattern_interrupt_every_sec: roundTo(-2.1 * completionDeficit + 1.6 * jankDeficit, 4),
    jank_guard: roundTo(13 * jankDeficit - 3.5 * completionDeficit, 3),
    story_coherence_guard: roundTo(9 * jankDeficit - 2.8 * completionDeficit, 3),
    silence_min_ms: roundTo(-110 * completionDeficit + 95 * jankDeficit, 2),
    filler_word_weight: roundTo(0.18 * outcomeDeficit, 4),
    redundancy_weight: roundTo(0.14 * outcomeDeficit, 4),
    spike_boost: roundTo(0.2 * hookDeficit, 4)
  }

  const filtered: ProposedDeltaMap = {}
  for (const [rawKey, rawValue] of Object.entries(deltas)) {
    const key = rawKey as keyof ProposedDeltaMap
    const value = Number(rawValue)
    if (!Number.isFinite(value) || Math.abs(value) < 0.01) continue
    filtered[key] = value
  }
  return filtered
}

const resolveSuggestedSubtitleMode = (editorMode: string | null): string | null => {
  if (!editorMode) return null
  if (editorMode === 'education' || editorMode === 'podcast' || editorMode === 'commentary') return 'premium_clean'
  if (editorMode === 'sports' || editorMode === 'reaction' || editorMode === 'gaming' || editorMode === 'savage-roast') return 'viral_pop'
  if (editorMode === 'vlog') return 'clean_high_contrast'
  return null
}

const buildSnapshot = (events: FeedbackSignalEvent[]): FeedbackBrainSnapshot => {
  const sampleSize = events.length
  if (!sampleSize) {
    return {
      generated_at: nowIso(),
      sample_size: 0,
      platform_feedback_share: 0,
      avg_outcome: 0,
      avg_hook_hold: null,
      avg_completion: null,
      avg_model_hook: null,
      avg_model_pacing: null,
      avg_model_jank: null,
      confidence: 0,
      predicted_delta_score: 0,
      recommended_editor_mode: null,
      recommended_strategy_profile: null,
      recommended_target_platform: null,
      rationale: ['No feedback-backed outcome samples found yet.'],
      proposed_param_deltas: {},
      mode_performance: [],
      strategy_performance: [],
      platform_performance: [],
      recent_signals: []
    }
  }

  const avgOutcome = roundTo(events.reduce((sum, event) => sum + event.signal_outcome, 0) / sampleSize, 4)
  const avgHookHold = averageOf(events.map((event) => event.hook_hold_percent))
  const avgCompletion = averageOf(events.map((event) => event.completion_percent))
  const avgModelHook = averageOf(events.map((event) => event.model_hook_score))
  const avgModelPacing = averageOf(events.map((event) => event.model_pacing_score))
  const avgModelJank = averageOf(events.map((event) => event.model_jank_score))
  const platformShare = roundTo(
    events.filter((event) => event.source_type === 'platform').length / Math.max(1, sampleSize),
    4
  )

  const modePerformance = aggregatePerformance(events, (event) => event.editor_mode)
  const strategyPerformance = aggregatePerformance(events, (event) => event.strategy_profile)
  const platformPerformance = aggregatePerformance(events, (event) => event.target_platform)

  const topMode = chooseTopProfile(modePerformance, avgOutcome)
  const topStrategy = chooseTopProfile(strategyPerformance, avgOutcome)
  const topPlatform = chooseTopProfile(platformPerformance, avgOutcome)
  const deltaMap = computeDeltaMap({
    avgOutcome,
    avgHookHold,
    avgCompletion,
    avgModelJank
  })
  const deltaMagnitude = roundTo(
    Object.values(deltaMap).reduce((sum, value) => sum + Math.abs(Number(value || 0)), 0),
    4
  )
  const predictedDeltaScore = roundTo(
    clamp(
      (0.72 - avgOutcome) * 0.45 +
      deltaMagnitude * 0.0024 +
      (topMode.margin > 0 ? Math.min(0.03, topMode.margin * 0.55) : 0),
      0,
      0.18
    ),
    4
  )
  const confidence = roundTo(
    clamp(
      (sampleSize / 120) * 0.62 +
      platformShare * 0.18 +
      Math.max(0, topMode.margin) * 1.9 +
      Math.max(0, topStrategy.margin) * 1.2,
      0,
      1
    ),
    4
  )

  const rationale: string[] = [
    `Built from ${sampleSize} feedback-linked completed renders.`,
    `Platform-verified feedback share: ${(platformShare * 100).toFixed(0)}%.`
  ]
  if (avgOutcome < 0.58) rationale.push('Global outcome trend is below target; applying stronger retention pressure.')
  else if (avgOutcome > 0.74) rationale.push('Outcome trend is healthy; only small adaptive steps are recommended.')
  if (topMode.key && topMode.margin > 0) rationale.push(`Best observed editor mode: ${topMode.key} (+${(topMode.margin * 100).toFixed(1)} pts).`)
  if (topStrategy.key && topStrategy.margin > 0) rationale.push(`Best observed retention strategy: ${topStrategy.key}.`)
  if (topPlatform.key && topPlatform.margin > 0) rationale.push(`Best observed target platform profile: ${topPlatform.key}.`)
  if (!Object.keys(deltaMap).length) rationale.push('No meaningful param drift detected from current feedback trends.')

  return {
    generated_at: nowIso(),
    sample_size: sampleSize,
    platform_feedback_share: platformShare,
    avg_outcome: avgOutcome,
    avg_hook_hold: avgHookHold,
    avg_completion: avgCompletion,
    avg_model_hook: avgModelHook,
    avg_model_pacing: avgModelPacing,
    avg_model_jank: avgModelJank,
    confidence,
    predicted_delta_score: predictedDeltaScore,
    recommended_editor_mode: topMode.key,
    recommended_strategy_profile: topStrategy.key,
    recommended_target_platform: topPlatform.key,
    rationale,
    proposed_param_deltas: Object.fromEntries(
      Object.entries(deltaMap).map(([key, value]) => [key, roundTo(Number(value), 4)])
    ),
    mode_performance: modePerformance,
    strategy_performance: strategyPerformance,
    platform_performance: platformPerformance,
    recent_signals: events.slice(0, MAX_RECENT_SIGNALS)
  }
}

const applyDeltasToConfig = ({
  base,
  deltas,
  confidence
}: {
  base: AlgorithmConfigParams
  deltas: Record<string, number>
  confidence: number
}) => {
  const next = parseConfigParams(base)
  const scale = clamp(0.42 + confidence * 0.64, 0.42, 1)

  for (const [rawKey, rawDelta] of Object.entries(deltas)) {
    if (!Object.prototype.hasOwnProperty.call(NUMERIC_PARAM_LIMITS, rawKey)) continue
    const key = rawKey as NumericParamKey
    const delta = Number(rawDelta)
    if (!Number.isFinite(delta)) continue
    const limits = NUMERIC_PARAM_LIMITS[key]
    const previous = Number(next[key])
    const targetRaw = previous + delta * scale
    const bounded = clamp(targetRaw, limits.min, limits.max)
    const value = limits.integer ? Math.round(bounded) : roundTo(bounded, 4)
    next[key] = value as never
  }

  return next
}

const elapsedMinutesSince = (iso: string | null) => {
  if (!iso) return Number.POSITIVE_INFINITY
  const parsed = new Date(iso)
  if (Number.isNaN(parsed.getTime())) return Number.POSITIVE_INFINITY
  return (Date.now() - parsed.getTime()) / 60_000
}

const shouldApply = ({
  settings,
  runtime,
  snapshot,
  forceApply
}: {
  settings: FeedbackLoopSettings
  runtime: FeedbackLoopRuntime
  snapshot: FeedbackBrainSnapshot
  forceApply: boolean
}) => {
  if (!settings.enabled && !forceApply) {
    return { ok: false, reason: 'Feedback loop is disabled.' }
  }
  if (!settings.auto_apply && !forceApply) {
    return { ok: false, reason: 'Auto-apply is disabled.' }
  }
  if (snapshot.sample_size < settings.min_feedback_samples) {
    return {
      ok: false,
      reason: `Need at least ${settings.min_feedback_samples} feedback samples, got ${snapshot.sample_size}.`
    }
  }
  if (snapshot.confidence < settings.min_confidence) {
    return {
      ok: false,
      reason: `Confidence ${snapshot.confidence.toFixed(3)} below threshold ${settings.min_confidence.toFixed(3)}.`
    }
  }
  if (snapshot.predicted_delta_score < settings.min_delta_score) {
    return {
      ok: false,
      reason: `Predicted uplift ${snapshot.predicted_delta_score.toFixed(4)} below threshold ${settings.min_delta_score.toFixed(4)}.`
    }
  }
  if (!Object.keys(snapshot.proposed_param_deltas).length) {
    return { ok: false, reason: 'No proposed parameter deltas to apply.' }
  }

  const elapsedMinutes = elapsedMinutesSince(runtime.last_applied_at)
  if (!forceApply && elapsedMinutes < settings.cooldown_minutes) {
    return {
      ok: false,
      reason: `Cooldown active (${elapsedMinutes.toFixed(1)}m elapsed / ${settings.cooldown_minutes}m required).`
    }
  }

  return { ok: true, reason: 'Eligible for adaptive apply.' }
}

const updateState = async (
  patch: Partial<FeedbackLoopState>
): Promise<FeedbackLoopState> => {
  await ensureStateLoaded()
  const next: FeedbackLoopState = {
    settings: patch.settings ? parseSettings(patch.settings) : stateCache.settings,
    runtime: patch.runtime ? parseRuntime(patch.runtime) : stateCache.runtime,
    updated_at: nowIso()
  }
  await persistState(next)
  return next
}

const createAdaptiveNote = ({
  trigger,
  snapshot
}: {
  trigger: FeedbackLoopTrigger
  snapshot: FeedbackBrainSnapshot
}) => {
  const mode = snapshot.recommended_editor_mode || 'mixed'
  const strategy = snapshot.recommended_strategy_profile || 'mixed'
  const platform = snapshot.recommended_target_platform || 'mixed'
  return [
    `Auto feedback update`,
    `trigger=${trigger}`,
    `samples=${snapshot.sample_size}`,
    `outcome=${snapshot.avg_outcome.toFixed(4)}`,
    `confidence=${snapshot.confidence.toFixed(4)}`,
    `predicted_delta=${snapshot.predicted_delta_score.toFixed(4)}`,
    `mode=${mode}`,
    `strategy=${strategy}`,
    `platform=${platform}`
  ].join(' | ')
}

export const getFeedbackLoopStatus = async (): Promise<FeedbackLoopStatus> => {
  await ensureStateLoaded()
  const events = await selectRecentFeedbackSignals(stateCache.settings.lookback_limit)
  const snapshot = buildSnapshot(events)
  return {
    settings: { ...stateCache.settings },
    runtime: { ...stateCache.runtime },
    brain_snapshot: snapshot
  }
}

export const updateFeedbackLoopSettings = async (
  patch: Partial<FeedbackLoopSettings>
): Promise<FeedbackLoopStatus> => {
  await ensureStateLoaded()
  const mergedSettings = parseSettings({
    ...stateCache.settings,
    ...(patch || {})
  })
  await updateState({ settings: mergedSettings, runtime: stateCache.runtime })
  return getFeedbackLoopStatus()
}

export const runFeedbackLoop = async ({
  trigger,
  actorUserId,
  forceApply = false
}: {
  trigger: FeedbackLoopTrigger
  actorUserId?: string | null
  forceApply?: boolean
}): Promise<FeedbackLoopRunResult> => {
  await ensureStateLoaded()
  const events = await selectRecentFeedbackSignals(stateCache.settings.lookback_limit)
  const snapshot = buildSnapshot(events)
  const eligibility = shouldApply({
    settings: stateCache.settings,
    runtime: stateCache.runtime,
    snapshot,
    forceApply
  })

  let applied = false
  let reason = eligibility.reason
  let config: AlgorithmConfigVersion | null = null
  let runtime: FeedbackLoopRuntime = {
    ...stateCache.runtime,
    last_run_at: nowIso(),
    last_run_reason: eligibility.reason,
    last_trigger: trigger
  }

  if (eligibility.ok) {
    try {
      const active = await getActiveConfigVersion()
      const nextParams = applyDeltasToConfig({
        base: active.params,
        deltas: snapshot.proposed_param_deltas,
        confidence: snapshot.confidence
      })
      const subtitleMode = resolveSuggestedSubtitleMode(snapshot.recommended_editor_mode)
      if (subtitleMode) nextParams.subtitle_style_mode = subtitleMode
      const note = createAdaptiveNote({ trigger, snapshot })
      config = await createConfigVersion({
        createdByUserId: actorUserId || null,
        presetName: active.preset_name || 'Adaptive Feedback Loop',
        params: parseConfigParams(nextParams),
        activate: true,
        note
      })
      applied = true
      reason = 'Applied adaptive feedback update.'
      runtime = {
        ...runtime,
        last_run_reason: reason,
        last_applied_at: nowIso(),
        last_applied_note: note,
        last_applied_config_version_id: config.id,
        last_apply_confidence: snapshot.confidence,
        last_apply_delta_score: snapshot.predicted_delta_score
      }
    } catch (error: any) {
      applied = false
      reason = `Adaptive apply failed: ${String(error?.message || 'unknown_error')}`
      runtime = {
        ...runtime,
        last_run_reason: reason
      }
    }
  }

  await updateState({
    settings: stateCache.settings,
    runtime
  })
  const status = await getFeedbackLoopStatus()
  return {
    applied,
    reason,
    config,
    status
  }
}
