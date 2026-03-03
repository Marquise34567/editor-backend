import crypto from 'crypto'
import { prisma } from '../db/prisma'

type SegmentLike = {
  start: number
  end: number
  speed?: number
  audioLeadInMs?: number
  audioTailMs?: number
  transitionStyle?: string
  [key: string]: any
}

type EngagementWindowLike = {
  start: number
  end: number
  score?: number
  speechIntensity?: number
}

type BaselineBoundaryLabel = {
  boundaryIndex: number
  time: number
  label: 'good' | 'bad'
  continuity: number
  context: number
  motion: number
  audio: number
  narrative: number
  notes: string | null
}

type HumanBaselineSample = {
  id: string
  userId: string
  sourceType: string
  sourceJobId: string | null
  videoUrl: string | null
  durationSeconds: number | null
  edl: any[]
  boundaryLabels: BaselineBoundaryLabel[]
  metadata: Record<string, any>
  createdAt: string
  updatedAt: string
}

export type BoundaryCriticModel = {
  version: string
  threshold: number
  weights: {
    continuity: number
    context: number
    motion: number
    audio: number
    narrative: number
    bias: number
  }
  metrics: {
    sampleCount: number
    accuracy: number
    precision: number
    recall: number
    f1: number
  }
  createdAt: string
}

type BoundaryFeature = {
  boundaryIndex: number
  time: number
  continuity: number
  context: number
  motion: number
  audio: number
  narrative: number
}

export type BoundaryCriticBoundaryScore = BoundaryFeature & {
  score: number
  highRisk: boolean
}

export type BoundaryCriticGateSummary = {
  modelVersion: string
  threshold: number
  worstScore: number
  averageScore: number
  highRiskCount: number
  boundaryCount: number
  fixesApplied: number
  passed: boolean
  reasons: string[]
}

type BoundaryCriticGateResult = {
  segments: SegmentLike[]
  summary: BoundaryCriticGateSummary
  boundaryScores: BoundaryCriticBoundaryScore[]
}

type MultiPassReport = {
  pass1: {
    name: 'story_beat_segmentation'
    segmentCountIn: number
    segmentCountOut: number
    notes: string[]
  }
  pass2: {
    name: 'cut_policy_selection'
    segmentCountIn: number
    segmentCountOut: number
    notes: string[]
    boundaryGate: BoundaryCriticGateSummary
  }
  pass3: {
    name: 'polish'
    segmentCountIn: number
    segmentCountOut: number
    notes: string[]
  }
}

export type MultiPassRefinementResult = {
  segments: SegmentLike[]
  report: MultiPassReport
}

type CreatorStyleProfile = {
  version: 1
  userId: string
  updatedAt: string
  sampleCount: number
  pacePreference: number
  cutAggression: number
  hookAggression: number
  preferredTransitionStyle: 'smooth' | 'jump' | 'mixed'
  qualityBias: number
  signals: {
    avgWatchPercent: number | null
    avgCompletionPercent: number | null
    avgHookHoldPercent: number | null
    avgRewatchRate: number | null
  }
}

type PolicyCandidate = {
  policyId: string
  variantScore: number
  predictedRetention?: number
}

export type PolicySelectionDecision = {
  selectedPolicyId: string
  reason: string
  leaderboard: Array<{
    policyId: string
    score: number
    learnedLift: number
    sampleCount: number
  }>
}

type PolicyStats = {
  policyId: string
  sampleCount: number
  mean: number
  variance: number
}

type PolicyPromotionCandidate = {
  policyId: string
  baselinePolicyId: string
  lift: number
  zScore: number
  sampleCount: number
  baselineSampleCount: number
  mean: number
  baselineMean: number
}

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))
const clamp01 = (value: number) => clamp(value, 0, 1)
const nowIso = () => new Date().toISOString()

const toFiniteNumber = (value: unknown, fallback = 0) => {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

const toNullableNumber = (value: unknown) => {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

const parseJsonArray = <T = any>(value: unknown): T[] => {
  if (Array.isArray(value)) return value as T[]
  if (typeof value !== 'string') return []
  try {
    const parsed = JSON.parse(value)
    return Array.isArray(parsed) ? (parsed as T[]) : []
  } catch {
    return []
  }
}

const parseJsonObject = (value: unknown): Record<string, any> => {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value as Record<string, any>
  if (typeof value !== 'string') return {}
  try {
    const parsed = JSON.parse(value)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? (parsed as Record<string, any>)
      : {}
  } catch {
    return {}
  }
}

const canRunRawSql = () =>
  typeof (prisma as any)?.$executeRawUnsafe === 'function' &&
  typeof (prisma as any)?.$queryRawUnsafe === 'function'

let infraEnsured = false
let lastEnsureAttemptAt = 0
let activeModelCache: BoundaryCriticModel | null = null

const inMemoryBaselineSamples = new Map<string, HumanBaselineSample>()
const inMemoryModels = new Map<string, BoundaryCriticModel>()
const inMemoryCreatorProfiles = new Map<string, CreatorStyleProfile>()
const inMemoryPolicyAssignments = new Map<string, any>()
const inMemoryPolicyOutcomes: Array<{
  userId: string
  jobId: string
  policyId: string
  variantId: string | null
  outcomeScore: number | null
  isPlatform: boolean
  outcomeSource: string | null
  metadata: Record<string, any>
  assignedAt: string
  outcomeAt: string | null
}> = []
const inMemoryRewardSignals: Array<{
  userId: string
  jobId: string
  source: string
  videoId: string | null
  perSecondRewards: number[]
  summary: Record<string, any>
  createdAt: string
}> = []

const DEFAULT_BOUNDARY_MODEL: BoundaryCriticModel = {
  version: 'heuristic-v1',
  threshold: 0.48,
  weights: {
    continuity: 1.24,
    context: 1.06,
    motion: 0.92,
    audio: 1.02,
    narrative: 1.15,
    bias: -2.52
  },
  metrics: {
    sampleCount: 0,
    accuracy: 0,
    precision: 0,
    recall: 0,
    f1: 0
  },
  createdAt: nowIso()
}

export const ensureEditorIntelligenceInfra = async () => {
  const now = Date.now()
  if (infraEnsured) return true
  if (now - lastEnsureAttemptAt < 10_000) return false
  lastEnsureAttemptAt = now
  if (!canRunRawSql()) return false
  try {
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS human_baseline_samples (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        source_type TEXT NOT NULL DEFAULT 'classic',
        source_job_id TEXT NULL,
        video_url TEXT NULL,
        duration_seconds DOUBLE PRECISION NULL,
        edl JSONB NOT NULL DEFAULT '[]'::jsonb,
        boundary_labels JSONB NOT NULL DEFAULT '[]'::jsonb,
        metadata JSONB NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS boundary_critic_models (
        id TEXT PRIMARY KEY,
        version TEXT NOT NULL UNIQUE,
        threshold DOUBLE PRECISION NOT NULL,
        weights JSONB NOT NULL,
        metrics JSONB NOT NULL,
        training_sample_count INTEGER NOT NULL DEFAULT 0,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS platform_reward_signals (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        source TEXT NOT NULL,
        video_id TEXT NULL,
        per_second_rewards JSONB NOT NULL DEFAULT '[]'::jsonb,
        summary JSONB NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS creator_style_profiles (
        user_id TEXT PRIMARY KEY,
        profile JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS policy_ab_assignments (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        policy_id TEXT NOT NULL,
        variant_id TEXT NULL,
        assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (job_id)
      )
    `)
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS policy_ab_outcomes (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        policy_id TEXT NOT NULL,
        variant_id TEXT NULL,
        outcome_score DOUBLE PRECISION NULL,
        outcome_source TEXT NULL,
        is_platform BOOLEAN NOT NULL DEFAULT FALSE,
        metadata JSONB NULL,
        assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        outcome_at TIMESTAMPTZ NULL
      )
    `)
    await (prisma as any).$executeRawUnsafe(`
      CREATE INDEX IF NOT EXISTS idx_policy_ab_outcomes_user_policy
      ON policy_ab_outcomes (user_id, policy_id)
    `)
    infraEnsured = true
    return true
  } catch (error) {
    console.warn('editor intelligence infra ensure failed, using in-memory fallback', error)
    return false
  }
}

const normalizeSegment = (segment: SegmentLike, durationSeconds: number): SegmentLike | null => {
  const start = clamp(toFiniteNumber(segment?.start, 0), 0, Math.max(0, durationSeconds - 0.05))
  const end = clamp(toFiniteNumber(segment?.end, 0), start + 0.05, durationSeconds)
  if (!Number.isFinite(start) || !Number.isFinite(end) || end - start < 0.05) return null
  const speed = clamp(toFiniteNumber(segment?.speed, 1), 0.8, 1.8)
  return {
    ...segment,
    start: Number(start.toFixed(3)),
    end: Number(end.toFixed(3)),
    speed: Number(speed.toFixed(3))
  }
}

const normalizeSegments = (segments: SegmentLike[], durationSeconds: number) => {
  const safeDuration = Math.max(0.5, toFiniteNumber(durationSeconds, 0))
  const normalized = (segments || [])
    .map((segment) => normalizeSegment(segment, safeDuration))
    .filter((segment): segment is SegmentLike => Boolean(segment))
    .sort((left, right) => left.start - right.start)
  if (!normalized.length) return []
  const out: SegmentLike[] = []
  for (const segment of normalized) {
    if (!out.length) {
      out.push({ ...segment })
      continue
    }
    const previous = out[out.length - 1]
    if (!previous) {
      out.push({ ...segment })
      continue
    }
    if (segment.start <= previous.end + 0.02) {
      previous.end = Number(Math.max(previous.end, segment.end).toFixed(3))
      previous.speed = Number((((previous.speed || 1) + (segment.speed || 1)) / 2).toFixed(3))
      continue
    }
    out.push({ ...segment })
  }
  return out
}

const scoreAtTime = (windows: EngagementWindowLike[], time: number) => {
  if (!windows.length) return 0.55
  for (const window of windows) {
    if (time >= window.start && time <= window.end) {
      return clamp01(toFiniteNumber(window.score, 0.55))
    }
  }
  let nearestDistance = Number.POSITIVE_INFINITY
  let nearestScore = 0.55
  for (const window of windows) {
    const midpoint = (toFiniteNumber(window.start, 0) + toFiniteNumber(window.end, 0)) / 2
    const distance = Math.abs(midpoint - time)
    if (distance < nearestDistance) {
      nearestDistance = distance
      nearestScore = clamp01(toFiniteNumber(window.score, 0.55))
    }
  }
  return nearestScore
}

const buildBoundaryFeatures = ({
  segments,
  durationSeconds,
  windows
}: {
  segments: SegmentLike[]
  durationSeconds: number
  windows?: EngagementWindowLike[]
}): BoundaryFeature[] => {
  const safeDuration = Math.max(0.5, toFiniteNumber(durationSeconds, 0))
  const ordered = normalizeSegments(segments, safeDuration)
  const engagementWindows = (windows || [])
    .map((window) => ({
      start: clamp(toFiniteNumber(window.start, 0), 0, safeDuration),
      end: clamp(toFiniteNumber(window.end, 0), 0, safeDuration),
      score: clamp01(toFiniteNumber(window.score, 0.55)),
      speechIntensity: clamp01(toFiniteNumber(window.speechIntensity, 0.5))
    }))
    .filter((window) => window.end > window.start)

  const boundaries: BoundaryFeature[] = []
  for (let index = 0; index < ordered.length - 1; index += 1) {
    const left = ordered[index]
    const right = ordered[index + 1]
    if (!left || !right) continue
    const boundaryTime = Number(((left.end + right.start) / 2).toFixed(3))
    const leftDuration = Math.max(0.05, left.end - left.start)
    const rightDuration = Math.max(0.05, right.end - right.start)
    const gap = Math.max(0, right.start - left.end)
    const overlap = Math.max(0, left.end - right.start)
    const durationDelta = Math.abs(leftDuration - rightDuration)
    const continuity = clamp01(
      1 -
      gap / 0.35 -
      overlap / 0.25 -
      durationDelta / Math.max(1.2, (leftDuration + rightDuration) / 1.8)
    )
    const context = clamp01(Math.min(leftDuration, rightDuration) / 2.4)
    const leftSpeed = clamp(toFiniteNumber(left.speed, 1), 0.8, 1.8)
    const rightSpeed = clamp(toFiniteNumber(right.speed, 1), 0.8, 1.8)
    const speedDelta = Math.abs(leftSpeed - rightSpeed)
    const motion = clamp01(1 - speedDelta / 0.62)
    const leadIn = clamp(toFiniteNumber(right.audioLeadInMs, 0), 0, 320)
    const tailOut = clamp(toFiniteNumber(left.audioTailMs, 0), 0, 320)
    const transitionStyle = String(left.transitionStyle || right.transitionStyle || '').trim().toLowerCase()
    const transitionBonus = transitionStyle === 'smooth' ? 0.1 : transitionStyle === 'jump' ? -0.04 : 0
    const audio = clamp01(0.62 * motion + 0.2 * clamp01((leadIn + tailOut) / 240) + 0.18 + transitionBonus)

    const local = scoreAtTime(engagementWindows, boundaryTime)
    const before = scoreAtTime(engagementWindows, Math.max(0, boundaryTime - 0.9))
    const after = scoreAtTime(engagementWindows, Math.min(safeDuration, boundaryTime + 0.9))
    const valleySignal = clamp01(((before + after) / 2) - local + 0.22)
    const turnSignal = clamp01(Math.abs(after - before) + 0.18)
    const narrative = clamp01(0.36 + valleySignal * 0.42 + turnSignal * 0.22)

    boundaries.push({
      boundaryIndex: index,
      time: boundaryTime,
      continuity: Number(continuity.toFixed(4)),
      context: Number(context.toFixed(4)),
      motion: Number(motion.toFixed(4)),
      audio: Number(audio.toFixed(4)),
      narrative: Number(narrative.toFixed(4))
    })
  }
  return boundaries
}

const sigmoid = (value: number) => 1 / (1 + Math.exp(-value))

const scoreBoundaryFeature = (feature: BoundaryFeature, model: BoundaryCriticModel) => {
  const weighted =
    model.weights.bias +
    model.weights.continuity * feature.continuity +
    model.weights.context * feature.context +
    model.weights.motion * feature.motion +
    model.weights.audio * feature.audio +
    model.weights.narrative * feature.narrative
  return clamp01(sigmoid(weighted))
}

const computeBoundaryScores = ({
  boundaries,
  model
}: {
  boundaries: BoundaryFeature[]
  model: BoundaryCriticModel
}) => boundaries.map((feature) => {
  const score = scoreBoundaryFeature(feature, model)
  return {
    ...feature,
    score: Number(score.toFixed(4)),
    highRisk: score < model.threshold
  } satisfies BoundaryCriticBoundaryScore
})

const summarizeBoundaryScores = ({
  scores,
  model,
  fixesApplied,
  reasons
}: {
  scores: BoundaryCriticBoundaryScore[]
  model: BoundaryCriticModel
  fixesApplied: number
  reasons?: string[]
}): BoundaryCriticGateSummary => {
  const boundaryCount = scores.length
  const worst = boundaryCount
    ? Math.min(...scores.map((score) => score.score))
    : 1
  const average = boundaryCount
    ? scores.reduce((sum, score) => sum + score.score, 0) / boundaryCount
    : 1
  const highRiskCount = scores.filter((score) => score.highRisk).length
  return {
    modelVersion: model.version,
    threshold: model.threshold,
    worstScore: Number(worst.toFixed(4)),
    averageScore: Number(average.toFixed(4)),
    highRiskCount,
    boundaryCount,
    fixesApplied,
    passed: highRiskCount === 0,
    reasons: reasons || []
  }
}

const cloneSegments = (segments: SegmentLike[]) => segments.map((segment) => ({ ...segment }))

export const applyBoundaryCriticHardGateWithModel = ({
  segments,
  durationSeconds,
  windows,
  model,
  maxFixes = 4
}: {
  segments: SegmentLike[]
  durationSeconds: number
  windows?: EngagementWindowLike[]
  model?: BoundaryCriticModel | null
  maxFixes?: number
}): BoundaryCriticGateResult => {
  const activeModel = model || activeModelCache || DEFAULT_BOUNDARY_MODEL
  let working = normalizeSegments(cloneSegments(segments || []), durationSeconds)
  let fixesApplied = 0
  const reasons: string[] = []
  if (working.length <= 1) {
    const scores = computeBoundaryScores({
      boundaries: buildBoundaryFeatures({ segments: working, durationSeconds, windows }),
      model: activeModel
    })
    return {
      segments: working,
      boundaryScores: scores,
      summary: summarizeBoundaryScores({
        scores,
        model: activeModel,
        fixesApplied,
        reasons: ['Single-segment timeline: no cut boundaries to gate.']
      })
    }
  }

  for (let attempt = 0; attempt < Math.max(0, maxFixes); attempt += 1) {
    const boundaryFeatures = buildBoundaryFeatures({
      segments: working,
      durationSeconds,
      windows
    })
    const scores = computeBoundaryScores({
      boundaries: boundaryFeatures,
      model: activeModel
    })
    const worst = scores.length
      ? scores.reduce((best, score) => (score.score < best.score ? score : best), scores[0])
      : null
    if (!worst || worst.score >= activeModel.threshold) {
      break
    }
    const left = working[worst.boundaryIndex]
    const right = working[worst.boundaryIndex + 1]
    if (!left || !right) break
    const merged: SegmentLike = {
      ...left,
      end: Number(right.end.toFixed(3)),
      speed: Number((((toFiniteNumber(left.speed, 1) + toFiniteNumber(right.speed, 1)) / 2)).toFixed(3)),
      transitionStyle: 'smooth',
      audioTailMs: Number(clamp(Math.max(toFiniteNumber(left.audioTailMs, 0), 120), 60, 260).toFixed(0))
    }
    working = [
      ...working.slice(0, worst.boundaryIndex),
      merged,
      ...working.slice(worst.boundaryIndex + 2)
    ]
    fixesApplied += 1
    reasons.push(`Merged segments around boundary ${worst.boundaryIndex} (${worst.time.toFixed(2)}s) score ${worst.score.toFixed(2)}.`)
    if (working.length <= 1) break
  }

  const finalSegments = normalizeSegments(working, durationSeconds)
  const finalScores = computeBoundaryScores({
    boundaries: buildBoundaryFeatures({
      segments: finalSegments,
      durationSeconds,
      windows
    }),
    model: activeModel
  })
  return {
    segments: finalSegments,
    boundaryScores: finalScores,
    summary: summarizeBoundaryScores({
      scores: finalScores,
      model: activeModel,
      fixesApplied,
      reasons
    })
  }
}

const mergeTinyStorySegments = (segments: SegmentLike[], minDuration = 0.45) => {
  if (segments.length <= 1) return segments
  const out = cloneSegments(segments)
  for (let index = 0; index < out.length; index += 1) {
    const current = out[index]
    if (!current) continue
    const duration = current.end - current.start
    if (duration >= minDuration) continue
    const previous = index > 0 ? out[index - 1] : null
    const next = index + 1 < out.length ? out[index + 1] : null
    if (previous && (!next || (previous.end - previous.start) <= (next.end - next.start))) {
      previous.end = Number(Math.max(previous.end, current.end).toFixed(3))
      out.splice(index, 1)
      index -= 1
      continue
    }
    if (next) {
      next.start = Number(Math.min(next.start, current.start).toFixed(3))
      out.splice(index, 1)
      index -= 1
    }
  }
  return out
}

const applyPacingNormalization = ({
  segments,
  profile
}: {
  segments: SegmentLike[]
  profile: CreatorStyleProfile | null
}) => {
  const pacePreference = profile?.pacePreference ?? 0
  const aggression = profile?.cutAggression ?? 0
  const speedBase = clamp(1 + pacePreference * 0.08 + aggression * 0.06, 0.94, 1.2)
  return segments.map((segment, index) => {
    const duration = Math.max(0.05, segment.end - segment.start)
    const durationBias = duration < 0.9 ? 0.04 : duration > 3.5 ? -0.04 : 0
    const speed = clamp(toFiniteNumber(segment.speed, 1) + durationBias, 0.88, 1.28)
    const adjustedSpeed = Number(clamp((speed + speedBase) / 2, 0.9, 1.28).toFixed(3))
    const isCutBoundary = index < segments.length - 1
    return {
      ...segment,
      speed: adjustedSpeed,
      transitionStyle: isCutBoundary
        ? (profile?.preferredTransitionStyle === 'jump' ? 'jump' : 'smooth')
        : segment.transitionStyle,
      audioLeadInMs: Number(clamp(Math.max(toFiniteNumber(segment.audioLeadInMs, 0), 90), 50, 260).toFixed(0)),
      audioTailMs: Number(clamp(Math.max(toFiniteNumber(segment.audioTailMs, 0), 90), 50, 260).toFixed(0))
    }
  })
}

export const runMultiPassRefinementWithModel = ({
  segments,
  durationSeconds,
  windows,
  model,
  creatorProfile
}: {
  segments: SegmentLike[]
  durationSeconds: number
  windows?: EngagementWindowLike[]
  model?: BoundaryCriticModel | null
  creatorProfile?: CreatorStyleProfile | null
}): MultiPassRefinementResult => {
  const safeDuration = Math.max(0.5, toFiniteNumber(durationSeconds, 0))
  const pass1Input = normalizeSegments(cloneSegments(segments || []), safeDuration)
  const pass1Merged = mergeTinyStorySegments(pass1Input, 0.5)
  const pass1 = normalizeSegments(pass1Merged, safeDuration)

  const gate = applyBoundaryCriticHardGateWithModel({
    segments: pass1,
    durationSeconds: safeDuration,
    windows,
    model
  })
  const pass2 = normalizeSegments(gate.segments, safeDuration)

  const polished = applyPacingNormalization({
    segments: pass2,
    profile: creatorProfile || null
  })
  const pass3 = normalizeSegments(polished, safeDuration)

  return {
    segments: pass3,
    report: {
      pass1: {
        name: 'story_beat_segmentation',
        segmentCountIn: pass1Input.length,
        segmentCountOut: pass1.length,
        notes: [
          'Merged micro-segments to preserve narrative context before policy scoring.'
        ]
      },
      pass2: {
        name: 'cut_policy_selection',
        segmentCountIn: pass1.length,
        segmentCountOut: pass2.length,
        notes: [
          'Applied boundary critic hard gate and merged pathological boundaries.'
        ],
        boundaryGate: gate.summary
      },
      pass3: {
        name: 'polish',
        segmentCountIn: pass2.length,
        segmentCountOut: pass3.length,
        notes: [
          'Applied pacing normalization and edge smoothing (J/L-friendly micro fades).'
        ]
      }
    }
  }
}

const normalizeBoundaryLabel = (entry: any): BaselineBoundaryLabel | null => {
  if (!entry || typeof entry !== 'object') return null
  const boundaryIndex = Math.max(0, Math.round(toFiniteNumber(entry.boundaryIndex ?? entry.index, -1)))
  if (!Number.isFinite(boundaryIndex) || boundaryIndex < 0) return null
  const labelRaw = String(entry.label || entry.class || '').trim().toLowerCase()
  const label = labelRaw === 'bad' || labelRaw === '1' || labelRaw === 'negative'
    ? 'bad'
    : 'good'
  const continuity = clamp01(toFiniteNumber(entry.continuity ?? entry.continuityScore, 0.5))
  const context = clamp01(toFiniteNumber(entry.context ?? entry.contextScore, 0.5))
  const motion = clamp01(toFiniteNumber(entry.motion ?? entry.motionScore, 0.5))
  const audio = clamp01(toFiniteNumber(entry.audio ?? entry.audioScore, 0.5))
  const narrative = clamp01(toFiniteNumber(entry.narrative ?? entry.narrativeScore, 0.5))
  const time = Math.max(0, toFiniteNumber(entry.time ?? entry.positionSeconds ?? 0, 0))
  const notes = String(entry.notes || '').trim() || null
  return {
    boundaryIndex,
    time: Number(time.toFixed(3)),
    label,
    continuity: Number(continuity.toFixed(4)),
    context: Number(context.toFixed(4)),
    motion: Number(motion.toFixed(4)),
    audio: Number(audio.toFixed(4)),
    narrative: Number(narrative.toFixed(4)),
    notes
  }
}

const rowToBaselineSample = (row: any): HumanBaselineSample | null => {
  if (!row || typeof row !== 'object') return null
  const id = String(row.id || '').trim()
  const userId = String(row.userId || row.user_id || '').trim()
  if (!id || !userId) return null
  return {
    id,
    userId,
    sourceType: String(row.sourceType || row.source_type || 'classic'),
    sourceJobId: String(row.sourceJobId || row.source_job_id || '').trim() || null,
    videoUrl: String(row.videoUrl || row.video_url || '').trim() || null,
    durationSeconds: toNullableNumber(row.durationSeconds || row.duration_seconds),
    edl: parseJsonArray(row.edl),
    boundaryLabels: parseJsonArray(row.boundaryLabels || row.boundary_labels)
      .map((entry) => normalizeBoundaryLabel(entry))
      .filter((entry): entry is BaselineBoundaryLabel => Boolean(entry)),
    metadata: parseJsonObject(row.metadata),
    createdAt: String(row.createdAt || row.created_at || nowIso()),
    updatedAt: String(row.updatedAt || row.updated_at || nowIso())
  }
}

export const collectHumanBaselineSample = async ({
  userId,
  sourceType,
  sourceJobId,
  videoUrl,
  durationSeconds,
  edl,
  boundaryLabels,
  metadata
}: {
  userId: string
  sourceType?: string
  sourceJobId?: string | null
  videoUrl?: string | null
  durationSeconds?: number | null
  edl?: any[]
  boundaryLabels?: any[]
  metadata?: Record<string, any>
}) => {
  const id = crypto.randomUUID()
  const normalizedLabels = (boundaryLabels || [])
    .map((entry) => normalizeBoundaryLabel(entry))
    .filter((entry): entry is BaselineBoundaryLabel => Boolean(entry))
  const sample: HumanBaselineSample = {
    id,
    userId,
    sourceType: String(sourceType || 'classic').trim() || 'classic',
    sourceJobId: sourceJobId ? String(sourceJobId).trim() : null,
    videoUrl: videoUrl ? String(videoUrl).trim() : null,
    durationSeconds: durationSeconds === null || durationSeconds === undefined ? null : Math.max(0.1, toFiniteNumber(durationSeconds, 0)),
    edl: Array.isArray(edl) ? edl : [],
    boundaryLabels: normalizedLabels,
    metadata: metadata && typeof metadata === 'object' ? metadata : {},
    createdAt: nowIso(),
    updatedAt: nowIso()
  }

  if (canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      await (prisma as any).$executeRawUnsafe(
        `
          INSERT INTO human_baseline_samples
            (id, user_id, source_type, source_job_id, video_url, duration_seconds, edl, boundary_labels, metadata, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, NOW(), NOW())
        `,
        sample.id,
        sample.userId,
        sample.sourceType,
        sample.sourceJobId,
        sample.videoUrl,
        sample.durationSeconds,
        JSON.stringify(sample.edl || []),
        JSON.stringify(sample.boundaryLabels || []),
        JSON.stringify(sample.metadata || {})
      )
      return sample
    } catch (error) {
      console.warn('collectHumanBaselineSample db write failed, using in-memory fallback', error)
    }
  }
  inMemoryBaselineSamples.set(sample.id, sample)
  return sample
}

export const labelHumanBaselineSampleBoundaries = async ({
  userId,
  sampleId,
  boundaryLabels,
  replace = false
}: {
  userId: string
  sampleId: string
  boundaryLabels: any[]
  replace?: boolean
}) => {
  const nextLabels = (boundaryLabels || [])
    .map((entry) => normalizeBoundaryLabel(entry))
    .filter((entry): entry is BaselineBoundaryLabel => Boolean(entry))
  if (!nextLabels.length) {
    throw new Error('no_boundary_labels')
  }

  if (canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        `
          SELECT
            id,
            user_id AS "userId",
            source_type AS "sourceType",
            source_job_id AS "sourceJobId",
            video_url AS "videoUrl",
            duration_seconds AS "durationSeconds",
            edl,
            boundary_labels AS "boundaryLabels",
            metadata,
            created_at AS "createdAt",
            updated_at AS "updatedAt"
          FROM human_baseline_samples
          WHERE id = $1 AND user_id = $2
          LIMIT 1
        `,
        sampleId,
        userId
      )
      const sample = rowToBaselineSample(Array.isArray(rows) && rows.length ? rows[0] : null)
      if (!sample) throw new Error('sample_not_found')
      const mergedMap = new Map<number, BaselineBoundaryLabel>()
      if (!replace) {
        for (const entry of sample.boundaryLabels) mergedMap.set(entry.boundaryIndex, entry)
      }
      for (const entry of nextLabels) mergedMap.set(entry.boundaryIndex, entry)
      const merged = Array.from(mergedMap.values()).sort((left, right) => left.boundaryIndex - right.boundaryIndex)
      await (prisma as any).$executeRawUnsafe(
        `
          UPDATE human_baseline_samples
          SET boundary_labels = $1::jsonb,
              updated_at = NOW()
          WHERE id = $2 AND user_id = $3
        `,
        JSON.stringify(merged),
        sampleId,
        userId
      )
      return {
        ...sample,
        boundaryLabels: merged,
        updatedAt: nowIso()
      }
    } catch (error: any) {
      const code = String(error?.message || '')
      if (code === 'sample_not_found') throw error
      console.warn('labelHumanBaselineSampleBoundaries db failed, using in-memory fallback', error)
    }
  }

  const cached = inMemoryBaselineSamples.get(sampleId)
  if (!cached || cached.userId !== userId) throw new Error('sample_not_found')
  const mergedMap = new Map<number, BaselineBoundaryLabel>()
  if (!replace) {
    for (const entry of cached.boundaryLabels) mergedMap.set(entry.boundaryIndex, entry)
  }
  for (const entry of nextLabels) mergedMap.set(entry.boundaryIndex, entry)
  const merged = Array.from(mergedMap.values()).sort((left, right) => left.boundaryIndex - right.boundaryIndex)
  const nextSample: HumanBaselineSample = {
    ...cached,
    boundaryLabels: merged,
    updatedAt: nowIso()
  }
  inMemoryBaselineSamples.set(sampleId, nextSample)
  return nextSample
}

export const listHumanBaselineSamples = async ({
  userId,
  limit = 40
}: {
  userId: string
  limit?: number
}) => {
  const take = clamp(Math.round(toFiniteNumber(limit, 40)), 1, 200)
  if (canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        `
          SELECT
            id,
            user_id AS "userId",
            source_type AS "sourceType",
            source_job_id AS "sourceJobId",
            video_url AS "videoUrl",
            duration_seconds AS "durationSeconds",
            edl,
            boundary_labels AS "boundaryLabels",
            metadata,
            created_at AS "createdAt",
            updated_at AS "updatedAt"
          FROM human_baseline_samples
          WHERE user_id = $1
          ORDER BY created_at DESC
          LIMIT $2
        `,
        userId,
        take
      )
      return (Array.isArray(rows) ? rows : [])
        .map((row) => rowToBaselineSample(row))
        .filter((sample): sample is HumanBaselineSample => Boolean(sample))
    } catch (error) {
      console.warn('listHumanBaselineSamples db read failed, using in-memory fallback', error)
    }
  }
  return Array.from(inMemoryBaselineSamples.values())
    .filter((sample) => sample.userId === userId)
    .sort((left, right) => new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime())
    .slice(0, take)
}

export const getHumanBaselineDatasetStats = async (userId: string) => {
  const samples = await listHumanBaselineSamples({ userId, limit: 5000 })
  const labels = samples.flatMap((sample) => sample.boundaryLabels || [])
  const goodCount = labels.filter((label) => label.label === 'good').length
  const badCount = labels.filter((label) => label.label === 'bad').length
  return {
    sampleCount: samples.length,
    labeledBoundaryCount: labels.length,
    goodBoundaryCount: goodCount,
    badBoundaryCount: badCount,
    coveragePercent: samples.length > 0
      ? Number(clamp((labels.length / samples.length) * 10, 0, 100).toFixed(2))
      : 0
  }
}

const featureVectorFromLabel = (label: BaselineBoundaryLabel) => ({
  continuity: clamp01(toFiniteNumber(label.continuity, 0.5)),
  context: clamp01(toFiniteNumber(label.context, 0.5)),
  motion: clamp01(toFiniteNumber(label.motion, 0.5)),
  audio: clamp01(toFiniteNumber(label.audio, 0.5)),
  narrative: clamp01(toFiniteNumber(label.narrative, 0.5)),
  y: label.label === 'good' ? 1 : 0
})

const computeMoments = (rows: Array<{ continuity: number; context: number; motion: number; audio: number; narrative: number; y: number }>, target: 0 | 1) => {
  const filtered = rows.filter((row) => row.y === target)
  const safe = filtered.length || 1
  const avg = {
    continuity: filtered.reduce((sum, row) => sum + row.continuity, 0) / safe,
    context: filtered.reduce((sum, row) => sum + row.context, 0) / safe,
    motion: filtered.reduce((sum, row) => sum + row.motion, 0) / safe,
    audio: filtered.reduce((sum, row) => sum + row.audio, 0) / safe,
    narrative: filtered.reduce((sum, row) => sum + row.narrative, 0) / safe
  }
  return {
    count: filtered.length,
    avg
  }
}

const calculateQualityMetrics = ({
  rows,
  model
}: {
  rows: Array<{ continuity: number; context: number; motion: number; audio: number; narrative: number; y: number }>
  model: BoundaryCriticModel
}) => {
  if (!rows.length) {
    return {
      sampleCount: 0,
      accuracy: 0,
      precision: 0,
      recall: 0,
      f1: 0
    }
  }
  let tp = 0
  let tn = 0
  let fp = 0
  let fn = 0
  for (const row of rows) {
    const feature: BoundaryFeature = {
      boundaryIndex: 0,
      time: 0,
      continuity: row.continuity,
      context: row.context,
      motion: row.motion,
      audio: row.audio,
      narrative: row.narrative
    }
    const score = scoreBoundaryFeature(feature, model)
    const predicted = score >= model.threshold ? 1 : 0
    if (predicted === 1 && row.y === 1) tp += 1
    else if (predicted === 0 && row.y === 0) tn += 1
    else if (predicted === 1 && row.y === 0) fp += 1
    else fn += 1
  }
  const total = tp + tn + fp + fn
  const accuracy = total > 0 ? (tp + tn) / total : 0
  const precision = tp + fp > 0 ? tp / (tp + fp) : 0
  const recall = tp + fn > 0 ? tp / (tp + fn) : 0
  const f1 = precision + recall > 0 ? (2 * precision * recall) / (precision + recall) : 0
  return {
    sampleCount: rows.length,
    accuracy: Number(accuracy.toFixed(4)),
    precision: Number(precision.toFixed(4)),
    recall: Number(recall.toFixed(4)),
    f1: Number(f1.toFixed(4))
  }
}

const normalizeWeight = (value: number) => {
  const scaled = clamp(value * 3.2, -2.4, 2.4)
  return Number(scaled.toFixed(4))
}

const rowToBoundaryModel = (row: any): BoundaryCriticModel | null => {
  if (!row || typeof row !== 'object') return null
  const weights = parseJsonObject(row.weights)
  const metrics = parseJsonObject(row.metrics)
  return {
    version: String(row.version || '').trim() || 'unknown',
    threshold: Number(clamp(toFiniteNumber(row.threshold, DEFAULT_BOUNDARY_MODEL.threshold), 0.2, 0.8).toFixed(4)),
    weights: {
      continuity: toFiniteNumber(weights.continuity, DEFAULT_BOUNDARY_MODEL.weights.continuity),
      context: toFiniteNumber(weights.context, DEFAULT_BOUNDARY_MODEL.weights.context),
      motion: toFiniteNumber(weights.motion, DEFAULT_BOUNDARY_MODEL.weights.motion),
      audio: toFiniteNumber(weights.audio, DEFAULT_BOUNDARY_MODEL.weights.audio),
      narrative: toFiniteNumber(weights.narrative, DEFAULT_BOUNDARY_MODEL.weights.narrative),
      bias: toFiniteNumber(weights.bias, DEFAULT_BOUNDARY_MODEL.weights.bias)
    },
    metrics: {
      sampleCount: Math.max(0, Math.round(toFiniteNumber(metrics.sampleCount, 0))),
      accuracy: clamp01(toFiniteNumber(metrics.accuracy, 0)),
      precision: clamp01(toFiniteNumber(metrics.precision, 0)),
      recall: clamp01(toFiniteNumber(metrics.recall, 0)),
      f1: clamp01(toFiniteNumber(metrics.f1, 0))
    },
    createdAt: String(row.createdAt || row.created_at || nowIso())
  }
}

const saveBoundaryModel = async (model: BoundaryCriticModel) => {
  activeModelCache = model
  inMemoryModels.set(model.version, model)
  if (canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      await (prisma as any).$executeRawUnsafe(
        `UPDATE boundary_critic_models SET is_active = FALSE WHERE is_active = TRUE`
      )
      await (prisma as any).$executeRawUnsafe(
        `
          INSERT INTO boundary_critic_models
            (id, version, threshold, weights, metrics, training_sample_count, is_active, created_at)
          VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, TRUE, NOW())
        `,
        crypto.randomUUID(),
        model.version,
        model.threshold,
        JSON.stringify(model.weights),
        JSON.stringify(model.metrics),
        model.metrics.sampleCount
      )
      return
    } catch (error) {
      console.warn('saveBoundaryModel db write failed, using in-memory fallback', error)
    }
  }
}

export const getActiveBoundaryCriticModel = async () => {
  if (activeModelCache) return activeModelCache
  if (canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        `
          SELECT
            version,
            threshold,
            weights,
            metrics,
            created_at AS "createdAt"
          FROM boundary_critic_models
          WHERE is_active = TRUE
          ORDER BY created_at DESC
          LIMIT 1
        `
      )
      const model = rowToBoundaryModel(Array.isArray(rows) && rows.length ? rows[0] : null)
      if (model) {
        activeModelCache = model
        return model
      }
    } catch (error) {
      console.warn('getActiveBoundaryCriticModel db read failed, using fallback', error)
    }
  }
  const latestInMemory = Array.from(inMemoryModels.values())
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt))[0]
  activeModelCache = latestInMemory || DEFAULT_BOUNDARY_MODEL
  return activeModelCache
}

export const trainBoundaryCriticModelFromBaseline = async ({
  userId,
  minSamples = 60
}: {
  userId: string
  minSamples?: number
}) => {
  const samples = await listHumanBaselineSamples({
    userId,
    limit: 5000
  })
  const rows = samples
    .flatMap((sample) => sample.boundaryLabels || [])
    .map((label) => featureVectorFromLabel(label))
  const required = Math.max(20, Math.round(toFiniteNumber(minSamples, 60)))
  if (rows.length < required) {
    const error = new Error(`not_enough_samples:${rows.length}`)
    ;(error as any).code = 'not_enough_samples'
    throw error
  }
  const goodMoments = computeMoments(rows, 1)
  const badMoments = computeMoments(rows, 0)
  const diff = {
    continuity: goodMoments.avg.continuity - badMoments.avg.continuity,
    context: goodMoments.avg.context - badMoments.avg.context,
    motion: goodMoments.avg.motion - badMoments.avg.motion,
    audio: goodMoments.avg.audio - badMoments.avg.audio,
    narrative: goodMoments.avg.narrative - badMoments.avg.narrative
  }
  const weights = {
    continuity: normalizeWeight(diff.continuity),
    context: normalizeWeight(diff.context),
    motion: normalizeWeight(diff.motion),
    audio: normalizeWeight(diff.audio),
    narrative: normalizeWeight(diff.narrative),
    bias: Number((-2.2 - (diff.continuity + diff.context + diff.motion + diff.audio + diff.narrative)).toFixed(4))
  }
  const version = `baseline-${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`
  const model: BoundaryCriticModel = {
    version,
    threshold: DEFAULT_BOUNDARY_MODEL.threshold,
    weights,
    metrics: {
      sampleCount: rows.length,
      accuracy: 0,
      precision: 0,
      recall: 0,
      f1: 0
    },
    createdAt: nowIso()
  }
  const metrics = calculateQualityMetrics({
    rows,
    model
  })
  model.metrics = metrics
  await saveBoundaryModel(model)
  return model
}

export const scoreBoundarySet = async ({
  segments,
  durationSeconds,
  windows
}: {
  segments: SegmentLike[]
  durationSeconds: number
  windows?: EngagementWindowLike[]
}) => {
  const model = await getActiveBoundaryCriticModel()
  const boundaryFeatures = buildBoundaryFeatures({
    segments,
    durationSeconds,
    windows
  })
  const scores = computeBoundaryScores({
    boundaries: boundaryFeatures,
    model
  })
  return {
    model,
    scores,
    summary: summarizeBoundaryScores({
      scores,
      model,
      fixesApplied: 0
    })
  }
}

export const applyBoundaryCriticHardGate = async ({
  segments,
  durationSeconds,
  windows,
  maxFixes = 4
}: {
  segments: SegmentLike[]
  durationSeconds: number
  windows?: EngagementWindowLike[]
  maxFixes?: number
}) => {
  const model = await getActiveBoundaryCriticModel()
  return applyBoundaryCriticHardGateWithModel({
    segments,
    durationSeconds,
    windows,
    model,
    maxFixes
  })
}

export const runMultiPassRefinement = async ({
  segments,
  durationSeconds,
  windows,
  creatorProfile
}: {
  segments: SegmentLike[]
  durationSeconds: number
  windows?: EngagementWindowLike[]
  creatorProfile?: CreatorStyleProfile | null
}) => {
  const model = await getActiveBoundaryCriticModel()
  return runMultiPassRefinementWithModel({
    segments,
    durationSeconds,
    windows,
    model,
    creatorProfile
  })
}

const normalizeCreatorStyleProfile = (profile: any, userId: string): CreatorStyleProfile => ({
  version: 1,
  userId,
  updatedAt: String(profile?.updatedAt || nowIso()),
  sampleCount: Math.max(0, Math.round(toFiniteNumber(profile?.sampleCount, 0))),
  pacePreference: Number(clamp(toFiniteNumber(profile?.pacePreference, 0), -1, 1).toFixed(4)),
  cutAggression: Number(clamp(toFiniteNumber(profile?.cutAggression, 0), -1, 1).toFixed(4)),
  hookAggression: Number(clamp(toFiniteNumber(profile?.hookAggression, 0), 0, 1).toFixed(4)),
  preferredTransitionStyle:
    profile?.preferredTransitionStyle === 'jump' || profile?.preferredTransitionStyle === 'mixed'
      ? profile.preferredTransitionStyle
      : 'smooth',
  qualityBias: Number(clamp(toFiniteNumber(profile?.qualityBias, 0), -1, 1).toFixed(4)),
  signals: {
    avgWatchPercent: toNullableNumber(profile?.signals?.avgWatchPercent),
    avgCompletionPercent: toNullableNumber(profile?.signals?.avgCompletionPercent),
    avgHookHoldPercent: toNullableNumber(profile?.signals?.avgHookHoldPercent),
    avgRewatchRate: toNullableNumber(profile?.signals?.avgRewatchRate)
  }
})

const defaultCreatorStyleProfile = (userId: string): CreatorStyleProfile => ({
  version: 1,
  userId,
  updatedAt: nowIso(),
  sampleCount: 0,
  pacePreference: 0,
  cutAggression: 0,
  hookAggression: 0.5,
  preferredTransitionStyle: 'smooth',
  qualityBias: 0,
  signals: {
    avgWatchPercent: null,
    avgCompletionPercent: null,
    avgHookHoldPercent: null,
    avgRewatchRate: null
  }
})

export const getCreatorStyleProfile = async (userId: string): Promise<CreatorStyleProfile> => {
  if (!userId) return defaultCreatorStyleProfile('unknown')
  if (canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        `
          SELECT user_id AS "userId", profile, updated_at AS "updatedAt"
          FROM creator_style_profiles
          WHERE user_id = $1
          LIMIT 1
        `,
        userId
      )
      const row = Array.isArray(rows) && rows.length ? rows[0] : null
      if (row) {
        const parsed = normalizeCreatorStyleProfile({
          ...parseJsonObject(row.profile),
          updatedAt: row.updatedAt
        }, userId)
        inMemoryCreatorProfiles.set(userId, parsed)
        return parsed
      }
    } catch (error) {
      console.warn('getCreatorStyleProfile db read failed, using in-memory fallback', error)
    }
  }
  return inMemoryCreatorProfiles.get(userId) || defaultCreatorStyleProfile(userId)
}

const rollingAverage = (current: number | null, next: number | null, count: number) => {
  if (next === null || !Number.isFinite(next)) return current
  if (current === null || !Number.isFinite(current)) return next
  const safeCount = Math.max(1, count)
  return Number((((current * (safeCount - 1)) + next) / safeCount).toFixed(4))
}

export const upsertCreatorStyleProfileFromFeedback = async ({
  userId,
  feedback
}: {
  userId: string
  feedback: Record<string, any>
}) => {
  const current = await getCreatorStyleProfile(userId)
  const nextSampleCount = current.sampleCount + 1
  const source = String(feedback?.source || '').toLowerCase()
  const category = String(feedback?.category || '').toLowerCase()
  const watchPercent = toNullableNumber(feedback?.watchPercent ?? feedback?.watch_percent)
  const completionPercent = toNullableNumber(feedback?.completionPercent ?? feedback?.completion_percent)
  const hookHoldPercent = toNullableNumber(feedback?.hookHoldPercent ?? feedback?.hook_hold_percent)
  const rewatchRate = toNullableNumber(feedback?.rewatchRate ?? feedback?.rewatch_rate)
  const manualScore = toNullableNumber(feedback?.manualScore ?? feedback?.manual_score)

  let pacePreference = current.pacePreference
  let cutAggression = current.cutAggression
  let hookAggression = current.hookAggression
  let preferredTransitionStyle: CreatorStyleProfile['preferredTransitionStyle'] = current.preferredTransitionStyle
  let qualityBias = current.qualityBias

  if (category === 'too_fast') {
    pacePreference -= 0.18
    cutAggression -= 0.16
    preferredTransitionStyle = 'smooth'
  } else if (category === 'too_generic') {
    cutAggression += 0.12
    hookAggression += 0.08
    preferredTransitionStyle = 'jump'
  } else if (category === 'bad_hook') {
    hookAggression += 0.12
  } else if (category === 'great_edit') {
    qualityBias += 0.1
  }

  if (watchPercent !== null) {
    if (watchPercent >= 58) {
      hookAggression += 0.04
      cutAggression += 0.03
    } else if (watchPercent <= 40) {
      cutAggression -= 0.06
    }
  }
  if (completionPercent !== null) {
    if (completionPercent >= 42) qualityBias += 0.05
    else if (completionPercent <= 26) pacePreference -= 0.04
  }
  if (rewatchRate !== null && rewatchRate >= 8) {
    hookAggression += 0.04
    cutAggression += 0.05
  }
  if (manualScore !== null) {
    qualityBias += (manualScore - 50) / 500
  }
  if (source.includes('platform')) {
    qualityBias += 0.02
  }

  const updated: CreatorStyleProfile = normalizeCreatorStyleProfile({
    ...current,
    updatedAt: nowIso(),
    sampleCount: nextSampleCount,
    pacePreference: clamp(pacePreference, -1, 1),
    cutAggression: clamp(cutAggression, -1, 1),
    hookAggression: clamp(hookAggression, 0, 1),
    preferredTransitionStyle,
    qualityBias: clamp(qualityBias, -1, 1),
    signals: {
      avgWatchPercent: rollingAverage(current.signals.avgWatchPercent, watchPercent, nextSampleCount),
      avgCompletionPercent: rollingAverage(current.signals.avgCompletionPercent, completionPercent, nextSampleCount),
      avgHookHoldPercent: rollingAverage(current.signals.avgHookHoldPercent, hookHoldPercent, nextSampleCount),
      avgRewatchRate: rollingAverage(current.signals.avgRewatchRate, rewatchRate, nextSampleCount)
    }
  }, userId)

  inMemoryCreatorProfiles.set(userId, updated)
  if (canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      await (prisma as any).$executeRawUnsafe(
        `
          INSERT INTO creator_style_profiles (user_id, profile, updated_at)
          VALUES ($1, $2::jsonb, NOW())
          ON CONFLICT (user_id) DO UPDATE
          SET profile = EXCLUDED.profile,
              updated_at = NOW()
        `,
        userId,
        JSON.stringify(updated)
      )
    } catch (error) {
      console.warn('upsertCreatorStyleProfileFromFeedback db write failed', error)
    }
  }
  return updated
}

const interpolateSeries = ({
  durationSeconds,
  points
}: {
  durationSeconds: number
  points: Array<{ t: number; value: number }>
}) => {
  const safeDuration = Math.max(1, Math.round(durationSeconds))
  const sorted = points
    .map((point) => ({
      t: clamp(toFiniteNumber(point.t, 0), 0, safeDuration),
      value: clamp(toFiniteNumber(point.value, 0), -1, 1)
    }))
    .sort((left, right) => left.t - right.t)
  const out = new Array<number>(safeDuration).fill(0)
  if (!sorted.length) return out
  for (let second = 0; second < safeDuration; second += 1) {
    const t = second
    let left = sorted[0]
    let right = sorted[sorted.length - 1]
    for (let index = 0; index < sorted.length; index += 1) {
      if (sorted[index].t <= t) left = sorted[index]
      if (sorted[index].t >= t) {
        right = sorted[index]
        break
      }
    }
    if (right.t === left.t) {
      out[second] = Number(left.value.toFixed(5))
      continue
    }
    const ratio = clamp01((t - left.t) / Math.max(0.001, right.t - left.t))
    out[second] = Number((left.value + (right.value - left.value) * ratio).toFixed(5))
  }
  return out
}

export const derivePerSecondRewardSignal = ({
  durationSeconds,
  retentionPoints,
  skipHotspots,
  rewatchHotspots
}: {
  durationSeconds: number
  retentionPoints?: Array<{ timestamp: number; watchedPct?: number; value?: number }>
  skipHotspots?: Array<{ second: number; weight?: number }>
  rewatchHotspots?: Array<{ second: number; weight?: number }>
}) => {
  const safeDuration = Math.max(1, Math.round(toFiniteNumber(durationSeconds, 0)))
  const retentionSeries = interpolateSeries({
    durationSeconds: safeDuration,
    points: (retentionPoints || []).map((point) => ({
      t: toFiniteNumber(point.timestamp, 0),
      value: ((toFiniteNumber(point.watchedPct ?? point.value, 0) / 100) * 2) - 1
    }))
  })
  const rewards = retentionSeries.map((value, second) => {
    let reward = value
    for (const hotspot of skipHotspots || []) {
      const distance = Math.abs(toFiniteNumber(hotspot.second, -9999) - second)
      if (distance > 2) continue
      reward -= clamp(toFiniteNumber(hotspot.weight, 0.2) * (1 - distance / 3), 0, 0.7)
    }
    for (const hotspot of rewatchHotspots || []) {
      const distance = Math.abs(toFiniteNumber(hotspot.second, -9999) - second)
      if (distance > 2) continue
      reward += clamp(toFiniteNumber(hotspot.weight, 0.2) * (1 - distance / 3), 0, 0.7)
    }
    return Number(clamp(reward, -1, 1).toFixed(5))
  })
  const average = rewards.length
    ? rewards.reduce((sum, value) => sum + value, 0) / rewards.length
    : 0
  const p95 = rewards.length
    ? rewards
      .slice()
      .sort((left, right) => left - right)[Math.min(rewards.length - 1, Math.floor(rewards.length * 0.95))]
    : 0
  return {
    perSecondRewards: rewards,
    summary: {
      averageReward: Number(average.toFixed(5)),
      p95Reward: Number((p95 || 0).toFixed(5)),
      durationSeconds: safeDuration
    }
  }
}

export const ingestPlatformRewardSignal = async ({
  userId,
  jobId,
  source,
  videoId,
  perSecondRewards,
  summary
}: {
  userId: string
  jobId: string
  source: string
  videoId?: string | null
  perSecondRewards: number[]
  summary?: Record<string, any>
}) => {
  const payload = {
    userId,
    jobId,
    source: String(source || 'platform').trim() || 'platform',
    videoId: videoId ? String(videoId).trim() : null,
    perSecondRewards: (perSecondRewards || []).map((value) => Number(clamp(toFiniteNumber(value, 0), -1, 1).toFixed(5))),
    summary: summary && typeof summary === 'object' ? summary : {},
    createdAt: nowIso()
  }
  inMemoryRewardSignals.push(payload)
  if (canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      await (prisma as any).$executeRawUnsafe(
        `
          INSERT INTO platform_reward_signals
            (user_id, job_id, source, video_id, per_second_rewards, summary, created_at)
          VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, NOW())
        `,
        payload.userId,
        payload.jobId,
        payload.source,
        payload.videoId,
        JSON.stringify(payload.perSecondRewards),
        JSON.stringify(payload.summary || {})
      )
    } catch (error) {
      console.warn('ingestPlatformRewardSignal db write failed', error)
    }
  }
  return payload
}

const deriveFeedbackOutcomeScore = (feedback: Record<string, any>) => {
  const watch = toNullableNumber(feedback.watchPercent ?? feedback.watch_percent)
  const completion = toNullableNumber(feedback.completionPercent ?? feedback.completion_percent)
  const hook = toNullableNumber(feedback.hookHoldPercent ?? feedback.hook_hold_percent)
  const rewatch = toNullableNumber(feedback.rewatchRate ?? feedback.rewatch_rate)
  const manual = toNullableNumber(feedback.manualScore ?? feedback.manual_score)
  const weights: Array<[number | null, number]> = [
    [watch, 0.35],
    [completion, 0.3],
    [hook, 0.2],
    [rewatch, 0.08],
    [manual, 0.07]
  ]
  let weightedTotal = 0
  let weightTotal = 0
  for (const [value, weight] of weights) {
    if (value === null || !Number.isFinite(value)) continue
    weightedTotal += clamp(value, 0, 100) * weight
    weightTotal += weight
  }
  if (weightTotal <= 0) return null
  return Number((weightedTotal / weightTotal).toFixed(4))
}

export const registerPolicyAssignment = async ({
  userId,
  jobId,
  policyId,
  variantId
}: {
  userId: string
  jobId: string
  policyId: string
  variantId?: string | null
}) => {
  if (!userId || !jobId || !policyId) return { ok: false }
  const assignment = {
    userId,
    jobId,
    policyId,
    variantId: variantId ? String(variantId).trim() : null,
    assignedAt: nowIso()
  }
  inMemoryPolicyAssignments.set(jobId, assignment)
  if (canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      await (prisma as any).$executeRawUnsafe(
        `
          INSERT INTO policy_ab_assignments (user_id, job_id, policy_id, variant_id, assigned_at)
          VALUES ($1, $2, $3, $4, NOW())
          ON CONFLICT (job_id) DO UPDATE
          SET user_id = EXCLUDED.user_id,
              policy_id = EXCLUDED.policy_id,
              variant_id = EXCLUDED.variant_id,
              assigned_at = NOW()
        `,
        assignment.userId,
        assignment.jobId,
        assignment.policyId,
        assignment.variantId
      )
    } catch (error) {
      console.warn('registerPolicyAssignment db write failed', error)
    }
  }
  return { ok: true, assignment }
}

export const registerPolicyOutcomeForJob = async ({
  userId,
  jobId,
  feedback,
  source,
  isPlatform,
  metadata
}: {
  userId: string
  jobId: string
  feedback: Record<string, any>
  source?: string | null
  isPlatform?: boolean
  metadata?: Record<string, any>
}) => {
  if (!userId || !jobId) return { ok: false, reason: 'missing_context' }
  let assignment = inMemoryPolicyAssignments.get(jobId) || null
  if (!assignment && canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        `
          SELECT
            user_id AS "userId",
            job_id AS "jobId",
            policy_id AS "policyId",
            variant_id AS "variantId",
            assigned_at AS "assignedAt"
          FROM policy_ab_assignments
          WHERE job_id = $1
          LIMIT 1
        `,
        jobId
      )
      assignment = Array.isArray(rows) && rows.length ? rows[0] : null
    } catch (error) {
      console.warn('registerPolicyOutcomeForJob assignment lookup failed', error)
    }
  }
  if (!assignment || String(assignment.userId || '') !== String(userId)) {
    return { ok: false, reason: 'assignment_not_found' }
  }
  const outcomeScore = deriveFeedbackOutcomeScore(feedback)
  const record = {
    userId,
    jobId,
    policyId: String(assignment.policyId || ''),
    variantId: assignment.variantId ? String(assignment.variantId) : null,
    outcomeScore,
    outcomeSource: source ? String(source) : null,
    isPlatform: Boolean(isPlatform),
    metadata: metadata && typeof metadata === 'object' ? metadata : {},
    assignedAt: String(assignment.assignedAt || nowIso()),
    outcomeAt: nowIso()
  }
  inMemoryPolicyOutcomes.push(record)
  if (canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      await (prisma as any).$executeRawUnsafe(
        `
          INSERT INTO policy_ab_outcomes
            (user_id, job_id, policy_id, variant_id, outcome_score, outcome_source, is_platform, metadata, assigned_at, outcome_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, NOW())
        `,
        record.userId,
        record.jobId,
        record.policyId,
        record.variantId,
        record.outcomeScore,
        record.outcomeSource,
        record.isPlatform,
        JSON.stringify(record.metadata || {}),
        record.assignedAt
      )
    } catch (error) {
      console.warn('registerPolicyOutcomeForJob db write failed', error)
    }
  }
  return { ok: true, outcome: record }
}

const aggregatePolicyStats = (rows: Array<{ policyId: string; outcomeScore: number | null }>): PolicyStats[] => {
  const grouped = new Map<string, number[]>()
  for (const row of rows) {
    if (!row.policyId) continue
    if (row.outcomeScore === null || !Number.isFinite(row.outcomeScore)) continue
    const values = grouped.get(row.policyId) || []
    values.push(clamp(row.outcomeScore, 0, 100))
    grouped.set(row.policyId, values)
  }
  return Array.from(grouped.entries()).map(([policyId, values]) => {
    const n = values.length
    const mean = n ? values.reduce((sum, value) => sum + value, 0) / n : 0
    const variance = n > 1
      ? values.reduce((sum, value) => sum + Math.pow(value - mean, 2), 0) / (n - 1)
      : 0
    return {
      policyId,
      sampleCount: n,
      mean: Number(mean.toFixed(4)),
      variance: Number(variance.toFixed(4))
    }
  })
}

const loadPolicyStatsForUser = async (userId: string): Promise<PolicyStats[]> => {
  if (canRunRawSql()) {
    try {
      await ensureEditorIntelligenceInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        `
          SELECT policy_id AS "policyId", outcome_score AS "outcomeScore"
          FROM policy_ab_outcomes
          WHERE user_id = $1 AND outcome_score IS NOT NULL
        `,
        userId
      )
      const data = Array.isArray(rows) ? rows : []
      return aggregatePolicyStats(
        data.map((row: any) => ({
          policyId: String(row.policyId || ''),
          outcomeScore: toNullableNumber(row.outcomeScore)
        }))
      )
    } catch (error) {
      console.warn('loadPolicyStatsForUser db read failed, using in-memory fallback', error)
    }
  }
  return aggregatePolicyStats(
    inMemoryPolicyOutcomes
      .filter((entry) => entry.userId === userId)
      .map((entry) => ({
        policyId: entry.policyId,
        outcomeScore: entry.outcomeScore
      }))
  )
}

const normalizeVariantScore = (value: number) => {
  if (!Number.isFinite(value)) return 0
  if (value <= 1) return clamp(value * 100, 0, 100)
  if (value <= 100) return clamp(value, 0, 100)
  return clamp(value / 2, 0, 100)
}

export const selectPolicyWinnerWithLearning = async ({
  userId,
  candidates,
  explorationRate = 0.14
}: {
  userId: string
  candidates: PolicyCandidate[]
  explorationRate?: number
}): Promise<PolicySelectionDecision | null> => {
  const pool = (candidates || [])
    .filter((candidate) => candidate && String(candidate.policyId || '').trim())
    .map((candidate) => ({
      policyId: String(candidate.policyId).trim(),
      variantScore: normalizeVariantScore(toFiniteNumber(candidate.variantScore, 0)),
      predictedRetention: normalizeVariantScore(toFiniteNumber(candidate.predictedRetention, 0))
    }))
  if (!pool.length) return null
  const stats = await loadPolicyStatsForUser(userId)
  const statsMap = new Map(stats.map((entry) => [entry.policyId, entry]))
  const totalSamples = stats.reduce((sum, entry) => sum + entry.sampleCount, 0)

  const ranked = pool.map((candidate) => {
    const learned = statsMap.get(candidate.policyId)
    const learnedMean = learned ? learned.mean : 50
    const learnedLift = learned ? (learned.mean - 50) / 8 : 0
    const uncertainty = learned
      ? Math.sqrt(Math.max(0.0001, learned.variance) / Math.max(1, learned.sampleCount))
      : 8
    const explorationBonus = Math.sqrt(2 * Math.log(totalSamples + 2) / Math.max(1, learned?.sampleCount || 1))
    const score =
      candidate.variantScore * 0.72 +
      learnedMean * 0.2 +
      candidate.predictedRetention * 0.08 +
      explorationBonus * 1.4 -
      uncertainty * 0.18
    return {
      policyId: candidate.policyId,
      score: Number(score.toFixed(4)),
      learnedLift: Number(learnedLift.toFixed(4)),
      sampleCount: learned?.sampleCount || 0
    }
  })
    .sort((left, right) => right.score - left.score || right.sampleCount - left.sampleCount || left.policyId.localeCompare(right.policyId))

  if (!ranked.length) return null
  const seed = Number.parseInt(
    crypto
      .createHash('sha1')
      .update(`${userId}:${ranked.map((entry) => `${entry.policyId}:${entry.score.toFixed(2)}`).join('|')}`)
      .digest('hex')
      .slice(0, 8),
    16
  ) / 0xffffffff
  const explore = ranked.length > 1 && seed < clamp01(explorationRate)
  const selected = explore ? ranked[Math.min(1, ranked.length - 1)] : ranked[0]
  return {
    selectedPolicyId: selected.policyId,
    reason: explore
      ? `Policy learner exploring ${selected.policyId} (seed ${(seed * 100).toFixed(1)}%).`
      : `Policy learner selected ${selected.policyId} from reward-weighted leaderboard.`,
    leaderboard: ranked
  }
}

export const listPolicyPromotionCandidates = async ({
  userId,
  minSamples = 12,
  minLift = 2.5,
  zThreshold = 1.96
}: {
  userId: string
  minSamples?: number
  minLift?: number
  zThreshold?: number
}): Promise<PolicyPromotionCandidate[]> => {
  const stats = await loadPolicyStatsForUser(userId)
  if (stats.length < 2) return []
  const eligible = stats
    .filter((entry) => entry.sampleCount >= Math.max(3, Math.round(minSamples)))
    .sort((left, right) => right.sampleCount - left.sampleCount || right.mean - left.mean)
  if (eligible.length < 2) return []
  const baseline = eligible[0]
  const winners: PolicyPromotionCandidate[] = []
  for (const candidate of eligible.slice(1)) {
    if (candidate.policyId === baseline.policyId) continue
    const lift = candidate.mean - baseline.mean
    if (lift < minLift) continue
    const se = Math.sqrt(
      (Math.max(0.0001, candidate.variance) / Math.max(1, candidate.sampleCount)) +
      (Math.max(0.0001, baseline.variance) / Math.max(1, baseline.sampleCount))
    )
    const zScore = se > 0 ? lift / se : 0
    if (zScore < zThreshold) continue
    winners.push({
      policyId: candidate.policyId,
      baselinePolicyId: baseline.policyId,
      lift: Number(lift.toFixed(4)),
      zScore: Number(zScore.toFixed(4)),
      sampleCount: candidate.sampleCount,
      baselineSampleCount: baseline.sampleCount,
      mean: candidate.mean,
      baselineMean: baseline.mean
    })
  }
  return winners.sort((left, right) => right.zScore - left.zScore || right.lift - left.lift)
}
