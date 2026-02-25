const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value))

const parseNumberEnv = (
  name: string,
  fallback: number,
  opts: { min?: number; max?: number; integer?: boolean } = {}
) => {
  const raw = Number(process.env[name] ?? fallback)
  if (!Number.isFinite(raw)) return fallback
  const bounded = clamp(
    raw,
    Number.isFinite(opts.min as number) ? Number(opts.min) : raw,
    Number.isFinite(opts.max as number) ? Number(opts.max) : raw
  )
  return opts.integer ? Math.round(bounded) : bounded
}

const parseBooleanEnv = (name: string, fallback: boolean) => {
  const raw = String(process.env[name] ?? '').trim().toLowerCase()
  if (!raw) return fallback
  if (raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on') return true
  if (raw === '0' || raw === 'false' || raw === 'no' || raw === 'off') return false
  return fallback
}

const hookMin = parseNumberEnv('EDITOR_HOOK_MIN', 5, { min: 3.5, max: 10 })
const hookMax = Math.max(hookMin + 0.2, parseNumberEnv('EDITOR_HOOK_MAX', 8, { min: 4, max: 11 }))
const cutMin = parseNumberEnv('EDITOR_CUT_MIN', 5, { min: 2.5, max: 10 })
const cutMax = Math.max(cutMin + 0.2, parseNumberEnv('EDITOR_CUT_MAX', 8, { min: 3.5, max: 12 }))

export const EDITOR_ENGINE_VERSION = String(process.env.EDITOR_ENGINE_VERSION || '2026.02.25').trim()
export const EDITOR_CONFIG_VERSION = String(process.env.EDITOR_CONFIG_VERSION || 'retention-v3').trim()

export type EmotionalNicheKey = 'high_energy' | 'education' | 'talking_head' | 'story'
export type EmotionalStyleKey = 'reaction' | 'vlog' | 'tutorial' | 'gaming' | 'story'

type EmotionalTuningEntry = {
  thresholdOffset: number
  spacingMultiplier: number
  leadTrimMultiplier: number
  splitLenBias: number
  openLoopBoost: number
  curiosityBoost: number
  contextPenaltyMultiplier: number
}

export const EMOTIONAL_NICHE_TUNING: Record<EmotionalNicheKey, EmotionalTuningEntry> = {
  high_energy: {
    thresholdOffset: -0.06,
    spacingMultiplier: 0.86,
    leadTrimMultiplier: 1.24,
    splitLenBias: 0.84,
    openLoopBoost: 1.18,
    curiosityBoost: 1.14,
    contextPenaltyMultiplier: 0.92
  },
  education: {
    thresholdOffset: 0.06,
    spacingMultiplier: 1.2,
    leadTrimMultiplier: 0.78,
    splitLenBias: 1.24,
    openLoopBoost: 0.92,
    curiosityBoost: 1.02,
    contextPenaltyMultiplier: 1.16
  },
  talking_head: {
    thresholdOffset: 0.04,
    spacingMultiplier: 1.14,
    leadTrimMultiplier: 0.82,
    splitLenBias: 1.16,
    openLoopBoost: 0.96,
    curiosityBoost: 1,
    contextPenaltyMultiplier: 1.1
  },
  story: {
    thresholdOffset: 0,
    spacingMultiplier: 1,
    leadTrimMultiplier: 0.95,
    splitLenBias: 1,
    openLoopBoost: 1,
    curiosityBoost: 1,
    contextPenaltyMultiplier: 1
  }
}

export const EMOTIONAL_STYLE_TUNING: Record<EmotionalStyleKey, EmotionalTuningEntry> = {
  reaction: {
    thresholdOffset: -0.04,
    spacingMultiplier: 0.9,
    leadTrimMultiplier: 1.18,
    splitLenBias: 0.88,
    openLoopBoost: 1.16,
    curiosityBoost: 1.12,
    contextPenaltyMultiplier: 0.94
  },
  vlog: {
    thresholdOffset: 0.01,
    spacingMultiplier: 1.06,
    leadTrimMultiplier: 0.94,
    splitLenBias: 1.04,
    openLoopBoost: 1.02,
    curiosityBoost: 1.03,
    contextPenaltyMultiplier: 1.02
  },
  tutorial: {
    thresholdOffset: 0.07,
    spacingMultiplier: 1.18,
    leadTrimMultiplier: 0.74,
    splitLenBias: 1.28,
    openLoopBoost: 0.9,
    curiosityBoost: 1.04,
    contextPenaltyMultiplier: 1.18
  },
  gaming: {
    thresholdOffset: -0.05,
    spacingMultiplier: 0.88,
    leadTrimMultiplier: 1.22,
    splitLenBias: 0.84,
    openLoopBoost: 1.14,
    curiosityBoost: 1.1,
    contextPenaltyMultiplier: 0.93
  },
  story: {
    thresholdOffset: 0,
    spacingMultiplier: 1,
    leadTrimMultiplier: 1,
    splitLenBias: 1,
    openLoopBoost: 1,
    curiosityBoost: 1,
    contextPenaltyMultiplier: 1
  }
}

export const EDITOR_RETENTION_CONFIG = {
  hookMin,
  hookMax,
  cutMin,
  cutMax,
  hookSelectionMatchStartToleranceSec: parseNumberEnv('HOOK_SELECTION_MATCH_START_TOLERANCE_SEC', 0.4, { min: 0.05, max: 1.5 }),
  hookSelectionMatchDurationToleranceSec: parseNumberEnv('HOOK_SELECTION_MATCH_DURATION_TOLERANCE_SEC', 0.8, { min: 0.05, max: 2 }),
  hookSelectionWaitMs: parseNumberEnv('HOOK_SELECTION_WAIT_MS', 12_000, { min: 0, max: 60_000, integer: true }),
  hookSelectionPollMs: parseNumberEnv('HOOK_SELECTION_POLL_MS', 500, { min: 120, max: 2_000, integer: true }),
  hookSelectionMaxCandidates: parseNumberEnv('HOOK_SELECTION_MAX_CANDIDATES', 3, { min: 3, max: 12, integer: true }),
  emotionalBeatThreshold: parseNumberEnv('EDITOR_EMOTIONAL_BEAT_THRESHOLD', 0.62, { min: 0.35, max: 0.95 }),
  emotionalBeatSpacingSec: parseNumberEnv('EDITOR_EMOTIONAL_BEAT_SPACING_SEC', 1.05, { min: 0.35, max: 3.5 }),
  emotionalLeadTrimSec: parseNumberEnv('EDITOR_EMOTIONAL_LEAD_TRIM_SEC', 1.8, { min: 0.5, max: 4 }),
  enforceStatusTransitions: parseBooleanEnv('ENFORCE_JOB_STATUS_TRANSITIONS', true)
} as const

export const JOB_STATUSES = [
  'queued',
  'uploading',
  'analyzing',
  'hooking',
  'cutting',
  'pacing',
  'subtitling',
  'audio',
  'story',
  'retention',
  'rendering',
  'completed',
  'failed'
] as const

export type JobStatusKey = typeof JOB_STATUSES[number]

const makeTransitions = (allowed: JobStatusKey[]) => new Set<JobStatusKey>(allowed)

export const JOB_STATUS_TRANSITIONS: Record<JobStatusKey, Set<JobStatusKey>> = {
  queued: makeTransitions(['queued', 'uploading', 'analyzing', 'failed']),
  uploading: makeTransitions(['uploading', 'queued', 'analyzing', 'failed']),
  analyzing: makeTransitions(['analyzing', 'queued', 'hooking', 'cutting', 'pacing', 'story', 'failed']),
  hooking: makeTransitions(['hooking', 'queued', 'cutting', 'pacing', 'story', 'failed']),
  cutting: makeTransitions(['cutting', 'queued', 'hooking', 'pacing', 'story', 'failed']),
  pacing: makeTransitions(['pacing', 'queued', 'hooking', 'story', 'failed']),
  subtitling: makeTransitions(['subtitling', 'queued', 'audio', 'retention', 'rendering', 'failed']),
  audio: makeTransitions(['audio', 'queued', 'retention', 'rendering', 'failed']),
  story: makeTransitions(['story', 'queued', 'hooking', 'pacing', 'subtitling', 'audio', 'retention', 'rendering', 'failed']),
  retention: makeTransitions(['retention', 'queued', 'rendering', 'failed']),
  rendering: makeTransitions(['rendering', 'queued', 'completed', 'failed']),
  completed: makeTransitions(['completed', 'queued']),
  failed: makeTransitions(['failed', 'queued'])
}

export const normalizeJobStatus = (value: unknown): JobStatusKey | null => {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return null
  return (JOB_STATUSES as readonly string[]).includes(normalized) ? (normalized as JobStatusKey) : null
}

export const isJobStatusTransitionAllowed = (fromStatus: JobStatusKey, toStatus: JobStatusKey) => {
  if (fromStatus === toStatus) return true
  const allowed = JOB_STATUS_TRANSITIONS[fromStatus]
  if (!allowed) return false
  return allowed.has(toStatus)
}
