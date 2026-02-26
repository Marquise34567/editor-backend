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
  autoOptimizeRequestSchema,
  algorithmConfigParamsSchema,
  applyPresetRequestSchema,
  createConfigRequestSchema,
  experimentStartRequestSchema,
  promptTuneRequestSchema,
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

type AlgorithmConfigParams = z.infer<typeof algorithmConfigParamsSchema>
type NumericParamKey = Exclude<keyof AlgorithmConfigParams, 'subtitle_style_mode'>

type PromptChange = {
  key: keyof AlgorithmConfigParams
  previous: number | string
  next: number | string
  delta: number | null
  source: 'prompt_directive' | 'prompt_intent' | 'suggestion_fallback'
  reason: string
}

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

const PARAM_ALIASES: Record<NumericParamKey, string[]> = {
  cut_aggression: ['cut aggression', 'aggression', 'cut_aggr'],
  min_clip_len_ms: ['min clip', 'minimum clip', 'min clip len', 'min clip length'],
  max_clip_len_ms: ['max clip', 'maximum clip', 'max clip len', 'max clip length'],
  silence_db_threshold: ['silence db threshold', 'silence threshold', 'silence db'],
  silence_min_ms: ['silence min', 'minimum silence', 'silence min ms', 'pause min'],
  filler_word_weight: ['filler weight', 'filler penalty', 'filler_word_weight'],
  redundancy_weight: ['redundancy weight', 'repeat penalty', 'redundancy'],
  energy_floor: ['energy floor', 'minimum energy'],
  spike_boost: ['spike boost', 'emotion spike boost'],
  pattern_interrupt_every_sec: ['pattern interrupt', 'interrupt cycle', 'pattern cadence'],
  hook_priority_weight: ['hook priority', 'hook weight', 'opening priority'],
  story_coherence_guard: ['story guard', 'coherence guard', 'narrative guard'],
  jank_guard: ['jank guard', 'continuity guard'],
  pacing_multiplier: ['pacing multiplier', 'tempo multiplier']
}

const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const normalizeSubtitleMode = (value: string) =>
  value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_\- ]+/g, ' ')
    .replace(/\s+/g, '_')
    .slice(0, 120) || 'premium_clean'

const applyNumericChange = ({
  next,
  changes,
  key,
  targetRaw,
  source,
  reason
}: {
  next: AlgorithmConfigParams
  changes: PromptChange[]
  key: NumericParamKey
  targetRaw: number
  source: PromptChange['source']
  reason: string
}) => {
  if (!Number.isFinite(targetRaw)) return
  const limits = NUMERIC_PARAM_LIMITS[key]
  const previous = Number(next[key])
  let target = clamp(targetRaw, limits.min, limits.max)
  if (limits.integer) target = Math.round(target)
  if (Math.abs(target - previous) < 0.0001) return
  next[key] = target as any
  changes.push({
    key,
    previous,
    next: target,
    delta: Number((target - previous).toFixed(4)),
    source,
    reason
  })
}

const applyNumericDelta = ({
  next,
  changes,
  key,
  delta,
  source,
  reason
}: {
  next: AlgorithmConfigParams
  changes: PromptChange[]
  key: NumericParamKey
  delta: number
  source: PromptChange['source']
  reason: string
}) => {
  if (!Number.isFinite(delta) || Math.abs(delta) < 0.0001) return
  applyNumericChange({
    next,
    changes,
    key,
    targetRaw: Number(next[key]) + delta,
    source,
    reason
  })
}

const applySubtitleModeChange = ({
  next,
  changes,
  nextModeRaw,
  source,
  reason
}: {
  next: AlgorithmConfigParams
  changes: PromptChange[]
  nextModeRaw: string
  source: PromptChange['source']
  reason: string
}) => {
  const previous = String(next.subtitle_style_mode || 'premium_clean')
  const normalized = normalizeSubtitleMode(nextModeRaw)
  if (!normalized || normalized === previous) return
  next.subtitle_style_mode = normalized
  changes.push({
    key: 'subtitle_style_mode',
    previous,
    next: normalized,
    delta: null,
    source,
    reason
  })
}

const applyPromptPresetIntent = ({
  preset,
  next,
  changes
}: {
  preset: 'balanced' | 'aggressive' | 'ultra'
  next: AlgorithmConfigParams
  changes: PromptChange[]
}) => {
  if (preset === 'balanced') {
    applyNumericDelta({
      next,
      changes,
      key: 'cut_aggression',
      delta: 2,
      source: 'prompt_intent',
      reason: 'Balanced preset intent'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'jank_guard',
      delta: 4,
      source: 'prompt_intent',
      reason: 'Balanced preset intent'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'story_coherence_guard',
      delta: 3,
      source: 'prompt_intent',
      reason: 'Balanced preset intent'
    })
    return
  }

  if (preset === 'aggressive') {
    applyNumericDelta({
      next,
      changes,
      key: 'cut_aggression',
      delta: 9,
      source: 'prompt_intent',
      reason: 'Aggressive preset intent'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'pacing_multiplier',
      delta: 0.14,
      source: 'prompt_intent',
      reason: 'Aggressive preset intent'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'pattern_interrupt_every_sec',
      delta: -1.8,
      source: 'prompt_intent',
      reason: 'Aggressive preset intent'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'silence_min_ms',
      delta: -120,
      source: 'prompt_intent',
      reason: 'Aggressive preset intent'
    })
    return
  }

  applyNumericDelta({
    next,
    changes,
    key: 'cut_aggression',
    delta: 14,
    source: 'prompt_intent',
    reason: 'Ultra preset intent'
  })
  applyNumericDelta({
    next,
    changes,
    key: 'pacing_multiplier',
    delta: 0.22,
    source: 'prompt_intent',
    reason: 'Ultra preset intent'
  })
  applyNumericDelta({
    next,
    changes,
    key: 'pattern_interrupt_every_sec',
    delta: -2.6,
    source: 'prompt_intent',
    reason: 'Ultra preset intent'
  })
  applyNumericDelta({
    next,
    changes,
    key: 'silence_min_ms',
    delta: -180,
    source: 'prompt_intent',
    reason: 'Ultra preset intent'
  })
  applyNumericDelta({
    next,
    changes,
    key: 'jank_guard',
    delta: -5,
    source: 'prompt_intent',
    reason: 'Ultra preset intent prioritizes speed over polish'
  })
}

type PlatformModeSelection = 'tiktok' | 'instagram_reels' | 'youtube_shorts' | 'long_form'
type ContentTypeModeSelection = 'auto' | 'reaction' | 'commentary' | 'vlog' | 'gaming' | 'sports' | 'education' | 'podcast'
type RetentionTiltSelection = 'safe' | 'balanced' | 'viral'
type FormatSelection = 'short' | 'long'
type OrientationSelection = 'vertical' | 'horizontal'

const applyNumericTargets = ({
  next,
  changes,
  targets,
  source,
  reasonPrefix
}: {
  next: AlgorithmConfigParams
  changes: PromptChange[]
  targets: Partial<Record<NumericParamKey, number>>
  source: PromptChange['source']
  reasonPrefix: string
}) => {
  for (const [rawKey, rawTarget] of Object.entries(targets)) {
    if (!Object.prototype.hasOwnProperty.call(NUMERIC_PARAM_LIMITS, rawKey)) continue
    const key = rawKey as NumericParamKey
    const target = Number(rawTarget)
    if (!Number.isFinite(target)) continue
    applyNumericChange({
      next,
      changes,
      key,
      targetRaw: target,
      source,
      reason: `${reasonPrefix}: set ${key}`
    })
  }
}

const applyNumericDeltas = ({
  next,
  changes,
  deltas,
  source,
  reasonPrefix
}: {
  next: AlgorithmConfigParams
  changes: PromptChange[]
  deltas: Partial<Record<NumericParamKey, number>>
  source: PromptChange['source']
  reasonPrefix: string
}) => {
  for (const [rawKey, rawDelta] of Object.entries(deltas)) {
    if (!Object.prototype.hasOwnProperty.call(NUMERIC_PARAM_LIMITS, rawKey)) continue
    const key = rawKey as NumericParamKey
    const delta = Number(rawDelta)
    if (!Number.isFinite(delta)) continue
    applyNumericDelta({
      next,
      changes,
      key,
      delta,
      source,
      reason: `${reasonPrefix}: adjust ${key}`
    })
  }
}

const ADVANCED_MODE_SPEC_MARKERS: RegExp[] = [
  /\bplatform\s+modes?\b/i,
  /\bcontent(?:\s*-\s*|\s+)type\s+modes?\b/i,
  /\bselected\s+modes?\b/i,
  /\bbest\s+primary\s+hook\b/i,
  /\bfull\s+edit\s+summary\b/i,
  /\bfinal\s+recommendations\b/i
]

const isAdvancedModeSpecPrompt = (prompt: string) => {
  const hitCount = ADVANCED_MODE_SPEC_MARKERS.reduce((count, marker) => (
    count + (marker.test(prompt) ? 1 : 0)
  ), 0)
  return hitCount >= 2
}

const normalizePlatformModeSelection = (raw: string): PlatformModeSelection | null => {
  const value = String(raw || '')
    .toLowerCase()
    .replace(/[\[\]{}()]/g, ' ')
    .replace(/[^a-z0-9+ _-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
  if (!value) return null
  const matches: PlatformModeSelection[] = []
  if (/(^| )(tiktok|tik tok|tt)( |$)/.test(value)) matches.push('tiktok')
  if (/(^| )(ig reels?|instagram reels?|reels?)( |$)/.test(value)) matches.push('instagram_reels')
  if (/(^| )(youtube shorts?|yt shorts?)( |$)/.test(value)) matches.push('youtube_shorts')
  if (/(^| )(long form|long-form|youtube long form|youtube style long)( |$)/.test(value)) matches.push('long_form')
  return matches.length === 1 ? matches[0] : null
}

const normalizeContentTypeModeSelection = (raw: string): ContentTypeModeSelection | null => {
  const value = String(raw || '')
    .toLowerCase()
    .replace(/[\[\]{}()]/g, ' ')
    .replace(/[^a-z0-9+ _-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
  if (!value) return null
  const matches: ContentTypeModeSelection[] = []
  if (/(^| )auto( |$)/.test(value)) matches.push('auto')
  if (/(^| )reaction( |$)/.test(value)) matches.push('reaction')
  if (/(^| )commentary( |$)/.test(value)) matches.push('commentary')
  if (/(^| )vlog( |$)/.test(value)) matches.push('vlog')
  if (/(^| )gaming( |$)/.test(value)) matches.push('gaming')
  if (/(^| )sports?( |$)/.test(value)) matches.push('sports')
  if (/(^| )education(?:al)?( |$)/.test(value)) matches.push('education')
  if (/(^| )podcast( |$)/.test(value)) matches.push('podcast')
  return matches.length === 1 ? matches[0] : null
}

const extractFirstMatchingMode = <T extends string>({
  prompt,
  patterns,
  normalize
}: {
  prompt: string
  patterns: RegExp[]
  normalize: (value: string) => T | null
}): T | null => {
  for (const pattern of patterns) {
    const match = prompt.match(pattern)
    if (!match?.[1]) continue
    const resolved = normalize(match[1])
    if (resolved) return resolved
  }
  return null
}

const collectMentionedModes = <T extends string>({
  prompt,
  patterns
}: {
  prompt: string
  patterns: Array<{ mode: T; pattern: RegExp }>
}) => {
  const found = new Set<T>()
  for (const entry of patterns) {
    if (entry.pattern.test(prompt)) found.add(entry.mode)
  }
  return Array.from(found)
}

const resolvePlatformModeFromPrompt = (prompt: string): PlatformModeSelection | null => {
  const explicit = extractFirstMatchingMode<PlatformModeSelection>({
    prompt,
    normalize: normalizePlatformModeSelection,
    patterns: [
      /\bplatform(?:\s+mode)?\s*[:=]\s*([^\n\r|;,]+)/i,
      /\bselected\s+platform\s*[:=]\s*([^\n\r|;,]+)/i,
      /\bplatform\s*\[\s*([^\]]+)\s*\]/i
    ]
  })
  if (explicit) return explicit
  const mentions = collectMentionedModes<PlatformModeSelection>({
    prompt,
    patterns: [
      { mode: 'tiktok', pattern: /\b(tiktok|tik\s*tok|tt)\b/i },
      { mode: 'instagram_reels', pattern: /\b(ig\s*reels?|instagram\s*reels?|reels?)\b/i },
      { mode: 'youtube_shorts', pattern: /\b(youtube\s*shorts?|yt\s*shorts?)\b/i },
      { mode: 'long_form', pattern: /\b(long[\s-]?form|youtube-style|horizontal(?:\/original)?)\b/i }
    ]
  })
  return mentions.length === 1 ? mentions[0] : null
}

const resolveContentTypeModeFromPrompt = (prompt: string): ContentTypeModeSelection | null => {
  const explicit = extractFirstMatchingMode<ContentTypeModeSelection>({
    prompt,
    normalize: normalizeContentTypeModeSelection,
    patterns: [
      /\bcontent(?:\s*-\s*|\s+)type(?:\s+mode)?\s*[:=]\s*([^\n\r|;,]+)/i,
      /\beditor\s+mode\s*[:=]\s*([^\n\r|;,]+)/i,
      /\bcontent\s+mode\s*[:=]\s*([^\n\r|;,]+)/i,
      /\bcontent(?:\s*-\s*|\s+)type\s*\[\s*([^\]]+)\s*\]/i
    ]
  })
  if (explicit) return explicit
  const mentions = collectMentionedModes<ContentTypeModeSelection>({
    prompt,
    patterns: [
      { mode: 'auto', pattern: /\bauto\b/i },
      { mode: 'reaction', pattern: /\breaction\b/i },
      { mode: 'commentary', pattern: /\bcommentary\b/i },
      { mode: 'vlog', pattern: /\bvlog\b/i },
      { mode: 'gaming', pattern: /\bgaming\b/i },
      { mode: 'sports', pattern: /\bsports?\b/i },
      { mode: 'education', pattern: /\beducation(?:al)?\b/i },
      { mode: 'podcast', pattern: /\bpodcast\b/i }
    ]
  })
  return mentions.length === 1 ? mentions[0] : null
}

const resolveRetentionTiltFromPrompt = (prompt: string): RetentionTiltSelection | null => {
  const explicit = extractFirstMatchingMode<RetentionTiltSelection>({
    prompt,
    normalize: (value) => {
      const normalized = String(value || '').toLowerCase()
      if (/\bsafe\b/.test(normalized)) return 'safe'
      if (/\bbalanced\b/.test(normalized)) return 'balanced'
      if (/\bviral\b/.test(normalized)) return 'viral'
      return null
    },
    patterns: [
      /\b(?:tilt|slider|retention\s+profile)\s*[:=]\s*(safe|balanced|viral)\b/i,
      /\b(?:safe|balanced|viral)\s+slider\s*[:=]\s*(safe|balanced|viral)\b/i
    ]
  })
  if (explicit) return explicit
  const mentions = collectMentionedModes<RetentionTiltSelection>({
    prompt,
    patterns: [
      { mode: 'safe', pattern: /\bsafe\b/i },
      { mode: 'balanced', pattern: /\bbalanced\b/i },
      { mode: 'viral', pattern: /\bviral\b/i }
    ]
  })
  return mentions.length === 1 ? mentions[0] : null
}

const resolveCaptionsPreferenceFromPrompt = (prompt: string): 'on' | 'off' | null => {
  const off = (
    /\bcaptions?\s*(?:=|:|to)?\s*(?:off|none|disabled?|no)\b/i.test(prompt) ||
    /\bno\s+captions?\b/i.test(prompt)
  )
  const on = (
    /\bcaptions?\s*(?:=|:|to)?\s*(?:on|enabled?|yes)\b/i.test(prompt) ||
    /\bwith\s+captions?\b/i.test(prompt)
  )
  if (off && on) return null
  if (off) return 'off'
  if (on) return 'on'
  return null
}

const resolveFormatFromPrompt = (prompt: string): FormatSelection | null => {
  const match = prompt.match(/\bformat\s*[:=]\s*(short(?:[\s-]?form)?|long(?:[\s-]?form)?)\b/i)
  if (!match?.[1]) return null
  const normalized = match[1].toLowerCase()
  if (normalized.includes('short')) return 'short'
  if (normalized.includes('long')) return 'long'
  return null
}

const resolveOrientationFromPrompt = (prompt: string): OrientationSelection | null => {
  const match = prompt.match(/\b(?:orientation|layout|aspect)\s*[:=]\s*(vertical|horizontal)\b/i)
  if (!match?.[1]) return null
  const normalized = match[1].toLowerCase()
  return normalized === 'vertical' ? 'vertical' : normalized === 'horizontal' ? 'horizontal' : null
}

const resolveHookAndCutOnlyFromPrompt = (prompt: string) => {
  return (
    /\b(?:setting|mode|option)\s*[:=]\s*(?:only\s*)?hook\s*(?:\+|and|&)\s*cut\b/i.test(prompt) ||
    /\bhook\s*(?:\+|and|&)\s*cut\s*(?:mode|setting)\s*[:=]\s*(?:on|enabled|true)\b/i.test(prompt)
  )
}

const resolveCutCountFromPrompt = (prompt: string): number | null => {
  const match =
    prompt.match(/\bcut\s*count\s*[:=]\s*(\d{1,2})\b/i) ||
    prompt.match(/\b(?:cuts?|edits?)\s*(?:count\s*)?(?:=|:|to)\s*(\d{1,2})\b/i)
  const value = Number(match?.[1] || NaN)
  if (!Number.isFinite(value)) return null
  return Math.round(clamp(value, 1, 15))
}

const PLATFORM_MODE_BASELINES: Record<PlatformModeSelection, {
  numeric: Partial<Record<NumericParamKey, number>>
  subtitleMode: string
  reason: string
}> = {
  tiktok: {
    numeric: {
      cut_aggression: 92,
      min_clip_len_ms: 280,
      max_clip_len_ms: 3_900,
      silence_db_threshold: -44,
      silence_min_ms: 140,
      filler_word_weight: 1.45,
      redundancy_weight: 1.26,
      energy_floor: 0.46,
      spike_boost: 1.85,
      pattern_interrupt_every_sec: 3.2,
      hook_priority_weight: 2.35,
      story_coherence_guard: 34,
      jank_guard: 45,
      pacing_multiplier: 1.62
    },
    subtitleMode: 'tiktok_kinetic_neon',
    reason: 'TikTok platform baseline'
  },
  instagram_reels: {
    numeric: {
      cut_aggression: 70,
      min_clip_len_ms: 520,
      max_clip_len_ms: 6_500,
      silence_db_threshold: -42,
      silence_min_ms: 220,
      filler_word_weight: 1.25,
      redundancy_weight: 1.12,
      energy_floor: 0.36,
      spike_boost: 1.2,
      pattern_interrupt_every_sec: 4.8,
      hook_priority_weight: 1.72,
      story_coherence_guard: 62,
      jank_guard: 74,
      pacing_multiplier: 1.2
    },
    subtitleMode: 'reels_polished',
    reason: 'IG Reels platform baseline'
  },
  youtube_shorts: {
    numeric: {
      cut_aggression: 58,
      min_clip_len_ms: 780,
      max_clip_len_ms: 9_200,
      silence_db_threshold: -41,
      silence_min_ms: 250,
      filler_word_weight: 1.2,
      redundancy_weight: 1.08,
      energy_floor: 0.31,
      spike_boost: 0.98,
      pattern_interrupt_every_sec: 6.8,
      hook_priority_weight: 1.56,
      story_coherence_guard: 74,
      jank_guard: 82,
      pacing_multiplier: 1.07
    },
    subtitleMode: 'shorts_value_clear',
    reason: 'YouTube Shorts platform baseline'
  },
  long_form: {
    numeric: {
      cut_aggression: 34,
      min_clip_len_ms: 1_300,
      max_clip_len_ms: 18_000,
      silence_db_threshold: -38,
      silence_min_ms: 360,
      filler_word_weight: 1.04,
      redundancy_weight: 1.1,
      energy_floor: 0.24,
      spike_boost: 0.56,
      pattern_interrupt_every_sec: 15.5,
      hook_priority_weight: 1.2,
      story_coherence_guard: 93,
      jank_guard: 91,
      pacing_multiplier: 0.84
    },
    subtitleMode: 'longform_accessible_clean',
    reason: 'Long-form platform baseline'
  }
}

const CONTENT_MODE_OVERLAYS: Record<ContentTypeModeSelection, {
  delta: Partial<Record<NumericParamKey, number>>
  subtitleMode?: string
  reason: string
}> = {
  auto: {
    delta: {},
    reason: 'Auto content-type overlay'
  },
  reaction: {
    delta: {
      cut_aggression: 10,
      pacing_multiplier: 0.14,
      pattern_interrupt_every_sec: -1.3,
      spike_boost: 0.34,
      hook_priority_weight: 0.2,
      story_coherence_guard: -5,
      jank_guard: -6
    },
    subtitleMode: 'reaction_expressive',
    reason: 'Reaction overlay'
  },
  commentary: {
    delta: {
      cut_aggression: 2,
      pacing_multiplier: -0.02,
      pattern_interrupt_every_sec: 1.4,
      story_coherence_guard: 10,
      jank_guard: 8,
      filler_word_weight: 0.14,
      redundancy_weight: 0.22,
      spike_boost: -0.06
    },
    subtitleMode: 'commentary_transcriptive',
    reason: 'Commentary overlay'
  },
  vlog: {
    delta: {
      cut_aggression: -4,
      pacing_multiplier: -0.05,
      pattern_interrupt_every_sec: 1.8,
      story_coherence_guard: 8,
      jank_guard: 6,
      spike_boost: -0.04
    },
    subtitleMode: 'vlog_narrative',
    reason: 'Vlog overlay'
  },
  gaming: {
    delta: {
      cut_aggression: 12,
      pacing_multiplier: 0.18,
      pattern_interrupt_every_sec: -2,
      spike_boost: 0.42,
      energy_floor: 0.08,
      hook_priority_weight: 0.14,
      story_coherence_guard: -8,
      jank_guard: -8
    },
    subtitleMode: 'gaming_hud_pop',
    reason: 'Gaming overlay'
  },
  sports: {
    delta: {
      cut_aggression: 14,
      pacing_multiplier: 0.22,
      pattern_interrupt_every_sec: -2.2,
      spike_boost: 0.5,
      energy_floor: 0.1,
      hook_priority_weight: 0.2,
      story_coherence_guard: -10,
      jank_guard: -10
    },
    subtitleMode: 'sports_score_overlay',
    reason: 'Sports overlay'
  },
  education: {
    delta: {
      cut_aggression: -12,
      pacing_multiplier: -0.14,
      pattern_interrupt_every_sec: 3.2,
      story_coherence_guard: 12,
      jank_guard: 10,
      filler_word_weight: 0.36,
      redundancy_weight: 0.42,
      spike_boost: -0.18,
      energy_floor: -0.06
    },
    subtitleMode: 'education_key_terms',
    reason: 'Education overlay'
  },
  podcast: {
    delta: {
      cut_aggression: -10,
      pacing_multiplier: -0.16,
      pattern_interrupt_every_sec: 6.4,
      story_coherence_guard: 14,
      jank_guard: 14,
      filler_word_weight: 0.38,
      redundancy_weight: 0.26,
      silence_min_ms: 220,
      spike_boost: -0.2,
      energy_floor: -0.08
    },
    subtitleMode: 'podcast_readable_dual',
    reason: 'Podcast overlay'
  }
}

const RETENTION_TILT_OVERLAYS: Record<RetentionTiltSelection, Partial<Record<NumericParamKey, number>>> = {
  safe: {
    cut_aggression: -8,
    pacing_multiplier: -0.08,
    pattern_interrupt_every_sec: 2.1,
    story_coherence_guard: 8,
    jank_guard: 7,
    hook_priority_weight: -0.05,
    spike_boost: -0.08,
    energy_floor: -0.04
  },
  balanced: {
    cut_aggression: 0,
    pacing_multiplier: 0,
    pattern_interrupt_every_sec: 0,
    story_coherence_guard: 0,
    jank_guard: 0
  },
  viral: {
    cut_aggression: 10,
    pacing_multiplier: 0.14,
    pattern_interrupt_every_sec: -1.8,
    hook_priority_weight: 0.2,
    story_coherence_guard: -8,
    jank_guard: -8,
    spike_boost: 0.24,
    energy_floor: 0.06
  }
}

const parsePromptIntoParams = async ({
  prompt,
  base,
  fallbackLimit,
  fallbackRange
}: {
  prompt: string
  base: AlgorithmConfigParams
  fallbackLimit?: number
  fallbackRange?: string
}) => {
  const normalizedPrompt = String(prompt || '').trim()
  const lower = normalizedPrompt.toLowerCase()
  const next: AlgorithmConfigParams = { ...base }
  const changes: PromptChange[] = []
  const warnings: string[] = []
  let strategy: 'prompt_directive' | 'prompt_intent' | 'suggestion_fallback' = 'prompt_intent'

  const directMatches: NumericParamKey[] = []
  for (const [key, aliases] of Object.entries(PARAM_ALIASES) as Array<[NumericParamKey, string[]]>) {
    const aliasPattern = aliases
      .map((value) => escapeRegExp(value).replace(/\s+/g, '[\\s_-]*'))
      .join('|')

    const setMatch = normalizedPrompt.match(new RegExp(`(?:${aliasPattern})\\s*(?:=|:|to)\\s*(-?\\d+(?:\\.\\d+)?)`, 'i'))
    if (setMatch?.[1] != null) {
      directMatches.push(key)
      applyNumericChange({
        next,
        changes,
        key,
        targetRaw: Number(setMatch[1]),
        source: 'prompt_directive',
        reason: `Direct ${key} assignment in prompt`
      })
      continue
    }

    const increaseMatch = normalizedPrompt.match(
      new RegExp(`(?:increase|raise|boost)\\s+(?:the\\s+)?(?:${aliasPattern})(?:\\s+by)?\\s*(-?\\d+(?:\\.\\d+)?)`, 'i')
    )
    if (increaseMatch?.[1] != null) {
      directMatches.push(key)
      applyNumericDelta({
        next,
        changes,
        key,
        delta: Math.abs(Number(increaseMatch[1])),
        source: 'prompt_directive',
        reason: `Increase ${key} from prompt`
      })
      continue
    }

    const decreaseMatch = normalizedPrompt.match(
      new RegExp(`(?:decrease|lower|reduce)\\s+(?:the\\s+)?(?:${aliasPattern})(?:\\s+by)?\\s*(-?\\d+(?:\\.\\d+)?)`, 'i')
    )
    if (decreaseMatch?.[1] != null) {
      directMatches.push(key)
      applyNumericDelta({
        next,
        changes,
        key,
        delta: -Math.abs(Number(decreaseMatch[1])),
        source: 'prompt_directive',
        reason: `Decrease ${key} from prompt`
      })
    }
  }
  if (directMatches.length) strategy = 'prompt_directive'

  const subtitleModeMatch = normalizedPrompt.match(
    /(?:subtitle(?:_style_mode)?|caption(?:s)?\s*style)\s*(?:=|:|to)\s*([a-z0-9 _-]{2,60})/i
  )
  if (subtitleModeMatch?.[1]) {
    strategy = strategy === 'prompt_directive' ? strategy : 'prompt_intent'
    applySubtitleModeChange({
      next,
      changes,
      nextModeRaw: subtitleModeMatch[1],
      source: strategy === 'prompt_directive' ? 'prompt_directive' : 'prompt_intent',
      reason: 'Subtitle style instruction in prompt'
    })
  }

  const advancedModeSpec = isAdvancedModeSpecPrompt(normalizedPrompt)
  const requestedPlatformMode = resolvePlatformModeFromPrompt(normalizedPrompt)
  const requestedContentMode = resolveContentTypeModeFromPrompt(normalizedPrompt)
  const requestedTilt = resolveRetentionTiltFromPrompt(normalizedPrompt)
  const requestedFormat = resolveFormatFromPrompt(normalizedPrompt)
  const requestedOrientation = resolveOrientationFromPrompt(normalizedPrompt)
  const requestedCutCount = resolveCutCountFromPrompt(normalizedPrompt)
  const captionsPreference = resolveCaptionsPreferenceFromPrompt(normalizedPrompt)
  const hookAndCutOnly = resolveHookAndCutOnlyFromPrompt(normalizedPrompt)

  const hasExplicitModeSelection = Boolean(
    requestedPlatformMode ||
    requestedContentMode ||
    requestedFormat ||
    requestedOrientation
  )
  const hasModeSelectionSignal = Boolean(
    hasExplicitModeSelection ||
    (advancedModeSpec && (
      requestedTilt ||
      requestedCutCount !== null ||
      captionsPreference ||
      hookAndCutOnly
    ))
  )

  if (hasModeSelectionSignal) {
    let resolvedPlatformMode = requestedPlatformMode
    if (!resolvedPlatformMode) {
      if (requestedFormat === 'short' || requestedOrientation === 'vertical') resolvedPlatformMode = 'youtube_shorts'
      else resolvedPlatformMode = 'long_form'
    }
    const resolvedContentMode = requestedContentMode || 'auto'
    const platformBaseline = PLATFORM_MODE_BASELINES[resolvedPlatformMode]
    const contentOverlay = CONTENT_MODE_OVERLAYS[resolvedContentMode]
    const hasExplicitSubtitleInstruction = Boolean(subtitleModeMatch?.[1])

    applyNumericTargets({
      next,
      changes,
      targets: platformBaseline.numeric,
      source: strategy === 'prompt_directive' ? 'prompt_directive' : 'prompt_intent',
      reasonPrefix: platformBaseline.reason
    })

    if (!hasExplicitSubtitleInstruction) {
      applySubtitleModeChange({
        next,
        changes,
        nextModeRaw: platformBaseline.subtitleMode,
        source: 'prompt_intent',
        reason: `${platformBaseline.reason}: subtitle baseline`
      })
    }

    applyNumericDeltas({
      next,
      changes,
      deltas: contentOverlay.delta,
      source: 'prompt_intent',
      reasonPrefix: contentOverlay.reason
    })

    if (!hasExplicitSubtitleInstruction && contentOverlay.subtitleMode) {
      applySubtitleModeChange({
        next,
        changes,
        nextModeRaw: contentOverlay.subtitleMode,
        source: 'prompt_intent',
        reason: `${contentOverlay.reason}: subtitle overlay`
      })
    }

    if (requestedTilt) {
      applyNumericDeltas({
        next,
        changes,
        deltas: RETENTION_TILT_OVERLAYS[requestedTilt],
        source: 'prompt_intent',
        reasonPrefix: `Retention tilt ${requestedTilt}`
      })
    }

    if (hookAndCutOnly) {
      applyNumericDeltas({
        next,
        changes,
        deltas: {
          cut_aggression: -10,
          pattern_interrupt_every_sec: 2.4,
          hook_priority_weight: 0.28,
          story_coherence_guard: 6,
          jank_guard: 6,
          pacing_multiplier: -0.06
        },
        source: 'prompt_intent',
        reasonPrefix: 'Hook + Cut only mode'
      })
    }

    if (requestedCutCount !== null) {
      const boundedCount = Math.round(clamp(requestedCutCount, 1, 15))
      const longFormMode = resolvedPlatformMode === 'long_form'
      const effectiveCount = longFormMode
        ? Math.max(1, Math.round(boundedCount * 0.6))
        : boundedCount
      const targetInterruptSec = longFormMode
        ? clamp(60 / Math.max(1, effectiveCount), 6, 28)
        : clamp(60 / Math.max(1, effectiveCount), 2.4, 20)
      const targetAggression = longFormMode
        ? clamp(22 + effectiveCount * 2.2, 24, 58)
        : clamp(28 + effectiveCount * 4.6, 30, 95)
      const targetPacing = longFormMode
        ? clamp(0.78 + effectiveCount * 0.018, 0.72, 1.14)
        : clamp(0.88 + effectiveCount * 0.055, 0.9, 1.95)
      applyNumericChange({
        next,
        changes,
        key: 'pattern_interrupt_every_sec',
        targetRaw: targetInterruptSec,
        source: 'prompt_intent',
        reason: `Cut-count instruction (${boundedCount})`
      })
      applyNumericChange({
        next,
        changes,
        key: 'cut_aggression',
        targetRaw: targetAggression,
        source: 'prompt_intent',
        reason: `Cut-count instruction (${boundedCount})`
      })
      applyNumericChange({
        next,
        changes,
        key: 'pacing_multiplier',
        targetRaw: targetPacing,
        source: 'prompt_intent',
        reason: `Cut-count instruction (${boundedCount})`
      })
    }

    if (captionsPreference === 'off') {
      applySubtitleModeChange({
        next,
        changes,
        nextModeRaw: 'captions_off_requested',
        source: 'prompt_intent',
        reason: 'Caption preference explicitly disabled'
      })
      warnings.push('Caption on/off is mapped to subtitle_style_mode in algorithm tuning; runtime caption toggle remains a render setting.')
    } else if (captionsPreference === 'on' && !hasExplicitSubtitleInstruction) {
      applySubtitleModeChange({
        next,
        changes,
        nextModeRaw: contentOverlay.subtitleMode || platformBaseline.subtitleMode,
        source: 'prompt_intent',
        reason: 'Caption preference explicitly enabled'
      })
    }

    if (!requestedPlatformMode && advancedModeSpec) {
      warnings.push('Platform mode was not explicitly selected; defaulted to Long-Form baseline for deterministic tuning.')
    }
    if (!requestedContentMode && advancedModeSpec) {
      warnings.push('Content-Type mode was not explicitly selected; defaulted to Auto overlay.')
    }

    strategy = strategy === 'prompt_directive' ? 'prompt_directive' : 'prompt_intent'
    const parsed = parseConfigParams(next)
    return {
      strategy,
      params: parsed,
      changes,
      warnings
    }
  }

  if (/\bultra\b/.test(lower)) {
    applyPromptPresetIntent({ preset: 'ultra', next, changes })
  } else if (/\baggressive\b/.test(lower)) {
    applyPromptPresetIntent({ preset: 'aggressive', next, changes })
  } else if (/\bbalanced\b/.test(lower)) {
    applyPromptPresetIntent({ preset: 'balanced', next, changes })
  }

  if (/(long[\s-]?form|podcast|episode|interview|60\s*min|1\s*hour|2\s*hour)/i.test(lower)) {
    applyNumericDelta({
      next,
      changes,
      key: 'story_coherence_guard',
      delta: 8,
      source: 'prompt_intent',
      reason: 'Prompt targets long-form context preservation'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'max_clip_len_ms',
      delta: 1_200,
      source: 'prompt_intent',
      reason: 'Prompt targets longer long-form moments'
    })
  }

  const maxSilenceMatch = normalizedPrompt.match(/max\s+silence\s*[:=]?\s*(\d+(?:\.\d+)?)\s*s/i)
  if (maxSilenceMatch?.[1]) {
    const maxSilenceMs = Number(maxSilenceMatch[1]) * 1000
    if (Number.isFinite(maxSilenceMs)) {
      applyNumericChange({
        next,
        changes,
        key: 'silence_min_ms',
        targetRaw: maxSilenceMs,
        source: 'prompt_directive',
        reason: 'Direct max silence threshold in prompt'
      })
    }
  }

  const cutsPerMinuteMatch = normalizedPrompt.match(
    /(\d+(?:\.\d+)?)\s*(?:-|to)\s*(\d+(?:\.\d+)?)\s*(?:cuts?|edits?)\s*(?:\/|per)?\s*(?:min|minute)/i
  )
  if (cutsPerMinuteMatch?.[1] && cutsPerMinuteMatch?.[2]) {
    const low = Number(cutsPerMinuteMatch[1])
    const high = Number(cutsPerMinuteMatch[2])
    const avgCuts = Number.isFinite(low) && Number.isFinite(high) ? (low + high) / 2 : NaN
    if (Number.isFinite(avgCuts) && avgCuts > 0) {
      const interruptSeconds = clamp(60 / avgCuts, 2, 20)
      applyNumericChange({
        next,
        changes,
        key: 'pattern_interrupt_every_sec',
        targetRaw: interruptSeconds,
        source: 'prompt_directive',
        reason: 'Direct cuts-per-minute target in prompt'
      })
    }
  }

  if (/(tangent killer|remove tangents|kill tangents|repeated points)/i.test(lower)) {
    applyNumericDelta({
      next,
      changes,
      key: 'redundancy_weight',
      delta: 0.24,
      source: 'prompt_intent',
      reason: 'Prompt requests stronger tangent/redundancy suppression'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'filler_word_weight',
      delta: 0.18,
      source: 'prompt_intent',
      reason: 'Prompt requests stronger tangent/filler suppression'
    })
  }

  if (/(hook|opening|first\s*(3|5|8)\s*s|intro)/i.test(lower)) {
    applyNumericDelta({
      next,
      changes,
      key: 'hook_priority_weight',
      delta: 0.18,
      source: 'prompt_intent',
      reason: 'Prompt emphasizes hook strength'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'pattern_interrupt_every_sec',
      delta: -1.4,
      source: 'prompt_intent',
      reason: 'Prompt requests stronger early retention cadence'
    })
  }

  if (/(faster|fast[-\s]?paced|snappy|snappier|viral|more cuts|punchy)/i.test(lower)) {
    applyNumericDelta({
      next,
      changes,
      key: 'cut_aggression',
      delta: 8,
      source: 'prompt_intent',
      reason: 'Prompt requests faster/high-retention pacing'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'pacing_multiplier',
      delta: 0.12,
      source: 'prompt_intent',
      reason: 'Prompt requests faster pacing multiplier'
    })
  }

  if (/(smooth|stable|stability|less jank|reduce jank|clean transitions|safer)/i.test(lower)) {
    applyNumericDelta({
      next,
      changes,
      key: 'jank_guard',
      delta: 8,
      source: 'prompt_intent',
      reason: 'Prompt requests smoother output'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'cut_aggression',
      delta: -6,
      source: 'prompt_intent',
      reason: 'Lower aggression to reduce abrupt transitions'
    })
  }

  if (/(story|narrative|context|coherence)/i.test(lower)) {
    applyNumericDelta({
      next,
      changes,
      key: 'story_coherence_guard',
      delta: 9,
      source: 'prompt_intent',
      reason: 'Prompt asks for stronger story continuity'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'max_clip_len_ms',
      delta: 900,
      source: 'prompt_intent',
      reason: 'Allow longer segments to preserve story context'
    })
  }

  if (/(filler|um\\b|uh\\b|dead words)/i.test(lower)) {
    applyNumericDelta({
      next,
      changes,
      key: 'filler_word_weight',
      delta: 0.22,
      source: 'prompt_intent',
      reason: 'Prompt requests stronger filler suppression'
    })
  }

  if (/(redundan|repeat|repetition)/i.test(lower)) {
    applyNumericDelta({
      next,
      changes,
      key: 'redundancy_weight',
      delta: 0.18,
      source: 'prompt_intent',
      reason: 'Prompt requests stronger redundancy suppression'
    })
  }

  if (/(emotion|emotional|spike|energy)/i.test(lower)) {
    applyNumericDelta({
      next,
      changes,
      key: 'spike_boost',
      delta: 0.16,
      source: 'prompt_intent',
      reason: 'Prompt requests stronger emotional spike emphasis'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'energy_floor',
      delta: 0.04,
      source: 'prompt_intent',
      reason: 'Prompt requests stronger energy baseline'
    })
  }

  if (/(silence|dead air|pauses?)/i.test(lower)) {
    if (/(cut more silence|remove silence|trim pauses|remove dead air)/i.test(lower)) {
      applyNumericDelta({
        next,
        changes,
        key: 'silence_min_ms',
        delta: -160,
        source: 'prompt_intent',
        reason: 'Prompt requests more aggressive silence removal'
      })
      applyNumericDelta({
        next,
        changes,
        key: 'silence_db_threshold',
        delta: 3,
        source: 'prompt_intent',
        reason: 'Prompt requests broader silence detection'
      })
    } else if (/(keep pauses|preserve pauses|less silence cut|natural pauses)/i.test(lower)) {
      applyNumericDelta({
        next,
        changes,
        key: 'silence_min_ms',
        delta: 180,
        source: 'prompt_intent',
        reason: 'Prompt requests preserving pauses'
      })
      applyNumericDelta({
        next,
        changes,
        key: 'silence_db_threshold',
        delta: -3,
        source: 'prompt_intent',
        reason: 'Prompt requests stricter silence detection'
      })
    }
  }

  if (!changes.length) {
    const report = await analyzeRenderImprovements({
      limit: Math.max(50, Math.min(5_000, Math.round(Number(fallbackLimit || 800)))),
      range: String(fallbackRange || '7d')
    })
    const fallback = report.suggestions.find((item) => item.predicted_delta_score > 0) || report.suggestions[0] || null
    if (fallback) {
      strategy = 'suggestion_fallback'
      if (Object.prototype.hasOwnProperty.call(fallback.change, 'rollback_to_config_version')) {
        warnings.push('Prompt did not map cleanly to config params. Suggested rollback action instead.')
      } else {
        for (const [rawKey, rawDelta] of Object.entries(fallback.change)) {
          if (!Object.prototype.hasOwnProperty.call(NUMERIC_PARAM_LIMITS, rawKey)) continue
          const key = rawKey as NumericParamKey
          const delta = Number(rawDelta)
          applyNumericDelta({
            next,
            changes,
            key,
            delta,
            source: 'suggestion_fallback',
            reason: `Fallback suggestion: ${fallback.title}`
          })
        }
      }
    } else {
      warnings.push('No deterministic prompt mapping or fallback suggestion was available.')
    }
  }

  if (!changes.length) {
    applyNumericDelta({
      next,
      changes,
      key: 'hook_priority_weight',
      delta: 0.08,
      source: 'suggestion_fallback',
      reason: 'Deterministic baseline fallback tune'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'cut_aggression',
      delta: 3,
      source: 'suggestion_fallback',
      reason: 'Deterministic baseline fallback tune'
    })
    applyNumericDelta({
      next,
      changes,
      key: 'jank_guard',
      delta: 4,
      source: 'suggestion_fallback',
      reason: 'Deterministic baseline fallback tune'
    })
    if (changes.length) {
      strategy = 'suggestion_fallback'
      warnings.push('Prompt mapped to baseline deterministic tuning due limited direct signal match.')
    }
  }

  const parsed = parseConfigParams(next)
  return {
    strategy,
    params: parsed,
    changes,
    warnings
  }
}

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

const promptChangeSchema = z
  .object({
    key: z.string(),
    previous: z.union([z.number(), z.string()]),
    next: z.union([z.number(), z.string()]),
    delta: z.number().nullable(),
    source: z.enum(['prompt_directive', 'prompt_intent', 'suggestion_fallback']),
    reason: z.string()
  })
  .strict()

const promptApplyResponseSchema = z
  .object({
    prompt: z.string(),
    strategy: z.enum(['prompt_directive', 'prompt_intent', 'suggestion_fallback']),
    warnings: z.array(z.string()),
    applied_changes: z.array(promptChangeSchema),
    config: configVersionResponseSchema
  })
  .strict()

const autoOptimizeResponseSchema = z
  .object({
    analyzed_sample_size: z.number(),
    suggestion: suggestionSchema,
    config: configVersionResponseSchema
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

router.post('/prompt/apply', async (req: any, res) => {
  const payload = parseWith(promptTuneRequestSchema, req.body || {}, res)
  if (!payload) return

  const active = await getActiveConfigVersion()
  const promptResult = await parsePromptIntoParams({
    prompt: payload.prompt,
    base: active.params,
    fallbackLimit: payload.fallback_limit,
    fallbackRange: payload.fallback_range
  })

  if (!promptResult.changes.length) {
    return res.status(422).json({
      error: 'prompt_not_actionable',
      message: 'Could not derive deterministic config changes from the prompt.'
    })
  }

  const created = await createConfigVersion({
    createdByUserId: req.user?.id || null,
    presetName: active.preset_name || 'Prompt Tuning',
    params: promptResult.params,
    activate: true,
    note: `Prompt tune: ${payload.prompt.slice(0, 160)}`
  })

  return sendValidated(
    res,
    promptApplyResponseSchema,
    {
      prompt: payload.prompt,
      strategy: promptResult.strategy,
      warnings: promptResult.warnings,
      applied_changes: promptResult.changes,
      config: {
        ...created,
        params: created.params
      }
    },
    201
  )
})

router.post('/auto-optimize', async (req: any, res) => {
  const payload = parseWith(autoOptimizeRequestSchema, req.body || {}, res)
  if (!payload) return

  const report = await analyzeRenderImprovements({
    limit: payload.limit || 1000,
    range: payload.range
  })
  const suggestion = report.suggestions.find((item) => item.predicted_delta_score > 0) || report.suggestions[0] || null
  if (!suggestion) {
    return res.status(422).json({
      error: 'no_optimization_suggestion',
      message: 'No deterministic optimization suggestion is currently available.'
    })
  }

  let config = null as Awaited<ReturnType<typeof createConfigVersion>> | null
  if (Object.prototype.hasOwnProperty.call(suggestion.change, 'rollback_to_config_version')) {
    const rolled = await rollbackConfigVersion()
    if (!rolled) {
      return res.status(422).json({
        error: 'rollback_unavailable',
        message: 'Suggestion requested rollback but no previous config was available.'
      })
    }
    config = rolled
  } else {
    const active = await getActiveConfigVersion()
    const next = parseConfigParams(active.params)
    for (const [rawKey, rawDelta] of Object.entries(suggestion.change)) {
      if (!Object.prototype.hasOwnProperty.call(NUMERIC_PARAM_LIMITS, rawKey)) continue
      applyNumericDelta({
        next,
        changes: [],
        key: rawKey as NumericParamKey,
        delta: Number(rawDelta),
        source: 'suggestion_fallback',
        reason: `Auto optimize: ${suggestion.title}`
      })
    }

    config = await createConfigVersion({
      createdByUserId: req.user?.id || null,
      presetName: active.preset_name || 'Auto Optimized',
      params: parseConfigParams(next),
      activate: true,
      note: `Auto optimize: ${suggestion.title}`
    })
  }

  return sendValidated(
    res,
    autoOptimizeResponseSchema,
    {
      analyzed_sample_size: report.summary.sample_size,
      suggestion,
      config: {
        ...config,
        params: config.params
      }
    },
    201
  )
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
