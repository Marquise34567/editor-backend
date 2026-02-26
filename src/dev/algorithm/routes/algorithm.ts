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
