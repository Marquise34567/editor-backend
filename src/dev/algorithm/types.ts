import { z } from 'zod'

export const algorithmConfigParamsSchema = z
  .object({
    cut_aggression: z.number().min(0).max(100),
    min_clip_len_ms: z.number().int().min(120).max(30_000),
    max_clip_len_ms: z.number().int().min(300).max(120_000),
    silence_db_threshold: z.number().min(-80).max(-5),
    silence_min_ms: z.number().int().min(80).max(8_000),
    filler_word_weight: z.number().min(0).max(4),
    redundancy_weight: z.number().min(0).max(4),
    energy_floor: z.number().min(0).max(1),
    spike_boost: z.number().min(0).max(3),
    pattern_interrupt_every_sec: z.number().min(2).max(60),
    hook_priority_weight: z.number().min(0).max(3),
    story_coherence_guard: z.number().min(0).max(100),
    jank_guard: z.number().min(0).max(100),
    pacing_multiplier: z.number().min(0.3).max(3),
    subtitle_style_mode: z.string().trim().min(1).max(120)
  })
  .strict()

export type AlgorithmConfigParams = z.infer<typeof algorithmConfigParamsSchema>

export type SegmentSignal = {
  index: number
  start_sec: number
  end_sec: number
  duration_sec: number
  energy: number
  info_density: number
  novelty: number
  emotion: number
  filler: number
  redundancy: number
  continuity_risk: number
  context_loss_risk: number
  audio_jank_risk: number
  is_context_segment: boolean
}

export type SegmentDecision = {
  index: number
  start_sec: number
  end_sec: number
  value_score: number
  risk_score: number
  keep_probability: number
  keep_recommendation: boolean
  continuity_risk: number
  context_loss_risk: number
  audio_jank_risk: number
  reasons: string[]
}

export type RetentionFeatures = {
  duration_sec: number
  silence_ratio: number
  filler_words_per_min: number
  avg_shot_len_sec: number
  cut_rate_per_min: number
  redundancy_score: number
  energy_mean: number
  energy_variance: number
  spike_density: number
  flat_segment_seconds: number
  jump_cut_severity: number
  audio_discontinuity_events: number
  caption_desync_events: number
  hook_time_to_payoff: number
  best_moment_in_first8s_score: number
  segment_signals: SegmentSignal[]
  segment_decisions: SegmentDecision[]
  keep_ratio: number
  drop_ratio: number
  missing_signals: string[]
}

export type RetentionSubscores = {
  H: number
  P: number
  E: number
  V: number
  S: number
  F: number
  J: number
}

export type ScoreFlags = {
  auto_safety_adjusted: boolean
  reason?: string
  jank_risk?: number
  micro_crossfade_required?: boolean
  adjusted_cut_aggression?: number
}

export type RetentionScoringResult = {
  score_total: number
  subscores: RetentionSubscores
  features: RetentionFeatures
  flags: ScoreFlags
}

export type AlgorithmConfigVersion = {
  id: string
  created_at: string
  created_by_user_id: string | null
  preset_name: string | null
  params: AlgorithmConfigParams
  is_active: boolean
  note: string | null
}

export type ExperimentArm = {
  config_version_id: string
  weight: number
}

export type AlgorithmExperiment = {
  id: string
  created_at: string
  created_by_user_id: string | null
  name: string
  status: 'draft' | 'running' | 'stopped'
  arms: ExperimentArm[]
  allocation: Record<string, number>
  reward_metric: string
  start_at: string | null
  end_at: string | null
}

export type RenderQualityMetric = {
  id: string
  job_id: string
  user_id: string | null
  created_at: string
  config_version_id: string
  score_total: number
  score_hook: number
  score_pacing: number
  score_emotion: number
  score_visual: number
  score_story: number
  score_jank: number
  features: RetentionFeatures
  flags: Record<string, unknown>
}

export const createConfigRequestSchema = z
  .object({
    preset_name: z.string().trim().min(1).max(120).nullable().optional(),
    params: algorithmConfigParamsSchema,
    activate: z.boolean().optional(),
    note: z.string().trim().max(1_000).nullable().optional()
  })
  .strict()

export const applyPresetRequestSchema = z
  .object({
    preset_key: z.string().trim().min(1).max(120),
    note: z.string().trim().max(1_000).nullable().optional()
  })
  .strict()

export const experimentStartRequestSchema = z
  .object({
    name: z.string().trim().min(3).max(120),
    arms: z
      .array(
        z
          .object({
            config_version_id: z.string().trim().min(1),
            weight: z.number().min(0).max(1)
          })
          .strict()
      )
      .min(2)
      .max(4),
    allocation: z.record(z.string(), z.number().min(0).max(100)),
    reward_metric: z.string().trim().min(1).max(120).optional(),
    start_at: z.string().datetime().optional(),
    end_at: z.string().datetime().optional()
  })
  .strict()

export const analyzeRendersRequestSchema = z
  .object({
    limit: z.number().int().min(50).max(5_000).optional(),
    range: z.string().trim().min(2).max(12).optional()
  })
  .strict()

export const sampleFootageTestRequestSchema = z
  .object({
    job_id: z.string().trim().min(1),
    params: algorithmConfigParamsSchema.optional()
  })
  .strict()
