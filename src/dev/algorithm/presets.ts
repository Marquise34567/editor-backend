import { AlgorithmConfigParams } from './types'

export type AlgorithmPresetTemplate = {
  key: string
  name: string
  description: string
  params: AlgorithmConfigParams
}

export const DEFAULT_ALGORITHM_PARAMS: AlgorithmConfigParams = {
  cut_aggression: 58,
  min_clip_len_ms: 650,
  max_clip_len_ms: 9_500,
  silence_db_threshold: -42,
  silence_min_ms: 260,
  filler_word_weight: 1.2,
  redundancy_weight: 1.05,
  energy_floor: 0.32,
  spike_boost: 0.85,
  pattern_interrupt_every_sec: 11,
  hook_priority_weight: 1.15,
  story_coherence_guard: 68,
  jank_guard: 72,
  pacing_multiplier: 1,
  subtitle_style_mode: 'clean_high_contrast'
}

export const ALGORITHM_PRESET_TEMPLATES: AlgorithmPresetTemplate[] = [
  {
    key: 'viral_mode',
    name: 'Viral Mode',
    description: 'Front-load hooks and interruptions for short-form retention spikes.',
    params: {
      ...DEFAULT_ALGORITHM_PARAMS,
      cut_aggression: 78,
      min_clip_len_ms: 420,
      max_clip_len_ms: 6_400,
      filler_word_weight: 1.4,
      redundancy_weight: 1.2,
      spike_boost: 1.3,
      pattern_interrupt_every_sec: 7,
      hook_priority_weight: 1.65,
      story_coherence_guard: 52,
      jank_guard: 58,
      pacing_multiplier: 1.32,
      subtitle_style_mode: 'viral_pop'
    }
  },
  {
    key: 'hyper_cut_mode',
    name: 'Hyper Cut Mode',
    description: 'Maximum cut density for high-motion content with strict anti-silence logic.',
    params: {
      ...DEFAULT_ALGORITHM_PARAMS,
      cut_aggression: 92,
      min_clip_len_ms: 280,
      max_clip_len_ms: 4_800,
      silence_db_threshold: -48,
      silence_min_ms: 180,
      filler_word_weight: 1.55,
      redundancy_weight: 1.35,
      spike_boost: 1.4,
      pattern_interrupt_every_sec: 5,
      hook_priority_weight: 1.45,
      story_coherence_guard: 44,
      jank_guard: 50,
      pacing_multiplier: 1.55,
      subtitle_style_mode: 'kinetic'
    }
  },
  {
    key: 'story_mode',
    name: 'Story Mode',
    description: 'Conservative pacing with strong coherence protection for narrative clips.',
    params: {
      ...DEFAULT_ALGORITHM_PARAMS,
      cut_aggression: 34,
      min_clip_len_ms: 1_150,
      max_clip_len_ms: 14_000,
      silence_db_threshold: -38,
      silence_min_ms: 340,
      filler_word_weight: 0.95,
      redundancy_weight: 0.92,
      energy_floor: 0.26,
      spike_boost: 0.55,
      pattern_interrupt_every_sec: 18,
      hook_priority_weight: 0.92,
      story_coherence_guard: 92,
      jank_guard: 84,
      pacing_multiplier: 0.86,
      subtitle_style_mode: 'documentary'
    }
  },
  {
    key: 'psychological_hook_mode',
    name: 'Psychological Hook Mode',
    description: 'Optimizes curiosity loops and early payoff without fully sacrificing continuity.',
    params: {
      ...DEFAULT_ALGORITHM_PARAMS,
      cut_aggression: 64,
      min_clip_len_ms: 560,
      max_clip_len_ms: 8_200,
      filler_word_weight: 1.25,
      redundancy_weight: 1.18,
      energy_floor: 0.29,
      spike_boost: 1.08,
      pattern_interrupt_every_sec: 8,
      hook_priority_weight: 1.9,
      story_coherence_guard: 66,
      jank_guard: 70,
      pacing_multiplier: 1.12,
      subtitle_style_mode: 'neuro_hook'
    }
  },
  {
    key: 'cinematic_mode',
    name: 'Cinematic Mode',
    description: 'Longer beats, smoother transitions, and stricter anti-jank guards.',
    params: {
      ...DEFAULT_ALGORITHM_PARAMS,
      cut_aggression: 26,
      min_clip_len_ms: 1_400,
      max_clip_len_ms: 18_000,
      silence_db_threshold: -36,
      silence_min_ms: 430,
      filler_word_weight: 0.82,
      redundancy_weight: 0.9,
      energy_floor: 0.24,
      spike_boost: 0.42,
      pattern_interrupt_every_sec: 22,
      hook_priority_weight: 0.88,
      story_coherence_guard: 86,
      jank_guard: 91,
      pacing_multiplier: 0.74,
      subtitle_style_mode: 'cinematic_minimal'
    }
  },
  {
    key: 'premium_creator_mode',
    name: 'Premium Creator Mode',
    description: 'Balanced premium profile tuned for broad creator content and stable quality.',
    params: {
      ...DEFAULT_ALGORITHM_PARAMS,
      cut_aggression: 56,
      min_clip_len_ms: 620,
      max_clip_len_ms: 10_800,
      silence_db_threshold: -40,
      silence_min_ms: 250,
      filler_word_weight: 1.1,
      redundancy_weight: 1.02,
      energy_floor: 0.31,
      spike_boost: 0.9,
      pattern_interrupt_every_sec: 12,
      hook_priority_weight: 1.22,
      story_coherence_guard: 74,
      jank_guard: 79,
      pacing_multiplier: 1.02,
      subtitle_style_mode: 'premium_clean'
    }
  }
]

export const getPresetByKey = (key: string) => {
  const normalized = String(key || '').trim().toLowerCase()
  return ALGORITHM_PRESET_TEMPLATES.find((preset) => preset.key === normalized) || null
}

export const getDefaultPreset = () => getPresetByKey('premium_creator_mode') || ALGORITHM_PRESET_TEMPLATES[0]
