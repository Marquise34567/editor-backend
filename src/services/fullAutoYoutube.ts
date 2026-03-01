type RenderMode = 'horizontal' | 'vertical'
type RetentionStrategyProfile = 'safe' | 'balanced' | 'viral'
type RetentionAggressionLevel = 'low' | 'medium' | 'high' | 'viral'
type RetentionTargetPlatform = 'auto' | 'tiktok' | 'instagram_reels' | 'youtube'
type PlatformProfile = 'auto' | 'tiktok' | 'instagram_reels' | 'youtube'
type EditorModeSelection =
  | 'auto'
  | 'reaction'
  | 'commentary'
  | 'savage-roast'
  | 'vlog'
  | 'gaming'
  | 'sports'
  | 'education'
  | 'podcast'
  | 'ultra'
  | 'retention-king'
type HookSelectionMode = 'manual' | 'auto'
type LongFormPreset = 'auto' | 'balanced' | 'aggressive' | 'ultra'

export type FullAutoYoutubeTarget = 'auto' | 'long_form' | 'shorts'
export type FullAutoYoutubeVibe = 'auto' | 'hype' | 'cinematic' | 'chill' | 'education'

export type FullAutoYoutubeRequest = {
  enabled: true
  target: FullAutoYoutubeTarget
  vibe: FullAutoYoutubeVibe
  renderMode: RenderMode
  includeSeoPack: boolean
  includePromptPack: boolean
  includeQueueHints: boolean
  preferAiBroll: boolean
}

export type FullAutoYoutubeDefaults = {
  retentionStrategyProfile: RetentionStrategyProfile
  retentionAggressionLevel: RetentionAggressionLevel
  retentionTargetPlatform: RetentionTargetPlatform
  platformProfile: PlatformProfile
  onlyCuts: boolean
  smartZoom: boolean
  transitions: boolean
  soundFx: boolean
  autoCaptions: boolean
  subtitleStyle: string
  maxCuts: number
  editorMode: EditorModeSelection | null
  hookSelectionMode: HookSelectionMode
  longFormPreset: LongFormPreset
  longFormAggression: number
  longFormClarityVsSpeed: number
  tangentKiller: boolean
  fastMode: boolean
}

export type FullAutoYoutubeProfile = {
  mode: 'full_auto_youtube'
  version: '2026.03'
  generatedAt: string
  target: Exclude<FullAutoYoutubeTarget, 'auto'>
  vibe: Exclude<FullAutoYoutubeVibe, 'auto'>
  recommendedRenderMode: RenderMode
  highlights: string[]
  transitionPack: string[]
  overlayPack: string[]
  soundFxPack: string[]
  musicPlan: {
    vibe: string
    source: 'local_pack'
    ducking: boolean
    fadeInSeconds: number
    fadeOutSeconds: number
  }
  exportPlan: {
    aspectRatio: '16:9' | '9:16'
    resolution: '1920x1080' | '1080x1920'
    codec: 'h264+aac'
    container: 'mp4'
    maxDurationSeconds: number
  }
  seoSuggestions: {
    titles: string[]
    thumbnailIdeas: string[]
    hashtags: string[]
  } | null
  subAiPrompts: {
    bgmSelector: string
    transitionPlanner: string
    overlayDirector: string
  } | null
  edgeCases: string[]
  cloudQueue: string[]
  preferAiBroll: boolean
}

export type FullAutoYoutubePreset = {
  defaults: FullAutoYoutubeDefaults
  profile: FullAutoYoutubeProfile
}

const TRUTHY = new Set(['1', 'true', 'yes', 'y', 'on', 'enabled', 'enable'])
const FALSY = new Set(['0', 'false', 'no', 'n', 'off', 'disabled', 'disable'])

const parseBooleanLike = (value: unknown): boolean | null => {
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return Number.isFinite(value) ? value !== 0 : null
  const raw = String(value ?? '').trim().toLowerCase()
  if (!raw) return null
  if (TRUTHY.has(raw)) return true
  if (FALSY.has(raw)) return false
  return null
}

const normalizeRenderMode = (value: unknown, fallback: RenderMode): RenderMode => {
  const raw = String(value ?? '').trim().toLowerCase()
  if (raw === 'vertical') return 'vertical'
  if (raw === 'horizontal' || raw === 'standard' || raw === 'long_form') return 'horizontal'
  return fallback
}

const normalizeTarget = (value: unknown): FullAutoYoutubeTarget => {
  const raw = String(value ?? '').trim().toLowerCase()
  if (!raw || raw === 'auto' || raw === 'automatic') return 'auto'
  if (raw === 'long' || raw === 'longform' || raw === 'long_form' || raw === 'long-form') return 'long_form'
  if (raw === 'short' || raw === 'shorts' || raw === 'youtube_shorts' || raw === 'yt_shorts') return 'shorts'
  return 'auto'
}

const normalizeVibe = (value: unknown): FullAutoYoutubeVibe => {
  const raw = String(value ?? '').trim().toLowerCase()
  if (!raw || raw === 'auto' || raw === 'automatic') return 'auto'
  if (raw === 'hype' || raw === 'energetic') return 'hype'
  if (raw === 'cinematic' || raw === 'film') return 'cinematic'
  if (raw === 'chill' || raw === 'calm' || raw === 'ambient') return 'chill'
  if (raw === 'education' || raw === 'tutorial' || raw === 'explainer') return 'education'
  return 'auto'
}

const resolveTarget = (target: FullAutoYoutubeTarget, renderMode: RenderMode): Exclude<FullAutoYoutubeTarget, 'auto'> => {
  if (target === 'long_form' || target === 'shorts') return target
  return renderMode === 'vertical' ? 'shorts' : 'long_form'
}

const resolveVibe = (
  vibe: FullAutoYoutubeVibe,
  target: Exclude<FullAutoYoutubeTarget, 'auto'>
): Exclude<FullAutoYoutubeVibe, 'auto'> => {
  if (vibe !== 'auto') return vibe
  return target === 'shorts' ? 'hype' : 'cinematic'
}

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value))

const buildSeoTitleSuggestions = ({
  target,
  vibe
}: {
  target: Exclude<FullAutoYoutubeTarget, 'auto'>
  vibe: Exclude<FullAutoYoutubeVibe, 'auto'>
}) => {
  if (target === 'shorts') {
    return [
      vibe === 'hype' ? 'You Wont Believe This Clip' : 'The Moment Everything Changed',
      'Watch This Before You Scroll',
      'Best Part in 20 Seconds'
    ]
  }
  if (vibe === 'education') {
    return [
      'The Simple Framework That Actually Works',
      'Step-by-Step Breakdown (No Fluff)',
      'How to Get Better Results Faster'
    ]
  }
  return [
    'I Recut This Video for Retention and It Worked',
    'The Editing System I Use to Keep Viewers Watching',
    'From Raw Footage to Publish-Ready in One Pass'
  ]
}

const buildThumbnailIdeas = ({
  target,
  vibe
}: {
  target: Exclude<FullAutoYoutubeTarget, 'auto'>
  vibe: Exclude<FullAutoYoutubeVibe, 'auto'>
}) => {
  if (target === 'shorts') {
    return [
      'Face close-up + huge 2-word hook',
      'Before/after split with bold arrow',
      'High-motion frame + mini caption bubble'
    ]
  }
  if (vibe === 'education') {
    return [
      'Single promise headline + clean icon',
      'Checklist visual + big outcome number',
      'Problem left / solution right layout'
    ]
  }
  return [
    'Emotion frame + curiosity headline',
    '3-step visual stack + strong contrast',
    'Reaction face + result screenshot'
  ]
}

const buildHashtags = ({
  target,
  vibe
}: {
  target: Exclude<FullAutoYoutubeTarget, 'auto'>
  vibe: Exclude<FullAutoYoutubeVibe, 'auto'>
}) => {
  const base = ['#youtube', '#contentcreator', '#videoediting', '#autoeditor']
  if (target === 'shorts') base.push('#shorts')
  if (vibe === 'hype') base.push('#viral', '#fastcut')
  if (vibe === 'education') base.push('#tutorial', '#learnonyoutube')
  if (vibe === 'cinematic') base.push('#cinematic', '#storytelling')
  if (vibe === 'chill') base.push('#calmvibes', '#ambient')
  return base
}

export const parseFullAutoYoutubeRequest = (
  payload: unknown,
  fallbackRenderMode: RenderMode
): FullAutoYoutubeRequest | null => {
  const source = payload && typeof payload === 'object' ? (payload as Record<string, any>) : {}
  const nestedRaw = source.fullAutoYoutube
  const nested = nestedRaw && typeof nestedRaw === 'object' ? (nestedRaw as Record<string, any>) : {}

  const enabledFlag = parseBooleanLike(
    nested.enabled ??
    source.fullAutoYoutubeEnabled ??
    source.full_auto_youtube_enabled ??
    source.autoYoutube ??
    source.auto_youtube ??
    source.fullAutoYoutube
  )
  const target = normalizeTarget(
    nested.target ??
    nested.mode ??
    source.fullAutoYoutubeTarget ??
    source.full_auto_youtube_target ??
    source.fullAutoYoutubeMode ??
    source.full_auto_youtube_mode
  )
  const vibe = normalizeVibe(
    nested.vibe ??
    source.fullAutoYoutubeVibe ??
    source.full_auto_youtube_vibe
  )
  const includeSeoPack = parseBooleanLike(
    nested.includeSeoPack ??
    source.includeSeoPack ??
    source.include_seo_pack
  ) ?? true
  const includePromptPack = parseBooleanLike(
    nested.includePromptPack ??
    source.includePromptPack ??
    source.include_prompt_pack
  ) ?? true
  const includeQueueHints = parseBooleanLike(
    nested.includeQueueHints ??
    source.includeQueueHints ??
    source.include_queue_hints
  ) ?? true
  const preferAiBroll = parseBooleanLike(
    nested.preferAiBroll ??
    source.preferAiBroll ??
    source.prefer_ai_broll
  ) ?? true
  const renderMode = normalizeRenderMode(
    nested.renderMode ?? source.renderMode,
    fallbackRenderMode
  )

  const enabled = enabledFlag === true || target !== 'auto' || vibe !== 'auto'
  if (!enabled) return null

  return {
    enabled: true,
    target,
    vibe,
    renderMode,
    includeSeoPack,
    includePromptPack,
    includeQueueHints,
    preferAiBroll
  }
}

export const buildFullAutoYoutubePreset = (request: FullAutoYoutubeRequest): FullAutoYoutubePreset => {
  const target = resolveTarget(request.target, request.renderMode)
  const vibe = resolveVibe(request.vibe, target)
  const recommendedRenderMode: RenderMode = target === 'shorts' ? 'vertical' : 'horizontal'

  const retentionStrategyProfile: RetentionStrategyProfile =
    target === 'shorts'
      ? 'viral'
      : vibe === 'education'
        ? 'safe'
        : 'balanced'
  const retentionAggressionLevel: RetentionAggressionLevel =
    target === 'shorts'
      ? 'viral'
      : vibe === 'education'
        ? 'low'
        : 'medium'
  const maxCuts = target === 'shorts'
    ? 12
    : vibe === 'education'
      ? 6
      : 8
  const editorModeByVibe: Record<Exclude<FullAutoYoutubeVibe, 'auto'>, EditorModeSelection | null> = {
    hype: 'gaming',
    cinematic: 'vlog',
    chill: 'commentary',
    education: 'education'
  }
  const subtitleStyleByVibe: Record<Exclude<FullAutoYoutubeVibe, 'auto'>, string> = {
    hype: 'mrbeast_animated',
    cinematic: 'outline_heavy',
    chill: 'caption_box',
    education: 'basic_clean'
  }
  const transitionPackByVibe: Record<Exclude<FullAutoYoutubeVibe, 'auto'>, string[]> = {
    hype: ['zoom_punch', 'glitch_pop', 'whip_pan'],
    cinematic: ['crossfade_film', 'dissolve_soft', 'push_in'],
    chill: ['soft_fade', 'light_wipe', 'slow_zoom'],
    education: ['clean_cut', 'chapter_slide', 'focus_zoom']
  }
  const overlayPackByVibe: Record<Exclude<FullAutoYoutubeVibe, 'auto'>, string[]> = {
    hype: ['text_pop', 'emoji_punch', 'reaction_lower_third'],
    cinematic: ['clean_lower_third', 'cinematic_lut', 'scene_title_cards'],
    chill: ['minimal_lower_third', 'ambient_gradient', 'soft_progress_marker'],
    education: ['step_cards', 'keyword_callout', 'chapter_lower_third']
  }
  const soundFxPackByVibe: Record<Exclude<FullAutoYoutubeVibe, 'auto'>, string[]> = {
    hype: ['whoosh', 'impact_hit', 'ding'],
    cinematic: ['soft_riser', 'sub_hit', 'transition_whoosh'],
    chill: ['light_chime', 'air_sweep', 'soft_click'],
    education: ['clean_click', 'subtle_whoosh', 'marker_ding']
  }
  const musicVibeByVibe: Record<Exclude<FullAutoYoutubeVibe, 'auto'>, string> = {
    hype: 'energetic-hybrid-electronic',
    cinematic: 'cinematic-ambient-pulse',
    chill: 'lofi-ambient',
    education: 'clean-ambient-focus'
  }

  const longFormPreset: LongFormPreset =
    target === 'long_form'
      ? (vibe === 'education' ? 'balanced' : 'aggressive')
      : 'auto'
  const longFormAggression =
    target === 'long_form'
      ? (vibe === 'education' ? 58 : 74)
      : 62
  const longFormClarityVsSpeed =
    target === 'long_form'
      ? (vibe === 'education' ? 78 : 56)
      : 46
  const tangentKiller = target === 'long_form'

  const defaults: FullAutoYoutubeDefaults = {
    retentionStrategyProfile,
    retentionAggressionLevel,
    retentionTargetPlatform: 'youtube',
    platformProfile: 'youtube',
    onlyCuts: false,
    smartZoom: true,
    transitions: true,
    soundFx: target === 'shorts' || vibe === 'hype',
    autoCaptions: true,
    subtitleStyle: subtitleStyleByVibe[vibe],
    maxCuts,
    editorMode: editorModeByVibe[vibe],
    hookSelectionMode: 'auto',
    longFormPreset,
    longFormAggression: clamp(Math.round(longFormAggression), 0, 100),
    longFormClarityVsSpeed: clamp(Math.round(longFormClarityVsSpeed), 0, 100),
    tangentKiller,
    fastMode: target === 'shorts' && vibe === 'hype'
  }

  const profile: FullAutoYoutubeProfile = {
    mode: 'full_auto_youtube',
    version: '2026.03',
    generatedAt: new Date().toISOString(),
    target,
    vibe,
    recommendedRenderMode,
    highlights: [
      target === 'shorts'
        ? 'Hook-first short-form pacing with denser pattern interrupts.'
        : 'Long-form clarity-first pacing with chapter-safe compression.',
      `Vibe-coded transitions and overlays tuned for ${vibe} delivery.`,
      'Auto captions + adaptive SFX + background music ducking included.'
    ],
    transitionPack: transitionPackByVibe[vibe],
    overlayPack: overlayPackByVibe[vibe],
    soundFxPack: soundFxPackByVibe[vibe],
    musicPlan: {
      vibe: musicVibeByVibe[vibe],
      source: 'local_pack',
      ducking: true,
      fadeInSeconds: target === 'shorts' ? 0.3 : 0.8,
      fadeOutSeconds: target === 'shorts' ? 0.45 : 1.2
    },
    exportPlan: {
      aspectRatio: target === 'shorts' ? '9:16' : '16:9',
      resolution: target === 'shorts' ? '1080x1920' : '1920x1080',
      codec: 'h264+aac',
      container: 'mp4',
      maxDurationSeconds: target === 'shorts' ? 65 : 4 * 60 * 60
    },
    seoSuggestions: request.includeSeoPack
      ? {
          titles: buildSeoTitleSuggestions({ target, vibe }),
          thumbnailIdeas: buildThumbnailIdeas({ target, vibe }),
          hashtags: buildHashtags({ target, vibe })
        }
      : null,
    subAiPrompts: request.includePromptPack
      ? {
          bgmSelector:
            `Select one royalty-free track for a ${vibe} ${target} YouTube edit. ` +
            'Return BPM, energy arc, loop points, ducking recommendations, and 2 fallback options.',
          transitionPlanner:
            `Generate transition timings for ${target} pacing with ${vibe} mood. ` +
            'Return timestamp ranges, transition type, and intensity score (0-1).',
          overlayDirector:
            `Suggest text pops/lower thirds/emoji cadence for a ${vibe} ${target} edit. ` +
            'Return concise overlay cues with start/end times and purpose.'
        }
      : null,
    edgeCases: [
      'If transcript confidence is low, fall back to energy+silence cutting with conservative pacing.',
      'If source audio is noisy, auto-enable denoise profile and reduce SFX intensity.',
      'If duration exceeds runtime budget, segment render into queueable chunks before final stitch.',
      'If no B-roll assets are available, downgrade to motion graphics overlays or AI placeholders.'
    ],
    cloudQueue: request.includeQueueHints
      ? [
          'Queue stage split: ingest -> analyze -> edit-plan -> render -> package.',
          'Use GPU workers for FFmpeg encode; CPU workers for transcript/metadata.',
          'Store intermediate timeline JSON so retries skip expensive analysis.',
          'Autoscale by input minutes and expected output variants (long/short).'
        ]
      : [],
    preferAiBroll: request.preferAiBroll
  }

  return { defaults, profile }
}
