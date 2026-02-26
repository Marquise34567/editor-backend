export type SubtitlePresetId =
  | 'basic_clean'
  | 'bold_pop'
  | 'outline_heavy'
  | 'caption_box'
  | 'karaoke_highlight'
  | 'mrbeast_animated'
  | 'neon_glow'

export type SubtitleFontId =
  | 'impact'
  | 'sans_bold'
  | 'condensed'
  | 'serif_bold'
  | 'display_black'
  | 'mono_bold'

export type SubtitleAnimationId = 'pop' | 'none'

export type SubtitlePresetDefinition = {
  id: SubtitlePresetId
  label: string
  description: string
}

export const SUBTITLE_PRESET_REGISTRY: SubtitlePresetDefinition[] = [
  { id: 'basic_clean', label: 'Minimal White', description: 'Clean white captions with subtle outline.' },
  { id: 'bold_pop', label: 'Bold Influencer', description: 'High-contrast styling that pops on mobile.' },
  { id: 'mrbeast_animated', label: 'High-Energy Animated', description: 'High-energy animated captions with punchy styling.' },
  { id: 'outline_heavy', label: 'Cinematic Serif', description: 'Film-style serif captions with strong outline.' },
  { id: 'caption_box', label: 'Black Box', description: 'Boxed captions for maximum readability.' },
  { id: 'neon_glow', label: 'Neon Glow', description: 'Bright glow treatment for stylized edits.' },
  { id: 'karaoke_highlight', label: 'Karaoke Highlight', description: 'Word-by-word highlight styling.' }
]

export const SUBTITLE_PRESET_IDS = SUBTITLE_PRESET_REGISTRY.map((preset) => preset.id)

export const DEFAULT_SUBTITLE_PRESET: SubtitlePresetId = 'basic_clean'

export type SubtitleStyleConfig = {
  preset: SubtitlePresetId
  fontId: SubtitleFontId
  fontSize: number
  textColor: string
  accentColor: string
  outlineColor: string
  outlineWidth: number
  animation: SubtitleAnimationId
}

const STYLE_CONFIG_DELIMITER = '::'
const STYLE_CONFIG_MAX_LENGTH = 320
const DEFAULT_MRBEAST_STYLE: Omit<SubtitleStyleConfig, 'preset'> = {
  fontId: 'impact',
  fontSize: 58,
  textColor: 'FFFFFF',
  accentColor: '00E5FF',
  outlineColor: '111111',
  outlineWidth: 6,
  animation: 'pop'
}

const FONT_IDS: SubtitleFontId[] = ['impact', 'sans_bold', 'condensed', 'serif_bold', 'display_black', 'mono_bold']
const ANIMATION_IDS: SubtitleAnimationId[] = ['pop', 'none']

const LEGACY_PRESET_ALIASES: Record<string, SubtitlePresetId> = {
  minimal: 'basic_clean',
  clean: 'basic_clean',
  bold: 'bold_pop',
  mrbeast: 'mrbeast_animated',
  'mr beast': 'mrbeast_animated',
  mrbeaststyle: 'mrbeast_animated',
  mrbeastanimated: 'mrbeast_animated',
  beast: 'mrbeast_animated',
  cinematic: 'outline_heavy',
  karaoke: 'karaoke_highlight',
  neon: 'neon_glow',
  'neon glow': 'neon_glow',
  'high contrast': 'bold_pop',
  'high-contrast': 'bold_pop',
  high_contrast: 'bold_pop',
  highcontrast: 'bold_pop',
  'black box': 'caption_box',
  black_box: 'caption_box',
  blackbox: 'caption_box',
  'caption box': 'caption_box',
  outline: 'outline_heavy',
  influencer: 'bold_pop'
}

const extractPresetToken = (value?: string | null) => {
  if (!value) return ''
  const raw = String(value).trim()
  if (!raw) return ''
  const splitIndex = raw.indexOf(STYLE_CONFIG_DELIMITER)
  return (splitIndex === -1 ? raw : raw.slice(0, splitIndex)).trim()
}

const normalizeHexColor = (value?: string | null) => {
  if (!value) return null
  const compact = String(value).trim().replace(/^#/, '')
  if (!/^[0-9a-f]{6}$/i.test(compact)) return null
  return compact.toUpperCase()
}

const parseStylePairs = (value: string) => {
  const out: Record<string, string> = {}
  const safe = String(value || '').slice(0, STYLE_CONFIG_MAX_LENGTH)
  const parts = safe.split(/[;,&]/).map((part) => part.trim()).filter(Boolean)
  for (const part of parts) {
    const [rawKey, ...rest] = part.split('=')
    if (!rawKey || rest.length === 0) continue
    const key = rawKey.trim().toLowerCase()
    const rawValue = rest.join('=').trim()
    if (!key || !rawValue) continue
    out[key] = rawValue
  }
  return out
}

export const normalizeSubtitlePreset = (value?: string | null): SubtitlePresetId | null => {
  if (!value) return null
  const normalized = extractPresetToken(value).toLowerCase()
  const alias = LEGACY_PRESET_ALIASES[normalized]
  const candidate = alias ?? normalized
  return SUBTITLE_PRESET_IDS.includes(candidate as SubtitlePresetId) ? (candidate as SubtitlePresetId) : null
}

export const isSubtitlePresetId = (value?: string | null): value is SubtitlePresetId => {
  return normalizeSubtitlePreset(value) !== null
}

const normalizeFontId = (value?: string | null): SubtitleFontId => {
  const raw = String(value || '').toLowerCase().trim()
  if (FONT_IDS.includes(raw as SubtitleFontId)) return raw as SubtitleFontId
  return DEFAULT_MRBEAST_STYLE.fontId
}

const normalizeAnimationId = (value?: string | null): SubtitleAnimationId => {
  const raw = String(value || '').toLowerCase().trim()
  if (ANIMATION_IDS.includes(raw as SubtitleAnimationId)) return raw as SubtitleAnimationId
  return DEFAULT_MRBEAST_STYLE.animation
}

const normalizeOutlineWidth = (value?: string | number | null) => {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return DEFAULT_MRBEAST_STYLE.outlineWidth
  return Math.max(1, Math.min(24, Math.round(parsed)))
}

const normalizeFontSize = (value?: string | number | null) => {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return DEFAULT_MRBEAST_STYLE.fontSize
  return Math.max(32, Math.min(220, Math.round(parsed)))
}

export const parseSubtitleStyleConfig = (value?: string | null): SubtitleStyleConfig => {
  const preset = normalizeSubtitlePreset(value) ?? DEFAULT_SUBTITLE_PRESET
  const defaults: SubtitleStyleConfig = {
    preset,
    ...DEFAULT_MRBEAST_STYLE
  }
  const raw = String(value || '')
  const splitIndex = raw.indexOf(STYLE_CONFIG_DELIMITER)
  if (splitIndex === -1) return defaults
  const configRaw = raw.slice(splitIndex + STYLE_CONFIG_DELIMITER.length)
  const parsed = parseStylePairs(configRaw)
  const textColor = normalizeHexColor(parsed.text ?? parsed.textcolor) ?? defaults.textColor
  const accentColor = normalizeHexColor(parsed.accent ?? parsed.accentcolor) ?? defaults.accentColor
  const outlineColor = normalizeHexColor(parsed.outline ?? parsed.outlinecolor) ?? defaults.outlineColor
  return {
    ...defaults,
    fontId: normalizeFontId(parsed.font ?? parsed.fontid),
    fontSize: normalizeFontSize(parsed.fontsize ?? parsed.size),
    textColor,
    accentColor,
    outlineColor,
    outlineWidth: normalizeOutlineWidth(parsed.outlinewidth ?? parsed.border),
    animation: normalizeAnimationId(parsed.animation)
  }
}

export const serializeSubtitleStyleConfig = (config: SubtitleStyleConfig) => {
  const preset = normalizeSubtitlePreset(config.preset) ?? DEFAULT_SUBTITLE_PRESET
  const supportsExtendedConfig = preset === 'mrbeast_animated' || preset === 'neon_glow'
  if (!supportsExtendedConfig) return preset
  const normalized = parseSubtitleStyleConfig(
    `${preset}${STYLE_CONFIG_DELIMITER}` +
    `font=${String(config.fontId || '')};` +
    `fontSize=${String(config.fontSize || '')};` +
    `text=${String(config.textColor || '')};` +
    `accent=${String(config.accentColor || '')};` +
    `outline=${String(config.outlineColor || '')};` +
    `outlineWidth=${String(config.outlineWidth || '')};` +
    `animation=${String(config.animation || '')}`
  )
  return `${preset}${STYLE_CONFIG_DELIMITER}` +
    `font=${normalized.fontId};` +
    `fontSize=${normalized.fontSize};` +
    `text=${normalized.textColor};` +
    `accent=${normalized.accentColor};` +
    `outline=${normalized.outlineColor};` +
    `outlineWidth=${normalized.outlineWidth};` +
    `animation=${normalized.animation}`
}
