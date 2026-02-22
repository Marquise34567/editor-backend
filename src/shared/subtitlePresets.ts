export type SubtitlePresetId =
  | 'basic_clean'
  | 'bold_pop'
  | 'outline_heavy'
  | 'caption_box'
  | 'karaoke_highlight'
  | 'neon_glow'

export type SubtitlePresetDefinition = {
  id: SubtitlePresetId
  label: string
  description: string
}

export const SUBTITLE_PRESET_REGISTRY: SubtitlePresetDefinition[] = [
  { id: 'basic_clean', label: 'Minimal White', description: 'Clean white captions with subtle outline.' },
  { id: 'bold_pop', label: 'Bold Influencer', description: 'High-contrast styling that pops on mobile.' },
  { id: 'outline_heavy', label: 'Cinematic Serif', description: 'Film-style serif captions with strong outline.' },
  { id: 'caption_box', label: 'Black Box', description: 'Boxed captions for maximum readability.' },
  { id: 'neon_glow', label: 'Neon Glow', description: 'Bright glow treatment for stylized edits.' },
  { id: 'karaoke_highlight', label: 'Karaoke Highlight', description: 'Word-by-word highlight styling.' }
]

export const SUBTITLE_PRESET_IDS = SUBTITLE_PRESET_REGISTRY.map((preset) => preset.id)

export const DEFAULT_SUBTITLE_PRESET: SubtitlePresetId = 'basic_clean'

const LEGACY_PRESET_ALIASES: Record<string, SubtitlePresetId> = {
  minimal: 'basic_clean',
  clean: 'basic_clean',
  bold: 'bold_pop',
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

export const normalizeSubtitlePreset = (value?: string | null): SubtitlePresetId | null => {
  if (!value) return null
  const normalized = String(value).trim().toLowerCase()
  const alias = LEGACY_PRESET_ALIASES[normalized]
  const candidate = alias ?? normalized
  return SUBTITLE_PRESET_IDS.includes(candidate as SubtitlePresetId) ? (candidate as SubtitlePresetId) : null
}

export const isSubtitlePresetId = (value?: string | null): value is SubtitlePresetId => {
  return normalizeSubtitlePreset(value) !== null
}
