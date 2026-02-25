import {
  DEFAULT_SUBTITLE_PRESET,
  normalizeSubtitlePreset,
  type SubtitlePresetId
} from './subtitlePresets'

export type PlatformProfileId = 'auto' | 'tiktok' | 'instagram_reels' | 'youtube'

export type PlatformEditProfile = {
  id: PlatformProfileId
  label: string
  description: string
  defaultSubtitlePreset: SubtitlePresetId
  verticalClipDurationDivisor: number
  verticalMinClipSeconds: number
  verticalMaxClipSeconds: number
  verticalSpacingRatio: number
  videoPreset: 'medium' | 'fast' | 'slow'
  crfDelta: number
  audioBitrateKbps: number
  audioSampleRate: number
}

export const PLATFORM_EDIT_PROFILES: Record<PlatformProfileId, PlatformEditProfile> = {
  auto: {
    id: 'auto',
    label: 'Auto',
    description: 'Balanced defaults when no explicit platform is selected.',
    defaultSubtitlePreset: DEFAULT_SUBTITLE_PRESET,
    verticalClipDurationDivisor: 3.2,
    verticalMinClipSeconds: 8,
    verticalMaxClipSeconds: 55,
    verticalSpacingRatio: 0.22,
    videoPreset: 'medium',
    crfDelta: 0,
    audioBitrateKbps: 192,
    audioSampleRate: 48000
  },
  tiktok: {
    id: 'tiktok',
    label: 'TikTok',
    description: 'Shorter highlight windows and mobile-first export tuning.',
    defaultSubtitlePreset: 'bold_pop',
    verticalClipDurationDivisor: 3.8,
    verticalMinClipSeconds: 7,
    verticalMaxClipSeconds: 42,
    verticalSpacingRatio: 0.28,
    videoPreset: 'fast',
    crfDelta: 1,
    audioBitrateKbps: 192,
    audioSampleRate: 48000
  },
  instagram_reels: {
    id: 'instagram_reels',
    label: 'Instagram Reels',
    description: 'Fast clip pacing with clean readability-focused subtitles.',
    defaultSubtitlePreset: 'caption_box',
    verticalClipDurationDivisor: 3.5,
    verticalMinClipSeconds: 8,
    verticalMaxClipSeconds: 48,
    verticalSpacingRatio: 0.24,
    videoPreset: 'fast',
    crfDelta: 1,
    audioBitrateKbps: 192,
    audioSampleRate: 48000
  },
  youtube: {
    id: 'youtube',
    label: 'YouTube',
    description: 'Context-preserving pacing with higher quality encode defaults.',
    defaultSubtitlePreset: 'basic_clean',
    verticalClipDurationDivisor: 2.9,
    verticalMinClipSeconds: 10,
    verticalMaxClipSeconds: 58,
    verticalSpacingRatio: 0.18,
    videoPreset: 'medium',
    crfDelta: -1,
    audioBitrateKbps: 224,
    audioSampleRate: 48000
  }
}

export const parsePlatformProfile = (
  value?: unknown,
  fallback: PlatformProfileId = 'auto'
): PlatformProfileId => {
  const raw = String(value || '').trim().toLowerCase()
  if (!raw || raw === 'auto' || raw === 'default') return 'auto'
  if (raw === 'tiktok' || raw === 'tt' || raw.includes('tik')) return 'tiktok'
  if (
    raw === 'instagram_reels' ||
    raw === 'instagram' ||
    raw === 'ig' ||
    raw === 'reels' ||
    raw.includes('reel') ||
    raw.includes('insta')
  ) {
    return 'instagram_reels'
  }
  if (raw === 'youtube' || raw === 'yt' || raw.includes('you')) return 'youtube'
  return fallback
}

export const getPlatformEditProfile = (value?: unknown): PlatformEditProfile => {
  const key = parsePlatformProfile(value)
  return PLATFORM_EDIT_PROFILES[key] || PLATFORM_EDIT_PROFILES.auto
}

export const normalizeProfileSubtitlePreset = (
  value?: string | null
): SubtitlePresetId => {
  return normalizeSubtitlePreset(value) || DEFAULT_SUBTITLE_PRESET
}
