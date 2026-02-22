import type { SubtitlePresetId } from './subtitlePresets'

export type PlanTier = 'free' | 'starter' | 'creator' | 'studio'
export type ExportQuality = '720p' | '1080p' | '4k'

export const PLAN_TIERS: PlanTier[] = ['free', 'starter', 'creator', 'studio']
export const QUALITY_ORDER: ExportQuality[] = ['720p', '1080p', '4k']

export type PlanConfig = {
  tier: PlanTier
  name: string
  priceMonthly: number
  priceLabel: string
  description: string
  maxRendersPerMonth: number | null
  maxMinutesPerMonth: number | null
  exportQuality: ExportQuality
  watermark: boolean
  priority: boolean
  allowedSubtitlePresets: SubtitlePresetId[] | 'ALL'
  autoZoomMax: number
  advancedEffects: boolean
  lifetime: boolean
  includesFutureFeatures: boolean
  badge?: 'popular' | null
  features: string[]
}

export const PLAN_CONFIG: Record<PlanTier, PlanConfig> = {
  free: {
    tier: 'free',
    name: 'Free',
    priceMonthly: 0,
    priceLabel: '$0',
    description: 'For trying AutoEditor on small projects.',
    maxRendersPerMonth: 12,
    maxMinutesPerMonth: 10,
    exportQuality: '720p',
    watermark: true,
    priority: false,
    allowedSubtitlePresets: ['basic_clean'],
    autoZoomMax: 1.1,
    advancedEffects: false,
    lifetime: false,
    includesFutureFeatures: false,
    badge: null,
    features: [
      '720p exports',
      '12 renders / month',
      'Watermark',
      'Standard queue',
      'Subtitles: 1 preset',
      'Auto zoom max 1.10'
    ]
  },
  starter: {
    tier: 'starter',
    name: 'Starter',
    priceMonthly: 9,
    priceLabel: '$9',
    description: 'For creators publishing regularly.',
    maxRendersPerMonth: 20,
    maxMinutesPerMonth: 30,
    exportQuality: '1080p',
    watermark: false,
    priority: false,
    allowedSubtitlePresets: ['basic_clean', 'bold_pop', 'caption_box'],
    autoZoomMax: 1.12,
    advancedEffects: false,
    lifetime: false,
    includesFutureFeatures: false,
    badge: null,
    features: [
      '1080p exports',
      '20 renders / month',
      'No watermark',
      'Subtitles: 3 presets',
      'Auto zoom max 1.12'
    ]
  },
  creator: {
    tier: 'creator',
    name: 'Creator',
    priceMonthly: 29,
    priceLabel: '$29',
    description: 'For teams shipping content at scale.',
    maxRendersPerMonth: 100,
    maxMinutesPerMonth: 120,
    exportQuality: '4k',
    watermark: false,
    priority: false,
    allowedSubtitlePresets: 'ALL',
    autoZoomMax: 1.15,
    advancedEffects: false,
    lifetime: false,
    includesFutureFeatures: false,
    badge: 'popular',
    features: [
      '4K exports',
      '100 renders / month',
      'No watermark',
      'Subtitles: All presets',
      'Karaoke highlight',
      'Auto zoom max 1.15'
    ]
  },
  studio: {
    tier: 'studio',
    name: 'Studio',
    priceMonthly: 99,
    priceLabel: '$99',
    description: 'For studios that need priority and scale.',
    maxRendersPerMonth: null,
    maxMinutesPerMonth: null,
    exportQuality: '4k',
    watermark: false,
    priority: true,
    allowedSubtitlePresets: 'ALL',
    autoZoomMax: 1.2,
    advancedEffects: true,
    lifetime: false,
    includesFutureFeatures: false,
    badge: null,
    features: [
      '4K exports',
      'Unlimited renders',
      'Priority queue',
      'All subtitle styles',
      'Full zoom control up to 1.20',
      'Advanced effects'
    ]
  }
}

export const isPaidTier = (tier: PlanTier) => tier !== 'free'

export const normalizeQuality = (value?: string): ExportQuality => {
  const raw = (value || '').toLowerCase()
  if (raw.includes('4k') || raw.includes('2160') || raw.includes('uhd') || raw.includes('high')) return '4k'
  if (raw.includes('1080') || raw.includes('full') || raw.includes('medium')) return '1080p'
  if (raw.includes('720') || raw.includes('hd') || raw.includes('low')) return '720p'
  return '720p'
}

export const clampQualityForTier = (quality: ExportQuality, tier: PlanTier): ExportQuality => {
  const maxQuality = PLAN_CONFIG[tier]?.exportQuality || '720p'
  const qualityIndex = QUALITY_ORDER.indexOf(quality)
  const maxIndex = QUALITY_ORDER.indexOf(maxQuality)
  if (qualityIndex === -1) return maxQuality
  return qualityIndex <= maxIndex ? quality : maxQuality
}

export const qualityToHeight = (quality: ExportQuality) => {
  if (quality === '4k') return 2160
  if (quality === '1080p') return 1080
  return 720
}

export type PlanFeatures = {
  resolution: '720p' | '1080p' | '4K'
  watermark: boolean
  subtitles: {
    enabled: boolean
    allowedPresets: SubtitlePresetId[] | 'ALL'
  }
  autoZoomMax: number
  queuePriority: 'priority' | 'standard'
  rendersPerMonth: number | null
  lifetime: boolean
  includesFutureFeatures: boolean
}

export const getPlanFeatures = (plan: PlanConfig): PlanFeatures => {
  const resolution = plan.exportQuality === '4k' ? '4K' : plan.exportQuality
  const allowedPresets = plan.allowedSubtitlePresets
  const subtitlesEnabled = allowedPresets === 'ALL' ? true : allowedPresets.length > 0
  return {
    resolution,
    watermark: plan.watermark,
    subtitles: {
      enabled: subtitlesEnabled,
      allowedPresets
    },
    autoZoomMax: plan.autoZoomMax,
    queuePriority: plan.priority ? 'priority' : 'standard',
    rendersPerMonth: plan.maxRendersPerMonth,
    lifetime: plan.lifetime,
    includesFutureFeatures: plan.includesFutureFeatures
  }
}

export const getMonthKey = (date: Date = new Date()) => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  return `${year}-${month}`
}
