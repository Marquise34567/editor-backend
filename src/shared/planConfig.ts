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
    badge: null,
    features: [
      '720p exports',
      '12 renders / month',
      '10 min videos',
      'Subtle watermark',
      'Standard queue'
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
    badge: null,
    features: [
      '1080p exports',
      '20 renders / month',
      '30 min videos',
      'No watermark',
      'Standard queue'
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
    badge: 'popular',
    features: [
      '4K exports',
      '100 renders / month',
      '120 min videos',
      'No watermark',
      'Standard queue'
    ]
  },
  studio: {
    tier: 'studio',
    name: 'Studio',
    priceMonthly: 99,
    priceLabel: '$99',
    description: 'For studios that need priority and scale.',
    maxRendersPerMonth: null,
    maxMinutesPerMonth: 999,
    exportQuality: '4k',
    watermark: false,
    priority: true,
    badge: null,
    features: [
      '4K exports',
      'Unlimited renders',
      '999 min videos',
      'No watermark',
      'Priority queue'
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

export const getMonthKey = (date: Date = new Date()) => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  return `${year}-${month}`
}
