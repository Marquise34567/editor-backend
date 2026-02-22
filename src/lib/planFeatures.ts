import { PLAN_CONFIG, QUALITY_ORDER, type ExportQuality, type PlanTier } from '../shared/planConfig'
import { normalizeSubtitlePreset, type SubtitlePresetId } from '../shared/subtitlePresets'

export type PlanFeatures = {
  tier: PlanTier
  resolution: '720p' | '1080p' | '4K'
  maxResolution: ExportQuality
  rendersPerMonth: number | null
  maxRendersPerMonth: number | null
  watermark: boolean
  queuePriority: 'priority' | 'standard'
  priorityQueue: boolean
  subtitleAccess: 'all' | 'limited' | 'none'
  subtitles: {
    enabled: boolean
    allowedPresets: SubtitlePresetId[] | 'ALL'
  }
  autoZoomMax: number
  advancedEffects: boolean
  lifetime: boolean
  includesFutureFeatures: boolean
}

export const getPlanFeatures = (tier: PlanTier): PlanFeatures => {
  const plan = PLAN_CONFIG[tier] ?? PLAN_CONFIG.free
  const resolution = plan.exportQuality === '4k' ? '4K' : plan.exportQuality
  const allowedPresets = plan.allowedSubtitlePresets
  const subtitlesEnabled = allowedPresets === 'ALL' ? true : allowedPresets.length > 0
  const subtitleAccess = allowedPresets === 'ALL' ? 'all' : subtitlesEnabled ? 'limited' : 'none'
  return {
    tier,
    resolution,
    maxResolution: plan.exportQuality,
    rendersPerMonth: plan.maxRendersPerMonth,
    maxRendersPerMonth: plan.maxRendersPerMonth,
    watermark: plan.watermark,
    queuePriority: plan.priority ? 'priority' : 'standard',
    priorityQueue: plan.priority,
    subtitleAccess,
    subtitles: {
      enabled: subtitlesEnabled,
      allowedPresets
    },
    autoZoomMax: plan.autoZoomMax,
    advancedEffects: plan.advancedEffects,
    lifetime: plan.lifetime,
    includesFutureFeatures: plan.includesFutureFeatures
  }
}

export const isSubtitlePresetAllowed = (preset?: string | null, tier: PlanTier = 'free') => {
  if (!preset) return true
  const normalized = normalizeSubtitlePreset(preset)
  if (!normalized) return false
  const features = getPlanFeatures(tier)
  if (!features.subtitles.enabled) return false
  if (features.subtitles.allowedPresets === 'ALL') return true
  return features.subtitles.allowedPresets.includes(normalized)
}

export const getRequiredPlanForQuality = (quality: ExportQuality): PlanTier => {
  const maxIndex = QUALITY_ORDER.indexOf(quality)
  if (maxIndex <= QUALITY_ORDER.indexOf('720p')) return 'free'
  if (maxIndex <= QUALITY_ORDER.indexOf('1080p')) return 'starter'
  return 'creator'
}

export const getRequiredPlanForAutoZoom = (value: number): PlanTier => {
  if (value <= 1.1) return 'free'
  if (value <= 1.12) return 'starter'
  if (value <= 1.15) return 'creator'
  if (value <= 1.2) return 'studio'
  return 'studio'
}

export const getRequiredPlanForSubtitlePreset = (preset?: string | null): PlanTier => {
  if (!preset) return 'free'
  const normalized = normalizeSubtitlePreset(preset)
  if (!normalized) return 'creator'
  const upgradeOrder: PlanTier[] = ['free', 'starter', 'creator', 'studio']
  for (const tier of upgradeOrder) {
    const allowed = PLAN_CONFIG[tier]?.allowedSubtitlePresets ?? PLAN_CONFIG.free.allowedSubtitlePresets
    if (allowed === 'ALL' || allowed.includes(normalized)) return tier
  }
  return 'studio'
}

export const getRequiredPlanForAdvancedEffects = (): PlanTier => 'studio'

export const getRequiredPlanForRenders = (currentTier: PlanTier): PlanTier => {
  const upgradeOrder: PlanTier[] = ['free', 'starter', 'creator', 'studio']
  const idx = upgradeOrder.indexOf(currentTier)
  if (idx === -1 || idx >= upgradeOrder.length - 1) return 'studio'
  return upgradeOrder[idx + 1]
}
