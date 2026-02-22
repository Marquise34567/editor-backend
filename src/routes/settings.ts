import express from 'express'
import { prisma } from '../db/prisma'
import { clampQualityForTier, normalizeQuality } from '../lib/gating'
import { getOrCreateUser } from '../services/users'
import { getUserPlan } from '../services/plans'
import {
  getPlanFeatures,
  getRequiredPlanForAdvancedEffects,
  getRequiredPlanForAutoZoom,
  getRequiredPlanForQuality,
  getRequiredPlanForSubtitlePreset,
  isSubtitlePresetAllowed
} from '../lib/planFeatures'
import { DEFAULT_SUBTITLE_PRESET, normalizeSubtitlePreset } from '../shared/subtitlePresets'

const router = express.Router()

const coerceAutoZoomMax = (value: any, maxValue: number) => {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return maxValue
  const clamped = Math.max(1, Math.min(parsed, maxValue))
  return Math.round(clamped * 100) / 100
}

const sendPlanLimit = (res: any, requiredPlan: string, feature: string, message: string) => {
  return res.status(403).json({ error: 'PLAN_LIMIT_EXCEEDED', requiredPlan, feature, message })
}

router.get('/', async (req: any, res) => {
  try {
    const userId = req.user.id
    await getOrCreateUser(userId, req.user?.email)
    const { tier } = await getUserPlan(userId)
    const features = getPlanFeatures(tier)
    const subtitlesEnabled = features.subtitles.enabled
    const settings = await prisma.userSettings.findUnique({ where: { userId } })
    const normalizedQuality = clampQualityForTier(normalizeQuality(settings?.exportQuality), tier)
    const rawSubtitle = settings?.subtitleStyle ?? DEFAULT_SUBTITLE_PRESET
    const normalizedSubtitle = normalizeSubtitlePreset(rawSubtitle) ?? DEFAULT_SUBTITLE_PRESET
    const enforcedSubtitle =
      subtitlesEnabled && isSubtitlePresetAllowed(normalizedSubtitle, tier) ? rawSubtitle : DEFAULT_SUBTITLE_PRESET
    const enforcedAutoZoomMax = coerceAutoZoomMax(settings?.autoZoomMax ?? features.autoZoomMax, features.autoZoomMax)
    const enforced = {
      userId,
      watermarkEnabled: features.watermark,
      exportQuality: normalizedQuality,
      autoCaptions: subtitlesEnabled ? (settings?.autoCaptions ?? false) : false,
      autoHookMove: settings?.autoHookMove ?? true,
      removeBoring: settings?.removeBoring ?? true,
      smartZoom: settings?.smartZoom ?? true,
      emotionalBoost: features.advancedEffects ? (settings?.emotionalBoost ?? true) : false,
      musicDuck: settings?.musicDuck ?? true,
      aggressiveMode: features.advancedEffects ? (settings?.aggressiveMode ?? false) : false,
      subtitleStyle: enforcedSubtitle,
      autoZoomMax: enforcedAutoZoomMax
    }
    res.json({ settings: enforced })
  } catch (err) {
    console.error('get settings', err)
    res.status(500).json({ error: 'server_error' })
  }
})

router.patch('/', async (req: any, res) => {
  try {
    const userId = req.user.id
    const payload = req.body || {}
    await getOrCreateUser(userId, req.user?.email)
    const { tier } = await getUserPlan(userId)
    const features = getPlanFeatures(tier)
    const subtitlesEnabled = features.subtitles.enabled
    const existing = await prisma.userSettings.findUnique({ where: { userId } })
    const requestedQuality = payload.exportQuality ? normalizeQuality(payload.exportQuality) : normalizeQuality(existing?.exportQuality)
    const existingSubtitle = existing?.subtitleStyle ?? DEFAULT_SUBTITLE_PRESET
    const requestedSubtitle = payload.subtitleStyle ? normalizeSubtitlePreset(payload.subtitleStyle) : null
    if (!subtitlesEnabled && (payload.autoCaptions === true || payload.subtitleStyle)) {
      return sendPlanLimit(res, 'creator', 'subtitles', 'Subtitles are temporarily disabled.')
    }
    if (features.watermark && payload.watermarkEnabled === false) {
      return sendPlanLimit(res, 'starter', 'watermark', 'Upgrade to remove watermark')
    }
    if (payload.exportQuality && requestedQuality !== clampQualityForTier(requestedQuality, tier)) {
      const requiredPlan = getRequiredPlanForQuality(requestedQuality)
      return sendPlanLimit(res, requiredPlan, 'quality', 'Upgrade to export higher quality')
    }
    if (payload.subtitleStyle && !requestedSubtitle) {
      return res.status(400).json({ error: 'invalid_subtitle_preset' })
    }
    if (subtitlesEnabled && payload.subtitleStyle && !isSubtitlePresetAllowed(requestedSubtitle, tier)) {
      const requiredPlan = getRequiredPlanForSubtitlePreset(requestedSubtitle)
      return sendPlanLimit(res, requiredPlan, 'subtitles', 'Upgrade to unlock subtitle styles')
    }
    if (payload.autoZoomMax && Number(payload.autoZoomMax) > features.autoZoomMax) {
      const requiredPlan = getRequiredPlanForAutoZoom(Number(payload.autoZoomMax))
      return sendPlanLimit(res, requiredPlan, 'autoZoomMax', 'Upgrade to unlock higher auto zoom limits')
    }
    const wantsAdvancedEffects = Boolean(payload.emotionalBoost) || Boolean(payload.aggressiveMode)
    if (wantsAdvancedEffects && !features.advancedEffects) {
      const requiredPlan = getRequiredPlanForAdvancedEffects()
      return sendPlanLimit(res, requiredPlan, 'advancedEffects', 'Upgrade to unlock advanced effects')
    }

    const nextAutoZoom = payload.autoZoomMax ?? existing?.autoZoomMax ?? features.autoZoomMax
    const sanitizedAutoZoom = coerceAutoZoomMax(nextAutoZoom, features.autoZoomMax)
    const sanitized = {
      watermarkEnabled: features.watermark,
      exportQuality: clampQualityForTier(requestedQuality, tier),
      autoCaptions: subtitlesEnabled ? (payload.autoCaptions ?? existing?.autoCaptions ?? false) : false,
      autoHookMove: payload.autoHookMove ?? existing?.autoHookMove ?? true,
      removeBoring: payload.removeBoring ?? existing?.removeBoring ?? true,
      smartZoom: payload.smartZoom ?? existing?.smartZoom ?? true,
      emotionalBoost: features.advancedEffects ? (payload.emotionalBoost ?? existing?.emotionalBoost ?? true) : false,
      musicDuck: payload.musicDuck ?? existing?.musicDuck ?? true,
      aggressiveMode: features.advancedEffects ? (payload.aggressiveMode ?? existing?.aggressiveMode ?? false) : false,
      subtitleStyle: subtitlesEnabled ? (payload.subtitleStyle ?? existingSubtitle) : DEFAULT_SUBTITLE_PRESET,
      autoZoomMax: sanitizedAutoZoom
    }
    const updated = await prisma.userSettings.upsert({ where: { userId }, create: { userId, ...sanitized }, update: sanitized })
    res.json({ settings: updated })
  } catch (err) {
    console.error('save settings', err)
    res.status(500).json({ error: 'server_error' })
  }
})

export default router
