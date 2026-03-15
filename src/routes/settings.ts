import express from 'express'
import { isStubDb, prisma } from '../db/prisma'
import { clampQualityForTier, normalizeQuality, type ExportQuality, type PlanTier } from '../lib/gating'
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
import { resolveDevAdminAccess } from '../lib/devAccounts'
import { getCaptionEngineStatus } from '../lib/captionEngine'
import {
  disableDailyPushSubscription,
  getDailyEngagementStatusForUser,
  updateDailyEngagementPreferences,
  upsertDailyPushSubscription
} from '../services/dailyEngagement'

const router = express.Router()
const CAPTIONS_PIPELINE_ENABLED = (() => {
  const raw = String(process.env.CAPTIONS_PIPELINE_ENABLED ?? 'true').trim().toLowerCase()
  if (!raw) return true
  if (['0', 'false', 'no', 'off', 'disabled'].includes(raw)) return false
  return true
})()

const DEFAULT_SETTINGS = {
  userId: null,
  watermarkEnabled: true,
  exportQuality: '720p',
  autoCaptions: false,
  autoHookMove: true,
  removeBoring: true,
  onlyCuts: false,
  smartZoom: true,
  jumpCuts: true,
  transitions: true,
  soundFx: true,
  emotionalBoost: true,
  musicDuck: true,
  subtitleStyle: DEFAULT_SUBTITLE_PRESET,
  autoZoomMax: 1.1
}

const resolveDefaultExportQuality = (tier: PlanTier): ExportQuality => {
  const baseline: ExportQuality = tier === 'free' ? '720p' : '1080p'
  return clampQualityForTier(baseline, tier)
}

const coerceAutoZoomMax = (value: any, maxValue: number) => {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return maxValue
  const clamped = Math.max(1, Math.min(parsed, maxValue))
  return Math.round(clamped * 100) / 100
}

const parseOptionalBoolean = (value: unknown): boolean | null => {
  if (typeof value === 'boolean') return value
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase()
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true
    if (['false', '0', 'no', 'off'].includes(normalized)) return false
  }
  return null
}

const sendPlanLimit = (res: any, requiredPlan: string, feature: string, message: string) => {
  return res.status(403).json({ error: 'PLAN_LIMIT_EXCEEDED', requiredPlan, feature, message })
}

const getCaptionCapabilities = () => {
  if (!CAPTIONS_PIPELINE_ENABLED) {
    return {
      captions: {
        available: false,
        provider: null,
        mode: 'disabled',
        reason: 'Captions are disabled in the editor pipeline.'
      }
    }
  }
  const status = getCaptionEngineStatus()
  return {
    captions: {
      available: status.available,
      provider: status.provider,
      mode: status.mode,
      reason: status.reason
    }
  }
}

router.get('/', async (req: any, res) => {
  try {
    // Require full env only in production with a real DB. Stub mode should stay usable in local dev.
    if (process.env.NODE_ENV === 'production' && !isStubDb()) {
      const requiredEnvs = ['DATABASE_URL', 'SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY']
      const missing = requiredEnvs.filter((k) => !process.env[k])
      if (missing.length > 0) {
        return res.status(500).json({ error: 'misconfigured', message: 'Missing env vars', missing })
      }
    }

    const userId = req.user?.id
    const capabilities = getCaptionCapabilities()
    // If not authenticated, return safe defaults (do not 500)
    if (!userId) return res.status(200).json({ settings: DEFAULT_SETTINGS, capabilities, dailyEngagement: null })

    await getOrCreateUser(userId, req.user?.email)
    const dailyEngagement = await getDailyEngagementStatusForUser({
      userId,
      email: req.user?.email
    })
    const { tier } = await getUserPlan(userId)
    const devAccess = await resolveDevAdminAccess(userId, req.user?.email)
    const effectiveTier = devAccess.emailAuthorized ? 'studio' : tier
    const features = getPlanFeatures(effectiveTier)
    const subtitlesEnabled = features.subtitles.enabled
    let settings = await prisma.userSettings.findUnique({ where: { userId } })

    // If settings missing in DB, create with defaults suitable for this tier
    if (!settings) {
      const created = {
        userId,
        watermarkEnabled: features.watermark,
        exportQuality: resolveDefaultExportQuality(effectiveTier),
        autoCaptions: subtitlesEnabled ? false : false,
        autoHookMove: true,
        removeBoring: true,
        onlyCuts: false,
        smartZoom: true,
        jumpCuts: true,
        transitions: true,
        soundFx: true,
        emotionalBoost: features.advancedEffects ? true : false,
        musicDuck: true,
        subtitleStyle: DEFAULT_SUBTITLE_PRESET,
        autoZoomMax: features.autoZoomMax ?? 1.1
      }
      try {
        settings = await prisma.userSettings.upsert({ where: { userId }, create: created as any, update: created as any })
      } catch (e) {
        // If DB write fails, fall back to defaults in-memory
        console.warn('failed to create settings, falling back to defaults', e)
        return res.status(200).json({ settings: { ...DEFAULT_SETTINGS, userId }, capabilities, dailyEngagement })
      }
    }

    const normalizedQuality = clampQualityForTier(normalizeQuality(settings?.exportQuality), effectiveTier)
    const rawSubtitle = settings?.subtitleStyle ?? DEFAULT_SUBTITLE_PRESET
    const normalizedSubtitle = normalizeSubtitlePreset(rawSubtitle) ?? DEFAULT_SUBTITLE_PRESET
    const enforcedSubtitle =
      subtitlesEnabled && isSubtitlePresetAllowed(normalizedSubtitle, effectiveTier) ? rawSubtitle : DEFAULT_SUBTITLE_PRESET
    const enforcedAutoZoomMax = coerceAutoZoomMax(settings?.autoZoomMax ?? features.autoZoomMax, features.autoZoomMax)
    const enforced = {
      userId,
      watermarkEnabled: features.watermark,
      exportQuality: normalizedQuality,
      autoCaptions: false,
      autoHookMove: settings?.autoHookMove ?? true,
      removeBoring: settings?.removeBoring ?? true,
      onlyCuts: settings?.onlyCuts ?? false,
      smartZoom: settings?.smartZoom ?? true,
      jumpCuts: settings?.jumpCuts ?? true,
      transitions: settings?.transitions ?? true,
      soundFx: settings?.soundFx ?? true,
      emotionalBoost: features.advancedEffects ? (settings?.emotionalBoost ?? true) : false,
      musicDuck: settings?.musicDuck ?? true,
      subtitleStyle: enforcedSubtitle,
      autoZoomMax: enforcedAutoZoomMax
    }
    res.json({ settings: enforced, capabilities, dailyEngagement })
  } catch (err: any) {
    // Log full stack to Railway logs for diagnosis (do not log auth tokens)
    console.error('get settings error', err?.stack || err)
    const message = err?.message || String(err) || 'Unknown error'
    res.status(500).json({ error: 'server_error', message, path: '/api/settings' })
  }
})

router.patch('/', async (req: any, res) => {
  try {
    const userId = req.user.id
    const payload = req.body || {}
    const capabilities = getCaptionCapabilities()
    await getOrCreateUser(userId, req.user?.email)
    const { tier } = await getUserPlan(userId)
    const devAccess = await resolveDevAdminAccess(userId, req.user?.email)
    const effectiveTier = devAccess.emailAuthorized ? 'studio' : tier
    const features = getPlanFeatures(effectiveTier)
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
    if (payload.exportQuality && requestedQuality !== clampQualityForTier(requestedQuality, effectiveTier)) {
      const requiredPlan = getRequiredPlanForQuality(requestedQuality)
      return sendPlanLimit(res, requiredPlan, 'quality', 'Upgrade to export higher quality')
    }
    if (payload.subtitleStyle && !requestedSubtitle) {
      return res.status(400).json({ error: 'invalid_subtitle_preset' })
    }
    if (subtitlesEnabled && payload.subtitleStyle && !isSubtitlePresetAllowed(requestedSubtitle, effectiveTier)) {
      const requiredPlan = getRequiredPlanForSubtitlePreset(requestedSubtitle)
      return sendPlanLimit(res, requiredPlan, 'subtitles', 'Upgrade to unlock subtitle styles')
    }
    if (payload.autoZoomMax && Number(payload.autoZoomMax) > features.autoZoomMax) {
      const requiredPlan = getRequiredPlanForAutoZoom(Number(payload.autoZoomMax))
      return sendPlanLimit(res, requiredPlan, 'autoZoomMax', 'Upgrade to unlock higher auto zoom limits')
    }
    const wantsAdvancedEffects = Boolean(payload.emotionalBoost)
    if (wantsAdvancedEffects && !features.advancedEffects) {
      const requiredPlan = getRequiredPlanForAdvancedEffects()
      return sendPlanLimit(res, requiredPlan, 'advancedEffects', 'Upgrade to unlock advanced effects')
    }

    const nextAutoZoom = payload.autoZoomMax ?? existing?.autoZoomMax ?? features.autoZoomMax
    const sanitizedAutoZoom = coerceAutoZoomMax(nextAutoZoom, features.autoZoomMax)
    const sanitized = {
      watermarkEnabled: features.watermark,
      exportQuality: clampQualityForTier(requestedQuality, effectiveTier),
      autoCaptions: false,
      autoHookMove: payload.autoHookMove ?? existing?.autoHookMove ?? true,
      removeBoring: payload.removeBoring ?? existing?.removeBoring ?? true,
      onlyCuts: payload.onlyCuts ?? existing?.onlyCuts ?? false,
      smartZoom: payload.smartZoom ?? existing?.smartZoom ?? true,
      jumpCuts: payload.jumpCuts ?? existing?.jumpCuts ?? true,
      transitions: payload.transitions ?? existing?.transitions ?? true,
      soundFx: payload.soundFx ?? existing?.soundFx ?? true,
      emotionalBoost: features.advancedEffects ? (payload.emotionalBoost ?? existing?.emotionalBoost ?? true) : false,
      musicDuck: payload.musicDuck ?? existing?.musicDuck ?? true,
      subtitleStyle: subtitlesEnabled ? (payload.subtitleStyle ?? existingSubtitle) : DEFAULT_SUBTITLE_PRESET,
      autoZoomMax: sanitizedAutoZoom
    }
    const updated = await prisma.userSettings.upsert({ where: { userId }, create: { userId, ...sanitized }, update: sanitized })
    const dailyEngagement = await getDailyEngagementStatusForUser({
      userId,
      email: req.user?.email
    })
    res.json({
      settings: {
        ...updated,
        autoCaptions: Boolean(updated?.autoCaptions)
      },
      capabilities,
      dailyEngagement
    })
  } catch (err: any) {
    console.error('save settings error', err?.stack || err)
    const message = err?.message || String(err) || 'Unknown error'
    res.status(500).json({ error: 'server_error', message, path: '/api/settings' })
  }
})

router.post('/engagement/preferences', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    await getOrCreateUser(userId, req.user?.email)

    const current = await getDailyEngagementStatusForUser({
      userId,
      email: req.user?.email
    })
    const requestedEnabled = parseOptionalBoolean(req.body?.enabled)
    const requestedEmailEnabled = parseOptionalBoolean(req.body?.emailEnabled)
    const emailEnabled = requestedEmailEnabled ?? current.emailEnabled
    const channelsEnabled = Boolean(emailEnabled || current.pushEnabled)
    const enabled = Boolean((requestedEnabled ?? channelsEnabled) && channelsEnabled)

    await updateDailyEngagementPreferences({
      userId,
      email: req.user?.email,
      enabled,
      emailEnabled
    })

    const dailyEngagement = await getDailyEngagementStatusForUser({
      userId,
      email: req.user?.email
    })
    return res.json({ ok: true, dailyEngagement })
  } catch (err: any) {
    console.error('save daily engagement preferences error', err?.stack || err)
    const message = err?.message || String(err) || 'Unknown error'
    return res.status(500).json({ error: 'server_error', message, path: '/api/settings/engagement/preferences' })
  }
})

router.post('/engagement/push-subscription', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    await getOrCreateUser(userId, req.user?.email)

    const current = await getDailyEngagementStatusForUser({
      userId,
      email: req.user?.email
    })
    if (!current.provider?.webPushConfigured) {
      return res.status(400).json({
        error: 'daily_engagement_push_provider_not_configured',
        message: 'Configure WEB_PUSH_VAPID_SUBJECT, WEB_PUSH_VAPID_PUBLIC_KEY, and WEB_PUSH_VAPID_PRIVATE_KEY first.'
      })
    }

    const endpoint = String(req.body?.endpoint || '').trim()
    const p256dh = String(req.body?.p256dh || req.body?.keys?.p256dh || '').trim()
    const auth = String(req.body?.auth || req.body?.keys?.auth || '').trim()
    if (!endpoint || !p256dh || !auth) {
      return res.status(400).json({
        error: 'invalid_push_subscription',
        message: 'Missing endpoint, p256dh, or auth.'
      })
    }

    await upsertDailyPushSubscription({
      userId,
      email: req.user?.email,
      endpoint,
      p256dh,
      auth
    })
    const dailyEngagement = await getDailyEngagementStatusForUser({
      userId,
      email: req.user?.email
    })
    return res.json({ ok: true, dailyEngagement })
  } catch (err: any) {
    console.error('save daily push subscription error', err?.stack || err)
    const message = err?.message || String(err) || 'Unknown error'
    return res.status(500).json({ error: 'server_error', message, path: '/api/settings/engagement/push-subscription' })
  }
})

router.delete('/engagement/push-subscription', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    await getOrCreateUser(userId, req.user?.email)

    await disableDailyPushSubscription({
      userId,
      email: req.user?.email
    })
    const dailyEngagement = await getDailyEngagementStatusForUser({
      userId,
      email: req.user?.email
    })
    return res.json({ ok: true, dailyEngagement })
  } catch (err: any) {
    console.error('disable daily push subscription error', err?.stack || err)
    const message = err?.message || String(err) || 'Unknown error'
    return res.status(500).json({ error: 'server_error', message, path: '/api/settings/engagement/push-subscription' })
  }
})

export default router
