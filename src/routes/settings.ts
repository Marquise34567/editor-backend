import express from 'express'
import { prisma } from '../db/prisma'
import { clampQualityForTier, normalizeQuality } from '../lib/gating'
import { createCheckoutUrlForUser } from '../services/billing'
import { getOrCreateUser } from '../services/users'
import { getUserPlan } from '../services/plans'

const router = express.Router()

router.get('/', async (req: any, res) => {
  try {
    const userId = req.user.id
    await getOrCreateUser(userId, req.user?.email)
    const { tier, plan } = await getUserPlan(userId)
    const settings = await prisma.userSettings.findUnique({ where: { userId } })
    const normalizedQuality = clampQualityForTier(normalizeQuality(settings?.exportQuality), tier)
    const enforced = {
      userId,
      watermarkEnabled: plan.watermark,
      exportQuality: normalizedQuality,
      autoCaptions: settings?.autoCaptions ?? false,
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
    const user = await getOrCreateUser(userId, req.user?.email)
    const { tier, plan } = await getUserPlan(userId)
    const existing = await prisma.userSettings.findUnique({ where: { userId } })
    const requestedQuality = payload.exportQuality ? normalizeQuality(payload.exportQuality) : normalizeQuality(existing?.exportQuality)
    if (plan.watermark && payload.watermarkEnabled === false) {
      const checkoutUrl = await createCheckoutUrlForUser(userId, 'starter', user.email).catch(() => null)
      return res.status(402).json({ error: 'payment_required', message: 'Upgrade to remove watermark', feature: 'watermark', checkoutUrl })
    }
    if (payload.exportQuality && requestedQuality !== clampQualityForTier(requestedQuality, tier)) {
      const checkoutUrl = await createCheckoutUrlForUser(userId, 'starter', user.email).catch(() => null)
      return res.status(402).json({ error: 'payment_required', message: 'Upgrade to export higher quality', feature: 'quality', checkoutUrl })
    }
    const sanitized = {
      watermarkEnabled: plan.watermark,
      exportQuality: clampQualityForTier(requestedQuality, tier),
      autoCaptions: payload.autoCaptions ?? existing?.autoCaptions ?? false,
    }
    const updated = await prisma.userSettings.upsert({ where: { userId }, create: { userId, ...sanitized }, update: sanitized })
    res.json({ settings: updated })
  } catch (err) {
    console.error('save settings', err)
    res.status(500).json({ error: 'server_error' })
  }
})

export default router
