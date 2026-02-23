import express from 'express'
import { getOrCreateUser } from '../services/users'
import { getUserPlan } from '../services/plans'
import { getUsageForMonth } from '../services/usage'
import { getRenderAttemptsForDay } from '../services/dailyRenderUsage'
import { getMonthKey } from '../shared/planConfig'
import { getPlanFeatures } from '../lib/planFeatures'
import { SUBTITLE_PRESET_REGISTRY } from '../shared/subtitlePresets'

const router = express.Router()
const DEV_ACCOUNT_EMAILS = (process.env.DEV_ACCOUNT_EMAILS || process.env.DEV_ACCOUNT_EMAIL || '')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean)
const DEV_ACCOUNT_USER_IDS = (process.env.DEV_ACCOUNT_USER_IDS || '')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean)
router.get('/', async (req: any, res) => {
  const id = req.user?.id
  if (!id) return res.status(401).json({ error: 'unauthenticated' })
  const user = await getOrCreateUser(id, req.user?.email)
  const { subscription, tier, plan } = await getUserPlan(id)
  const month = getMonthKey()
  const usage = await getUsageForMonth(id, month)
  const dailyUsage = tier === 'free' ? await getRenderAttemptsForDay(id) : null
  const email = String(user.email || '').toLowerCase()
  const isDev = Boolean(
    (email && DEV_ACCOUNT_EMAILS.includes(email)) ||
      (DEV_ACCOUNT_USER_IDS.length && DEV_ACCOUNT_USER_IDS.includes(user.id))
  )
  res.json({
    user: { id: user.id, email: user.email, createdAt: user.createdAt },
    subscription: subscription
      ? {
          tier,
          status: subscription.status,
          currentPeriodEnd: subscription.currentPeriodEnd,
          cancelAtPeriodEnd: subscription.cancelAtPeriodEnd
        }
      : { tier: 'free', status: 'free', currentPeriodEnd: null, cancelAtPeriodEnd: false },
    flags: { dev: isDev },
    usage: {
      month,
      rendersUsed: usage?.rendersUsed ?? 0,
      minutesUsed: usage?.minutesUsed ?? 0
    },
    usageDaily: dailyUsage
      ? {
          day: dailyUsage.dayKey,
          rendersUsed: dailyUsage.rendersCount,
          rendersLimit: 1
        }
      : null,
    limits: {
      maxRendersPerMonth: tier === 'free' ? null : plan.maxRendersPerMonth,
      maxRendersPerDay: tier === 'free' ? 1 : null,
      maxMinutesPerMonth: plan.maxMinutesPerMonth,
      exportQuality: plan.exportQuality,
      watermark: plan.watermark,
      priority: plan.priority
    }
  })
})

router.get('/plan', async (req: any, res) => {
  const id = req.user?.id
  if (!id) return res.status(401).json({ error: 'unauthenticated' })
  const { subscription, tier } = await getUserPlan(id)
  res.json({
    tier,
    status: subscription?.status ?? 'free',
    currentPeriodEnd: subscription?.currentPeriodEnd ?? null,
    stripeCustomerId: subscription?.stripeCustomerId ?? null,
    stripeSubscriptionId: subscription?.stripeSubscriptionId ?? null,
    priceId: subscription?.priceId ?? null
  })
})

router.get('/subscription', async (req: any, res) => {
  const id = req.user?.id
  if (!id) return res.status(401).json({ error: 'unauthenticated' })
  const { subscription, tier } = await getUserPlan(id)
  const features = getPlanFeatures(tier)
  res.json({
    plan: tier,
    status: subscription?.status ?? 'free',
    currentPeriodEnd: subscription?.currentPeriodEnd ?? null,
    features,
    subtitlePresets: SUBTITLE_PRESET_REGISTRY
  })
})

export default router
