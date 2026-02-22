import express from 'express'
import { getOrCreateUser } from '../services/users'
import { getUserPlan } from '../services/plans'
import { getUsageForMonth } from '../services/usage'
import { getMonthKey } from '../shared/planConfig'
import { getPlanFeatures } from '../lib/planFeatures'
import { SUBTITLE_PRESET_REGISTRY } from '../shared/subtitlePresets'

const router = express.Router()
router.get('/', async (req: any, res) => {
  const id = req.user?.id
  if (!id) return res.status(401).json({ error: 'unauthenticated' })
  const user = await getOrCreateUser(id, req.user?.email)
  const { subscription, tier, plan } = await getUserPlan(id)
  const month = getMonthKey()
  const usage = await getUsageForMonth(id, month)
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
    usage: {
      month,
      rendersUsed: usage?.rendersUsed ?? 0,
      minutesUsed: usage?.minutesUsed ?? 0
    },
    limits: {
      maxRendersPerMonth: plan.maxRendersPerMonth,
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
