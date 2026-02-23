import express from 'express'
import { getOrCreateUser } from '../services/users'
import { getUserPlan } from '../services/plans'
import { getUsageForMonth } from '../services/usage'
import { getRenderModeUsageForMonth } from '../services/renderModeUsage'
import { getMonthKey } from '../shared/planConfig'
import { getPlanFeatures } from '../lib/planFeatures'
import { SUBTITLE_PRESET_REGISTRY } from '../shared/subtitlePresets'
import { isDevAccount } from '../lib/devAccounts'

const router = express.Router()

const FREE_VERTICAL_MONTHLY_RENDER_LIMIT = 1

router.get('/', async (req: any, res) => {
  const id = req.user?.id
  if (!id) return res.status(401).json({ error: 'unauthenticated' })
  const user = await getOrCreateUser(id, req.user?.email)
  const { subscription, tier, plan } = await getUserPlan(id)
  const month = getMonthKey()
  const usage = await getUsageForMonth(id, month)
  const standardModeUsage = await getRenderModeUsageForMonth(id, 'standard')
  const verticalModeUsage = await getRenderModeUsageForMonth(id, 'vertical')
  const isDev = isDevAccount(user.id, user.email)
  const rendersUsed = tier === 'free' ? standardModeUsage.rendersCount : (usage?.rendersUsed ?? 0)

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
      rendersUsed,
      minutesUsed: usage?.minutesUsed ?? 0
    },
    usageByMode: {
      month,
      standardRendersUsed: standardModeUsage.rendersCount,
      verticalRendersUsed: verticalModeUsage.rendersCount
    },
    usageDaily: null,
    limits: {
      maxRendersPerMonth: isDev ? null : plan.maxRendersPerMonth,
      maxRendersPerDay: null,
      maxVerticalRendersPerMonth: isDev
        ? null
        : tier === 'free'
          ? FREE_VERTICAL_MONTHLY_RENDER_LIMIT
          : plan.maxRendersPerMonth,
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
