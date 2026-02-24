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

const resolveEffectiveSubscriptionStatus = (rawStatus?: string | null, trialActive?: boolean) => {
  if (trialActive) return 'trial'
  if (rawStatus === 'trialing') return 'free'
  return rawStatus || 'free'
}

router.get('/', async (req: any, res) => {
  const id = req.user?.id
  if (!id) return res.status(401).json({ error: 'unauthenticated' })
  const user = await getOrCreateUser(id, req.user?.email)
  const { subscription, tier, plan, trial } = await getUserPlan(id)
  const effectiveStatus = resolveEffectiveSubscriptionStatus(subscription?.status, trial?.active)
  const month = getMonthKey()
  const usage = await getUsageForMonth(id, month)
  const horizontalModeUsage = await getRenderModeUsageForMonth(id, 'horizontal')
  const verticalModeUsage = await getRenderModeUsageForMonth(id, 'vertical')
  const isDev = isDevAccount(user.id, user.email)
  const rendersUsed = usage?.rendersUsed ?? 0

  res.json({
    user: { id: user.id, email: user.email, createdAt: user.createdAt },
    subscription: subscription
      ? {
          tier,
          status: effectiveStatus,
          currentPeriodEnd: trial?.active ? trial.endsAt : subscription.currentPeriodEnd,
          cancelAtPeriodEnd: subscription.cancelAtPeriodEnd,
          trial
        }
      : {
          tier,
          status: trial?.active ? 'trial' : 'free',
          currentPeriodEnd: trial?.active ? trial.endsAt : null,
          cancelAtPeriodEnd: false,
          trial
        },
    flags: { dev: isDev },
    usage: {
      month,
      rendersUsed,
      minutesUsed: usage?.minutesUsed ?? 0
    },
    usageByMode: {
      month,
      horizontalRendersUsed: horizontalModeUsage.rendersCount,
      standardRendersUsed: horizontalModeUsage.rendersCount,
      verticalRendersUsed: verticalModeUsage.rendersCount
    },
    usageDaily: null,
    limits: {
      maxRendersPerMonth: isDev ? null : plan.maxRendersPerMonth,
      maxRendersPerDay: null,
      maxVerticalRendersPerMonth: isDev ? null : plan.maxRendersPerMonth,
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
  const { subscription, tier, trial } = await getUserPlan(id)
  const effectiveStatus = resolveEffectiveSubscriptionStatus(subscription?.status, trial?.active)
  res.json({
    tier,
    status: effectiveStatus,
    currentPeriodEnd: trial?.active ? trial.endsAt : subscription?.currentPeriodEnd ?? null,
    stripeCustomerId: subscription?.stripeCustomerId ?? null,
    stripeSubscriptionId: subscription?.stripeSubscriptionId ?? null,
    priceId: subscription?.priceId ?? null,
    trial
  })
})

router.get('/subscription', async (req: any, res) => {
  const id = req.user?.id
  if (!id) return res.status(401).json({ error: 'unauthenticated' })
  const { subscription, tier, trial } = await getUserPlan(id)
  const effectiveStatus = resolveEffectiveSubscriptionStatus(subscription?.status, trial?.active)
  const features = getPlanFeatures(tier)
  res.json({
    plan: tier,
    status: effectiveStatus,
    currentPeriodEnd: trial?.active ? trial.endsAt : subscription?.currentPeriodEnd ?? null,
    features,
    subtitlePresets: SUBTITLE_PRESET_REGISTRY,
    trial
  })
})

export default router
