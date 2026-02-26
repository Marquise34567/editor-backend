import express from 'express'
import { getOrCreateUser } from '../services/users'
import { getUserPlan } from '../services/plans'
import { getUsageForMonth } from '../services/usage'
import { getRenderModeUsageForMonth } from '../services/renderModeUsage'
import { getRerenderUsageForDay } from '../services/rerenderUsage'
import { getMonthKey } from '../shared/planConfig'
import { getPlanFeatures } from '../lib/planFeatures'
import { SUBTITLE_PRESET_REGISTRY } from '../shared/subtitlePresets'
import { resolveDevAdminAccess } from '../lib/devAccounts'

const router = express.Router()
const FREE_MINUTES_WARNING_THRESHOLD = 40

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
  const rerenderUsageDaily = await getRerenderUsageForDay(id)
  const devAccess = await resolveDevAdminAccess(user.id, user.email)
  const isDev = devAccess.emailAuthorized
  const clientTier = isDev ? 'studio' : tier
  const rendersUsed = usage?.rendersUsed ?? 0
  const minutesUsed = usage?.minutesUsed ?? 0
  const maxMinutesPerMonth = isDev ? null : plan.maxMinutesPerMonth
  const freeMinutesWarning = !isDev && tier === 'free' && maxMinutesPerMonth !== null
    ? {
        threshold: FREE_MINUTES_WARNING_THRESHOLD,
        limit: maxMinutesPerMonth,
        used: minutesUsed,
        reached: minutesUsed >= FREE_MINUTES_WARNING_THRESHOLD,
        blocked: minutesUsed >= maxMinutesPerMonth
      }
    : null

  res.json({
    user: { id: user.id, email: user.email, createdAt: user.createdAt },
    subscription: subscription
      ? {
          tier: clientTier,
          status: effectiveStatus,
          currentPeriodEnd: trial?.active ? trial.endsAt : subscription.currentPeriodEnd,
          cancelAtPeriodEnd: subscription.cancelAtPeriodEnd,
          trial
        }
      : {
          tier: clientTier,
          status: trial?.active ? 'trial' : 'free',
          currentPeriodEnd: trial?.active ? trial.endsAt : null,
          cancelAtPeriodEnd: false,
          trial
        },
    flags: {
      dev: isDev,
      role: devAccess.role,
      isAdmin: devAccess.allowed
    },
    usage: {
      month,
      rendersUsed,
      minutesUsed
    },
    usageWarnings: {
      freeMinutes: freeMinutesWarning
    },
    usageByMode: {
      month,
      horizontalRendersUsed: horizontalModeUsage.rendersCount,
      standardRendersUsed: horizontalModeUsage.rendersCount,
      verticalRendersUsed: verticalModeUsage.rendersCount
    },
    rerenderUsageDaily: {
      day: rerenderUsageDaily.dayKey,
      rerendersUsed: rerenderUsageDaily.rerendersUsed,
      rerendersLimit: isDev ? null : plan.maxRerendersPerDay
    },
    usageDaily: null,
    limits: {
      maxRendersPerMonth: isDev ? null : plan.maxRendersPerMonth,
      maxRendersPerDay: null,
      maxRerendersPerDay: isDev ? null : plan.maxRerendersPerDay,
      maxVerticalRendersPerMonth: isDev ? null : plan.maxRendersPerMonth,
      maxMinutesPerMonth,
      exportQuality: isDev ? '4k' : plan.exportQuality,
      watermark: isDev ? false : plan.watermark,
      priority: isDev ? true : plan.priority
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
  const devAccess = await resolveDevAdminAccess(id, req.user?.email)
  const isDev = devAccess.emailAuthorized
  const featureTier = isDev ? 'studio' : tier
  const effectiveStatus = resolveEffectiveSubscriptionStatus(subscription?.status, trial?.active)
  const features = getPlanFeatures(featureTier)
  res.json({
    plan: tier,
    status: effectiveStatus,
    currentPeriodEnd: trial?.active ? trial.endsAt : subscription?.currentPeriodEnd ?? null,
    features,
    subtitlePresets: SUBTITLE_PRESET_REGISTRY,
    trial,
    devOverride: isDev
  })
})

export default router
