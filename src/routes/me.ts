import express from 'express'
import { getOrCreateUser } from '../services/users'
import { getUserPlan } from '../services/plans'
import { getUsageForMonth } from '../services/usage'
import { getRenderModeUsageForMonth } from '../services/renderModeUsage'
import { getRerenderUsageForDay } from '../services/rerenderUsage'
import { applyReferralCodeForUser, getReferralOverviewForUser } from '../services/referrals'
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
  const referral = await getReferralOverviewForUser(id, req.user?.email)
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
    user: { id: user.id, email: user.email, createdAt: user.createdAt, referralCode: referral.referralCode },
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
    referral,
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

router.get('/referrals', async (req: any, res) => {
  const id = req.user?.id
  if (!id) return res.status(401).json({ error: 'unauthenticated' })
  const referral = await getReferralOverviewForUser(id, req.user?.email)
  res.json(referral)
})

router.post('/referrals/apply', async (req: any, res) => {
  try {
    const id = req.user?.id
    if (!id) return res.status(401).json({ error: 'unauthenticated' })
    const code = req.body?.code
    const result = await applyReferralCodeForUser(id, req.user?.email, code)
    const referral = await getReferralOverviewForUser(id, req.user?.email)
    return res.json({
      applied: true,
      result,
      referral
    })
  } catch (error: any) {
    const code = String(error?.code || '').toLowerCase()
    if (code === 'invalid_referral_code') {
      return res.status(400).json({ error: 'invalid_referral_code', message: 'Referral code is invalid.' })
    }
    if (code === 'referral_not_found') {
      return res.status(404).json({ error: 'referral_not_found', message: 'Referral code not found.' })
    }
    if (code === 'self_referral_not_allowed') {
      return res.status(400).json({ error: 'self_referral_not_allowed', message: 'You cannot use your own referral code.' })
    }
    if (code === 'referral_already_applied') {
      return res.status(409).json({ error: 'referral_already_applied', message: 'Referral is already applied on this account.' })
    }
    console.error('apply referral failed', error)
    return res.status(500).json({ error: 'server_error' })
  }
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
