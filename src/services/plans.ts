import { prisma } from '../db/prisma'
import { PLAN_CONFIG, type PlanTier } from '../shared/planConfig'

export const isActiveSubscriptionStatus = (status?: string | null) =>
  status === 'active' || status === 'trialing'

const TRIAL_LENGTH_DAYS = (() => {
  const value = Number(process.env.FREE_TRIAL_DAYS || 3)
  if (!Number.isFinite(value) || value <= 0) return 3
  return Math.round(Math.min(14, Math.max(1, value)))
})()

export const coercePlanTier = (tier?: string | null): PlanTier => {
  if (!tier) return 'free'
  const normalized = String(tier).toLowerCase()
  if (normalized === 'founder') return 'founder'
  if (normalized === 'starter') return 'starter'
  if (normalized === 'creator') return 'creator'
  if (normalized === 'studio') return 'studio'
  return 'free'
}

export const getSubscriptionForUser = async (userId: string) => {
  return prisma.subscription.findUnique({ where: { userId } })
}

const resolveTrialTier = (): PlanTier => {
  const configured = coercePlanTier(process.env.FREE_TRIAL_UNLOCK_TIER || 'studio')
  if (configured === 'free') return 'studio'
  return configured
}

const buildTrialInfo = (createdAt?: Date | null) => {
  if (!createdAt) {
    return {
      active: false,
      startedAt: null as string | null,
      endsAt: null as string | null,
      daysRemaining: 0,
      trialTier: null as PlanTier | null
    }
  }
  const start = new Date(createdAt)
  const end = new Date(start.getTime() + TRIAL_LENGTH_DAYS * 24 * 60 * 60 * 1000)
  const now = Date.now()
  const remainingMs = end.getTime() - now
  if (remainingMs <= 0) {
    return {
      active: false,
      startedAt: start.toISOString(),
      endsAt: end.toISOString(),
      daysRemaining: 0,
      trialTier: null as PlanTier | null
    }
  }
  const trialTier = resolveTrialTier()
  return {
    active: true,
    startedAt: start.toISOString(),
    endsAt: end.toISOString(),
    daysRemaining: Math.max(1, Math.ceil(remainingMs / (24 * 60 * 60 * 1000))),
    trialTier
  }
}

export const getUserPlan = async (userId: string) => {
  const subscription = await getSubscriptionForUser(userId)
  const paidTier =
    subscription && isActiveSubscriptionStatus(subscription.status)
      ? coercePlanTier(subscription.planTier)
      : null
  if (paidTier) {
    return {
      subscription,
      tier: paidTier,
      plan: PLAN_CONFIG[paidTier],
      trial: {
        active: false,
        startedAt: null as string | null,
        endsAt: null as string | null,
        daysRemaining: 0,
        trialTier: null as PlanTier | null
      }
    }
  }
  const user = await prisma.user.findUnique({
    where: { id: userId },
    select: { createdAt: true }
  })
  const trial = buildTrialInfo(user?.createdAt ?? null)
  const tier = trial.active && trial.trialTier ? trial.trialTier : 'free'
  const plan = PLAN_CONFIG[tier]
  return { subscription, tier, plan, trial }
}
