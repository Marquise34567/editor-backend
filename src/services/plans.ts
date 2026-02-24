import { prisma } from '../db/prisma'
import { PLAN_CONFIG, type PlanTier } from '../shared/planConfig'

export const isActiveSubscriptionStatus = (status?: string | null) =>
  status === 'active' || status === 'trialing'

type TrialInfo = {
  active: boolean
  startedAt: string | null
  endsAt: string | null
  daysRemaining: number
  trialTier: PlanTier | null
}

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

const emptyTrialInfo = (): TrialInfo => ({
  active: false,
  startedAt: null,
  endsAt: null,
  daysRemaining: 0,
  trialTier: null
})

const buildSubscriptionTrialInfo = (currentPeriodEnd?: Date | null, trialTier?: PlanTier): TrialInfo => {
  if (!currentPeriodEnd) return emptyTrialInfo()
  const end = new Date(currentPeriodEnd)
  const remainingMs = end.getTime() - Date.now()
  if (remainingMs <= 0) {
    return {
      ...emptyTrialInfo(),
      endsAt: end.toISOString()
    }
  }
  return {
    active: true,
    startedAt: null,
    endsAt: end.toISOString(),
    daysRemaining: Math.max(1, Math.ceil(remainingMs / (24 * 60 * 60 * 1000))),
    trialTier: trialTier && trialTier !== 'free' ? trialTier : null
  }
}

type UserPlanResult = {
  subscription: Awaited<ReturnType<typeof getSubscriptionForUser>>
  tier: PlanTier
  plan: (typeof PLAN_CONFIG)[PlanTier]
  trial: TrialInfo
}

export const getUserPlan = async (userId: string): Promise<UserPlanResult> => {
  const subscription = await getSubscriptionForUser(userId)
  if (subscription && isActiveSubscriptionStatus(subscription.status)) {
    const paidTier = coercePlanTier(subscription.planTier)
    const trial =
      subscription.status === 'trialing'
        ? buildSubscriptionTrialInfo(subscription.currentPeriodEnd ?? null, paidTier)
        : emptyTrialInfo()
    return {
      subscription,
      tier: paidTier,
      plan: PLAN_CONFIG[paidTier],
      trial
    }
  }
  const trial = emptyTrialInfo()
  return {
    subscription,
    tier: 'free',
    plan: PLAN_CONFIG.free,
    trial
  }
}
