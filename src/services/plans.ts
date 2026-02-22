import { prisma } from '../db/prisma'
import { PLAN_CONFIG, type PlanTier } from '../shared/planConfig'

export const isActiveSubscriptionStatus = (status?: string | null) =>
  status === 'active' || status === 'trialing'

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

export const getUserPlan = async (userId: string) => {
  const subscription = await getSubscriptionForUser(userId)
  const tier =
    subscription && isActiveSubscriptionStatus(subscription.status)
      ? coercePlanTier(subscription.planTier)
      : 'free'
  const plan = PLAN_CONFIG[tier]
  return { subscription, tier, plan }
}
