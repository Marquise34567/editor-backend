import { prisma } from '../db/prisma'
import { PLAN_CONFIG, type PlanTier } from '../shared/planConfig'

export const isActiveSubscriptionStatus = (status?: string | null) =>
  status === 'active' || status === 'trialing'

const DAY_MS = 24 * 60 * 60 * 1000
const FREE_TRIAL_PRICE_ID = 'manual_free_trial_3d'
const FREE_TRIAL_DAYS = (() => {
  const value = Number(process.env.FREE_TRIAL_DAYS || 3)
  if (!Number.isFinite(value)) return 3
  return Math.min(14, Math.max(1, Math.round(value)))
})()

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

export const getFreeTrialUnlockTier = (): PlanTier => {
  const configured = coercePlanTier(process.env.FREE_TRIAL_UNLOCK_TIER || 'studio')
  if (configured === 'free' || configured === 'founder') return 'studio'
  return configured
}

export const getFreeTrialDurationDays = () => FREE_TRIAL_DAYS

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

const resolveTrialTier = (tier?: string | null): PlanTier => {
  const coerced = coercePlanTier(tier || getFreeTrialUnlockTier())
  if (coerced === 'free' || coerced === 'founder') return getFreeTrialUnlockTier()
  return coerced
}

const parseEpoch = (value?: Date | string | null) => {
  if (!value) return null
  const parsed = new Date(value).getTime()
  return Number.isFinite(parsed) ? parsed : null
}

const buildSubscriptionTrialInfo = (
  currentPeriodEnd?: Date | string | null,
  trialTier?: PlanTier,
  startedAt?: Date | string | null
): TrialInfo => {
  const now = Date.now()
  const configuredTier = resolveTrialTier(trialTier)
  const stripeEndMs = parseEpoch(currentPeriodEnd)
  const startMs = parseEpoch(startedAt)
  const maxWindowEndMs = startMs !== null ? startMs + FREE_TRIAL_DAYS * DAY_MS : null
  let endMs = stripeEndMs
  if (endMs !== null && maxWindowEndMs !== null) {
    endMs = Math.min(endMs, maxWindowEndMs)
  } else if (endMs === null && maxWindowEndMs !== null) {
    endMs = maxWindowEndMs
  }
  if (endMs === null) return emptyTrialInfo()
  const remainingMs = endMs - now
  if (remainingMs <= 0) {
    return {
      ...emptyTrialInfo(),
      startedAt: startMs !== null ? new Date(startMs).toISOString() : null,
      endsAt: new Date(endMs).toISOString(),
      trialTier: configuredTier
    }
  }
  return {
    active: true,
    startedAt: startMs !== null ? new Date(startMs).toISOString() : null,
    endsAt: new Date(endMs).toISOString(),
    daysRemaining: Math.max(1, Math.ceil(remainingMs / DAY_MS)),
    trialTier: configuredTier
  }
}

type ActivateManualFreeTrialResult = {
  alreadyActive: boolean
  alreadyUsed: boolean
  tier: PlanTier
  trial: TrialInfo
}

export const activateManualFreeTrial = async (userId: string): Promise<ActivateManualFreeTrialResult> => {
  const now = new Date()
  const unlockTier = getFreeTrialUnlockTier()
  const endsAt = new Date(now.getTime() + FREE_TRIAL_DAYS * DAY_MS)
  const existing = await getSubscriptionForUser(userId)
  const hasUsedManualTrial = existing?.priceId === FREE_TRIAL_PRICE_ID || existing?.status === 'trialing'
  if (hasUsedManualTrial) {
    const existingTrialInfo = buildSubscriptionTrialInfo(
      existing?.currentPeriodEnd ?? null,
      resolveTrialTier(existing?.planTier),
      existing?.updatedAt ?? null
    )
    const alreadyActive = existing?.status === 'trialing' && existingTrialInfo.active
    return {
      alreadyActive,
      alreadyUsed: true,
      tier: existingTrialInfo.trialTier ?? unlockTier,
      trial: existingTrialInfo
    }
  }
  await prisma.subscription.upsert({
    where: { userId },
    create: {
      userId,
      stripeCustomerId: existing?.stripeCustomerId ?? null,
      stripeSubscriptionId: null,
      status: 'trialing',
      planTier: unlockTier,
      priceId: FREE_TRIAL_PRICE_ID,
      currentPeriodEnd: endsAt,
      cancelAtPeriodEnd: true
    },
    update: {
      status: 'trialing',
      planTier: unlockTier,
      priceId: FREE_TRIAL_PRICE_ID,
      currentPeriodEnd: endsAt,
      cancelAtPeriodEnd: true
    }
  })
  await prisma.user.updateMany({
    where: { id: userId },
    data: {
      planStatus: 'active',
      currentPeriodEnd: endsAt
    }
  })
  return {
    alreadyActive: false,
    alreadyUsed: false,
    tier: unlockTier,
    trial: buildSubscriptionTrialInfo(endsAt, unlockTier, now)
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
    if (subscription.status === 'trialing') {
      const trialTier = resolveTrialTier(subscription.planTier)
      const trial = buildSubscriptionTrialInfo(
        subscription.currentPeriodEnd ?? null,
        trialTier,
        subscription.updatedAt ?? null
      )
      if (trial.active) {
        return {
          subscription,
          tier: trialTier,
          plan: PLAN_CONFIG[trialTier],
          trial
        }
      }
      return {
        subscription,
        tier: 'free',
        plan: PLAN_CONFIG.free,
        trial
      }
    }
    const paidTier = coercePlanTier(subscription.planTier)
    return {
      subscription,
      tier: paidTier,
      plan: PLAN_CONFIG[paidTier],
      trial: emptyTrialInfo()
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
