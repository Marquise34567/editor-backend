import { prisma } from '../db/prisma'
import { getFreeTrialUnlockTier, getSubscriptionForUser } from './plans'
import { getOrCreateUser, parseReferralCode } from './users'

export const REFERRALS_PER_FREE_MONTH = 3
export const MANUAL_REFERRAL_PRICE_PREFIX = 'manual_referral_reward_'

const addCalendarMonths = (base: Date, months: number) => {
  const safeMonths = Math.max(0, Math.round(months))
  const next = new Date(base.getTime())
  next.setMonth(next.getMonth() + safeMonths)
  return next
}

const getFutureAnchorDate = (value: Date | string | null | undefined, fallback: Date) => {
  if (!value) return fallback
  const parsed = new Date(value)
  if (!Number.isFinite(parsed.getTime())) return fallback
  return parsed.getTime() > fallback.getTime() ? parsed : fallback
}

export const parseReferralRewardMonthsFromPriceId = (priceId?: string | null) => {
  const raw = String(priceId || '').trim().toLowerCase()
  if (!raw.startsWith(MANUAL_REFERRAL_PRICE_PREFIX)) return null
  const match = raw.match(/manual_referral_reward_(\d+)m/)
  if (!match) return 1
  const months = Number(match[1])
  if (!Number.isFinite(months) || months <= 0) return 1
  return Math.min(12, Math.max(1, Math.round(months)))
}

type GrantResult = {
  monthsGranted: number
  currentPeriodEnd: string | null
}

const grantFreeMonths = async (userId: string, months: number): Promise<GrantResult> => {
  const normalizedMonths = Math.max(0, Math.round(months))
  if (normalizedMonths <= 0) return { monthsGranted: 0, currentPeriodEnd: null }

  const now = new Date()
  const unlockTier = getFreeTrialUnlockTier()
  const existing = await getSubscriptionForUser(userId)
  const anchorDate = getFutureAnchorDate(existing?.currentPeriodEnd ?? null, now)
  const nextPeriodEnd = addCalendarMonths(anchorDate, normalizedMonths)
  const hasStripeActiveSubscription = existing?.status === 'active' && Boolean(existing?.stripeSubscriptionId)

  if (hasStripeActiveSubscription) {
    await prisma.subscription.upsert({
      where: { userId },
      create: {
        userId,
        stripeCustomerId: existing?.stripeCustomerId ?? null,
        stripeSubscriptionId: existing?.stripeSubscriptionId ?? null,
        status: existing?.status || 'active',
        planTier: existing?.planTier || unlockTier,
        priceId: existing?.priceId ?? null,
        currentPeriodEnd: nextPeriodEnd,
        cancelAtPeriodEnd: Boolean(existing?.cancelAtPeriodEnd)
      },
      update: {
        currentPeriodEnd: nextPeriodEnd
      }
    })
    await prisma.user.updateMany({
      where: { id: userId },
      data: {
        planStatus: 'active',
        currentPeriodEnd: nextPeriodEnd
      }
    })
    return {
      monthsGranted: normalizedMonths,
      currentPeriodEnd: nextPeriodEnd.toISOString()
    }
  }

  await prisma.subscription.upsert({
    where: { userId },
    create: {
      userId,
      stripeCustomerId: existing?.stripeCustomerId ?? null,
      stripeSubscriptionId: existing?.stripeSubscriptionId ?? null,
      status: 'trialing',
      planTier: unlockTier,
      priceId: `${MANUAL_REFERRAL_PRICE_PREFIX}${normalizedMonths}m`,
      currentPeriodEnd: nextPeriodEnd,
      cancelAtPeriodEnd: true
    },
    update: {
      status: 'trialing',
      planTier: unlockTier,
      priceId: `${MANUAL_REFERRAL_PRICE_PREFIX}${normalizedMonths}m`,
      currentPeriodEnd: nextPeriodEnd,
      cancelAtPeriodEnd: true
    }
  })

  await prisma.user.updateMany({
    where: { id: userId },
    data: {
      planStatus: 'active',
      currentPeriodEnd: nextPeriodEnd
    }
  })

  return {
    monthsGranted: normalizedMonths,
    currentPeriodEnd: nextPeriodEnd.toISOString()
  }
}

const computeReferralProgress = (count: number) => {
  const normalizedCount = Math.max(0, Math.floor(Number(count) || 0))
  const inCurrentCycle = normalizedCount % REFERRALS_PER_FREE_MONTH
  const referralsToNextReward = REFERRALS_PER_FREE_MONTH - inCurrentCycle
  return {
    inCurrentCycle,
    referralsToNextReward: referralsToNextReward === 0 ? REFERRALS_PER_FREE_MONTH : referralsToNextReward
  }
}

export const getReferralOverviewForUser = async (userId: string, email?: string | null) => {
  const user = await getOrCreateUser(userId, email)
  const referredUsersCount = await prisma.user.count({ where: { referredByUserId: user.id } })
  const rewardsGranted = Math.max(0, Math.floor(Number(user.referralRewardsGranted || 0)))
  const progress = computeReferralProgress(referredUsersCount)
  return {
    referralCode: user.referralCode,
    referredUsersCount,
    rewardsGranted,
    rewardsEarnedMonths: rewardsGranted,
    referralsPerReward: REFERRALS_PER_FREE_MONTH,
    referralsToNextReward: progress.referralsToNextReward,
    progressInCurrentCycle: progress.inCurrentCycle
  }
}

export type ApplyReferralResult = {
  applied: boolean
  referralCode: string
  referrerUserId: string
  reward: GrantResult
}

export const applyReferralCodeForUser = async (
  refereeUserId: string,
  refereeEmail: string | null | undefined,
  rawCode: unknown
): Promise<ApplyReferralResult> => {
  const referralCode = parseReferralCode(rawCode)
  if (!referralCode) {
    const error: any = new Error('invalid_referral_code')
    error.code = 'invalid_referral_code'
    throw error
  }

  const referee = await getOrCreateUser(refereeUserId, refereeEmail)
  if (referee.referredByUserId) {
    const error: any = new Error('referral_already_applied')
    error.code = 'referral_already_applied'
    throw error
  }

  const referrer = await prisma.user.findUnique({ where: { referralCode } })
  if (!referrer?.id) {
    const error: any = new Error('referral_not_found')
    error.code = 'referral_not_found'
    throw error
  }
  if (referrer.id === referee.id) {
    const error: any = new Error('self_referral_not_allowed')
    error.code = 'self_referral_not_allowed'
    throw error
  }

  await prisma.user.update({
    where: { id: referee.id },
    data: { referredByUserId: referrer.id }
  })

  const referredUsersCount = await prisma.user.count({ where: { referredByUserId: referrer.id } })
  const rewardsAlreadyGranted = Math.max(0, Math.floor(Number(referrer.referralRewardsGranted || 0)))
  const totalEligibleRewards = Math.floor(referredUsersCount / REFERRALS_PER_FREE_MONTH)
  const rewardsToGrant = Math.max(0, totalEligibleRewards - rewardsAlreadyGranted)

  let reward: GrantResult = { monthsGranted: 0, currentPeriodEnd: null }
  if (rewardsToGrant > 0) {
    reward = await grantFreeMonths(referrer.id, rewardsToGrant)
    await prisma.user.update({
      where: { id: referrer.id },
      data: {
        referralRewardsGranted: rewardsAlreadyGranted + rewardsToGrant
      }
    })
  }

  return {
    applied: true,
    referralCode,
    referrerUserId: referrer.id,
    reward
  }
}
