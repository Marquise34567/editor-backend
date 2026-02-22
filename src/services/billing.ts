import { prisma } from '../db/prisma'
import { createCheckoutSession, createPortalSession, isStripeEnabled, stripe } from './stripe'
import { getEnv } from '../lib/env'
import { getStripeConfig } from '../lib/stripeConfig'
import { getOrCreateUser } from './users'
import { type PlanTier } from '../shared/planConfig'
import { resolvePlanFromPriceId } from '../lib/stripePlans'

const env = getEnv()

export const ensureStripeCustomer = async (userId: string, email?: string | null) => {
  const user = await getOrCreateUser(userId, email)
  if (user.stripeCustomerId) return user.stripeCustomerId
  if (!isStripeEnabled()) return null
  const customer = await stripe.customers.create({
    email: user.email || undefined,
    metadata: { userId }
  })
  await prisma.user.update({ where: { id: userId }, data: { stripeCustomerId: customer.id } })
  return customer.id
}

export type BillingInterval = 'monthly' | 'annual'

const getPriceIdForTier = (tier: PlanTier, interval: BillingInterval, useTrial: boolean) => {
  const { priceIds } = getStripeConfig()
  if (tier === 'founder') return priceIds.founder || ''
  if (useTrial && tier === 'starter' && priceIds.trial) return priceIds.trial
  const bucket = interval === 'annual' ? priceIds.annual : priceIds.monthly
  if (tier === 'starter') return bucket.starter || ''
  if (tier === 'creator') return bucket.creator || ''
  if (tier === 'studio') return bucket.studio || ''
  return ''
}

export const createCheckoutUrlForUser = async (
  userId: string,
  tier: PlanTier,
  email?: string | null,
  interval: BillingInterval = 'monthly',
  useTrial = false
) => {
  const priceId = getPriceIdForTier(tier, interval, useTrial)
  if (!priceId) return null
  const baseUrl = process.env.APP_URL || process.env.FRONTEND_URL || env.FRONTEND_URL || 'http://localhost:3000'
  const customerId = await ensureStripeCustomer(userId, email)
  const mode = tier === 'founder' ? 'payment' : 'subscription'
  const session = await createCheckoutSession({
    customerId,
    customerEmail: email,
    priceId,
    mode,
    successUrl: `${baseUrl}/billing/success?session_id={CHECKOUT_SESSION_ID}`,
    cancelUrl: `${baseUrl}/pricing`,
    metadata: { userId, plan: tier, planType: tier, interval, trial: useTrial ? 'true' : 'false' }
  })
  return session?.url ?? null
}

export const createCheckoutUrlForPrice = async (
  userId: string,
  priceId: string,
  email?: string | null
) => {
  const plan = resolvePlanFromPriceId(priceId)
  if (!plan || plan === 'free') return null
  const baseUrl = process.env.APP_URL || process.env.FRONTEND_URL || env.FRONTEND_URL || 'http://localhost:3000'
  const customerId = await ensureStripeCustomer(userId, email)
  const mode = plan === 'founder' ? 'payment' : 'subscription'
  const session = await createCheckoutSession({
    customerId,
    customerEmail: email,
    priceId,
    mode,
    successUrl: `${baseUrl}/billing/success?session_id={CHECKOUT_SESSION_ID}`,
    cancelUrl: `${baseUrl}/pricing`,
    metadata: { userId, plan, planType: plan }
  })
  return session?.url ?? null
}

export const createPortalUrlForUser = async (userId: string) => {
  const user = await prisma.user.findUnique({ where: { id: userId } })
  if (!user?.stripeCustomerId) return null
  const returnUrl = process.env.APP_URL || process.env.FRONTEND_URL || env.FRONTEND_URL || 'http://localhost:3000/'
  const session = await createPortalSession(user.stripeCustomerId, returnUrl)
  return session?.url ?? null
}
