import express from 'express'
import { createCheckoutUrlForUser, createPortalUrlForUser, type BillingInterval } from '../services/billing'
import { getOrCreateUser } from '../services/users'
import { getSubscriptionForUser, getUserPlan } from '../services/plans'
import { ensureFounderAvailable, FounderSoldOutError } from '../services/founder'
import { PLAN_CONFIG, PLAN_TIERS, type PlanTier } from '../shared/planConfig'
import { isPaidTier, getPlanFeatures } from '../shared/planConfig'
import { resolveDevAdminAccess } from '../lib/devAccounts'
import { getStripeConfig } from '../lib/stripeConfig'

const router = express.Router()
const MANUAL_TRIAL_PRICE_ID = 'manual_free_trial_3d'

const parseTier = (value: any): PlanTier | null => {
  const raw = String(value || '').toLowerCase()
  if (PLAN_TIERS.includes(raw as PlanTier) && raw !== 'free') return raw as PlanTier
  return null
}

const parseInterval = (value: any): BillingInterval => {
  const raw = String(value || '').toLowerCase()
  if (raw === 'annual' || raw === 'yearly' || raw === 'year') return 'annual'
  return 'monthly'
}

const parseBool = (value: any) => {
  if (typeof value === 'boolean') return value
  if (value == null) return false
  const raw = String(value).toLowerCase()
  return raw === 'true' || raw === '1' || raw === 'yes'
}

const resolveEffectiveStatus = (rawStatus?: string | null, trialActive?: boolean) => {
  if (trialActive) return 'trial'
  if (rawStatus === 'trialing') return 'free'
  return rawStatus || 'free'
}

const handleCheckout = async (req: any, res: any) => {
  try {
    const user = req.user
    if (!user) return res.status(401).json({ error: 'Unauthorized' })
    await getOrCreateUser(user.id, user.email)
    const tier = parseTier(req.body?.tier)
    if (!tier) return res.status(400).json({ error: 'invalid_tier' })
    const interval = parseInterval(req.body?.interval || req.body?.billingInterval)
    const wantTrial = parseBool(req.body?.trial)
    const useTrial = tier === 'starter' && wantTrial
    if (useTrial) {
      const existingSubscription = await getSubscriptionForUser(user.id)
      const existingStatus = String(existingSubscription?.status || '').toLowerCase()
      const trialPriceId = getStripeConfig().priceIds.trial
      const hasUsedTrialPrice =
        existingSubscription?.priceId === trialPriceId || existingSubscription?.priceId === MANUAL_TRIAL_PRICE_ID
      if (existingStatus === 'active') {
        return res.status(409).json({
          error: 'already_subscribed',
          message: 'Active subscription already has unlocked features. Manage billing to change plans.'
        })
      }
      if (existingStatus === 'trialing') {
        return res.status(409).json({
          error: 'trial_already_active',
          message: 'Free trial is already active on this account.'
        })
      }
      if (hasUsedTrialPrice) {
        return res.status(409).json({
          error: 'trial_already_used',
          message: 'Free trial already used. Upgrade to keep full access.'
        })
      }
    }
    if (tier === 'founder') {
      await ensureFounderAvailable()
    }
    const url = await createCheckoutUrlForUser(user.id, tier, user.email, interval, useTrial)
    if (!url) {
      return res.status(500).json({
        error: useTrial ? 'trial_checkout_not_configured' : 'missing_price_config',
        message: useTrial ? 'Free trial checkout is not configured.' : 'Missing price config'
      })
    }
    res.json({ url })
  } catch (err: any) {
    if (err instanceof FounderSoldOutError) {
      return res.status(err.status).json({ error: err.code, message: err.message })
    }
    console.error('create-checkout-session', err)
    res.status(500).json({ error: 'Failed to create session' })
  }
}

// Create Checkout Session (new)
router.post('/checkout', handleCheckout)

const handlePortal = async (req: any, res: any) => {
  try {
    const user = req.user
    if (!user) return res.status(401).json({ error: 'Unauthorized' })
    await getOrCreateUser(user.id, user.email)
    const url = await createPortalUrlForUser(user.id)
    if (!url) return res.status(400).json({ error: 'No customer attached' })
    res.json({ url })
  } catch (err) {
    console.error('create-portal-session', err)
    res.status(500).json({ error: 'Failed to create portal session' })
  }
}

// Create Portal Session (new)
router.post('/portal', handlePortal)

const handleStatus = async (req: any, res: any) => {
  try {
    const user = req.user
    if (!user) return res.status(401).json({ error: 'Unauthorized' })
    const { subscription, tier, trial } = await getUserPlan(user.id)
    res.json({
      tier,
      status: resolveEffectiveStatus(subscription?.status, trial?.active),
      current_period_end: trial?.active ? trial.endsAt : subscription?.currentPeriodEnd ?? null
    })
  } catch (err) {
    console.error('billing me', err)
    res.status(500).json({ error: 'Failed to get billing info' })
  }
}

// Billing status
router.get('/status', handleStatus)

// Entitlements: server source-of-truth for feature flags
router.get('/entitlements', async (req: any, res) => {
  try {
    const user = req.user
    if (!user) return res.status(401).json({ error: 'Unauthorized' })
    const { tier, plan } = await getUserPlan(user.id)
    const devAccess = await resolveDevAdminAccess(user.id, user.email)
    const isDev = devAccess.emailAuthorized
    const effectiveTier = isDev ? 'studio' : tier
    const planKey = effectiveTier
    const isPaid = isDev || isPaidTier(tier)
    const features = getPlanFeatures(isDev ? PLAN_CONFIG.studio : plan)
    const entitlements = {
      autoDownloadAllowed: isPaid,
      canExport4k: features.resolution === '4K',
      watermark: features.watermark,
      priorityQueue: features.queuePriority === 'priority',
      rendersPerMonth: isDev ? null : features.rendersPerMonth
    }
    res.json({ planKey, isPaid, entitlements, devOverride: isDev })
  } catch (err) {
    console.error('entitlements', err)
    res.status(500).json({ error: 'server_error' })
  }
})

// Backwards-compatible aliases
router.post('/create-checkout-session', async (req, res) => {
  const tier = parseTier(req.body?.tier) || 'starter'
  req.body = { ...req.body, tier }
  return handleCheckout(req, res)
})

router.post('/create-portal-session', handlePortal)

router.get('/me', handleStatus)

export default router
