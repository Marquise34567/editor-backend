import express from 'express'
import { createCheckoutUrlForUser, createPortalUrlForUser, type BillingInterval } from '../services/billing'
import { getOrCreateUser } from '../services/users'
import { getUserPlan } from '../services/plans'
import { PLAN_TIERS, type PlanTier } from '../shared/planConfig'

const router = express.Router()

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
    const url = await createCheckoutUrlForUser(user.id, tier, user.email, interval, useTrial)
    if (!url) return res.status(500).json({ error: 'Missing price config' })
    res.json({ url })
  } catch (err) {
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
    const { subscription, tier } = await getUserPlan(user.id)
    res.json({
      tier,
      status: subscription?.status ?? 'free',
      current_period_end: subscription?.currentPeriodEnd ?? null
    })
  } catch (err) {
    console.error('billing me', err)
    res.status(500).json({ error: 'Failed to get billing info' })
  }
}

// Billing status
router.get('/status', handleStatus)

// Backwards-compatible aliases
router.post('/create-checkout-session', async (req, res) => {
  const tier = parseTier(req.body?.tier) || 'starter'
  req.body = { ...req.body, tier }
  return handleCheckout(req, res)
})

router.post('/create-portal-session', handlePortal)

router.get('/me', handleStatus)

export default router
