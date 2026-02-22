import express from 'express'
import { createCheckoutUrlForPrice } from '../services/billing'
import { isStripeEnabled } from '../services/stripe'
import { isKnownPriceId, resolvePlanFromPriceId } from '../lib/stripePlans'
import { getOrCreateUser } from '../services/users'
import { ensureFounderAvailable, FounderSoldOutError } from '../services/founder'

const router = express.Router()

const handleCheckout = async (req: any, res: any) => {
  try {
    const user = req.user
    if (!user) return res.status(401).json({ error: 'unauthorized' })
    const priceId = String(req.body?.priceId || '')
    if (!priceId) return res.status(400).json({ error: 'price_id_required' })
    const allowMock = String(process.env.STRIPE_ALLOW_MOCK || '').toLowerCase() === 'true'
    if (!isStripeEnabled() && !allowMock && process.env.NODE_ENV === 'production') {
      console.error('create-checkout-session: stripe disabled or misconfigured')
      return res.status(500).json({ error: 'stripe_not_configured', message: 'Stripe secret key is missing.' })
    }
    if (!isKnownPriceId(priceId)) return res.status(400).json({ error: 'invalid_price_id' })
    const plan = resolvePlanFromPriceId(priceId)
    if (!plan || plan === 'free') return res.status(400).json({ error: 'invalid_price_id' })
    if (plan === 'founder') {
      await ensureFounderAvailable()
    }
    await getOrCreateUser(user.id, user.email)
    const url = await createCheckoutUrlForPrice(user.id, priceId, user.email)
    if (!url) return res.status(400).json({ error: 'invalid_price_id' })
    res.json({ url })
  } catch (err: any) {
    if (err instanceof FounderSoldOutError) {
      return res.status(err.status).json({ error: err.code, message: err.message })
    }
    console.error('create-checkout-session', {
      requestId: req.requestId,
      error: err?.message || err,
      stack: err?.stack,
      priceId: req.body?.priceId,
      userId: req.user?.id
    })
    res.status(500).json({ error: 'checkout_failed', message: 'Failed to create session' })
  }
}

router.post('/create-checkout-session', handleCheckout)
router.post('/checkout/create-session', handleCheckout)

export default router
