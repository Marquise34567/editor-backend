import express from 'express'
import { createCheckoutUrlForPrice } from '../services/billing'
import { resolvePlanFromPriceId } from '../lib/stripePlans'
import { getOrCreateUser } from '../services/users'

const router = express.Router()

router.post('/create-checkout-session', async (req: any, res: any) => {
  try {
    const user = req.user
    if (!user) return res.status(401).json({ error: 'unauthorized' })
    const priceId = String(req.body?.priceId || '')
    if (!priceId) return res.status(400).json({ error: 'price_id_required' })
    const plan = resolvePlanFromPriceId(priceId)
    if (!plan || plan === 'free') return res.status(400).json({ error: 'invalid_price_id' })
    await getOrCreateUser(user.id, user.email)
    const url = await createCheckoutUrlForPrice(user.id, priceId, user.email)
    if (!url) return res.status(400).json({ error: 'invalid_price_id' })
    res.json({ url })
  } catch (err: any) {
    console.error('create-checkout-session', err)
    res.status(500).json({ error: 'Failed to create session' })
  }
})

export default router
