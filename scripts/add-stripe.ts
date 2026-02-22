import fs from 'fs'
import path from 'path'

const base = path.join(__dirname, '..')

function safeWrite(rel: string, content: string) {
  const p = path.join(base, rel)
  if (fs.existsSync(p)) {
    console.log('Exists, skipping', rel)
    return
  }
  fs.mkdirSync(path.dirname(p), { recursive: true })
  fs.writeFileSync(p, content)
  console.log('Created', rel)
}

// Billing route is usually present; if not, create a minimal one
safeWrite('src/routes/billing.ts', `import express from 'express'
import { stripe } from '../services/stripe'
import { prisma } from '../db/prisma'

const router = express.Router()

router.post('/create-checkout-session', async (req: any, res) => {
  const user = req.user
  if (!user) return res.status(401).json({ error: 'Unauthorized' })
  const price = process.env.STRIPE_PRICE_ID_MONTHLY
  if (!price) return res.status(500).json({ error: 'Missing price config' })
  let dbUser = await prisma.user.findUnique({ where: { id: user.id } })
  if (!dbUser) dbUser = await prisma.user.create({ data: { id: user.id, email: user.email ?? '' } })
  let customerId = dbUser.stripeCustomerId
  if (!customerId) {
    const customer = await stripe.customers.create({ email: user.email || undefined })
    customerId = customer.id
    await prisma.user.update({ where: { id: user.id }, data: { stripeCustomerId: customerId } })
  }
  const session = await stripe.checkout.sessions.create({
    mode: 'subscription',
    payment_method_types: ['card'],
    customer: customerId,
    line_items: [{ price, quantity: 1 }],
    success_url: `${process.env.FRONTEND_URL}/billing/success`,
    cancel_url: `${process.env.FRONTEND_URL}/billing/cancel`,
    metadata: { userId: user.id }
  })
  res.json({ url: session.url })
})

router.post('/create-portal-session', async (req:any, res) => {
  const user = req.user
  if (!user) return res.status(401).json({ error: 'Unauthorized' })
  const dbUser = await prisma.user.findUnique({ where: { id: user.id } })
  if (!dbUser?.stripeCustomerId) return res.status(400).json({ error: 'No customer attached' })
  const session = await stripe.billingPortal.sessions.create({ customer: dbUser.stripeCustomerId, return_url: process.env.FRONTEND_URL })
  res.json({ url: session.url })
})

export default router
`)

console.log('Stripe scaffolding complete')
import fs from 'fs'
import path from 'path'

const base = path.join(__dirname, '..')

const billingRoute = `import express from 'express'
import { prisma } from '../db/prisma'
import { stripe } from '../services/stripe'
const router = express.Router()

router.post('/create-checkout-session', async (req: any, res) => {
  const userId = req.user?.sub
  if (!userId) return res.status(401).json({ error: 'unauthenticated' })
  const dbUser = await prisma.user.findUnique({ where: { id: userId } })
  const price = process.env.STRIPE_PRICE_ID_MONTHLY
  if (!price) return res.status(500).json({ error: 'missing_price' })
  let customer = dbUser?.stripeCustomerId
  if (!customer) {
    const c = await stripe.customers.create({ email: dbUser?.email ?? undefined })
    customer = c.id
    await prisma.user.update({ where: { id: userId }, data: { stripeCustomerId: customer } })
  }
  const session = await stripe.checkout.sessions.create({
    mode: 'subscription', payment_method_types: ['card'], customer, line_items: [{ price, quantity: 1 }],
    success_url: `${process.env.FRONTEND_URL}/billing/success`, cancel_url: `${process.env.FRONTEND_URL}/billing/cancel`,
    metadata: { userId }
  })
  res.json({ url: session.url })
})

export default router
`

const p = path.join(base, 'src', 'routes', 'billing.ts')
if (!fs.existsSync(p)) {
  fs.writeFileSync(p, billingRoute)
  console.log('Created billing route')
} else {
  console.log('Billing route exists, skipping')
}

console.log('Stripe scaffolding complete')
