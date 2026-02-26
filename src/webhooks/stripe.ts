import express from 'express'
import { stripe } from '../stripeClient'
import { prisma } from '../db/prisma'
import { coercePlanTier, isActiveSubscriptionStatus } from '../services/plans'
import { getStripeConfig } from '../lib/stripeConfig'
import { resolvePlanFromPriceId } from '../lib/stripePlans'
import { incrementFounderPurchase } from '../services/founder'
import { storeStripeWebhookEvent } from '../services/adminTelemetry'
import { getMonthKey } from '../shared/planConfig'

const router = express.Router()

const resetMonthlyUsageForUser = async (userId: string) => {
  const month = getMonthKey()
  await prisma.usageMonthly.upsert({
    where: { userId_month: { userId, month } },
    create: {
      userId,
      month,
      rendersUsed: 0,
      minutesUsed: 0
    },
    update: {
      rendersUsed: 0,
      minutesUsed: 0
    }
  })
}

// raw body is handled in index.ts for this route
router.post('/', async (req: any, res) => {
  if (!stripe) return res.status(500).send('Stripe not configured')
  const sig = req.headers['stripe-signature'] as string | undefined
  const { webhookSecret } = getStripeConfig()
  if (!sig || !webhookSecret) return res.status(400).send('Missing signature or webhook secret')

  let event
  try {
    event = stripe.webhooks.constructEvent(req.rawBody, sig, webhookSecret)
  } catch (err: any) {
    console.error('Webhook signature verification failed.', err.message)
    return res.status(400).send(`Webhook Error: ${err.message}`)
  }

  try {
    await storeStripeWebhookEvent(event).catch(() => null)
    switch (event.type) {
      case 'checkout.session.completed': {
        const session = event.data.object as any
        const userId = session.metadata?.userId
        const customer = session.customer
        const subscription = session.subscription
        const metadataTier = coercePlanTier(session.metadata?.plan)
        let priceId: string | null = session?.line_items?.data?.[0]?.price?.id ?? null
        let currentPeriodEnd: Date | null = null
        let status = 'active'
        if (!priceId && subscription) {
          try {
            const sub = await stripe.subscriptions.retrieve(String(subscription))
            priceId = sub.items?.data?.[0]?.price?.id ?? null
            currentPeriodEnd = sub.current_period_end ? new Date(sub.current_period_end * 1000) : null
            status = sub.status || status
          } catch (err) {
            console.warn('checkout.session.completed: failed to fetch subscription for priceId', err)
          }
        }
        if (!priceId && session?.id) {
          try {
            const lineItems = await stripe.checkout.sessions.listLineItems(String(session.id), { limit: 1 })
            priceId = lineItems.data?.[0]?.price?.id ?? null
          } catch (err) {
            console.warn('checkout.session.completed: failed to fetch line items', err)
          }
        }
        const tier = metadataTier !== 'free' ? metadataTier : resolvePlanFromPriceId(priceId)
        if (userId) {
          const existing = await prisma.user.findUnique({ where: { id: userId } })
          if (existing) {
            await prisma.user.update({
              where: { id: userId },
              data: {
                stripeCustomerId: String(customer),
                stripeSubscriptionId: subscription ? String(subscription) : null,
                stripePriceId: priceId,
                currentPeriodEnd,
                planStatus: 'active'
              }
            })
          } else {
            const email = session.customer_email || `${userId}@autoeditor.local`
            await prisma.user.create({
              data: {
                id: userId,
                email,
                stripeCustomerId: String(customer),
                stripeSubscriptionId: subscription ? String(subscription) : null,
                stripePriceId: priceId,
                currentPeriodEnd,
                planStatus: 'active'
              }
            })
          }
          await prisma.subscription.upsert({
            where: { userId },
            create: {
              userId,
              stripeCustomerId: String(customer),
              stripeSubscriptionId: subscription ? String(subscription) : null,
              status,
              planTier: tier,
              priceId,
              currentPeriodEnd,
              cancelAtPeriodEnd: false
            },
            update: {
              stripeCustomerId: String(customer),
              stripeSubscriptionId: subscription ? String(subscription) : null,
              status,
              planTier: tier,
              priceId,
              currentPeriodEnd,
              cancelAtPeriodEnd: false
            }
          })
          await resetMonthlyUsageForUser(userId)
          if (tier === 'founder') {
            await incrementFounderPurchase()
          }
        }
        break
      }
      case 'customer.subscription.created':
      case 'customer.subscription.updated': {
        const subscription = event.data.object as any
        const customerId = subscription.customer
        const status = subscription.status
        const priceId = subscription.items?.data?.[0]?.price?.id
        const currentPeriodEnd = subscription.current_period_end ? new Date(subscription.current_period_end * 1000) : undefined
        const mapped =
          status === 'active' || status === 'trialing'
            ? 'active'
            : status === 'past_due' || status === 'unpaid' || status === 'incomplete'
            ? 'past_due'
            : 'canceled'
        const tier = resolvePlanFromPriceId(priceId)
        const shouldBeFree = !isActiveSubscriptionStatus(status)
        await prisma.user.updateMany({ where: { stripeCustomerId: String(customerId) }, data: { stripeSubscriptionId: subscription.id, planStatus: mapped, stripePriceId: priceId, currentPeriodEnd } })
        const user = await prisma.user.findUnique({ where: { stripeCustomerId: String(customerId) } })
        if (user) {
          await prisma.subscription.upsert({
            where: { userId: user.id },
            create: {
              userId: user.id,
              stripeCustomerId: String(customerId),
              stripeSubscriptionId: subscription.id,
              status,
              priceId,
              planTier: shouldBeFree ? 'free' : tier,
              currentPeriodEnd,
              cancelAtPeriodEnd: !!subscription.cancel_at_period_end
            },
            update: {
              stripeSubscriptionId: subscription.id,
              status,
              priceId,
              planTier: shouldBeFree ? 'free' : tier,
              currentPeriodEnd,
              cancelAtPeriodEnd: !!subscription.cancel_at_period_end
            }
          })
        }
        break
      }
      case 'customer.subscription.deleted': {
        const subscription = event.data.object as any
        const customerId = subscription.customer
        await prisma.user.updateMany({ where: { stripeCustomerId: String(customerId) }, data: { planStatus: 'canceled', stripeSubscriptionId: null } })
        await prisma.subscription.updateMany({
          where: { stripeCustomerId: String(customerId) },
          data: { status: 'canceled', planTier: 'free', stripeSubscriptionId: null }
        })
        break
      }
      case 'invoice.payment_succeeded':
      case 'invoice.paid': {
        const invoice = event.data.object as any
        const subId = invoice.subscription
        if (subId) {
          await prisma.user.updateMany({ where: { stripeSubscriptionId: String(subId) }, data: { planStatus: 'active' } })
          await prisma.subscription.updateMany({ where: { stripeSubscriptionId: String(subId) }, data: { status: 'active' } })
        }
        break
      }
      case 'invoice.payment_failed': {
        const invoice = event.data.object as any
        const subId = invoice.subscription
        if (subId) {
          await prisma.user.updateMany({ where: { stripeSubscriptionId: String(subId) }, data: { planStatus: 'past_due' } })
          await prisma.subscription.updateMany({ where: { stripeSubscriptionId: String(subId) }, data: { status: 'past_due', planTier: 'free' } })
        }
        break
      }
      case 'charge.refunded': {
        // Event is stored in telemetry store for admin dashboard reporting.
        break
      }
      default:
        console.log('Unhandled stripe event', event.type)
    }
    res.json({ received: true })
  } catch (err) {
    console.error('Error handling webhook', err)
    res.status(500).send('server error')
  }
})

export default router
