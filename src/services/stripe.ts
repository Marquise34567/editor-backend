import Stripe from 'stripe'
import { loadEnv } from '../lib/loadEnv'
import { getStripeConfig } from '../lib/stripeConfig'

loadEnv()

const { mode, secretKey } = getStripeConfig()
let stripeClient: any = null
let useMock = false
if (!secretKey || String(secretKey).includes('placeholder')) {
  useMock = true
  console.warn(`Stripe ${mode} key missing or placeholder — using mock Stripe client`)
} else {
  stripeClient = new Stripe(secretKey, { apiVersion: '2022-11-15' })
}

export const stripe = stripeClient
export const isStripeEnabled = () => !useMock && !!stripeClient

const normalizeStatementDescriptor = (value?: string | null) => {
  const raw = String(value || '')
    .toUpperCase()
    .replace(/[^A-Z0-9 ]+/g, '')
    .replace(/\s+/g, ' ')
    .trim()
  if (!raw) return undefined
  return raw.slice(0, 22)
}

export const createCheckoutSession = async (args: {
  customerId?: string | null
  customerEmail?: string | null
  priceId: string
  successUrl: string
  cancelUrl: string
  mode?: 'subscription' | 'payment'
  metadata?: Record<string,string>
  allowPromotionCodes?: boolean
  statementDescriptor?: string
}) => {
  if (useMock) {
    return { id: `sess_${Math.random().toString(36).slice(2,9)}`, url: `${args.successUrl}?mock_session=1` }
  }
  if (!args.priceId) {
    throw new Error('missing_price_id')
  }
  const mode = args.mode || 'subscription'
  const sessionPayload: Record<string, any> = {
    mode,
    payment_method_types: ['card'],
    customer: args.customerId ?? undefined,
    customer_email: args.customerId ? undefined : args.customerEmail ?? undefined,
    line_items: [{ price: args.priceId, quantity: 1 }],
    success_url: args.successUrl,
    cancel_url: args.cancelUrl,
    metadata: args.metadata,
  }
  if (typeof args.allowPromotionCodes === 'boolean') {
    sessionPayload.allow_promotion_codes = args.allowPromotionCodes
  }
  if (mode === 'payment') {
    const statementDescriptor = normalizeStatementDescriptor(args.statementDescriptor)
    if (statementDescriptor) {
      sessionPayload.payment_intent_data = { statement_descriptor: statementDescriptor }
    }
  }
  const session = await stripeClient.checkout.sessions.create(sessionPayload)
  return session
}

export const createPortalSession = async (customerId: string, returnUrl: string) => {
  if (useMock) {
    return { url: `${returnUrl}?mock_portal=1` }
  }
  const session = await stripeClient.billingPortal.sessions.create({ customer: customerId, return_url: returnUrl })
  return session
}
