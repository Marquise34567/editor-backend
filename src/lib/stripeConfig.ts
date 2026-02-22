export type StripeMode = 'live' | 'test'

export type StripePriceIds = {
  monthly: {
    starter: string
    creator: string
    studio: string
  }
  annual: {
    starter: string
    creator: string
    studio: string
  }
  trial: string
  founder: string
}

export type StripeConfig = {
  mode: StripeMode
  secretKey: string
  webhookSecret: string
  priceIds: StripePriceIds
}

const normalizeMode = (value?: string) => (String(value || '').toLowerCase() === 'test' ? 'test' : 'live')

const pickByMode = (mode: StripeMode, liveKey: string, testKey: string) => {
  const key = mode === 'test' ? testKey : liveKey
  return process.env[key] || ''
}

export const getStripeConfig = (): StripeConfig => {
  const mode = normalizeMode(process.env.STRIPE_MODE)
  return {
    mode,
    secretKey: pickByMode(mode, 'STRIPE_SECRET_KEY', 'STRIPE_TEST_SECRET_KEY'),
    webhookSecret: pickByMode(mode, 'STRIPE_WEBHOOK_SECRET', 'STRIPE_TEST_WEBHOOK_SECRET'),
    priceIds: {
      monthly: {
        starter: pickByMode(mode, 'STRIPE_PRICE_ID_STARTER', 'STRIPE_TEST_PRICE_ID_STARTER'),
        creator: pickByMode(mode, 'STRIPE_PRICE_ID_CREATOR', 'STRIPE_TEST_PRICE_ID_CREATOR'),
        studio: pickByMode(mode, 'STRIPE_PRICE_ID_STUDIO', 'STRIPE_TEST_PRICE_ID_STUDIO')
      },
      annual: {
        starter: pickByMode(mode, 'STRIPE_PRICE_ID_STARTER_ANNUAL', 'STRIPE_TEST_PRICE_ID_STARTER_ANNUAL'),
        creator: pickByMode(mode, 'STRIPE_PRICE_ID_CREATOR_ANNUAL', 'STRIPE_TEST_PRICE_ID_CREATOR_ANNUAL'),
        studio: pickByMode(mode, 'STRIPE_PRICE_ID_STUDIO_ANNUAL', 'STRIPE_TEST_PRICE_ID_STUDIO_ANNUAL')
      },
      trial: pickByMode(mode, 'STRIPE_PRICE_ID_TRIAL', 'STRIPE_TEST_PRICE_ID_TRIAL'),
      founder: pickByMode(mode, 'STRIPE_PRICE_ID_FOUNDER', 'STRIPE_TEST_PRICE_ID_FOUNDER')
    }
  }
}
