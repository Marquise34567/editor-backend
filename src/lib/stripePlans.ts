import { getStripeConfig } from './stripeConfig'
import { type PlanTier } from '../shared/planConfig'

export const resolvePlanFromPriceId = (priceId?: string | null): PlanTier => {
  if (!priceId) return 'free'
  const { priceIds } = getStripeConfig()
  const starters = [priceIds.monthly.starter, priceIds.annual.starter, priceIds.trial].filter(Boolean)
  const creators = [priceIds.monthly.creator, priceIds.annual.creator].filter(Boolean)
  const studios = [priceIds.monthly.studio, priceIds.annual.studio].filter(Boolean)
  if (starters.includes(priceId)) return 'starter'
  if (creators.includes(priceId)) return 'creator'
  if (studios.includes(priceId)) return 'studio'
  return 'free'
}

export const isKnownPriceId = (priceId?: string | null) => resolvePlanFromPriceId(priceId) !== 'free'

export const listAllPriceIds = () => {
  const { priceIds } = getStripeConfig()
  return [
    priceIds.monthly.starter,
    priceIds.monthly.creator,
    priceIds.monthly.studio,
    priceIds.annual.starter,
    priceIds.annual.creator,
    priceIds.annual.studio,
    priceIds.trial
  ].filter(Boolean)
}
