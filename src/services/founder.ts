import { prisma } from '../db/prisma'

const FOUNDER_INVENTORY_ID = 'founder'
export const FOUNDER_MAX_PURCHASES = 100

export type FounderAvailability = {
  maxPurchases: number
  purchasedCount: number
  remaining: number
  soldOut: boolean
}

export const getFounderInventory = async () => {
  return prisma.founderInventory.upsert({
    where: { id: FOUNDER_INVENTORY_ID },
    create: { id: FOUNDER_INVENTORY_ID, maxPurchases: FOUNDER_MAX_PURCHASES, purchasedCount: 0 },
    update: {}
  })
}

export const getFounderAvailability = async (): Promise<FounderAvailability> => {
  const inventory = await getFounderInventory()
  const maxPurchases = Number.isFinite(inventory?.maxPurchases) ? inventory.maxPurchases : FOUNDER_MAX_PURCHASES
  const purchasedCount = Number.isFinite(inventory?.purchasedCount) ? inventory.purchasedCount : 0
  const remaining = Math.max(0, maxPurchases - purchasedCount)
  const soldOut = purchasedCount >= maxPurchases
  return { maxPurchases, purchasedCount, remaining, soldOut }
}

export class FounderSoldOutError extends Error {
  status = 409
  code = 'FOUNDER_SOLD_OUT'
  constructor() {
    super('Founder plan is sold out.')
  }
}

export const ensureFounderAvailable = async () => {
  const availability = await getFounderAvailability()
  if (availability.soldOut) throw new FounderSoldOutError()
  return availability
}

export const incrementFounderPurchase = async () => {
  const inventory = await getFounderInventory()
  const maxPurchases = Number.isFinite(inventory?.maxPurchases) ? inventory.maxPurchases : FOUNDER_MAX_PURCHASES
  const result = await prisma.founderInventory.updateMany({
    where: { id: FOUNDER_INVENTORY_ID, purchasedCount: { lt: maxPurchases } },
    data: { purchasedCount: { increment: 1 } }
  })
  return result.count > 0
}
