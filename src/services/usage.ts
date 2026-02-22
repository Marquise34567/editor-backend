import { prisma } from '../db/prisma'
import { getMonthKey } from '../shared/planConfig'

export const getUsageForMonth = async (userId: string, monthKey: string = getMonthKey()) => {
  const existing = await prisma.usageMonthly.findUnique({
    where: { userId_month: { userId, month: monthKey } }
  })
  if (existing) return existing
  return prisma.usageMonthly.upsert({
    where: { userId_month: { userId, month: monthKey } },
    create: { userId, month: monthKey, rendersUsed: 0, minutesUsed: 0 },
    update: {}
  })
}

export const incrementUsageForMonth = async (
  userId: string,
  monthKey: string,
  deltaRenders: number,
  deltaMinutes: number
) => {
  const existing = await getUsageForMonth(userId, monthKey)
  return prisma.usageMonthly.upsert({
    where: { userId_month: { userId, month: monthKey } },
    create: {
      userId,
      month: monthKey,
      rendersUsed: deltaRenders,
      minutesUsed: deltaMinutes
    },
    update: {
      rendersUsed: (existing?.rendersUsed ?? 0) + deltaRenders,
      minutesUsed: (existing?.minutesUsed ?? 0) + deltaMinutes
    }
  })
}
