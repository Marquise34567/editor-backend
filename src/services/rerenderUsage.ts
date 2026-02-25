import { prisma } from '../db/prisma'

const DAY_MS = 24 * 60 * 60 * 1000

const getUtcDayKey = (date: Date = new Date()) => {
  const start = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()))
  const end = new Date(start.getTime() + DAY_MS)
  const dayKey = start.toISOString().slice(0, 10)
  return { start, end, dayKey }
}

export const getRerenderUsageForDay = async (userId: string, date: Date = new Date()) => {
  const { dayKey } = getUtcDayKey(date)
  const usage = await prisma.usageDaily.findUnique({
    where: { userId_date: { userId, date: dayKey } }
  })
  return {
    dayKey,
    rerendersUsed: Number(usage?.rerenderCount ?? 0)
  }
}

export const incrementRerenderUsageForDay = async (
  userId: string,
  date: Date = new Date(),
  delta = 1
) => {
  const { dayKey } = getUtcDayKey(date)
  const usage = await prisma.usageDaily.findUnique({
    where: { userId_date: { userId, date: dayKey } }
  })
  const next = Math.max(0, Number(usage?.rerenderCount ?? 0) + Math.max(0, Math.round(delta)))
  await prisma.usageDaily.upsert({
    where: { userId_date: { userId, date: dayKey } },
    create: {
      userId,
      date: dayKey,
      rerenderCount: next
    },
    update: {
      rerenderCount: next
    }
  })
  return {
    dayKey,
    rerendersUsed: next
  }
}
