import { prisma } from '../db/prisma'

const DAY_MS = 24 * 60 * 60 * 1000

export const getUtcDayBounds = (date: Date = new Date()) => {
  const start = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()))
  const end = new Date(start.getTime() + DAY_MS)
  const dayKey = start.toISOString().slice(0, 10)
  return { start, end, dayKey }
}

export const getRenderAttemptsForDay = async (userId: string, date: Date = new Date()) => {
  const { start, end, dayKey } = getUtcDayBounds(date)
  const rendersCount = await prisma.job.count({
    where: {
      userId,
      createdAt: {
        gte: start,
        lt: end
      }
    }
  })
  return { rendersCount, dayKey }
}
