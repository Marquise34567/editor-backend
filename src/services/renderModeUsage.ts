import { prisma } from '../db/prisma'
import { getMonthKey } from '../shared/planConfig'

type RenderMode = 'horizontal' | 'vertical'

const parseRenderMode = (value?: any): RenderMode => {
  const raw = String(value || '').trim().toLowerCase()
  if (raw === 'vertical') return 'vertical'
  if (raw === 'horizontal' || raw === 'standard') return 'horizontal'
  return 'horizontal'
}

const getUtcMonthBounds = (date: Date = new Date()) => {
  const start = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1))
  const end = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1))
  return { start, end, monthKey: getMonthKey(date) }
}

export const getRenderModeUsageForMonth = async (
  userId: string,
  mode: RenderMode,
  date: Date = new Date(),
  excludeJobId?: string | null
) => {
  const { start, end, monthKey } = getUtcMonthBounds(date)
  const jobs = await prisma.job.findMany({
    where: {
      userId,
      createdAt: {
        gte: start,
        lt: end
      }
    }
  })

  const rendersCount = (Array.isArray(jobs) ? jobs : []).filter((job: any) => {
    if (excludeJobId && job?.id === excludeJobId) return false
    const createdAt = new Date(job?.createdAt || 0)
    if (Number.isNaN(createdAt.getTime())) return false
    if (createdAt < start || createdAt >= end) return false
    const renderMode = parseRenderMode((job?.analysis as any)?.renderMode)
    return renderMode === mode
  }).length

  return { rendersCount, monthKey }
}
