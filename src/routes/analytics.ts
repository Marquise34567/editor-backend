import { Router } from 'express'
import prisma from '../prisma'

const router = Router()

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const toIsoDay = (value: Date) => value.toISOString().slice(0, 10)

const normalizeString = (value: unknown, maxLen = 80) => {
  if (typeof value !== 'string') return null
  const trimmed = value.trim()
  if (!trimmed) return null
  return trimmed.slice(0, maxLen)
}

const normalizeEventName = (value: unknown) => {
  const normalized = normalizeString(value, 64)
  if (!normalized) return null
  return normalized
    .toLowerCase()
    .replace(/[^a-z0-9:_-]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 64)
}

const normalizeCategory = (value: unknown) => {
  const normalized = normalizeString(value, 32)?.toLowerCase()
  if (!normalized) return 'interaction'
  if (normalized === 'page_view') return 'page_view'
  if (normalized === 'feedback') return 'feedback'
  if (normalized === 'system') return 'system'
  return 'interaction'
}

const normalizeMetadata = (value: unknown) => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null
  try {
    const serialized = JSON.stringify(value)
    if (serialized.length > 8000) {
      return { truncated: true }
    }
    return JSON.parse(serialized)
  } catch {
    return null
  }
}

const incrementMap = (map: Map<string, number>, key: string | null | undefined, amount = 1) => {
  if (!key) return
  const normalized = key.trim()
  if (!normalized) return
  map.set(normalized, (map.get(normalized) || 0) + amount)
}

const asTopList = (map: Map<string, number>, limit = 5) => {
  const rows = Array.from(map.entries())
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, limit)
  const total = rows.reduce((sum, row) => sum + row.count, 0)
  return rows.map((row) => ({
    ...row,
    share: total > 0 ? Number((row.count / total).toFixed(4)) : 0,
  }))
}

const readJsonField = (value: unknown) => (value && typeof value === 'object' && !Array.isArray(value) ? (value as any) : null)

router.post('/track', async (req: any, res) => {
  try {
    const eventName = normalizeEventName(req.body?.eventName ?? req.body?.event)
    if (!eventName) {
      return res.status(400).json({ error: 'invalid_event_name' })
    }

    const metadata = normalizeMetadata(req.body?.metadata)
    const retentionProfile =
      normalizeString(req.body?.retentionProfile ?? req.body?.retentionStrategyProfile, 40) ??
      normalizeString(metadata?.retentionProfile ?? metadata?.retentionStrategyProfile, 40)
    const targetPlatform =
      normalizeString(req.body?.targetPlatform ?? req.body?.retentionTargetPlatform ?? req.body?.platform, 40) ??
      normalizeString(metadata?.targetPlatform ?? metadata?.retentionTargetPlatform ?? metadata?.platform, 40)
    const captionStyle =
      normalizeString(req.body?.captionStyle ?? req.body?.subtitleStyle, 80) ??
      normalizeString(metadata?.captionStyle ?? metadata?.subtitleStyle, 80)

    const created = await prisma.siteAnalyticsEvent.create({
      data: {
        userId: String(req.user?.id),
        sessionId: normalizeString(req.body?.sessionId, 80),
        eventName,
        category: normalizeCategory(req.body?.category),
        pagePath: normalizeString(req.body?.pagePath, 180),
        retentionProfile,
        targetPlatform,
        captionStyle,
        jobId: normalizeString(req.body?.jobId, 120),
        metadata: metadata ?? undefined,
      },
    })

    return res.json({ ok: true, eventId: created.id })
  } catch (err) {
    console.error('analytics track failed', err)
    return res.status(500).json({ error: 'server_error' })
  }
})

router.get('/control-panel', async (req: any, res) => {
  try {
    const requestedDays = Number(req.query?.days ?? 90)
    const rangeDays = Number.isFinite(requestedDays) ? clamp(Math.round(requestedDays), 7, 365) : 90
    const since = new Date(Date.now() - rangeDays * 24 * 60 * 60 * 1000)
    const trendDays = clamp(Math.round(rangeDays / 2), 14, 30)
    const trendStart = new Date(Date.now() - (trendDays - 1) * 24 * 60 * 60 * 1000)
    trendStart.setHours(0, 0, 0, 0)

    const [eventsRaw, jobsRaw] = await Promise.all([
      prisma.siteAnalyticsEvent.findMany({
        where: { createdAt: { gte: since } },
        orderBy: { createdAt: 'asc' },
        select: {
          userId: true,
          eventName: true,
          category: true,
          createdAt: true,
          retentionProfile: true,
          targetPlatform: true,
          captionStyle: true,
          metadata: true,
        },
      }),
      prisma.job.findMany({
        where: { createdAt: { gte: since } },
        select: {
          userId: true,
          status: true,
          retentionScore: true,
          renderSettings: true,
          analysis: true,
          createdAt: true,
        },
      }),
    ])

    const events = Array.isArray(eventsRaw) ? eventsRaw : []
    const jobs = Array.isArray(jobsRaw) ? jobsRaw : []

    const usersTracked = new Set(events.map((event: any) => String(event.userId || ''))).size
    const impressions =
      events.filter((event: any) => event.category === 'page_view' || String(event.eventName || '').includes('page_view')).length ||
      jobs.length
    const clicks =
      events.filter((event: any) => {
        const eventName = String(event.eventName || '').toLowerCase()
        return event.category === 'interaction' || /(click|select|submit|toggle|open|save|apply)/.test(eventName)
      }).length ||
      jobs.filter((job: any) => String(job.status || '').toLowerCase() === 'completed').length
    const ctr = impressions > 0 ? Number(((clicks / impressions) * 100).toFixed(2)) : 0

    const retentionScores = jobs
      .map((job: any) => Number(job.retentionScore))
      .filter((score: number) => Number.isFinite(score))
    const averageRetention = retentionScores.length
      ? retentionScores.reduce((sum: number, score: number) => sum + score, 0) / retentionScores.length
      : 88
    const position = Number(clamp(Number((averageRetention / 10).toFixed(1)), 1, 10))

    const trendMap = new Map<string, number>()
    for (let index = 0; index < trendDays; index += 1) {
      const day = new Date(trendStart)
      day.setDate(trendStart.getDate() + index)
      trendMap.set(toIsoDay(day), 0)
    }
    events.forEach((event: any) => {
      const eventDate = new Date(event.createdAt)
      if (Number.isNaN(eventDate.getTime())) return
      if (eventDate.getTime() < trendStart.getTime()) return
      const eventName = String(event.eventName || '').toLowerCase()
      const isClickLike = event.category === 'interaction' || /(click|select|submit|toggle|open|save|apply)/.test(eventName)
      if (!isClickLike) return
      const dayKey = toIsoDay(eventDate)
      trendMap.set(dayKey, (trendMap.get(dayKey) || 0) + 1)
    })
    const trend = Array.from(trendMap.entries()).map(([date, value]) => ({ date, value }))

    const retentionProfileCounts = new Map<string, number>()
    const platformCounts = new Map<string, number>()
    const captionCounts = new Map<string, number>()
    const feedbackCounts = new Map<string, number>()

    events.forEach((event: any) => {
      const metadata = readJsonField(event.metadata)
      incrementMap(
        retentionProfileCounts,
        normalizeString(event.retentionProfile ?? metadata?.retentionProfile ?? metadata?.retentionStrategyProfile, 40),
      )
      incrementMap(
        platformCounts,
        normalizeString(event.targetPlatform ?? metadata?.targetPlatform ?? metadata?.retentionTargetPlatform, 40),
      )
      incrementMap(
        captionCounts,
        normalizeString(event.captionStyle ?? metadata?.captionStyle ?? metadata?.subtitleStyle, 80),
      )
      if (event.category === 'feedback') {
        incrementMap(feedbackCounts, normalizeString(metadata?.category ?? metadata?.feedbackCategory ?? event.eventName, 60))
      }
    })

    jobs.forEach((job: any) => {
      const renderSettings = readJsonField(job.renderSettings)
      const analysis = readJsonField(job.analysis)
      incrementMap(
        retentionProfileCounts,
        normalizeString(renderSettings?.retentionStrategyProfile ?? renderSettings?.retentionStrategy, 40),
      )
      incrementMap(
        platformCounts,
        normalizeString(
          renderSettings?.retentionTargetPlatform ??
          renderSettings?.retention_target_platform ??
          renderSettings?.platform,
          40,
        ),
      )
      incrementMap(
        captionCounts,
        normalizeString(renderSettings?.subtitleStyle ?? renderSettings?.captionStyle, 80),
      )
      const creatorHistory = Array.isArray(analysis?.creator_feedback_history) ? analysis.creator_feedback_history : []
      creatorHistory.forEach((item: any) => {
        incrementMap(feedbackCounts, normalizeString(item?.category, 60))
      })
    })

    return res.json({
      ok: true,
      rangeDays,
      metrics: {
        clicks,
        impressions,
        ctr,
        position,
      },
      trend,
      topSelections: {
        retentionProfiles: asTopList(retentionProfileCounts),
        targetPlatforms: asTopList(platformCounts),
        captionStyles: asTopList(captionCounts),
      },
      feedback: asTopList(feedbackCounts, 6),
      totals: {
        usersTracked,
        events: events.length,
      },
      generatedAt: new Date().toISOString(),
    })
  } catch (err) {
    console.error('analytics control-panel failed', err)
    return res.status(500).json({ error: 'server_error' })
  }
})

export default router
