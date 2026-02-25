import express from 'express'
import fetch from 'node-fetch'
import { prisma } from '../db/prisma'
import { rateLimit } from '../middleware/rateLimit'
import { requireDevAdmin } from '../middleware/requireDevAdmin'
import {
  ensureAdminTelemetryInfra,
  getAdminErrorLogs,
  getStripeWebhookEvents,
  subscribeToAdminErrorStream
} from '../services/adminTelemetry'
import {
  getRealtimeActiveUsersCount,
  getRealtimeActiveUsersSeries,
  getRealtimePresenceSessions
} from '../realtime'
import { isActiveSubscriptionStatus } from '../services/plans'

const router = express.Router()

const RANGE_MS: Record<string, number> = {
  '24h': 24 * 60 * 60 * 1000,
  '7d': 7 * 24 * 60 * 60 * 1000,
  '30d': 30 * 24 * 60 * 60 * 1000,
  '90d': 90 * 24 * 60 * 60 * 1000
}

const QUEUE_STATUSES = new Set([
  'queued',
  'uploading',
  'analyzing',
  'hooking',
  'cutting',
  'pacing',
  'subtitling',
  'audio',
  'story',
  'retention',
  'rendering'
])

type FeedbackSentiment = 'positive' | 'negative' | 'bug' | 'request'
type FeedbackItem = {
  id: string
  category: string
  sentiment: FeedbackSentiment
  source: string
  note: string | null
  createdAt: string
  jobId: string | null
}

const parseRange = (raw: unknown, fallback: keyof typeof RANGE_MS) =>
  RANGE_MS[String(raw || fallback).trim().toLowerCase()] ?? RANGE_MS[fallback]

const asMs = (value: unknown) => {
  const ms = new Date(value as any).getTime()
  return Number.isFinite(ms) ? ms : 0
}

const renderSeconds = (job: any) => {
  const start = asMs(job?.createdAt)
  const end = asMs(job?.updatedAt)
  return start > 0 && end > start ? (end - start) / 1000 : null
}

const getJobsSince = async (rangeMs: number) => {
  const floor = new Date(Date.now() - rangeMs)
  let rows: any[] = []
  try {
    rows = await prisma.job.findMany({ where: { createdAt: { gte: floor } }, orderBy: { createdAt: 'asc' } })
  } catch {
    rows = []
  }
  if (!Array.isArray(rows)) return []
  const floorMs = floor.getTime()
  return rows.filter((row) => asMs(row?.createdAt) >= floorMs || asMs(row?.updatedAt) >= floorMs)
}

const getSubscriptions = async () => {
  try {
    const rows = await prisma.subscription.findMany({ orderBy: { updatedAt: 'desc' } })
    return Array.isArray(rows) ? rows : []
  } catch {
    return []
  }
}

const getUsers = async () => {
  try {
    const rows = await prisma.user.findMany({ orderBy: { createdAt: 'desc' } })
    return Array.isArray(rows) ? rows : []
  } catch {
    return []
  }
}

const countQueue = async () => {
  try {
    const count = await prisma.job.count({ where: { status: { in: Array.from(QUEUE_STATUSES) } } })
    return Number.isFinite(Number(count)) ? Number(count) : 0
  } catch {
    const jobs = await getJobsSince(RANGE_MS['7d'])
    return jobs.filter((job) => QUEUE_STATUSES.has(String(job?.status || '').toLowerCase())).length
  }
}

const normalizeSentiment = (category: string, note?: string | null): FeedbackSentiment => {
  const key = category.toLowerCase()
  if (key === 'great_edit' || key === 'positive') return 'positive'
  if (key === 'bad_hook' || key === 'bug') return 'bug'
  if (key === 'too_fast' || key === 'request') return 'request'
  if (key === 'too_generic' || key === 'negative') return 'negative'
  const text = String(note || '').toLowerCase()
  if (/bug|error|fail|crash|broken/.test(text)) return 'bug'
  if (/feature|please|would like|need/.test(text)) return 'request'
  if (/great|good|love|awesome/.test(text)) return 'positive'
  return 'negative'
}

const extractFeedback = (jobs: any[], rangeMs: number): FeedbackItem[] => {
  const floorMs = Date.now() - rangeMs
  const out: FeedbackItem[] = []
  for (const job of jobs) {
    const analysis = ((job as any)?.analysis || {}) as Record<string, any>
    const jobId = job?.id ? String(job.id) : null
    const push = (entry: any, category: string, source: string) => {
      const createdAt = entry?.submittedAt || entry?.createdAt || job?.updatedAt || job?.createdAt
      const createdMs = asMs(createdAt)
      if (!createdMs || createdMs < floorMs) return
      const note = entry?.notes ? String(entry.notes) : null
      out.push({
        id: `${jobId || 'job'}:${source}:${category}:${createdMs}`,
        category,
        source,
        sentiment: normalizeSentiment(category, note),
        note,
        createdAt: new Date(createdMs).toISOString(),
        jobId
      })
    }
    if (analysis?.creator_feedback && typeof analysis.creator_feedback === 'object') {
      push(
        analysis.creator_feedback,
        String(analysis.creator_feedback?.category || 'creator_feedback'),
        String(analysis.creator_feedback?.source || 'creator_feedback')
      )
    }
    const history = Array.isArray(analysis?.creator_feedback_history) ? analysis.creator_feedback_history : []
    for (const entry of history) {
      push(entry, String(entry?.category || 'creator_feedback'), String(entry?.source || 'creator_feedback'))
    }
    if (analysis?.retention_feedback && typeof analysis.retention_feedback === 'object') {
      push(
        analysis.retention_feedback,
        String(analysis.retention_feedback?.sourceType || 'retention_feedback'),
        String(analysis.retention_feedback?.source || 'retention_feedback')
      )
    }
  }
  return out.sort((a, b) => asMs(b.createdAt) - asMs(a.createdAt)).slice(0, 300)
}

const aiSuggestions = async (failures: Array<{ reason: string; count: number }>) => {
  const fallback = [
    {
      title: `Harden ${failures[0]?.reason || 'top failure mode'} with deterministic fallback`,
      expectedImpact: 'Reduce render failures by 20-35%.',
      difficulty: 'Medium',
      priority: 1
    },
    {
      title: 'Optimize slow pipeline steps with cache + parallel execution',
      expectedImpact: 'Lower median render time by 15-25%.',
      difficulty: 'High',
      priority: 2
    },
    {
      title: 'Add nightly quality regression checks',
      expectedImpact: 'Catch quality regressions before deployment.',
      difficulty: 'Low',
      priority: 3
    },
    {
      title: 'Prioritize recurring user feature requests in next sprint',
      expectedImpact: 'Increase satisfaction and reduce support load.',
      difficulty: 'Medium',
      priority: 4
    },
    {
      title: 'Add stronger abandonment diagnostics between upload and render start',
      expectedImpact: 'Expose funnel drop points for UX fixes.',
      difficulty: 'Medium',
      priority: 5
    }
  ]
  const apiKey = String(process.env.OPENAI_API_KEY || '').trim()
  if (!apiKey) return fallback
  try {
    const response = await fetch('https://api.openai.com/v1/responses', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: process.env.OPENAI_ADMIN_MODEL || 'gpt-4.1-mini',
        input: `Return strict JSON array of 5 improvements. Failures: ${JSON.stringify(failures.slice(0, 6))}`,
        temperature: 0.2
      })
    })
    if (!response.ok) return fallback
    const data: any = await response.json()
    const text = String(data?.output_text || '').trim()
    const parsed = JSON.parse(text)
    return Array.isArray(parsed) && parsed.length ? parsed.slice(0, 5) : fallback
  } catch {
    return fallback
  }
}

router.use(requireDevAdmin)
router.use(rateLimit({
  windowMs: 60_000,
  max: 180,
  keyFn: (req) => `admin:${req.user?.id || req.ip || 'unknown'}`
}))
router.use(async (_req, _res, next) => {
  await ensureAdminTelemetryInfra()
  next()
})

router.get('/overview', async (_req, res) => {
  const jobs24h = await getJobsSince(RANGE_MS['24h'])
  const jobs30d = await getJobsSince(RANGE_MS['30d'])
  const subscriptions = await getSubscriptions()
  const payments7d = await getStripeWebhookEvents({ rangeMs: RANGE_MS['7d'] })
  const completed24h = jobs24h.filter((job) => String(job?.status || '').toLowerCase() === 'completed').length
  const failed24h = jobs24h.filter((job) => String(job?.status || '').toLowerCase() === 'failed').length
  const durations = jobs30d.map((job) => renderSeconds(job)).filter((v): v is number => v !== null)
  const avgRenderTime = durations.length ? Number((durations.reduce((sum, v) => sum + v, 0) / durations.length).toFixed(2)) : 0
  const successRate = completed24h + failed24h > 0 ? Number((completed24h / (completed24h + failed24h)).toFixed(4)) : 1
  const revenue7d = Number((payments7d.reduce((sum, ev) => {
    const type = String(ev?.type || '').toLowerCase()
    const amount = Number(ev?.amountCents || 0)
    if (!Number.isFinite(amount)) return sum
    if (type === 'charge.refunded') return sum - Math.abs(amount)
    if (type === 'invoice.paid' || type === 'invoice.payment_succeeded' || type === 'checkout.session.completed') return sum + amount
    return sum
  }, 0) / 100).toFixed(2))

  res.json({
    summary: {
      activeUsers: getRealtimeActiveUsersCount(),
      jobsInQueue: await countQueue(),
      jobsFailed24h: failed24h,
      revenue7d,
      activeSubscriptions: subscriptions.filter((sub) => isActiveSubscriptionStatus(String(sub?.status || '').toLowerCase())).length,
      avgRenderTime,
      successRate,
      usersTotal: (await getUsers()).length
    },
    graphs: {
      activeUsers: getRealtimeActiveUsersSeries(),
      jobSuccessVsFailure: jobs30d.map((job) => ({
        t: new Date(job?.updatedAt || job?.createdAt || Date.now()).toISOString(),
        success: String(job?.status || '').toLowerCase() === 'completed' ? 1 : 0,
        failure: String(job?.status || '').toLowerCase() === 'failed' ? 1 : 0
      })),
      jobFailureRate: jobs30d.map((job) => ({
        t: new Date(job?.updatedAt || job?.createdAt || Date.now()).toISOString(),
        v: String(job?.status || '').toLowerCase() === 'failed' ? 1 : 0
      })),
      renderTimeAvg: jobs30d.map((job) => ({
        t: new Date(job?.updatedAt || job?.createdAt || Date.now()).toISOString(),
        v: Number(renderSeconds(job) || 0)
      })),
      revenue: payments7d.map((event) => ({
        t: event.createdAt,
        v: Number((Number(event.amountCents || 0) / 100).toFixed(2))
      }))
    },
    updatedAt: new Date().toISOString()
  })
})

router.get('/errors', async (req, res) => {
  const rangeMs = parseRange(req.query.range, '24h')
  const severity = typeof req.query.severity === 'string' ? req.query.severity : null
  const items = await getAdminErrorLogs({ rangeMs, severity })
  res.json({ rangeMs, severity, total: items.length, items })
})

router.get('/realtime-users', async (_req, res) => {
  res.json({
    activeUsers: getRealtimeActiveUsersCount(),
    sessions: getRealtimePresenceSessions(),
    updatedAt: new Date().toISOString()
  })
})

router.get('/payments', async (req, res) => {
  const rangeMs = parseRange(req.query.range, '7d')
  const events = await getStripeWebhookEvents({ rangeMs })
  const paid = new Set(['invoice.paid', 'invoice.payment_succeeded', 'checkout.session.completed'])
  const flagged = new Set(['charge.refunded', 'charge.dispute.created'])
  const recentPayments = events
    .filter((event) => paid.has(String(event?.type || '').toLowerCase()))
    .map((event) => ({
      eventId: event.eventId,
      type: event.type,
      amount: Number((Number(event.amountCents || 0) / 100).toFixed(2)),
      currency: event.currency || 'USD',
      status: event.status || 'unknown',
      createdAt: event.createdAt,
      userId: event.userId || null
    }))
  const refundsOrChargebacks = events
    .filter((event) => flagged.has(String(event?.type || '').toLowerCase()))
    .map((event) => ({
      eventId: event.eventId,
      type: event.type,
      amount: Number((Math.abs(Number(event.amountCents || 0)) / 100).toFixed(2)),
      currency: event.currency || 'USD',
      status: event.status || 'flagged',
      createdAt: event.createdAt
    }))
  const revenueByDay = Array.from(events.reduce((map, event) => {
    const day = new Date(asMs(event.createdAt)).toISOString().slice(0, 10)
    const amount = Number(event.amountCents || 0)
    if (!day || !Number.isFinite(amount)) return map
    const type = String(event.type || '').toLowerCase()
    const delta = type === 'charge.refunded' ? -Math.abs(amount) : amount
    map.set(day, (map.get(day) || 0) + delta)
    return map
  }, new Map<string, number>()).entries()).map(([day, cents]) => ({ t: `${day}T00:00:00.000Z`, v: Number((cents / 100).toFixed(2)) }))
  res.json({ rangeMs, revenueTotal: Number(revenueByDay.reduce((sum, row) => sum + row.v, 0).toFixed(2)), recentPayments, refundsOrChargebacks, revenueByDay })
})

router.get('/subscriptions', async (req, res) => {
  const rangeMs = parseRange(req.query.range, '30d')
  const subs = await getSubscriptions()
  const users = await getUsers()
  const distribution = { free: 0, starter: 0, pro: 0, founder: 0 }
  const active = subs.filter((sub) => isActiveSubscriptionStatus(String(sub?.status || '').toLowerCase()))
  for (const sub of active) {
    const plan = String(sub?.planTier || 'free').toLowerCase()
    if (plan === 'starter') distribution.starter += 1
    else if (plan === 'founder') distribution.founder += 1
    else if (plan === 'creator' || plan === 'studio' || plan === 'pro') distribution.pro += 1
    else distribution.free += 1
  }
  if (users.length) distribution.free = Math.max(0, users.length - distribution.starter - distribution.pro - distribution.founder)
  const floor = Date.now() - rangeMs
  const churnCount = subs.filter((sub) => String(sub?.status || '').toLowerCase() === 'canceled' && asMs(sub?.updatedAt) >= floor).length
  const upcomingRenewals = subs.filter((sub) => {
    const end = asMs(sub?.currentPeriodEnd)
    return isActiveSubscriptionStatus(String(sub?.status || '').toLowerCase()) && end > Date.now() && end <= Date.now() + RANGE_MS['30d']
  }).map((sub) => ({ userId: sub?.userId || null, planTier: sub?.planTier || 'free', currentPeriodEnd: sub?.currentPeriodEnd || null }))
  const bucketMs = rangeMs <= RANGE_MS['7d'] ? 24 * 60 * 60 * 1000 : 3 * 24 * 60 * 60 * 1000
  const start = Date.now() - rangeMs
  const trendBuckets: number[] = []
  for (let t = Math.floor(start / bucketMs) * bucketMs; t <= Date.now(); t += bucketMs) trendBuckets.push(t)
  const trend = trendBuckets.map((bucket) => {
    const bucketEnd = bucket + bucketMs
    const value = active.filter((sub) => asMs(sub?.updatedAt || sub?.currentPeriodEnd || Date.now()) <= bucketEnd).length
    return { t: new Date(bucket).toISOString(), v: value }
  })
  res.json({ distribution, activeSubscriptions: active.length, churnCount, upcomingRenewals, trend, updatedAt: new Date().toISOString() })
})

router.get('/editor-insights', async (req, res) => {
  const rangeMs = parseRange(req.query.range, '30d')
  const jobs = await getJobsSince(rangeMs)
  const failed = jobs.filter((job) => String(job?.status || '').toLowerCase() === 'failed')
  const failureMap = failed.reduce((map: Map<string, number>, job: any) => {
    const key = String(job?.error || 'unknown_failure')
    map.set(key, (map.get(key) || 0) + 1)
    return map
  }, new Map<string, number>())
  const failures = Array.from(failureMap.entries())
    .map(([reason, count]) => ({ reason, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10)
  const feedback = extractFeedback(jobs, rangeMs)
  const dropOff = feedback.filter((item) => item.sentiment === 'negative' || item.sentiment === 'bug').reduce((map, item) => {
    map.set(item.category, (map.get(item.category) || 0) + 1)
    return map
  }, new Map<string, number>())
  const topDropOffPoints = Array.from(dropOff.entries()).map(([label, count]) => ({ label, count })).sort((a, b) => b.count - a.count).slice(0, 8)
  const mostRequestedFeatures = feedback.filter((item) => item.sentiment === 'request').reduce((map, item) => {
    map.set(item.category, (map.get(item.category) || 0) + 1)
    return map
  }, new Map<string, number>())
  const requested = Array.from(mostRequestedFeatures.entries()).map(([feature, count]) => ({ feature, count })).sort((a, b) => b.count - a.count).slice(0, 10)
  const samples = jobs.map((job) => renderSeconds(job)).filter((v): v is number => v !== null)
  const avgUploadToRender = samples.length ? Number((samples.reduce((sum, v) => sum + v, 0) / samples.length).toFixed(2)) : 0
  const abandonmentPoints = jobs.filter((job) => {
    const status = String(job?.status || '').toLowerCase()
    return status !== 'completed' && status !== 'failed' && asMs(job?.createdAt) < Date.now() - 2 * 60 * 60 * 1000
  }).length
  const qualityComplaintsCount = feedback.filter((item) => item.sentiment === 'bug' || item.sentiment === 'negative').length
  res.json({
    rangeMs,
    aggregates: {
      commonFailureReasons: failures,
      slowestPipelineSteps: [] as any[],
      averageUploadToRenderSeconds: avgUploadToRender,
      abandonmentPoints,
      qualityComplaintsCount
    },
    topDropOffPoints,
    mostRequestedFeatures: requested,
    suggestedPipelineUpgrades: await aiSuggestions(failures),
    updatedAt: new Date().toISOString()
  })
})

router.get('/feedback', async (req, res) => {
  const rangeMs = parseRange(req.query.range, '30d')
  const jobs = await getJobsSince(rangeMs)
  const items = extractFeedback(jobs, rangeMs)
  const sentimentCounts = items.reduce((acc, item) => {
    acc[item.sentiment] = (acc[item.sentiment] || 0) + 1
    return acc
  }, {} as Record<FeedbackSentiment, number>)
  const issueMap = items.reduce((map: Map<string, number>, item) => {
    map.set(item.category, (map.get(item.category) || 0) + 1)
    return map
  }, new Map<string, number>())
  const topIssues = Array.from(issueMap.entries())
    .map(([issue, count]) => ({ issue, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 15)
  res.json({ rangeMs, total: items.length, sentimentCounts, topIssues, items: items.slice(0, 200), updatedAt: new Date().toISOString() })
})

router.get('/stream', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache, no-transform')
  res.setHeader('Connection', 'keep-alive')
  res.setHeader('X-Accel-Buffering', 'no')
  ;(res as any).flushHeaders?.()

  const send = (eventName: string, payload: any) => {
    res.write(`event: ${eventName}\n`)
    res.write(`data: ${JSON.stringify(payload)}\n\n`)
  }
  const realtimePayload = async () => {
    const jobs = await getJobsSince(RANGE_MS['24h'])
    return {
      activeUsers: getRealtimeActiveUsersCount(),
      jobsInQueue: await countQueue(),
      jobsFailed24h: jobs.filter((job) => String(job?.status || '').toLowerCase() === 'failed').length,
      t: new Date().toISOString()
    }
  }

  send('ready', { ok: true, t: new Date().toISOString() })
  send('realtime', await realtimePayload())

  const unsubscribe = subscribeToAdminErrorStream((entry) => {
    send('new_error', {
      id: entry.id,
      severity: entry.severity,
      message: entry.message,
      endpoint: entry.endpoint,
      route: entry.route,
      count: entry.count,
      lastSeen: entry.lastSeen
    })
  })

  const realtimeTimer = setInterval(async () => send('realtime', await realtimePayload()), 10_000)
  const keepaliveTimer = setInterval(() => res.write(':keepalive\n\n'), 15_000)
  realtimeTimer.unref()
  keepaliveTimer.unref()
  req.on('close', () => {
    clearInterval(realtimeTimer)
    clearInterval(keepaliveTimer)
    unsubscribe()
    res.end()
  })
})

export default router
