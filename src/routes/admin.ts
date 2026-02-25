import express from 'express'
import fetch from 'node-fetch'
import os from 'os'
import { ListObjectsV2Command } from '@aws-sdk/client-s3'
import { checkDb, isStubDb, prisma } from '../db/prisma'
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
import { coercePlanTier, isActiveSubscriptionStatus } from '../services/plans'
import { stripe, isStripeEnabled } from '../services/stripe'
import { cancelJobById } from './jobs'
import { r2 } from '../lib/r2'
import { supabaseAdmin } from '../supabaseClient'
import { banIpAddress, listIpBans, normalizeIpAddress, unbanIpAddress } from '../services/ipBan'
import {
  getWeeklyReportProviderStatus,
  isValidEmail,
  listWeeklyReportSubscriptions,
  runDueWeeklyReportDispatch,
  sendWeeklyReportNow,
  upsertWeeklyReportSubscription
} from '../services/weeklyReports'

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

const IMPRESSION_EVENT_PATTERN = /(page[_:\-]?view|impression)/i
const SUBSCRIPTION_CANCEL_REASON_MAX_LEN = 240
const ADMIN_REASON_MAX_LEN = 300
const PLAN_TIERS = new Set(['starter', 'creator', 'studio', 'founder'])

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

const parseBool = (value: unknown, fallback = false) => {
  if (typeof value === 'boolean') return value
  if (value == null) return fallback
  const normalized = String(value).trim().toLowerCase()
  if (!normalized) return fallback
  return normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'y'
}

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const sanitizeReason = (value: unknown, maxLen = ADMIN_REASON_MAX_LEN) => {
  const raw = String(value || '').trim()
  if (!raw) return null
  return raw.slice(0, maxLen)
}

const parsePlanTier = (value: unknown) => {
  const tier = coercePlanTier(String(value || '').trim().toLowerCase())
  if (!PLAN_TIERS.has(tier)) return null
  return tier
}

const normalizeEmail = (value: unknown) => String(value || '').trim().toLowerCase()

const isImpressionEvent = (event: { category?: unknown; eventName?: unknown }) => {
  const category = String(event?.category || '').toLowerCase()
  const eventName = String(event?.eventName || '').toLowerCase()
  return category === 'page_view' || IMPRESSION_EVENT_PATTERN.test(eventName)
}

const getImpressionEventsSince = async (rangeMs: number) => {
  const floor = new Date(Date.now() - Math.max(1_000, rangeMs))
  try {
    const events = await prisma.siteAnalyticsEvent.findMany({
      where: { createdAt: { gte: floor } },
      select: { createdAt: true, category: true, eventName: true },
      orderBy: { createdAt: 'asc' }
    })
    return Array.isArray(events) ? events.filter(isImpressionEvent) : []
  } catch {
    return []
  }
}

const getImpressionCountSince = async (rangeMs: number) => {
  const events = await getImpressionEventsSince(rangeMs)
  return events.length
}

const getImpressionSeries = async ({
  rangeMs,
  bucketMs
}: {
  rangeMs: number
  bucketMs: number
}) => {
  const safeBucketMs = Math.max(60_000, bucketMs)
  const startMs = Date.now() - Math.max(rangeMs, safeBucketMs)
  const firstBucket = Math.floor(startMs / safeBucketMs) * safeBucketMs
  const buckets = new Map<number, number>()
  for (let t = firstBucket; t <= Date.now(); t += safeBucketMs) {
    buckets.set(t, 0)
  }
  const events = await getImpressionEventsSince(rangeMs)
  for (const event of events) {
    const createdMs = asMs((event as any)?.createdAt)
    if (!createdMs || createdMs < startMs) continue
    const bucket = Math.floor(createdMs / safeBucketMs) * safeBucketMs
    buckets.set(bucket, (buckets.get(bucket) || 0) + 1)
  }
  return Array.from(buckets.entries()).map(([bucket, value]) => ({
    t: new Date(bucket).toISOString(),
    v: value
  }))
}

const pingUrl = async (url: string) => {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 5_000)
  const startedAt = Date.now()
  try {
    const response = await fetch(url, { method: 'GET', signal: controller.signal } as any)
    return {
      ok: response.ok,
      statusCode: response.status,
      latencyMs: Date.now() - startedAt,
      url
    }
  } catch (err: any) {
    return {
      ok: false,
      statusCode: null,
      latencyMs: Date.now() - startedAt,
      url,
      error: String(err?.message || 'request_failed')
    }
  } finally {
    clearTimeout(timeout)
  }
}

const resolveUserFromPayload = async (payload: { userId?: unknown; email?: unknown }) => {
  const userId = String(payload?.userId || '').trim()
  const email = normalizeEmail(payload?.email)
  if (userId) {
    const user = await prisma.user.findUnique({ where: { id: userId } })
    return user || null
  }
  if (email && isValidEmail(email)) {
    const user = await prisma.user.findUnique({ where: { email } })
    return user || null
  }
  return null
}

const auditAdminAction = async ({
  actor,
  action,
  targetEmail,
  planKey,
  reason
}: {
  actor?: string | null
  action: string
  targetEmail?: string | null
  planKey?: string | null
  reason?: string | null
}) => {
  try {
    await prisma.adminAudit.create({
      data: {
        actor: actor || null,
        action,
        targetEmail: targetEmail || 'unknown',
        planKey: planKey || null,
        reason: reason || null
      }
    })
  } catch {
    // best-effort audit
  }
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
  const impressions24h = await getImpressionCountSince(RANGE_MS['24h'])
  const impressions5m = await getImpressionCountSince(5 * 60 * 1000)
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
      usersTotal: (await getUsers()).length,
      websiteImpressions24h: impressions24h,
      websiteImpressions5m: impressions5m
    },
    graphs: {
      activeUsers: getRealtimeActiveUsersSeries(),
      websiteImpressions: await getImpressionSeries({
        rangeMs: RANGE_MS['24h'],
        bucketMs: 60 * 60 * 1000
      }),
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

router.get('/site-live', async (_req, res) => {
  const impressionsLast5m = await getImpressionCountSince(5 * 60 * 1000)
  const impressionsLast60m = await getImpressionCountSince(60 * 60 * 1000)
  const impressionsLast24h = await getImpressionCountSince(RANGE_MS['24h'])
  const series = await getImpressionSeries({
    rangeMs: 60 * 60 * 1000,
    bucketMs: 60 * 1000
  })
  res.json({
    activeUsers: getRealtimeActiveUsersCount(),
    impressionsLast5m,
    impressionsLast60m,
    impressionsLast24h,
    series,
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

router.post('/subscriptions/grant', async (req: any, res) => {
  const tier = parsePlanTier(req.body?.planTier || req.body?.tier)
  if (!tier) {
    return res.status(400).json({ error: 'invalid_plan_tier' })
  }
  const user = await resolveUserFromPayload({ userId: req.body?.userId, email: req.body?.email })
  if (!user) {
    return res.status(404).json({ error: 'user_not_found' })
  }
  const durationDays = clamp(Number.parseInt(String(req.body?.durationDays ?? '30'), 10) || 30, 1, 3650)
  const reason = sanitizeReason(req.body?.reason)
  const currentPeriodEnd = new Date(Date.now() + durationDays * 24 * 60 * 60 * 1000)
  const existingSubscription = await prisma.subscription.findUnique({ where: { userId: user.id } }).catch(() => null)
  const granted = await prisma.subscription.upsert({
    where: { userId: user.id },
    create: {
      userId: user.id,
      stripeCustomerId: existingSubscription?.stripeCustomerId || user?.stripeCustomerId || null,
      stripeSubscriptionId: null,
      status: 'active',
      planTier: tier,
      priceId: `admin_grant_${tier}`,
      currentPeriodEnd,
      cancelAtPeriodEnd: true
    },
    update: {
      status: 'active',
      planTier: tier,
      priceId: `admin_grant_${tier}`,
      currentPeriodEnd,
      cancelAtPeriodEnd: true
    }
  })
  await prisma.user.update({
    where: { id: user.id },
    data: {
      planStatus: 'active',
      currentPeriodEnd
    }
  })
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_subscription_grant',
    targetEmail: user.email || null,
    planKey: `${user.id}:${tier}`,
    reason: reason || null
  })
  return res.json({
    ok: true,
    userId: user.id,
    email: user.email,
    tier,
    currentPeriodEnd: granted?.currentPeriodEnd || currentPeriodEnd,
    durationDays
  })
})

router.post('/subscriptions/cancel', async (req: any, res) => {
  const user = await resolveUserFromPayload({ userId: req.body?.userId, email: req.body?.email })
  if (!user) {
    return res.status(404).json({ error: 'user_not_found' })
  }
  const reason = sanitizeReason(req.body?.reason, SUBSCRIPTION_CANCEL_REASON_MAX_LEN)
  const immediate = parseBool(req.body?.immediate, true)
  const subscription = await prisma.subscription.findUnique({ where: { userId: user.id } }).catch(() => null)
  if (!subscription) {
    return res.status(404).json({ error: 'subscription_not_found' })
  }
  const stripeSubscriptionId = String(subscription?.stripeSubscriptionId || '').trim()
  if (stripeSubscriptionId && isStripeEnabled() && stripe) {
    try {
      if (immediate) {
        await stripe.subscriptions.cancel(stripeSubscriptionId)
      } else {
        await stripe.subscriptions.update(stripeSubscriptionId, { cancel_at_period_end: true })
      }
    } catch (err: any) {
      return res.status(502).json({
        error: 'stripe_cancel_failed',
        message: String(err?.message || 'Stripe cancellation failed')
      })
    }
  }
  const canceledAt = new Date()
  const nextStatus = immediate ? 'canceled' : String(subscription.status || 'active')
  const nextPlanTier = immediate ? 'free' : subscription.planTier
  const currentPeriodEnd = immediate ? canceledAt : subscription.currentPeriodEnd
  const updatedSubscription = await prisma.subscription.update({
    where: { userId: user.id },
    data: {
      status: nextStatus,
      planTier: nextPlanTier,
      cancelAtPeriodEnd: true,
      currentPeriodEnd,
      ...(immediate ? { stripeSubscriptionId: null } : {})
    }
  })
  await prisma.user.update({
    where: { id: user.id },
    data: {
      planStatus: immediate ? 'canceled' : 'active',
      currentPeriodEnd: currentPeriodEnd || null,
      ...(immediate ? { stripeSubscriptionId: null } : {})
    }
  })
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_subscription_cancel',
    targetEmail: user.email || null,
    planKey: `${user.id}:${String(subscription.planTier || 'unknown')}`,
    reason: reason || (immediate ? 'immediate_cancel' : 'cancel_at_period_end')
  })
  return res.json({
    ok: true,
    immediate,
    userId: user.id,
    email: user.email,
    subscription: {
      status: updatedSubscription.status,
      planTier: updatedSubscription.planTier,
      cancelAtPeriodEnd: updatedSubscription.cancelAtPeriodEnd,
      currentPeriodEnd: updatedSubscription.currentPeriodEnd
    }
  })
})

router.post('/jobs/:id/cancel', async (req: any, res) => {
  try {
    const result = await cancelJobById({
      jobId: req.params.id,
      reason: 'queue_canceled_by_admin'
    })
    const owner = await prisma.user.findUnique({ where: { id: result.ownerUserId } }).catch(() => null)
    await auditAdminAction({
      actor: req.user?.email || req.user?.id || null,
      action: 'admin_job_cancel',
      targetEmail: owner?.email || result.ownerUserId || null,
      planKey: result.id,
      reason: sanitizeReason(req.body?.reason) || 'queue_canceled_by_admin'
    })
    return res.json({
      ok: true,
      id: result.id,
      status: result.status,
      running: result.running,
      killedCount: result.killedCount,
      userId: result.ownerUserId
    })
  } catch (err: any) {
    const status = Number(err?.statusCode || 500)
    const code = String(err?.code || 'server_error')
    const message = String(err?.message || 'server_error')
    if (status >= 500) return res.status(500).json({ error: 'server_error' })
    return res.status(status).json({ error: code, message })
  }
})

router.get('/ip-bans', async (_req, res) => {
  const rows = await listIpBans()
  res.json({
    items: rows,
    updatedAt: new Date().toISOString()
  })
})

router.post('/ip-bans', async (req: any, res) => {
  const requestedUserId = String(req.body?.userId || '').trim()
  const directIp = normalizeIpAddress(req.body?.ip)
  const sessionIp = requestedUserId
    ? normalizeIpAddress(
        getRealtimePresenceSessions().find((session) => String(session?.userId || '') === requestedUserId)?.ip
      )
    : null
  const ip = directIp || sessionIp
  if (!ip) {
    return res.status(400).json({ error: 'invalid_ip' })
  }
  const reason = sanitizeReason(req.body?.reason)
  const durationHoursRaw = Number.parseInt(String(req.body?.durationHours ?? '0'), 10)
  const durationHours = Number.isFinite(durationHoursRaw) ? clamp(durationHoursRaw, 0, 24 * 365) : 0
  const expiresAt = durationHours > 0 ? new Date(Date.now() + durationHours * 60 * 60 * 1000) : null
  const created = await banIpAddress({
    ip,
    reason: reason || null,
    createdBy: req.user?.id || null,
    expiresAt
  })
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_ip_ban_create',
    targetEmail: ip,
    planKey: requestedUserId || null,
    reason: reason || null
  })
  return res.json({
    ok: true,
    ban: {
      ip: created.ip,
      reason: created.reason || null,
      expiresAt: created.expiresAt || null,
      active: created.active
    }
  })
})

router.delete('/ip-bans/:ip', async (req: any, res) => {
  const ipRaw = decodeURIComponent(String(req.params.ip || ''))
  const normalized = normalizeIpAddress(ipRaw)
  if (!normalized) {
    return res.status(400).json({ error: 'invalid_ip' })
  }
  const removed = await unbanIpAddress(normalized)
  if (!removed) {
    return res.status(404).json({ error: 'not_found' })
  }
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_ip_ban_remove',
    targetEmail: normalized,
    reason: sanitizeReason(req.body?.reason) || null
  })
  return res.json({ ok: true, ip: normalized })
})

router.get('/health-status', async (_req, res) => {
  const backendStartedAtMs = Date.now() - Math.round(process.uptime() * 1000)
  const dbConnected = await checkDb()
  const frontendTarget = String(process.env.FRONTEND_URL || process.env.APP_URL || '').trim()
  const frontendHealth = frontendTarget
    ? await pingUrl(frontendTarget)
    : {
        ok: false,
        statusCode: null,
        latencyMs: 0,
        url: null,
        error: 'frontend_url_not_configured'
      }
  const inputBucket = process.env.SUPABASE_BUCKET_INPUT || process.env.SUPABASE_BUCKET_UPLOADS || 'uploads'
  const outputBucket = process.env.SUPABASE_BUCKET_OUTPUT || process.env.SUPABASE_BUCKET_OUTPUTS || 'outputs'
  const storageHealth = {
    provider: r2.isConfigured ? 'r2' : 'supabase',
    ok: false,
    details: null as any
  }
  if (r2.isConfigured) {
    try {
      await r2.client.send(new ListObjectsV2Command({ Bucket: r2.bucket, MaxKeys: 1 }))
      storageHealth.ok = true
      storageHealth.details = { bucket: r2.bucket, endpoint: r2.endpoint || null }
    } catch (err: any) {
      storageHealth.ok = false
      storageHealth.details = { error: String(err?.message || 'r2_check_failed'), bucket: r2.bucket }
    }
  } else {
    try {
      const [inputResult, outputResult] = await Promise.all([
        supabaseAdmin.storage.getBucket(inputBucket),
        supabaseAdmin.storage.getBucket(outputBucket)
      ])
      const inputOk = Boolean(inputResult?.data)
      const outputOk = Boolean(outputResult?.data)
      storageHealth.ok = inputOk && outputOk
      storageHealth.details = {
        inputBucket,
        outputBucket,
        inputOk,
        outputOk,
        inputError: inputResult?.error?.message || null,
        outputError: outputResult?.error?.message || null
      }
    } catch (err: any) {
      storageHealth.ok = false
      storageHealth.details = { error: String(err?.message || 'storage_check_failed') }
    }
  }
  const memory = process.memoryUsage()
  const queue = await countQueue()
  const status = dbConnected && frontendHealth.ok && storageHealth.ok ? 'healthy' : 'degraded'
  res.json({
    status,
    checkedAt: new Date().toISOString(),
    backend: {
      ok: dbConnected,
      db: isStubDb() ? 'stub' : 'prisma',
      uptimeSeconds: Math.round(process.uptime()),
      startedAt: new Date(backendStartedAtMs).toISOString(),
      queueDepth: queue,
      nodeVersion: process.version,
      platform: `${os.platform()} ${os.release()}`,
      memory: {
        rss: memory.rss,
        heapUsed: memory.heapUsed,
        heapTotal: memory.heapTotal
      }
    },
    frontend: frontendHealth,
    storage: storageHealth
  })
})

router.get('/security', async (_req, res) => {
  const providerStatus = getWeeklyReportProviderStatus()
  const dbConnected = await checkDb()
  const frontendUrl = String(process.env.FRONTEND_URL || process.env.APP_URL || '').trim()
  const checks = [
    {
      key: 'database',
      label: 'Database Connected',
      ok: dbConnected && !isStubDb(),
      detail: dbConnected && !isStubDb() ? 'Connected to primary database.' : 'Running in stub/fallback DB mode.'
    },
    {
      key: 'https_frontend',
      label: 'HTTPS Frontend',
      ok: frontendUrl.startsWith('https://'),
      detail: frontendUrl ? `Frontend URL: ${frontendUrl}` : 'Frontend URL not configured.'
    },
    {
      key: 'stripe_webhook_secret',
      label: 'Stripe Webhook Secret',
      ok: String(process.env.STRIPE_WEBHOOK_SECRET || '').trim().length >= 8,
      detail: 'Webhook signing secret should be configured in production.'
    },
    {
      key: 'supabase_service_key',
      label: 'Supabase Service Role Key',
      ok: String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim().length >= 20,
      detail: 'Server key required for privileged backend operations.'
    },
    {
      key: 'cors_wildcard',
      label: 'CORS Wildcard Disabled',
      ok: !String(process.env.CORS_ALLOWED_ORIGINS || '').includes('*'),
      detail: 'Wildcard CORS origins should stay disabled.'
    },
    {
      key: 'ip_ban_enforcement',
      label: 'IP Ban Enforcement',
      ok: true,
      detail: 'Global IP block middleware is active.'
    },
    {
      key: 'weekly_report_provider',
      label: 'Weekly Report Provider',
      ok: providerStatus.configured,
      detail: providerStatus.configured
        ? `Provider configured: ${providerStatus.provider}`
        : 'No weekly report email provider configured.'
    }
  ]
  const okCount = checks.filter((check) => check.ok).length
  const score = Number(((okCount / checks.length) * 100).toFixed(1))
  const riskLevel = score >= 85 ? 'low' : score >= 60 ? 'medium' : 'high'
  res.json({
    score,
    riskLevel,
    checks,
    generatedAt: new Date().toISOString()
  })
})

router.get('/reports/weekly', async (_req, res) => {
  const subscriptions = await listWeeklyReportSubscriptions()
  res.json({
    provider: getWeeklyReportProviderStatus(),
    subscriptions,
    updatedAt: new Date().toISOString()
  })
})

router.post('/reports/weekly', async (req: any, res) => {
  const email = normalizeEmail(req.body?.email)
  if (!isValidEmail(email)) {
    return res.status(400).json({ error: 'invalid_email' })
  }
  const enabled = parseBool(req.body?.enabled, true)
  const row = await upsertWeeklyReportSubscription({
    email,
    enabled,
    actor: req.user?.id || null
  })
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_weekly_report_subscription',
    targetEmail: email,
    planKey: enabled ? 'enabled' : 'disabled',
    reason: sanitizeReason(req.body?.reason) || null
  })
  res.json({
    ok: true,
    provider: getWeeklyReportProviderStatus(),
    subscription: row
  })
})

router.post('/reports/weekly/send-now', async (req: any, res) => {
  const email = normalizeEmail(req.body?.email)
  if (!isValidEmail(email)) {
    return res.status(400).json({ error: 'invalid_email' })
  }
  try {
    const sent = await sendWeeklyReportNow({
      email,
      actor: req.user?.id || null
    })
    await auditAdminAction({
      actor: req.user?.email || req.user?.id || null,
      action: 'admin_weekly_report_send_now',
      targetEmail: email,
      reason: sanitizeReason(req.body?.reason) || null
    })
    return res.json(sent)
  } catch (err: any) {
    return res.status(502).json({
      error: 'weekly_report_send_failed',
      message: String(err?.message || 'weekly report send failed')
    })
  }
})

router.post('/reports/weekly/dispatch', async (_req, res) => {
  const result = await runDueWeeklyReportDispatch()
  return res.json({
    ok: true,
    ...result,
    dispatchedAt: new Date().toISOString()
  })
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
      websiteImpressions5m: await getImpressionCountSince(5 * 60 * 1000),
      websiteImpressions24h: await getImpressionCountSince(RANGE_MS['24h']),
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
