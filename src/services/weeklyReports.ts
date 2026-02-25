import fetch from 'node-fetch'
import { prisma } from '../db/prisma'
import { getStripeWebhookEvents } from './adminTelemetry'
import { isActiveSubscriptionStatus } from './plans'
import { getRealtimeActiveUsersCount } from '../realtime'

type WeeklyStatsSnapshot = {
  generatedAt: string
  activeUsersNow: number
  usersTotal: number
  activeSubscriptions: number
  impressions7d: number
  jobsCompleted7d: number
  jobsFailed7d: number
  revenue7d: number
}

const WEEK_MS = 7 * 24 * 60 * 60 * 1000
const SCHEDULER_INTERVAL_MS = 60 * 60 * 1000
const DEFAULT_REPORT_DAY_UTC = 1
const DEFAULT_REPORT_HOUR_UTC = 15
let schedulerStarted = false
let schedulerTimer: NodeJS.Timeout | null = null

const asMs = (value: unknown) => {
  const ms = new Date(value as any).getTime()
  return Number.isFinite(ms) ? ms : 0
}

const parseIntInRange = (value: unknown, min: number, max: number, fallback: number) => {
  const parsed = Number.parseInt(String(value ?? ''), 10)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(min, Math.min(max, parsed))
}

const normalizeEmail = (value: unknown) => String(value || '').trim().toLowerCase()

export const isValidEmail = (value: unknown) =>
  /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalizeEmail(value))

export const getWeeklyReportProviderStatus = () => {
  const hasWebhook = Boolean(String(process.env.WEEKLY_REPORT_WEBHOOK_URL || '').trim())
  const hasResend = Boolean(String(process.env.RESEND_API_KEY || '').trim())
  return {
    configured: hasWebhook || hasResend,
    provider: hasWebhook ? 'webhook' : hasResend ? 'resend' : 'none'
  }
}

const computeNextWeeklySendAt = (fromDate?: Date) => {
  const from = fromDate ? new Date(fromDate) : new Date()
  const day = parseIntInRange(process.env.WEEKLY_REPORT_DAY_UTC, 0, 6, DEFAULT_REPORT_DAY_UTC)
  const hour = parseIntInRange(process.env.WEEKLY_REPORT_HOUR_UTC, 0, 23, DEFAULT_REPORT_HOUR_UTC)
  const next = new Date(Date.UTC(
    from.getUTCFullYear(),
    from.getUTCMonth(),
    from.getUTCDate(),
    hour,
    0,
    0,
    0
  ))
  const currentDay = next.getUTCDay()
  let delta = day - currentDay
  if (delta < 0) delta += 7
  next.setUTCDate(next.getUTCDate() + delta)
  if (next.getTime() <= from.getTime()) {
    next.setUTCDate(next.getUTCDate() + 7)
  }
  return next
}

const collectWeeklyStats = async (): Promise<WeeklyStatsSnapshot> => {
  const since = new Date(Date.now() - WEEK_MS)
  const [users, subscriptions, jobs, events, payments] = await Promise.all([
    prisma.user.findMany({}),
    prisma.subscription.findMany({}),
    prisma.job.findMany({ where: { createdAt: { gte: since } } }),
    prisma.siteAnalyticsEvent.findMany({
      where: { createdAt: { gte: since } },
      select: { eventName: true, category: true }
    }),
    getStripeWebhookEvents({ rangeMs: WEEK_MS })
  ])

  const paidEvents = new Set(['invoice.paid', 'invoice.payment_succeeded', 'checkout.session.completed'])
  const revenue7d = Number(
    (
      payments.reduce((sum, event) => {
        const type = String(event?.type || '').toLowerCase()
        const amount = Number(event?.amountCents || 0)
        if (!Number.isFinite(amount)) return sum
        if (type === 'charge.refunded') return sum - Math.abs(amount)
        if (paidEvents.has(type)) return sum + amount
        return sum
      }, 0) / 100
    ).toFixed(2)
  )
  const impressions7d = (Array.isArray(events) ? events : []).filter((event: any) => {
    const category = String(event?.category || '').toLowerCase()
    const eventName = String(event?.eventName || '').toLowerCase()
    return category === 'page_view' || eventName.includes('page_view') || eventName.includes('impression')
  }).length
  const completed = (Array.isArray(jobs) ? jobs : []).filter(
    (job: any) => String(job?.status || '').toLowerCase() === 'completed'
  ).length
  const failed = (Array.isArray(jobs) ? jobs : []).filter(
    (job: any) => String(job?.status || '').toLowerCase() === 'failed'
  ).length
  const activeSubscriptions = (Array.isArray(subscriptions) ? subscriptions : []).filter((sub: any) =>
    isActiveSubscriptionStatus(String(sub?.status || '').toLowerCase())
  ).length

  return {
    generatedAt: new Date().toISOString(),
    activeUsersNow: getRealtimeActiveUsersCount(),
    usersTotal: Array.isArray(users) ? users.length : 0,
    activeSubscriptions,
    impressions7d,
    jobsCompleted7d: completed,
    jobsFailed7d: failed,
    revenue7d
  }
}

const buildEmailContent = (stats: WeeklyStatsSnapshot) => {
  const subject = `Auto Editor Weekly Report - ${new Date(stats.generatedAt).toISOString().slice(0, 10)}`
  const text = [
    `Weekly stats generated at: ${stats.generatedAt}`,
    '',
    `Active users now: ${stats.activeUsersNow}`,
    `Total users: ${stats.usersTotal}`,
    `Active subscriptions: ${stats.activeSubscriptions}`,
    `Website impressions (7d): ${stats.impressions7d}`,
    `Jobs completed (7d): ${stats.jobsCompleted7d}`,
    `Jobs failed (7d): ${stats.jobsFailed7d}`,
    `Revenue (7d): $${stats.revenue7d.toFixed(2)}`
  ].join('\n')
  const html = `
    <h2>Auto Editor Weekly Report</h2>
    <p>Generated at: ${stats.generatedAt}</p>
    <ul>
      <li><strong>Active users now:</strong> ${stats.activeUsersNow}</li>
      <li><strong>Total users:</strong> ${stats.usersTotal}</li>
      <li><strong>Active subscriptions:</strong> ${stats.activeSubscriptions}</li>
      <li><strong>Website impressions (7d):</strong> ${stats.impressions7d}</li>
      <li><strong>Jobs completed (7d):</strong> ${stats.jobsCompleted7d}</li>
      <li><strong>Jobs failed (7d):</strong> ${stats.jobsFailed7d}</li>
      <li><strong>Revenue (7d):</strong> $${stats.revenue7d.toFixed(2)}</li>
    </ul>
  `.trim()
  return { subject, text, html }
}

const sendWeeklyReportEmail = async ({
  email,
  subject,
  text,
  html
}: {
  email: string
  subject: string
  text: string
  html: string
}) => {
  const webhookUrl = String(process.env.WEEKLY_REPORT_WEBHOOK_URL || '').trim()
  if (webhookUrl) {
    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ to: email, subject, text, html })
    })
    if (!response.ok) {
      const body = await response.text().catch(() => '')
      throw new Error(`weekly_report_webhook_failed:${response.status}:${body}`)
    }
    return { provider: 'webhook' as const }
  }

  const resendKey = String(process.env.RESEND_API_KEY || '').trim()
  if (!resendKey) {
    throw new Error('weekly_report_email_provider_not_configured')
  }
  const from = String(process.env.REPORTS_EMAIL_FROM || 'reports@autoeditor.app').trim()
  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${resendKey}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      from,
      to: [email],
      subject,
      text,
      html
    })
  })
  if (!response.ok) {
    const body = await response.text().catch(() => '')
    throw new Error(`weekly_report_resend_failed:${response.status}:${body}`)
  }
  return { provider: 'resend' as const }
}

const persistSendSuccess = async (email: string, sendAt: Date, actor?: string | null) => {
  const next = new Date(sendAt.getTime() + WEEK_MS)
  await prisma.weeklyReportSubscription.upsert({
    where: { email },
    create: {
      email,
      enabled: true,
      createdBy: actor || null,
      lastSentAt: sendAt,
      nextSendAt: next,
      lastError: null
    },
    update: {
      enabled: true,
      lastSentAt: sendAt,
      nextSendAt: next,
      lastError: null
    }
  })
}

const persistSendFailure = async (email: string, err: unknown) => {
  const message = String((err as any)?.message || err || 'send_failed').slice(0, 1000)
  await prisma.weeklyReportSubscription
    .update({
      where: { email },
      data: { lastError: message }
    })
    .catch(() => null)
}

export const listWeeklyReportSubscriptions = async () => {
  try {
    const rows = await prisma.weeklyReportSubscription.findMany({ orderBy: { updatedAt: 'desc' } })
    return Array.isArray(rows) ? rows : []
  } catch {
    return []
  }
}

export const upsertWeeklyReportSubscription = async ({
  email,
  enabled,
  actor
}: {
  email: string
  enabled: boolean
  actor?: string | null
}) => {
  const normalized = normalizeEmail(email)
  if (!isValidEmail(normalized)) {
    const err: any = new Error('invalid_email')
    err.code = 'invalid_email'
    throw err
  }
  const existing = await prisma.weeklyReportSubscription.findUnique({ where: { email: normalized } })
  const nextSendAt = enabled
    ? existing?.nextSendAt && asMs(existing.nextSendAt) > Date.now()
      ? existing.nextSendAt
      : computeNextWeeklySendAt()
    : null
  const row = await prisma.weeklyReportSubscription.upsert({
    where: { email: normalized },
    create: {
      email: normalized,
      enabled,
      createdBy: actor || null,
      nextSendAt
    },
    update: {
      enabled,
      nextSendAt,
      lastError: null
    }
  })
  return row
}

export const sendWeeklyReportNow = async ({
  email,
  actor
}: {
  email: string
  actor?: string | null
}) => {
  const normalized = normalizeEmail(email)
  if (!isValidEmail(normalized)) {
    const err: any = new Error('invalid_email')
    err.code = 'invalid_email'
    throw err
  }
  const stats = await collectWeeklyStats()
  const message = buildEmailContent(stats)
  const sendAt = new Date()
  const result = await sendWeeklyReportEmail({
    email: normalized,
    subject: message.subject,
    text: message.text,
    html: message.html
  })
  await persistSendSuccess(normalized, sendAt, actor || null)
  return {
    ok: true,
    email: normalized,
    provider: result.provider,
    sentAt: sendAt.toISOString(),
    stats
  }
}

export const runDueWeeklyReportDispatch = async () => {
  const dueNow = new Date()
  const rows = await prisma.weeklyReportSubscription
    .findMany({
      where: {
        enabled: true,
        nextSendAt: { lte: dueNow }
      }
    })
    .catch(() => [])
  const due = Array.isArray(rows) ? rows : []
  if (!due.length) return { processed: 0, sent: 0, failed: 0 }

  let sent = 0
  let failed = 0
  for (const row of due) {
    const email = normalizeEmail(row?.email)
    if (!isValidEmail(email)) {
      failed += 1
      await persistSendFailure(email, 'invalid_email')
      continue
    }
    try {
      const stats = await collectWeeklyStats()
      const message = buildEmailContent(stats)
      await sendWeeklyReportEmail({
        email,
        subject: message.subject,
        text: message.text,
        html: message.html
      })
      sent += 1
      await persistSendSuccess(email, new Date(), row?.createdBy || null)
    } catch (err) {
      failed += 1
      await persistSendFailure(email, err)
    }
  }
  return { processed: due.length, sent, failed }
}

export const initWeeklyReportScheduler = () => {
  if (schedulerStarted) return
  schedulerStarted = true

  const tick = async () => {
    try {
      await runDueWeeklyReportDispatch()
    } catch (err) {
      console.warn('weekly report scheduler tick failed', err)
    }
  }
  schedulerTimer = setInterval(() => {
    void tick()
  }, SCHEDULER_INTERVAL_MS)
  schedulerTimer.unref?.()
  void tick()
}

export const stopWeeklyReportSchedulerForTests = () => {
  if (schedulerTimer) {
    clearInterval(schedulerTimer)
    schedulerTimer = null
  }
  schedulerStarted = false
}
