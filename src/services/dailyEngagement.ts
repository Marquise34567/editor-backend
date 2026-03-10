import fetch from 'node-fetch'
import webpush from 'web-push'
import { prisma } from '../db/prisma'

const DAY_MS = 24 * 60 * 60 * 1000
const RETRY_INTERVAL_MS = 6 * 60 * 60 * 1000
const SCHEDULER_INTERVAL_MS = 60 * 60 * 1000
const DEFAULT_SEND_HOUR_UTC = 15
let schedulerStarted = false
let schedulerTimer: NodeJS.Timeout | null = null
let webPushInitialized = false

type DailyNudgeContent = {
  title: string
  body: string
  emailSubject: string
  emailIntro: string
  ctaLabel: string
  ctaUrl: string
}

type DailyNudgeResult = {
  processed: number
  sent: number
  failed: number
  sentEmail: number
  sentPush: number
  skipped?: 'provider_not_configured'
}

const DAILY_TIPS: Array<{ fact: string; tip: string }> = [
  { fact: 'The first 3 seconds decide most scroll behavior.', tip: 'Open on tension, not context. Start with the payoff question, then backfill setup.' },
  { fact: 'Viewers tolerate hard cuts when intent is clear.', tip: 'Cut at sentence boundaries and keep audio continuity tighter than visual continuity.' },
  { fact: 'Retention often drops right after a resolved thought.', tip: 'Insert a micro-open-loop every 8-15 seconds to keep curiosity active.' },
  { fact: 'Silent viewers rely on visual clarity over perfect audio.', tip: 'Keep captions concise and front-load meaning in each subtitle line.' },
  { fact: 'Static framing feels slower even at the same pace.', tip: 'Use deliberate punch-ins only on emphasis words or reaction beats.' },
  { fact: 'Most weak hooks fail from being generic, not too short.', tip: 'Replace broad claims with one concrete detail in the opener.' },
  { fact: 'Pacing is a pattern, not just speed.', tip: 'Alternate fast and medium sections so the edit has rhythm, not noise.' },
  { fact: 'Cut density should rise when energy rises.', tip: 'Map cut frequency to emotional intensity instead of forcing one tempo.' },
  { fact: 'Good endings improve next-video starts.', tip: 'End with a sequel cue: one unresolved thread and one clear promise.' },
  { fact: 'Over-polished edits can feel less authentic.', tip: 'Keep one human imperfection if it supports trust and personality.' },
  { fact: 'Jump cuts work best when purpose is obvious.', tip: 'Cut dead-air aggressively, but preserve setup lines before punchlines.' },
  { fact: 'Story clarity beats effect quantity.', tip: 'Use transitions only when scene intent changes, not on every cut.' },
  { fact: 'Audience drop-off is often topic-transition friction.', tip: 'Bridge transitions with one sentence that links old and new context.' },
  { fact: 'A/B opener tests teach faster than whole-video changes.', tip: 'Test two 5-8 second hooks while keeping the rest of the timeline stable.' },
  { fact: 'Long-form retention improves with periodic re-hooks.', tip: 'Place re-hook lines near predictable dips around 30%, 55%, and 75%.' },
  { fact: 'Music can hide weak pacing for only a short time.', tip: 'Fix structure first, then use music ducking to protect speech intelligibility.' },
  { fact: 'Creators often explain before proving.', tip: 'Show one result early, then explain how you got there.' },
  { fact: 'Visual contrast drives stop-power in feeds.', tip: 'Increase first-frame contrast and subject separation before posting.' },
  { fact: 'High retention needs fewer redundant sentences.', tip: 'Trim repeated meaning, not just repeated words.' },
  { fact: 'Completion rate jumps when stakes are explicit.', tip: 'State what the viewer gets if they stay until the end.' }
]

const normalizeEmail = (value: unknown) => String(value || '').trim().toLowerCase()
const isValidEmail = (value: unknown) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalizeEmail(value))

const parseIntInRange = (value: unknown, min: number, max: number, fallback: number) => {
  const parsed = Number.parseInt(String(value ?? ''), 10)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(min, Math.min(max, parsed))
}

const hashString = (value: string) => {
  let hash = 0
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(index)
    hash |= 0
  }
  return Math.abs(hash)
}

const computeNextDailySendAt = (fromDate?: Date) => {
  const from = fromDate ? new Date(fromDate) : new Date()
  const hour = parseIntInRange(process.env.DAILY_ENGAGEMENT_HOUR_UTC, 0, 23, DEFAULT_SEND_HOUR_UTC)
  const next = new Date(Date.UTC(
    from.getUTCFullYear(),
    from.getUTCMonth(),
    from.getUTCDate(),
    hour,
    0,
    0,
    0
  ))
  if (next.getTime() <= from.getTime()) {
    next.setUTCDate(next.getUTCDate() + 1)
  }
  return next
}

const getEditorUrl = () => {
  const raw = String(process.env.APP_URL || process.env.FRONTEND_URL || 'https://www.autoeditor.app').trim()
  if (!raw) return 'https://www.autoeditor.app/editor'
  try {
    const base = new URL(raw)
    return new URL('/editor', base).toString()
  } catch {
    return 'https://www.autoeditor.app/editor'
  }
}

const getTipForUserAndDay = (userId: string, date = new Date()) => {
  const daySeed = Math.floor(date.getTime() / DAY_MS)
  const tipIndex = (daySeed + hashString(userId || 'guest')) % DAILY_TIPS.length
  return DAILY_TIPS[tipIndex]
}

const buildDailyNudge = ({ userId }: { userId: string }): DailyNudgeContent => {
  const tip = getTipForUserAndDay(userId)
  const editorUrl = getEditorUrl()
  return {
    title: 'Daily Creator Nudge',
    body: `${tip.fact} ${tip.tip}`,
    emailSubject: 'Daily Creator Nudge: level up today\'s edit',
    emailIntro: `Fun fact: ${tip.fact}`,
    ctaLabel: 'Open Editor',
    ctaUrl: editorUrl
  }
}

export const getDailyEngagementProviderStatus = () => {
  const hasWebhook = Boolean(String(process.env.DAILY_ENGAGEMENT_WEBHOOK_URL || '').trim())
  const hasResend = Boolean(String(process.env.RESEND_API_KEY || '').trim())
  const vapidPublicKey = String(process.env.WEB_PUSH_VAPID_PUBLIC_KEY || '').trim()
  const hasVapidPublic = Boolean(vapidPublicKey)
  const hasVapidPrivate = Boolean(String(process.env.WEB_PUSH_VAPID_PRIVATE_KEY || '').trim())
  const hasVapidSubject = Boolean(String(process.env.WEB_PUSH_VAPID_SUBJECT || '').trim())
  return {
    emailConfigured: hasWebhook || hasResend,
    emailProvider: hasWebhook ? 'webhook' : hasResend ? 'resend' : 'none',
    webPushConfigured: hasVapidPublic && hasVapidPrivate && hasVapidSubject,
    webPushPublicKey: vapidPublicKey || null
  }
}

const ensureWebPushInitialized = () => {
  if (webPushInitialized) return
  const status = getDailyEngagementProviderStatus()
  if (!status.webPushConfigured) return
  const subject = String(process.env.WEB_PUSH_VAPID_SUBJECT || '').trim()
  const publicKey = String(process.env.WEB_PUSH_VAPID_PUBLIC_KEY || '').trim()
  const privateKey = String(process.env.WEB_PUSH_VAPID_PRIVATE_KEY || '').trim()
  webpush.setVapidDetails(subject, publicKey, privateKey)
  webPushInitialized = true
}

const sendDailyEmail = async ({
  email,
  content
}: {
  email: string
  content: DailyNudgeContent
}) => {
  const webhookUrl = String(process.env.DAILY_ENGAGEMENT_WEBHOOK_URL || '').trim()
  const text = [
    content.emailIntro,
    '',
    content.body,
    '',
    `${content.ctaLabel}: ${content.ctaUrl}`
  ].join('\n')
  const html = `
    <h2>${content.title}</h2>
    <p>${content.emailIntro}</p>
    <p>${content.body}</p>
    <p><a href="${content.ctaUrl}">${content.ctaLabel}</a></p>
  `.trim()

  if (webhookUrl) {
    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ to: email, subject: content.emailSubject, text, html })
    })
    if (!response.ok) {
      const body = await response.text().catch(() => '')
      throw new Error(`daily_engagement_webhook_failed:${response.status}:${body}`)
    }
    return
  }

  const resendKey = String(process.env.RESEND_API_KEY || '').trim()
  if (!resendKey) {
    const err: any = new Error('daily_engagement_email_provider_not_configured')
    err.code = 'daily_engagement_email_provider_not_configured'
    throw err
  }
  const from = String(process.env.REPORTS_EMAIL_FROM || 'updates@autoeditor.app').trim()
  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${resendKey}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      from,
      to: [email],
      subject: content.emailSubject,
      text,
      html
    })
  })
  if (!response.ok) {
    const body = await response.text().catch(() => '')
    throw new Error(`daily_engagement_resend_failed:${response.status}:${body}`)
  }
}

const sendDailyPush = async ({
  endpoint,
  p256dh,
  auth,
  content
}: {
  endpoint: string
  p256dh: string
  auth: string
  content: DailyNudgeContent
}) => {
  ensureWebPushInitialized()
  if (!webPushInitialized) {
    const err: any = new Error('daily_engagement_push_provider_not_configured')
    err.code = 'daily_engagement_push_provider_not_configured'
    throw err
  }

  const payload = JSON.stringify({
    title: content.title,
    body: content.body,
    url: content.ctaUrl,
    tag: 'daily-creator-nudge',
    icon: '/favicon-32x32.png'
  })

  try {
    await webpush.sendNotification({
      endpoint,
      keys: {
        p256dh,
        auth
      }
    }, payload, {
      TTL: 60 * 60,
      urgency: 'normal'
    })
  } catch (error: any) {
    const statusCode = Number(error?.statusCode || 0)
    if (statusCode === 404 || statusCode === 410) {
      const err: any = new Error('daily_engagement_push_subscription_expired')
      err.code = 'daily_engagement_push_subscription_expired'
      throw err
    }
    throw error
  }
}

const getSafeSubscription = async ({ userId, email }: { userId: string; email?: string | null }) => {
  const normalizedEmail = normalizeEmail(email)
  const fallbackEmail = isValidEmail(normalizedEmail)
    ? normalizedEmail
    : `${userId}@autoeditor.local`
  const existing = await prisma.dailyEngagementSubscription.findUnique({ where: { userId } })
  const nextSendAt = existing?.nextSendAt && new Date(existing.nextSendAt).getTime() > Date.now()
    ? existing.nextSendAt
    : computeNextDailySendAt()
  const row = await prisma.dailyEngagementSubscription.upsert({
    where: { userId },
    create: {
      userId,
      email: fallbackEmail,
      enabled: true,
      emailEnabled: true,
      pushEnabled: false,
      nextSendAt
    },
    update: {
      email: fallbackEmail,
      nextSendAt
    }
  })
  return row
}

export const getOrCreateDailyEngagementSubscription = async ({
  userId,
  email
}: {
  userId: string
  email?: string | null
}) => {
  return getSafeSubscription({ userId, email })
}

export const updateDailyEngagementPreferences = async ({
  userId,
  email,
  enabled,
  emailEnabled
}: {
  userId: string
  email?: string | null
  enabled: boolean
  emailEnabled: boolean
}) => {
  const current = await getSafeSubscription({ userId, email })
  const nextSendAt = enabled && (emailEnabled || current.pushEnabled)
    ? current.nextSendAt && new Date(current.nextSendAt).getTime() > Date.now()
      ? current.nextSendAt
      : computeNextDailySendAt()
    : null
  return prisma.dailyEngagementSubscription.update({
    where: { userId },
    data: {
      email: normalizeEmail(email) || current.email,
      enabled,
      emailEnabled,
      nextSendAt,
      lastError: null
    }
  })
}

export const upsertDailyPushSubscription = async ({
  userId,
  email,
  endpoint,
  p256dh,
  auth
}: {
  userId: string
  email?: string | null
  endpoint: string
  p256dh: string
  auth: string
}) => {
  const row = await getSafeSubscription({ userId, email })
  const nextSendAt = row.nextSendAt && new Date(row.nextSendAt).getTime() > Date.now()
    ? row.nextSendAt
    : computeNextDailySendAt()
  return prisma.dailyEngagementSubscription.update({
    where: { userId },
    data: {
      pushEnabled: true,
      enabled: true,
      pushEndpoint: endpoint,
      pushP256dh: p256dh,
      pushAuth: auth,
      nextSendAt,
      lastError: null
    }
  })
}

export const disableDailyPushSubscription = async ({
  userId,
  email
}: {
  userId: string
  email?: string | null
}) => {
  const row = await getSafeSubscription({ userId, email })
  const channelsEnabled = Boolean(row.emailEnabled)
  return prisma.dailyEngagementSubscription.update({
    where: { userId },
    data: {
      pushEnabled: false,
      pushEndpoint: null,
      pushP256dh: null,
      pushAuth: null,
      enabled: channelsEnabled,
      nextSendAt: channelsEnabled
        ? row.nextSendAt && new Date(row.nextSendAt).getTime() > Date.now()
          ? row.nextSendAt
          : computeNextDailySendAt()
        : null,
      lastError: null
    }
  })
}

const persistSendSuccess = async (userId: string, sendAt: Date) => {
  await prisma.dailyEngagementSubscription.update({
    where: { userId },
    data: {
      lastSentAt: sendAt,
      nextSendAt: computeNextDailySendAt(sendAt),
      lastError: null
    }
  })
}

const persistSendFailure = async (userId: string, err: unknown, retryAt?: Date) => {
  const message = String((err as any)?.message || err || 'send_failed').slice(0, 1000)
  await prisma.dailyEngagementSubscription
    .update({
      where: { userId },
      data: {
        lastError: message,
        nextSendAt: retryAt || new Date(Date.now() + RETRY_INTERVAL_MS)
      }
    })
    .catch(() => null)
}

export const runDueDailyEngagementDispatch = async (): Promise<DailyNudgeResult> => {
  const providerStatus = getDailyEngagementProviderStatus()
  if (!providerStatus.emailConfigured && !providerStatus.webPushConfigured) {
    return { processed: 0, sent: 0, failed: 0, sentEmail: 0, sentPush: 0, skipped: 'provider_not_configured' }
  }

  const dueNow = new Date()
  const rows = await prisma.dailyEngagementSubscription
    .findMany({
      where: {
        enabled: true,
        nextSendAt: { lte: dueNow }
      }
    })
    .catch(() => [])

  const due = Array.isArray(rows) ? rows : []
  if (!due.length) {
    return { processed: 0, sent: 0, failed: 0, sentEmail: 0, sentPush: 0 }
  }

  let sent = 0
  let failed = 0
  let sentEmail = 0
  let sentPush = 0

  for (const row of due) {
    const email = normalizeEmail(row?.email)
    const userId = String(row?.userId || '')
    const content = buildDailyNudge({ userId })
    let sentAny = false
    const errors: string[] = []

    if (row?.emailEnabled) {
      if (providerStatus.emailConfigured && isValidEmail(email)) {
        try {
          await sendDailyEmail({ email, content })
          sentAny = true
          sentEmail += 1
        } catch (err: any) {
          errors.push(String(err?.message || 'daily_email_failed'))
        }
      } else {
        errors.push('daily_email_provider_not_configured')
      }
    }

    if (row?.pushEnabled && row?.pushEndpoint && row?.pushP256dh && row?.pushAuth) {
      if (providerStatus.webPushConfigured) {
        try {
          await sendDailyPush({
            endpoint: String(row.pushEndpoint),
            p256dh: String(row.pushP256dh),
            auth: String(row.pushAuth),
            content
          })
          sentAny = true
          sentPush += 1
        } catch (err: any) {
          const code = String(err?.code || '')
          if (code === 'daily_engagement_push_subscription_expired') {
            await prisma.dailyEngagementSubscription
              .update({
                where: { userId },
                data: {
                  pushEnabled: false,
                  pushEndpoint: null,
                  pushP256dh: null,
                  pushAuth: null,
                  lastError: 'daily_engagement_push_subscription_expired'
                }
              })
              .catch(() => null)
          }
          errors.push(String(err?.message || 'daily_push_failed'))
        }
      } else {
        errors.push('daily_push_provider_not_configured')
      }
    }

    if (sentAny) {
      sent += 1
      await persistSendSuccess(userId, new Date())
    } else {
      failed += 1
      await persistSendFailure(userId, errors.join(';') || 'no_delivery_channel')
    }
  }

  return { processed: due.length, sent, failed, sentEmail, sentPush }
}

export const initDailyEngagementScheduler = () => {
  if (schedulerStarted) return
  schedulerStarted = true

  const tick = async () => {
    try {
      await runDueDailyEngagementDispatch()
    } catch (err) {
      console.warn('daily engagement scheduler tick failed', err)
    }
  }

  schedulerTimer = setInterval(() => {
    void tick()
  }, SCHEDULER_INTERVAL_MS)
  schedulerTimer.unref?.()
  void tick()
}

export const stopDailyEngagementSchedulerForTests = () => {
  if (schedulerTimer) {
    clearInterval(schedulerTimer)
    schedulerTimer = null
  }
  schedulerStarted = false
}

export const getDailyEngagementStatusForUser = async ({
  userId,
  email
}: {
  userId: string
  email?: string | null
}) => {
  const row = await getSafeSubscription({ userId, email })
  const providerStatus = getDailyEngagementProviderStatus()
  return {
    enabled: Boolean(row.enabled),
    emailEnabled: Boolean(row.emailEnabled),
    pushEnabled: Boolean(row.pushEnabled && row.pushEndpoint && row.pushP256dh && row.pushAuth),
    nextSendAt: row.nextSendAt || null,
    lastSentAt: row.lastSentAt || null,
    provider: providerStatus
  }
}
