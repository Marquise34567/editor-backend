import crypto from 'crypto'
import { EventEmitter } from 'events'
import { prisma } from '../db/prisma'

export type AdminErrorSeverity = 'low' | 'medium' | 'high' | 'critical'

export type AdminErrorLogEntry = {
  id: string
  severity: AdminErrorSeverity
  message: string
  stackSnippet: string | null
  route: string | null
  endpoint: string | null
  userId: string | null
  jobId: string | null
  count: number
  createdAt: string
  lastSeen: string
}

export type StripeWebhookEventRecord = {
  eventId: string
  type: string
  createdAt: string
  amountCents: number | null
  currency: string | null
  status: string | null
  customerId: string | null
  subscriptionId: string | null
  userId: string | null
  payload: any
}

const errorEmitter = new EventEmitter()
errorEmitter.setMaxListeners(100)

const inMemoryErrorLogs = new Map<string, AdminErrorLogEntry>()
const inMemoryStripeEvents = new Map<string, StripeWebhookEventRecord>()

let infraEnsured = false
let lastEnsureAttemptAt = 0

const canRunRawSql = () =>
  typeof (prisma as any)?.$executeRawUnsafe === 'function' &&
  typeof (prisma as any)?.$queryRawUnsafe === 'function'

const nowIso = () => new Date().toISOString()

const normalizeSeverity = (value?: string | null): AdminErrorSeverity => {
  const raw = String(value || '').trim().toLowerCase()
  if (raw === 'critical') return 'critical'
  if (raw === 'high') return 'high'
  if (raw === 'medium') return 'medium'
  return 'low'
}

const toFiniteNumber = (value: unknown): number | null => {
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

const extractAmountCents = (payload: any): number | null => {
  const candidates = [
    payload?.amount_total,
    payload?.amount_paid,
    payload?.amount_due,
    payload?.amount,
    payload?.amount_captured,
    payload?.total
  ]
  for (const candidate of candidates) {
    const value = toFiniteNumber(candidate)
    if (value !== null) return Math.round(value)
  }
  return null
}

const extractStatus = (payload: any): string | null => {
  const value = payload?.status || payload?.payment_status || payload?.billing_reason || null
  const normalized = String(value || '').trim()
  return normalized || null
}

const buildErrorFingerprint = (message: string, route?: string | null, endpoint?: string | null) => {
  const digest = crypto.createHash('sha1')
  digest.update(String(message || 'unknown_error'))
  digest.update('|')
  digest.update(String(route || 'unknown_route'))
  digest.update('|')
  digest.update(String(endpoint || 'unknown_endpoint'))
  return digest.digest('hex')
}

export const ensureAdminTelemetryInfra = async () => {
  const now = Date.now()
  if (infraEnsured) return true
  if (now - lastEnsureAttemptAt < 10_000) return false
  lastEnsureAttemptAt = now
  if (!canRunRawSql()) return false
  try {
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS admin_error_logs (
        id BIGSERIAL PRIMARY KEY,
        fingerprint TEXT NOT NULL UNIQUE,
        severity TEXT NOT NULL DEFAULT 'medium',
        message TEXT NOT NULL,
        stack_snippet TEXT NULL,
        route TEXT NULL,
        endpoint TEXT NULL,
        user_id TEXT NULL,
        job_id TEXT NULL,
        count INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS stripe_webhook_events (
        id BIGSERIAL PRIMARY KEY,
        event_id TEXT NOT NULL UNIQUE,
        event_type TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        amount_cents BIGINT NULL,
        currency TEXT NULL,
        status TEXT NULL,
        customer_id TEXT NULL,
        subscription_id TEXT NULL,
        user_id TEXT NULL,
        payload JSONB NULL
      )
    `)
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS admin_feedback (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        user_id TEXT NULL,
        source TEXT NULL,
        sentiment TEXT NULL,
        category TEXT NULL,
        message TEXT NULL
      )
    `)
    // Best-effort role/admin fields for dev admin access.
    await (prisma as any).$executeRawUnsafe(`
      ALTER TABLE profiles
      ADD COLUMN IF NOT EXISTS role TEXT DEFAULT 'USER'
    `)
    await (prisma as any).$executeRawUnsafe(`
      ALTER TABLE profiles
      ADD COLUMN IF NOT EXISTS is_dev_admin BOOLEAN DEFAULT FALSE
    `)
    infraEnsured = true
    return true
  } catch (err) {
    console.warn('ensureAdminTelemetryInfra failed; using in-memory fallbacks', err)
    return false
  }
}

export const resolveProfileAdminFlags = async (userId: string) => {
  const fallback = {
    role: 'USER',
    isDevAdmin: false
  }
  if (!userId || !canRunRawSql()) return fallback
  try {
    await ensureAdminTelemetryInfra()
    const rows = await (prisma as any).$queryRawUnsafe(
      'SELECT to_jsonb(p) AS profile FROM profiles p WHERE p.id = $1 LIMIT 1',
      userId
    )
    const payload = Array.isArray(rows) && rows.length ? (rows[0] as any)?.profile : null
    if (!payload || typeof payload !== 'object') return fallback
    const roleRaw =
      payload.role ??
      payload.dev_role ??
      payload.user_role ??
      payload.Role ??
      payload.DEV_ROLE ??
      'USER'
    const role = String(roleRaw || 'USER').trim().toUpperCase() || 'USER'
    const isDevAdmin = Boolean(
      payload.is_dev_admin ??
      payload.isDevAdmin ??
      payload.is_admin ??
      payload.isAdmin ??
      false
    )
    return {
      role,
      isDevAdmin
    }
  } catch {
    return fallback
  }
}

export const recordAdminErrorLog = async ({
  severity,
  message,
  stackSnippet,
  route,
  endpoint,
  userId,
  jobId
}: {
  severity?: string | null
  message: string
  stackSnippet?: string | null
  route?: string | null
  endpoint?: string | null
  userId?: string | null
  jobId?: string | null
}) => {
  const safeMessage = String(message || 'internal_error').trim().slice(0, 600)
  if (!safeMessage) return null
  const safeSeverity = normalizeSeverity(severity)
  const fingerprint = buildErrorFingerprint(safeMessage, route, endpoint)
  const seenAt = nowIso()
  const existing = inMemoryErrorLogs.get(fingerprint)
  const next: AdminErrorLogEntry = existing
    ? {
        ...existing,
        severity: safeSeverity,
        stackSnippet: stackSnippet || existing.stackSnippet,
        endpoint: endpoint || existing.endpoint,
        route: route || existing.route,
        userId: userId || existing.userId,
        jobId: jobId || existing.jobId,
        count: existing.count + 1,
        lastSeen: seenAt
      }
    : {
        id: fingerprint,
        severity: safeSeverity,
        message: safeMessage,
        stackSnippet: stackSnippet || null,
        route: route || null,
        endpoint: endpoint || null,
        userId: userId || null,
        jobId: jobId || null,
        count: 1,
        createdAt: seenAt,
        lastSeen: seenAt
      }
  inMemoryErrorLogs.set(fingerprint, next)
  errorEmitter.emit('error:logged', next)

  if (canRunRawSql()) {
    try {
      await ensureAdminTelemetryInfra()
      await (prisma as any).$executeRawUnsafe(
        `
          INSERT INTO admin_error_logs
            (fingerprint, severity, message, stack_snippet, route, endpoint, user_id, job_id, count, created_at, last_seen)
          VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9, $9)
          ON CONFLICT (fingerprint) DO UPDATE
          SET severity = EXCLUDED.severity,
              stack_snippet = COALESCE(EXCLUDED.stack_snippet, admin_error_logs.stack_snippet),
              route = COALESCE(EXCLUDED.route, admin_error_logs.route),
              endpoint = COALESCE(EXCLUDED.endpoint, admin_error_logs.endpoint),
              user_id = COALESCE(EXCLUDED.user_id, admin_error_logs.user_id),
              job_id = COALESCE(EXCLUDED.job_id, admin_error_logs.job_id),
              count = admin_error_logs.count + 1,
              last_seen = EXCLUDED.last_seen
        `,
        fingerprint,
        safeSeverity,
        safeMessage,
        stackSnippet || null,
        route || null,
        endpoint || null,
        userId || null,
        jobId || null,
        seenAt
      )
    } catch (err) {
      console.warn('recordAdminErrorLog db write failed', err)
    }
  }
  return next
}

export const getAdminErrorLogs = async ({
  rangeMs,
  severity
}: {
  rangeMs: number
  severity?: string | null
}) => {
  const floor = new Date(Date.now() - Math.max(60_000, rangeMs))
  const severityFilter = severity ? normalizeSeverity(severity) : null
  if (canRunRawSql()) {
    try {
      await ensureAdminTelemetryInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        `
          SELECT
            fingerprint AS id,
            severity,
            message,
            stack_snippet AS "stackSnippet",
            route,
            endpoint,
            user_id AS "userId",
            job_id AS "jobId",
            count,
            created_at AS "createdAt",
            last_seen AS "lastSeen"
          FROM admin_error_logs
          WHERE last_seen >= $1
            AND ($2::text IS NULL OR severity = $2)
          ORDER BY last_seen DESC
          LIMIT 250
        `,
        floor.toISOString(),
        severityFilter
      )
      if (Array.isArray(rows)) {
        return rows.map((row: any) => ({
          id: String(row.id),
          severity: normalizeSeverity(row.severity),
          message: String(row.message || ''),
          stackSnippet: row.stackSnippet ? String(row.stackSnippet) : null,
          route: row.route ? String(row.route) : null,
          endpoint: row.endpoint ? String(row.endpoint) : null,
          userId: row.userId ? String(row.userId) : null,
          jobId: row.jobId ? String(row.jobId) : null,
          count: Math.max(1, Number(row.count || 1)),
          createdAt: row.createdAt ? new Date(row.createdAt).toISOString() : nowIso(),
          lastSeen: row.lastSeen ? new Date(row.lastSeen).toISOString() : nowIso()
        })) as AdminErrorLogEntry[]
      }
    } catch (err) {
      console.warn('getAdminErrorLogs db read failed, using memory fallback', err)
    }
  }

  return Array.from(inMemoryErrorLogs.values())
    .filter((entry) => {
      const seen = new Date(entry.lastSeen).getTime()
      if (!Number.isFinite(seen) || seen < floor.getTime()) return false
      if (severityFilter && entry.severity !== severityFilter) return false
      return true
    })
    .sort((a, b) => new Date(b.lastSeen).getTime() - new Date(a.lastSeen).getTime())
    .slice(0, 250)
}

export const subscribeToAdminErrorStream = (handler: (entry: AdminErrorLogEntry) => void) => {
  const listener = (entry: AdminErrorLogEntry) => handler(entry)
  errorEmitter.on('error:logged', listener)
  return () => errorEmitter.off('error:logged', listener)
}

export const storeStripeWebhookEvent = async (event: any) => {
  if (!event || typeof event !== 'object') return null
  const payload = event?.data?.object ?? {}
  const record: StripeWebhookEventRecord = {
    eventId: String(event.id || `evt_${crypto.randomUUID()}`),
    type: String(event.type || 'unknown'),
    createdAt: event.created
      ? new Date(Number(event.created) * 1000).toISOString()
      : nowIso(),
    amountCents: extractAmountCents(payload),
    currency: payload?.currency ? String(payload.currency).toUpperCase() : null,
    status: extractStatus(payload),
    customerId: payload?.customer ? String(payload.customer) : null,
    subscriptionId: payload?.subscription ? String(payload.subscription) : null,
    userId: payload?.metadata?.userId ? String(payload.metadata.userId) : null,
    payload
  }
  inMemoryStripeEvents.set(record.eventId, record)

  if (canRunRawSql()) {
    try {
      await ensureAdminTelemetryInfra()
      await (prisma as any).$executeRawUnsafe(
        `
          INSERT INTO stripe_webhook_events
            (event_id, event_type, created_at, amount_cents, currency, status, customer_id, subscription_id, user_id, payload)
          VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
          ON CONFLICT (event_id) DO UPDATE
          SET event_type = EXCLUDED.event_type,
              created_at = EXCLUDED.created_at,
              amount_cents = EXCLUDED.amount_cents,
              currency = EXCLUDED.currency,
              status = EXCLUDED.status,
              customer_id = EXCLUDED.customer_id,
              subscription_id = EXCLUDED.subscription_id,
              user_id = EXCLUDED.user_id,
              payload = EXCLUDED.payload
        `,
        record.eventId,
        record.type,
        record.createdAt,
        record.amountCents,
        record.currency,
        record.status,
        record.customerId,
        record.subscriptionId,
        record.userId,
        JSON.stringify(record.payload || {})
      )
    } catch (err) {
      console.warn('storeStripeWebhookEvent db write failed', err)
    }
  }
  return record
}

export const getStripeWebhookEvents = async ({ rangeMs }: { rangeMs: number }) => {
  const floor = new Date(Date.now() - Math.max(60_000, rangeMs))
  if (canRunRawSql()) {
    try {
      await ensureAdminTelemetryInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        `
          SELECT
            event_id AS "eventId",
            event_type AS type,
            created_at AS "createdAt",
            amount_cents AS "amountCents",
            currency,
            status,
            customer_id AS "customerId",
            subscription_id AS "subscriptionId",
            user_id AS "userId",
            payload
          FROM stripe_webhook_events
          WHERE created_at >= $1
          ORDER BY created_at DESC
          LIMIT 1000
        `,
        floor.toISOString()
      )
      if (Array.isArray(rows)) {
        return rows.map((row: any) => ({
          eventId: String(row.eventId),
          type: String(row.type),
          createdAt: row.createdAt ? new Date(row.createdAt).toISOString() : nowIso(),
          amountCents: row.amountCents === null || row.amountCents === undefined ? null : Number(row.amountCents),
          currency: row.currency ? String(row.currency) : null,
          status: row.status ? String(row.status) : null,
          customerId: row.customerId ? String(row.customerId) : null,
          subscriptionId: row.subscriptionId ? String(row.subscriptionId) : null,
          userId: row.userId ? String(row.userId) : null,
          payload: row.payload ?? null
        })) as StripeWebhookEventRecord[]
      }
    } catch (err) {
      console.warn('getStripeWebhookEvents db read failed, using memory fallback', err)
    }
  }

  return Array.from(inMemoryStripeEvents.values())
    .filter((entry) => new Date(entry.createdAt).getTime() >= floor.getTime())
    .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
}

