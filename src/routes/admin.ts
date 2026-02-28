import express from 'express'
import fetch from 'node-fetch'
import os from 'os'
import crypto from 'crypto'
import fs from 'fs/promises'
import path from 'path'
import { ListObjectsV2Command } from '@aws-sdk/client-s3'
import { checkDb, isStubDb, prisma } from '../db/prisma'
import { rateLimit } from '../middleware/rateLimit'
import { requireDevAdmin } from '../middleware/requireDevAdmin'
import {
  ensureAdminTelemetryInfra,
  getAdminErrorLogs,
  getStripeWebhookEvents,
  storeStripeWebhookEvent,
  subscribeToAdminErrorStream
} from '../services/adminTelemetry'
import {
  getConnectedRealtimeClientCount,
  getRealtimeActiveUsersCount,
  getRealtimeActiveUsersSeries,
  getRealtimePresenceSessions
} from '../realtime'
import { coercePlanTier, isActiveSubscriptionStatus } from '../services/plans'
import { stripe, isStripeEnabled } from '../services/stripe'
import { cancelJobById, enqueuePipeline, updateJob } from './jobs'
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
import { PLAN_CONFIG } from '../shared/planConfig'
import { getFounderAvailability } from '../services/founder'
import { summarizeRequestMetrics } from '../services/requestMetrics'
import { getFeatureLabControls, updateFeatureLabControls } from '../services/featureLab'

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

type EnrichedAdminErrorLogEntry = {
  id: string
  severity: string
  message: string
  stackSnippet: string | null
  route: string | null
  endpoint: string | null
  userId: string | null
  jobId: string | null
  count: number
  createdAt: string
  lastSeen: string
  planTier: string
  browser: string | null
  videoSizeMb: number | null
  retryable: boolean
}

type GeoLookup = {
  country: string | null
  region: string | null
  city: string | null
  latitude: number | null
  longitude: number | null
}

type LiveGeoHeatmapPoint = {
  country: string | null
  city: string | null
  latitude: number | null
  longitude: number | null
  sessions: number
  users: number
}

const DAY_MS = 24 * 60 * 60 * 1000
const YEAR_MS = 365 * DAY_MS
const SYSTEM_LATENCY_SPIKE_MS = 1500
const BANK_TAKEOUT_MAX_USD = 1000
const BANK_TAKEOUT_COOLDOWN_MS = 10 * 60 * 1000
const BANK_TAKEOUT_CURRENCY = 'usd'
const ADMIN_PROMPT_MAX_CHARS = 24_000
const ADMIN_PROMPT_TITLE_MAX_CHARS = 140
const ADMIN_PROMPT_RECENT_LIMIT = 80
const PROJECT_ROOT_DIR = path.resolve(__dirname, '../../..')
const ADMIN_PROMPT_INBOX_DIR = path.join(PROJECT_ROOT_DIR, 'output', 'vscode-prompts')
const geoCache = new Map<string, { expiresAt: number; payload: GeoLookup }>()
const bankTakeoutCooldownByActor = new Map<string, { nextAllowedAt: number; lastPayoutId: string | null; lastAmountUsd: number; lastTakeoutAt: string }>()
let cpuSample: { usage: NodeJS.CpuUsage; hrtime: bigint } | null = null

const parseRange = (raw: unknown, fallback: keyof typeof RANGE_MS) =>
  RANGE_MS[String(raw || fallback).trim().toLowerCase()] ?? RANGE_MS[fallback]

const parseStreamIntervalMs = (raw: unknown) => {
  const parsed = Number.parseInt(String(raw || '4000'), 10)
  if (!Number.isFinite(parsed)) return 4000
  return Math.max(2000, Math.min(parsed, 15000))
}

const asMs = (value: unknown) => {
  const ms = new Date(value as any).getTime()
  return Number.isFinite(ms) ? ms : 0
}

const toMb = (value: number) => Number((Math.max(0, Number(value || 0)) / (1024 * 1024)).toFixed(2))
const toGb = (value: number) => Number((Math.max(0, Number(value || 0)) / (1024 * 1024 * 1024)).toFixed(2))
const toPct = (numerator: number, denominator: number) =>
  denominator > 0 ? Number(((numerator / denominator) * 100).toFixed(1)) : 0
const toFixedNumber = (value: number, digits = 2) => Number((Number.isFinite(value) ? value : 0).toFixed(digits))

const medianOfNumbers = (values: number[]) => {
  const sorted = values
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b)
  if (!sorted.length) return 0
  const mid = Math.floor(sorted.length / 2)
  if (sorted.length % 2 === 0) return (sorted[mid - 1] + sorted[mid]) / 2
  return sorted[mid]
}

const parseIpForGeo = (value: unknown) => {
  const raw = String(value || '').trim()
  if (!raw) return null
  const normalized = raw.replace(/^::ffff:/i, '').split(',')[0].trim()
  return normalized || null
}

const isPrivateIp = (ip: string) => {
  if (!ip) return true
  if (ip === '::1' || ip === 'localhost') return true
  if (ip.includes(':') && !ip.includes('.')) return true
  if (/^10\./.test(ip)) return true
  if (/^192\.168\./.test(ip)) return true
  if (/^172\.(1[6-9]|2[0-9]|3[0-1])\./.test(ip)) return true
  if (/^127\./.test(ip)) return true
  return false
}

const browserFromUserAgent = (value: unknown) => {
  const ua = String(value || '').trim()
  if (!ua) return null
  if (/edg\//i.test(ua)) return 'Edge'
  if (/chrome\//i.test(ua) && !/edg\//i.test(ua)) return 'Chrome'
  if (/safari\//i.test(ua) && !/chrome\//i.test(ua)) return 'Safari'
  if (/firefox\//i.test(ua)) return 'Firefox'
  if (/postmanruntime/i.test(ua)) return 'Postman'
  return ua.slice(0, 80)
}

const getCpuUsagePercent = () => {
  const now = process.hrtime.bigint()
  const usage = process.cpuUsage()
  const cpuCount = Math.max(1, os.cpus().length)
  if (!cpuSample) {
    cpuSample = { usage, hrtime: now }
    const load = os.loadavg?.()?.[0] || 0
    if (!load) return 0
    return Number(clamp((load / cpuCount) * 100, 0, 100).toFixed(1))
  }
  const elapsedMs = Number(now - cpuSample.hrtime) / 1_000_000
  const delta = process.cpuUsage(cpuSample.usage)
  cpuSample = { usage, hrtime: now }
  if (!Number.isFinite(elapsedMs) || elapsedMs <= 0) return 0
  const cpuMs = (Number(delta.user || 0) + Number(delta.system || 0)) / 1000
  const pct = (cpuMs / elapsedMs) * (100 / cpuCount)
  return Number(clamp(pct, 0, 100).toFixed(1))
}

const readVideoSizeBytesFromJob = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const candidates = [
    analysis?.size,
    analysis?.input_size_bytes,
    analysis?.source_size_bytes,
    analysis?.sourceFileSizeBytes,
    analysis?.metadata_summary?.sourceSizeBytes
  ]
  for (const candidate of candidates) {
    const value = Number(candidate)
    if (Number.isFinite(value) && value > 0) return value
  }
  return 0
}

const enrichErrorItems = async (items: any[]): Promise<EnrichedAdminErrorLogEntry[]> => {
  const subscriptions = await getSubscriptions()
  const planByUserId = subscriptions.reduce((map, sub) => {
    const userId = String((sub as any)?.userId || '').trim()
    if (!userId) return map
    map.set(userId, String((sub as any)?.planTier || 'free').toLowerCase())
    return map
  }, new Map<string, string>())

  const jobIds = Array.from(
    new Set(
      items
        .map((item) => String(item?.jobId || '').trim())
        .filter(Boolean)
    )
  )
  const jobs = await Promise.all(
    jobIds.map(async (id) => {
      try {
        return await prisma.job.findUnique({ where: { id } })
      } catch {
        return null
      }
    })
  )
  const jobsById = jobs.reduce((map, job) => {
    const id = String((job as any)?.id || '').trim()
    if (!id) return map
    map.set(id, job)
    return map
  }, new Map<string, any>())

  return items.map((item) => {
    const userId = item?.userId ? String(item.userId) : null
    const jobId = item?.jobId ? String(item.jobId) : null
    const linkedJob = jobId ? jobsById.get(jobId) : null
    const status = String(linkedJob?.status || '').toLowerCase()
    return {
      id: String(item?.id || ''),
      severity: String(item?.severity || 'medium'),
      message: String(item?.message || 'unknown_error'),
      stackSnippet: item?.stackSnippet ? String(item.stackSnippet) : null,
      route: item?.route ? String(item.route) : null,
      endpoint: item?.endpoint ? String(item.endpoint) : null,
      userId,
      jobId,
      count: Math.max(1, Number(item?.count || 1)),
      createdAt: item?.createdAt ? new Date(item.createdAt).toISOString() : new Date().toISOString(),
      lastSeen: item?.lastSeen ? new Date(item.lastSeen).toISOString() : new Date().toISOString(),
      planTier: userId ? planByUserId.get(userId) || 'free' : 'free',
      browser: browserFromUserAgent((item as any)?.userAgent),
      videoSizeMb: linkedJob ? toMb(readVideoSizeBytesFromJob(linkedJob)) : null,
      retryable: Boolean(jobId && (status === 'failed' || status === 'completed'))
    }
  })
}

const getR2StorageUsage = async () => {
  if (!r2.isConfigured) {
    return {
      provider: 'supabase',
      bytes: 0,
      gb: 0,
      objects: 0,
      estimated: true,
      note: 'R2 not configured'
    }
  }
  let continuationToken: string | undefined = undefined
  let pages = 0
  let bytes = 0
  let objects = 0
  let estimated = false
  try {
    while (pages < 6) {
      const response = await r2.client.send(
        new ListObjectsV2Command({
          Bucket: r2.bucket,
          ContinuationToken: continuationToken,
          MaxKeys: 1000
        })
      )
      const rows = Array.isArray(response.Contents) ? response.Contents : []
      for (const row of rows) {
        const size = Number((row as any)?.Size || 0)
        if (!Number.isFinite(size) || size < 0) continue
        bytes += size
        objects += 1
      }
      pages += 1
      if (!response.IsTruncated || !response.NextContinuationToken) break
      continuationToken = response.NextContinuationToken
      estimated = true
    }
    return {
      provider: 'r2',
      bytes,
      gb: toGb(bytes),
      objects,
      estimated
    }
  } catch (err: any) {
    return {
      provider: 'r2',
      bytes: 0,
      gb: 0,
      objects: 0,
      estimated: true,
      error: String(err?.message || 'r2_usage_unavailable')
    }
  }
}

const lookupGeo = async (ipRaw: unknown): Promise<GeoLookup> => {
  const ip = parseIpForGeo(ipRaw)
  if (!ip || isPrivateIp(ip)) {
    return {
      country: 'Local',
      region: null,
      city: null,
      latitude: null,
      longitude: null
    }
  }
  const cached = geoCache.get(ip)
  if (cached && cached.expiresAt > Date.now()) return cached.payload
  const fallback: GeoLookup = {
    country: 'Unknown',
    region: null,
    city: null,
    latitude: null,
    longitude: null
  }
  try {
    const response = await fetch(`https://ipwho.is/${encodeURIComponent(ip)}`)
    if (!response.ok) {
      geoCache.set(ip, { expiresAt: Date.now() + 15 * 60 * 1000, payload: fallback })
      return fallback
    }
    const data: any = await response.json()
    const payload: GeoLookup = {
      country: data?.success === false ? 'Unknown' : String(data?.country || 'Unknown'),
      region: data?.success === false ? null : (data?.region ? String(data.region) : null),
      city: data?.success === false ? null : (data?.city ? String(data.city) : null),
      latitude: Number.isFinite(Number(data?.latitude)) ? Number(data.latitude) : null,
      longitude: Number.isFinite(Number(data?.longitude)) ? Number(data.longitude) : null
    }
    geoCache.set(ip, { expiresAt: Date.now() + 12 * 60 * 60 * 1000, payload })
    return payload
  } catch {
    geoCache.set(ip, { expiresAt: Date.now() + 15 * 60 * 1000, payload: fallback })
    return fallback
  }
}

const buildLiveGeoHeatmap = async (
  sessions: Array<any>,
  limit = 30
): Promise<LiveGeoHeatmapPoint[]> => {
  const liveMapRows = await Promise.all(
    sessions.map(async (session) => {
      const geo = await lookupGeo((session as any)?.ip)
      return {
        sessionId: String((session as any)?.sessionId || ''),
        userId: String((session as any)?.userId || ''),
        country: geo.country,
        city: geo.city,
        latitude: geo.latitude,
        longitude: geo.longitude
      }
    })
  )

  return Array.from(
    liveMapRows.reduce((map, row) => {
      const key = `${row.country || 'Unknown'}|${row.city || '-'}|${row.latitude || '-'}|${row.longitude || '-'}`
      const existing = map.get(key) || {
        country: row.country,
        city: row.city,
        latitude: row.latitude,
        longitude: row.longitude,
        sessions: 0,
        users: new Set<string>()
      }
      existing.sessions += 1
      if (row.userId) existing.users.add(row.userId)
      map.set(key, existing)
      return map
    }, new Map<string, any>()).values()
  )
    .map((row) => ({
      country: row.country,
      city: row.city,
      latitude: row.latitude,
      longitude: row.longitude,
      sessions: row.sessions,
      users: row.users.size
    }))
    .sort((a, b) => b.sessions - a.sessions)
    .slice(0, Math.max(1, limit))
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

const resolveAdminActorKey = (req: any) => {
  const userId = String(req?.user?.id || '').trim()
  const email = String(req?.user?.email || '').trim().toLowerCase()
  const ip = String(req?.ip || '').trim()
  return userId || email || ip || 'unknown'
}

const parseUsdAmount = (value: unknown) => {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return null
  const rounded = Math.round(parsed * 100) / 100
  if (rounded <= 0) return null
  return rounded
}

const slugifyToken = (value: string, fallback = 'prompt') => {
  const normalized = String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
  return normalized || fallback
}

const sanitizePromptTitle = (value: unknown) => {
  const raw = String(value || '').trim()
  if (!raw) return 'Untitled Prompt'
  return raw.slice(0, ADMIN_PROMPT_TITLE_MAX_CHARS)
}

const sanitizePromptBody = (value: unknown) =>
  String(value || '')
    .replace(/\r\n?/g, '\n')
    .trim()
    .slice(0, ADMIN_PROMPT_MAX_CHARS)

const ensurePromptInboxDir = async () => {
  await fs.mkdir(ADMIN_PROMPT_INBOX_DIR, { recursive: true })
}

const toProjectRelativePath = (absolutePath: string) =>
  path.relative(PROJECT_ROOT_DIR, absolutePath).replace(/\\/g, '/')

const resolveSafeProjectFile = (value: unknown) => {
  const raw = String(value || '').trim().replace(/\\/g, '/')
  if (!raw) return null
  if (raw.startsWith('/') || /^[a-z]:/i.test(raw)) return null
  const resolved = path.resolve(PROJECT_ROOT_DIR, raw)
  const rootWithSep = `${path.resolve(PROJECT_ROOT_DIR)}${path.sep}`
  if (!resolved.startsWith(rootWithSep)) return null
  return resolved
}

const loadRecentPromptInboxEntries = async () => {
  await ensurePromptInboxDir()
  const fileNames = await fs.readdir(ADMIN_PROMPT_INBOX_DIR)
  const jsonFiles = fileNames.filter((name) => name.endsWith('.json'))
  const parsed = await Promise.all(
    jsonFiles.map(async (name) => {
      const absolutePath = path.join(ADMIN_PROMPT_INBOX_DIR, name)
      try {
        const raw = await fs.readFile(absolutePath, 'utf8')
        const payload = JSON.parse(raw) as any
        const createdAt = new Date(payload?.createdAt || payload?.updatedAt || Date.now()).toISOString()
        return {
          id: String(payload?.id || name.replace(/\.json$/i, '')),
          title: sanitizePromptTitle(payload?.title),
          promptPreview: sanitizePromptBody(payload?.prompt).slice(0, 320),
          targetPath: payload?.targetPath ? String(payload.targetPath) : null,
          inboxPath: toProjectRelativePath(absolutePath),
          createdAt,
          createdBy: payload?.createdBy ? String(payload.createdBy) : null
        }
      } catch {
        return null
      }
    })
  )
  return parsed
    .filter((entry): entry is {
      id: string
      title: string
      promptPreview: string
      targetPath: string | null
      inboxPath: string
      createdAt: string
      createdBy: string | null
    } => Boolean(entry))
    .sort((a, b) => asMs(b.createdAt) - asMs(a.createdAt))
    .slice(0, ADMIN_PROMPT_RECENT_LIMIT)
}

const parseTakeoutAmountFromAuditReason = (reason: unknown) => {
  const raw = String(reason || '').trim().toLowerCase()
  const match = raw.match(/^takeout_([0-9]+(?:\.[0-9]{1,2})?)_/)
  if (!match) return null
  const parsed = Number(match[1])
  return Number.isFinite(parsed) ? parsed : null
}

const cleanExpiredTakeoutCooldown = (actorKey: string) => {
  const existing = bankTakeoutCooldownByActor.get(actorKey)
  if (!existing) return null
  if (existing.nextAllowedAt <= Date.now()) {
    bankTakeoutCooldownByActor.delete(actorKey)
    return null
  }
  return existing
}

const readTakeoutCooldownFromAudit = async (actor: string | null) => {
  const normalizedActor = String(actor || '').trim()
  if (!normalizedActor) return null
  try {
    const rows = await prisma.adminAudit.findMany({
      where: { action: 'admin_bank_takeout' },
      orderBy: { createdAt: 'desc' },
      take: 25
    } as any)
    const list = Array.isArray(rows) ? rows : []
    const latest = list
      .filter((row: any) => String(row?.action || '') === 'admin_bank_takeout')
      .filter((row: any) => String(row?.actor || '').trim() === normalizedActor)
      .sort((a: any, b: any) => asMs(b?.createdAt) - asMs(a?.createdAt))[0]
    if (!latest) return null
    const createdMs = asMs((latest as any)?.createdAt)
    if (!createdMs) return null
    const nextAllowedAt = createdMs + BANK_TAKEOUT_COOLDOWN_MS
    if (nextAllowedAt <= Date.now()) return null
    return {
      nextAllowedAt,
      lastPayoutId: String((latest as any)?.planKey || '').trim() || null,
      lastAmountUsd: parseTakeoutAmountFromAuditReason((latest as any)?.reason),
      lastTakeoutAt: new Date(createdMs).toISOString()
    }
  } catch {
    return null
  }
}

const readAvailableUsdBalance = async () => {
  if (!isStripeEnabled() || !stripe) return null
  try {
    const balance = await stripe.balance.retrieve()
    const available = Array.isArray(balance?.available) ? balance.available : []
    const usd = available.find((row: any) => String(row?.currency || '').toLowerCase() === BANK_TAKEOUT_CURRENCY)
    const amountCents = Math.max(0, Number(usd?.amount || 0))
    return Number((amountCents / 100).toFixed(2))
  } catch {
    return null
  }
}

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

const buildDistributionCurve = (scores: number[]) => {
  const buckets = [
    { label: '0-20', min: 0, max: 20 },
    { label: '21-40', min: 21, max: 40 },
    { label: '41-60', min: 41, max: 60 },
    { label: '61-80', min: 61, max: 80 },
    { label: '81-100', min: 81, max: 100 }
  ]
  return buckets.map((bucket) => ({
    label: bucket.label,
    count: scores.filter((score) => score >= bucket.min && score <= bucket.max).length
  }))
}

const getPlanMonthlyPrice = (tier: string) => {
  const key = String(tier || '').toLowerCase() as keyof typeof PLAN_CONFIG
  const plan = (PLAN_CONFIG as any)[key]
  return Number.isFinite(Number(plan?.priceMonthly)) ? Number(plan.priceMonthly) : 0
}

const dayIso = (value: number | string | Date) => {
  const ms = typeof value === 'number' ? value : new Date(value as any).getTime()
  if (!Number.isFinite(ms)) return new Date().toISOString().slice(0, 10)
  return new Date(ms).toISOString().slice(0, 10)
}

const formatDayPoint = (day: string, value: number) => ({
  t: `${day}T00:00:00.000Z`,
  v: Number((Number.isFinite(value) ? value : 0).toFixed(3))
})

const inferHookScore = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const explicit = Number(
    analysis?.hook_score ??
      analysis?.hookStrength ??
      analysis?.hook_strength ??
      analysis?.hook_strength_score ??
      NaN
  )
  if (Number.isFinite(explicit)) return clamp(explicit, 0, 100)
  const retention = Number(job?.retentionScore || 0)
  if (!Number.isFinite(retention)) return 0
  return clamp(Math.round(retention * 0.92 + 6), 0, 100)
}

const inferFirst8SecEngagement = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const explicit = Number(
    analysis?.first8_sec_engagement ??
      analysis?.first8sEngagement ??
      analysis?.engagement_first_8s ??
      analysis?.intro_engagement ??
      NaN
  )
  if (Number.isFinite(explicit)) return clamp(explicit, 0, 100)
  const hookScore = inferHookScore(job)
  const retention = Number(job?.retentionScore || 0)
  return clamp(Number((hookScore * 0.65 + retention * 0.35).toFixed(1)), 0, 100)
}

const inferEmotionalIntensity = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const explicit = Number(
    analysis?.emotional_intensity ??
      analysis?.emotion_score ??
      analysis?.emotionIntensity ??
      analysis?.energy_score ??
      NaN
  )
  if (Number.isFinite(explicit)) return clamp(explicit, 0, 100)
  const hookScore = inferHookScore(job)
  const retention = Number(job?.retentionScore || 0)
  return clamp(Number((hookScore * 0.45 + retention * 0.55).toFixed(1)), 0, 100)
}

const inferPredictedRetention = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const explicit = Number(
    analysis?.predicted_retention ??
      analysis?.predictedRetention ??
      analysis?.retention_prediction_score ??
      NaN
  )
  if (Number.isFinite(explicit)) return clamp(explicit, 0, 100)
  const retention = Number(job?.retentionScore || 0)
  const hook = inferHookScore(job)
  const engagement8s = inferFirst8SecEngagement(job)
  return clamp(Number((retention * 0.5 + hook * 0.3 + engagement8s * 0.2).toFixed(1)), 0, 100)
}

const inferStoryCoherenceScore = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const explicit = Number(
    analysis?.story_coherence_score ??
      analysis?.storyCoherenceScore ??
      analysis?.narrative_score ??
      NaN
  )
  if (Number.isFinite(explicit)) return clamp(explicit, 0, 100)
  const retention = Number(job?.retentionScore || 0)
  const pacing = inferPacingScore(job)
  return clamp(Number((retention * 0.65 + pacing * 0.35).toFixed(1)), 0, 100)
}

const inferPacingScore = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const explicit = Number(
    analysis?.pacing_score ??
      analysis?.pacingScore ??
      analysis?.edit_pacing ??
      NaN
  )
  if (Number.isFinite(explicit)) return clamp(explicit, 0, 100)
  const duration = Number(job?.inputDurationSeconds || 0)
  if (!Number.isFinite(duration) || duration <= 0) {
    const retention = Number(job?.retentionScore || 0)
    return clamp(retention, 0, 100)
  }
  const ideal = duration < 60 ? 42 : duration < 600 ? 55 : 66
  const retention = Number(job?.retentionScore || 0)
  return clamp(Number(((retention * 0.7) + ideal * 0.3).toFixed(1)), 0, 100)
}

const inferViralityScore = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const explicit = Number(
    analysis?.virality_probability ??
      analysis?.viralityScore ??
      analysis?.viral_score ??
      NaN
  )
  if (Number.isFinite(explicit)) return clamp(explicit, 0, 100)
  const retention = Number(job?.retentionScore || 0)
  const hook = inferHookScore(job)
  const engagement8s = inferFirst8SecEngagement(job)
  return clamp(Number((retention * 0.45 + hook * 0.35 + engagement8s * 0.2).toFixed(1)), 0, 100)
}

const parseEmotionMoments = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const candidates = [
    analysis?.emotion_spikes,
    analysis?.emotionalSpikes,
    analysis?.peak_moments,
    analysis?.retention_peaks
  ]
  for (const candidate of candidates) {
    if (!Array.isArray(candidate)) continue
    const points = candidate
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value) && value >= 0)
      .slice(0, 8)
    if (points.length) return points
  }
  const duration = Number(job?.inputDurationSeconds || 0)
  if (!Number.isFinite(duration) || duration <= 0) return []
  return [0.18, 0.39, 0.62, 0.84].map((ratio) => Number((ratio * duration).toFixed(1)))
}

const normalizeFeedbackPercent = (value: unknown) => {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return null
  if (numeric >= 0 && numeric <= 1) return Number((numeric * 100).toFixed(2))
  return Number(clamp(numeric, 0, 100).toFixed(2))
}

const readRetentionFeedbackSignals = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const direct = analysis?.retention_feedback && typeof analysis.retention_feedback === 'object'
    ? analysis.retention_feedback
    : null
  const history = Array.isArray(analysis?.retention_feedback_history)
    ? analysis.retention_feedback_history
    : []
  const latestHistory = history.length ? history[history.length - 1] : null
  const payload = (direct || latestHistory || null) as Record<string, any> | null
  if (!payload) {
    return {
      watchPct: null as number | null,
      hookHoldPct: null as number | null,
      completionPct: null as number | null,
      rewatchPct: null as number | null
    }
  }
  return {
    watchPct: normalizeFeedbackPercent(
      payload.watchPercent ??
      payload.watch_percent ??
      payload.avgWatchPercent ??
      payload.averageWatchPercent
    ),
    hookHoldPct: normalizeFeedbackPercent(
      payload.hookHoldPercent ??
      payload.hook_hold_percent ??
      payload.first8sRetention ??
      payload.hookRetention
    ),
    completionPct: normalizeFeedbackPercent(
      payload.completionPercent ??
      payload.completion_percent ??
      payload.finishRate
    ),
    rewatchPct: normalizeFeedbackPercent(
      payload.rewatchRate ??
      payload.rewatch_rate ??
      payload.loopRate
    )
  }
}

const readJobDurationSeconds = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const metadataSummary = (analysis?.metadata_summary || {}) as Record<string, any>
  const timeline = (metadataSummary?.timeline || {}) as Record<string, any>
  const candidates = [
    job?.inputDurationSeconds,
    timeline?.sourceDurationSeconds,
    analysis?.durationSeconds,
    analysis?.duration_seconds
  ]
  for (const candidate of candidates) {
    const value = Number(candidate)
    if (Number.isFinite(value) && value > 0) return clamp(value, 0.1, 60 * 60 * 8)
  }
  return null
}

type TimelineMetricEvent = {
  t: number
  type: string
}

const readTimelineEvents = (job: any): TimelineMetricEvent[] => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const timeline = analysis?.edit_decision_timeline as Record<string, any> | null
  const eventsRaw = Array.isArray(timeline?.events) ? timeline.events : []
  return eventsRaw
    .map((entry: any) => ({
      t: Number(entry?.t),
      type: String(entry?.type || '').trim().toLowerCase()
    }))
    .filter((entry) => Number.isFinite(entry.t) && entry.t >= 0 && Boolean(entry.type))
}

const getTimelineEventWeight = (eventType: string) => {
  const key = String(eventType || '').toLowerCase()
  if (key === 'hook') return 1.3
  if (key === 'pattern_interrupt') return 1.15
  if (key === 'cut') return 0.92
  if (key === 'zoom') return 0.72
  if (key === 'caption_emphasis') return 0.64
  if (key === 'auto_escalation') return 0.86
  if (key === 'broll') return 0.46
  return 0.4
}

const getBucketIndex = (timeSeconds: number, durationSeconds: number, bucketCount: number) => {
  if (!Number.isFinite(timeSeconds) || !Number.isFinite(durationSeconds) || durationSeconds <= 0 || bucketCount <= 0) {
    return -1
  }
  const ratio = clamp(timeSeconds / Math.max(durationSeconds, 0.01), 0, 0.999999)
  return Math.min(bucketCount - 1, Math.max(0, Math.floor(ratio * bucketCount)))
}

const readTimelineFeatureSignals = (job: any) => {
  const analysis = (job?.analysis || {}) as Record<string, any>
  const metadataSummary = (analysis?.metadata_summary || {}) as Record<string, any>
  const retentionSummary = (metadataSummary?.retention || {}) as Record<string, any>
  const styleTimelineFeatures = (retentionSummary?.styleTimelineFeatures || analysis?.style_timeline_features || {}) as Record<string, any>
  if (!styleTimelineFeatures || typeof styleTimelineFeatures !== 'object') {
    return {
      captionRatePer10s: null as number | null,
      energySpikeDensityPer10s: null as number | null,
      patternSpacingSeconds: null as number | null,
      cutsPer10s: null as number | null
    }
  }
  const asNumber = (value: unknown) => {
    const numeric = Number(value)
    return Number.isFinite(numeric) ? numeric : null
  }
  return {
    captionRatePer10s: asNumber(
      styleTimelineFeatures.captionEmphasisRatePer10Seconds ??
      styleTimelineFeatures.caption_emphasis_rate_per_10_seconds
    ),
    energySpikeDensityPer10s: asNumber(
      styleTimelineFeatures.energySpikeDensityPer10Seconds ??
      styleTimelineFeatures.energy_spike_density_per_10_seconds
    ),
    patternSpacingSeconds: asNumber(
      styleTimelineFeatures.patternInterruptSpacingSeconds ??
      styleTimelineFeatures.pattern_interrupt_spacing_seconds
    ),
    cutsPer10s: asNumber(
      styleTimelineFeatures.cutsPer10Seconds ??
      styleTimelineFeatures.cuts_per_10_seconds
    )
  }
}

const groupErrorsBySignature = (items: EnrichedAdminErrorLogEntry[]) => {
  const map = new Map<string, { type: string; count: number; severity: string; lastSeen: string }>()
  for (const item of items) {
    const key = `${item.severity}:${item.message}`
    const existing = map.get(key) || {
      type: item.message,
      count: 0,
      severity: item.severity,
      lastSeen: item.lastSeen
    }
    existing.count += Number(item.count || 1)
    if (asMs(item.lastSeen) > asMs(existing.lastSeen)) existing.lastSeen = item.lastSeen
    map.set(key, existing)
  }
  return Array.from(map.values())
    .sort((a, b) => b.count - a.count || asMs(b.lastSeen) - asMs(a.lastSeen))
    .slice(0, 20)
}

const estimatedRunwayMonths = ({
  cashReserveUsd,
  monthlyRevenueUsd,
  monthlyBurnUsd
}: {
  cashReserveUsd: number
  monthlyRevenueUsd: number
  monthlyBurnUsd: number
}) => {
  const netBurn = Math.max(0, monthlyBurnUsd - monthlyRevenueUsd)
  if (!Number.isFinite(netBurn) || netBurn <= 0) return 999
  return Number((cashReserveUsd / netBurn).toFixed(1))
}

const buildCommandCenterPayload = async () => {
  const [jobs24h, jobs7d, jobs30d, subscriptions, users, stripe30d, allErrors24h, featureLab, founderAvailability] =
    await Promise.all([
      getJobsSince(RANGE_MS['24h']),
      getJobsSince(RANGE_MS['7d']),
      getJobsSince(RANGE_MS['30d']),
      getSubscriptions(),
      getUsers(),
      getStripeWebhookEvents({ rangeMs: RANGE_MS['30d'] }),
      getAdminErrorLogs({ rangeMs: RANGE_MS['24h'], severity: null }),
      getFeatureLabControls(),
      getFounderAvailability()
    ])

  const completed30d = jobs30d.filter((job) => String(job?.status || '').toLowerCase() === 'completed')
  const failed24h = jobs24h.filter((job) => String(job?.status || '').toLowerCase() === 'failed')
  const failedReasons = Array.from(
    failed24h.reduce((map: Map<string, number>, job: any) => {
      const reason = String(job?.error || 'unknown_failure')
      map.set(reason, (map.get(reason) || 0) + 1)
      return map
    }, new Map<string, number>()).entries()
  )
    .map(([reason, count]) => ({ reason, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10)

  const queueLength = await countQueue()
  const memory = process.memoryUsage()
  const totalMem = os.totalmem()
  const requestSummary = summarizeRequestMetrics({
    rangeMs: RANGE_MS['24h'],
    latencySpikeMs: SYSTEM_LATENCY_SPIKE_MS
  })
  const storageUsage = await getR2StorageUsage()
  const webhookEvents24h = stripe30d.filter((event) => asMs(event?.createdAt) >= Date.now() - RANGE_MS['24h'])
  const latestWebhook = webhookEvents24h[0] || stripe30d[0] || null
  const webhookHealthy = Boolean(
    String(process.env.STRIPE_WEBHOOK_SECRET || '').trim() &&
      latestWebhook &&
      asMs(latestWebhook.createdAt) >= Date.now() - RANGE_MS['7d']
  )

  const sessions = getRealtimePresenceSessions()
  const activeUsersOnSite = getRealtimeActiveUsersCount()
  const sessionsByUser = sessions.reduce((map, session) => {
    const userId = String((session as any)?.userId || '').trim()
    if (!userId) return map
    const existing = map.get(userId) || []
    existing.push(session)
    map.set(userId, existing)
    return map
  }, new Map<string, any[]>())

  const latestJobByUser = jobs30d.reduce((map, job) => {
    const userId = String(job?.userId || '').trim()
    if (!userId) return map
    const existing = map.get(userId)
    if (!existing || asMs(job?.updatedAt || job?.createdAt) > asMs(existing?.updatedAt || existing?.createdAt)) {
      map.set(userId, job)
    }
    return map
  }, new Map<string, any>())

  const currentlyRenderingUsers = Array.from(sessionsByUser.keys()).filter((userId) => {
    const latest = latestJobByUser.get(userId)
    const status = String(latest?.status || '').toLowerCase()
    return QUEUE_STATUSES.has(status)
  }).length

  const exportingWindowMs = 12 * 60 * 1000
  const currentlyExportingUsers = Array.from(sessionsByUser.keys()).filter((userId) => {
    const latest = latestJobByUser.get(userId)
    const status = String(latest?.status || '').toLowerCase()
    const updatedMs = asMs(latest?.updatedAt || latest?.createdAt)
    return status === 'completed' && updatedMs > Date.now() - exportingWindowMs
  }).length

  const avgSessionMinutes = sessions.length
    ? Number(
        (
          sessions.reduce((sum, session) => {
            const connectedMs = asMs((session as any)?.connectedAt)
            if (!connectedMs) return sum
            return sum + Math.max(0, (Date.now() - connectedMs) / 60_000)
          }, 0) / sessions.length
        ).toFixed(1)
      )
    : 0

  const liveMap = await buildLiveGeoHeatmap(sessions)

  const activeSubscriptions = subscriptions.filter((sub) =>
    isActiveSubscriptionStatus(String(sub?.status || '').toLowerCase())
  )
  const activeByTier = activeSubscriptions.reduce((map, sub) => {
    const tier = String((sub as any)?.planTier || 'free').toLowerCase()
    map[tier] = (map[tier] || 0) + 1
    return map
  }, {} as Record<string, number>)
  const recurringActive = activeSubscriptions.filter(
    (sub) => String((sub as any)?.planTier || '').toLowerCase() !== 'founder'
  )
  const mrr = Number(
    recurringActive
      .reduce((sum, sub) => sum + getPlanMonthlyPrice(String((sub as any)?.planTier || 'free')), 0)
      .toFixed(2)
  )
  const arr = Number((mrr * 12).toFixed(2))
  const churnCount30d = subscriptions.filter(
    (sub) =>
      String((sub as any)?.status || '').toLowerCase() === 'canceled' &&
      asMs((sub as any)?.updatedAt) >= Date.now() - RANGE_MS['30d']
  ).length
  const churnRate = toPct(churnCount30d, activeSubscriptions.length + churnCount30d)
  const arpu = recurringActive.length ? mrr / recurringActive.length : 0
  const churnRateRatio = churnRate > 0 ? churnRate / 100 : 0
  const ltv = Number((churnRateRatio > 0 ? arpu / churnRateRatio : arpu * 24).toFixed(2))
  const estimatedMarketingSpend = Number(process.env.ESTIMATED_MONTHLY_MARKETING_SPEND_USD || 0)
  const newPaidSubs30d = subscriptions.filter((sub) => {
    const planTier = String((sub as any)?.planTier || '').toLowerCase()
    return planTier !== 'free' && asMs((sub as any)?.updatedAt) >= Date.now() - RANGE_MS['30d']
  }).length
  const cacEstimate = Number((newPaidSubs30d > 0 ? estimatedMarketingSpend / newPaidSubs30d : 0).toFixed(2))

  const refunds = stripe30d.filter((event) => String(event?.type || '').toLowerCase() === 'charge.refunded')
  const failedPayments = stripe30d.filter((event) => /payment_failed/.test(String(event?.type || '').toLowerCase()))
  const revenueByPlan = activeSubscriptions.reduce((map, sub) => {
    const tier = String((sub as any)?.planTier || 'free').toLowerCase()
    map[tier] = Number(((map[tier] || 0) + getPlanMonthlyPrice(tier)).toFixed(2))
    return map
  }, {} as Record<string, number>)

  const renderDurations = completed30d.map((job) => renderSeconds(job)).filter((value): value is number => value !== null)
  const avgRenderTimeSec = renderDurations.length
    ? Number((renderDurations.reduce((sum, value) => sum + value, 0) / renderDurations.length).toFixed(2))
    : 0
  const fileSizes = completed30d.map((job) => readVideoSizeBytesFromJob(job)).filter((value) => value > 0)
  const avgFileSizeMb = fileSizes.length
    ? Number((fileSizes.reduce((sum, value) => sum + value, 0) / fileSizes.length / (1024 * 1024)).toFixed(2))
    : 0
  const retentionScores = completed30d
    .map((job) => Number((job as any)?.retentionScore))
    .filter((value) => Number.isFinite(value))
  const avgRetentionScore = retentionScores.length
    ? Number((retentionScores.reduce((sum, value) => sum + value, 0) / retentionScores.length).toFixed(1))
    : 0
  const subtitlesCount = completed30d.filter((job) => Boolean((job as any)?.renderSettings?.autoCaptions)).length
  const hookDetectedCount = completed30d.filter((job) => {
    const analysis = ((job as any)?.analysis || {}) as Record<string, any>
    return Number.isFinite(Number(analysis?.hook_start_time)) || Array.isArray(analysis?.hook_candidates)
  }).length
  const autoZoomCount = completed30d.filter((job) => Boolean((job as any)?.renderSettings?.smartZoom)).length
  const verticalCount = completed30d.filter((job) => {
    const mode = String((job as any)?.renderSettings?.mode || '').toLowerCase()
    return mode === 'vertical'
  }).length

  const feedbackItems = extractFeedback(jobs30d, RANGE_MS['30d'])
  const feedbackByJobId = feedbackItems.reduce((map, entry) => {
    const key = String(entry?.jobId || '').trim()
    if (!key) return map
    const list = map.get(key) || []
    list.push(entry)
    map.set(key, list)
    return map
  }, new Map<string, FeedbackItem[]>())
  const feedbackHeatmap = Array.from(
    feedbackItems.reduce((map, item) => {
      const key = String(item.category || 'unknown')
      map.set(key, (map.get(key) || 0) + 1)
      return map
    }, new Map<string, number>()).entries()
  )
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10)
  const requestedFeaturesTop = Array.from(
    feedbackItems
      .filter((item) => item.sentiment === 'request')
      .reduce((map, item) => {
        const key = String(item.category || 'unknown')
        map.set(key, (map.get(key) || 0) + 1)
        return map
      }, new Map<string, number>()).entries()
  )
    .map(([feature, count]) => ({ feature, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 12)
  const ratingSamples = feedbackItems.map((item) => {
    if (item.sentiment === 'positive') return 5
    if (item.sentiment === 'request') return 3
    if (item.sentiment === 'negative') return 2
    return 1
  })
  const averageUserRating = ratingSamples.length
    ? Number((ratingSamples.reduce((sum, value) => sum + value, 0) / ratingSamples.length).toFixed(2))
    : 0
  const hookSuccessEvaluations = completed30d
    .map((job) => {
      const analysis = ((job as any)?.analysis || {}) as Record<string, any>
      const hasHook = Number.isFinite(Number(analysis?.hook_score)) || Number.isFinite(Number(analysis?.hook_start_time))
      if (!hasHook) return null
      const feedbackSignals = readRetentionFeedbackSignals(job)
      const retentionScore = Number((job as any)?.retentionScore ?? 0)
      const successFromFeedback = (
        (feedbackSignals.hookHoldPct !== null && feedbackSignals.hookHoldPct >= 60) ||
        (feedbackSignals.watchPct !== null && feedbackSignals.watchPct >= 52) ||
        (feedbackSignals.completionPct !== null && feedbackSignals.completionPct >= 46)
      )
      if (successFromFeedback) return 1
      if (Number.isFinite(retentionScore)) return retentionScore >= 70 ? 1 : 0
      return 0
    })
    .filter((value): value is 0 | 1 => value === 0 || value === 1)
  const hookSuccessRate = hookSuccessEvaluations.length
    ? toPct(hookSuccessEvaluations.reduce<number>((sum, value) => sum + value, 0), hookSuccessEvaluations.length)
    : 0

  const predictionComparisons: number[] = completed30d
    .map((job) => {
      const predictedDropRisk = inferPredictedRetention(job) < 70
      const feedbackSignals = readRetentionFeedbackSignals(job)
      const observedDropFromFeedback =
        (feedbackSignals.completionPct !== null && feedbackSignals.completionPct < 44) ||
        (feedbackSignals.watchPct !== null && feedbackSignals.watchPct < 48) ||
        (feedbackSignals.hookHoldPct !== null && feedbackSignals.hookHoldPct < 40)
      const jobFeedback = feedbackByJobId.get(String((job as any)?.id || '')) || []
      const observedDropFromSentiment = jobFeedback.some((entry) => entry.sentiment === 'negative' || entry.sentiment === 'bug')
      let actualDropSignal = observedDropFromFeedback || observedDropFromSentiment
      if (!observedDropFromFeedback && !observedDropFromSentiment) {
        const score = Number((job as any)?.retentionScore ?? NaN)
        if (Number.isFinite(score)) actualDropSignal = score < 62
      }
      return predictedDropRisk === actualDropSignal ? 1 : 0
    })
    .map((value) => Number(value))
  const dropOffPredictionAccuracy = predictionComparisons.length
    ? Number(
        (
          (predictionComparisons.reduce((sum, value) => sum + value, 0) / predictionComparisons.length) *
          100
        ).toFixed(1)
      )
    : 0

  const enrichedErrors = await enrichErrorItems(allErrors24h)
  const failedUploads24h = failed24h.filter((job) => /upload|multipart|r2|supabase|input_file/i.test(String(job?.error || ''))).length
  const failedWebhooks24h = webhookEvents24h.filter((event) => /payment_failed|dispute/i.test(String(event?.type || '').toLowerCase())).length

  const events30d = await prisma.siteAnalyticsEvent
    .findMany({
      where: { createdAt: { gte: new Date(Date.now() - RANGE_MS['30d']) } },
      select: {
        userId: true,
        eventName: true,
        category: true,
        createdAt: true
      },
      orderBy: { createdAt: 'asc' }
    })
    .catch(() => [] as any[])

  const shareEvents = events30d.filter((event: any) => /share/.test(String(event?.eventName || '').toLowerCase()))
  const downloadSignals = feedbackItems.filter((item) => /download/i.test(String(item.source || item.category || '')))
  const usersByEventTime: Map<string, number[]> = events30d.reduce((map: Map<string, number[]>, event: any) => {
    const userId = String(event?.userId || '').trim()
    if (!userId) return map
    const rows = map.get(userId) || []
    rows.push(asMs(event?.createdAt))
    map.set(userId, rows)
    return map
  }, new Map<string, number[]>())
  const returningUsers24h = Array.from(usersByEventTime.values()).filter((times) => {
    const sorted = times.filter((time) => Number.isFinite(time)).sort((a, b) => a - b)
    for (let index = 1; index < sorted.length; index += 1) {
      const gap = sorted[index] - sorted[index - 1]
      if (gap > 5 * 60 * 1000 && gap <= DAY_MS) return true
    }
    return false
  }).length
  const avgVideosPerUser = users.length ? Number((jobs30d.length / users.length).toFixed(2)) : 0

  const visitors = await getImpressionCountSince(RANGE_MS['30d'])
  const signups = users.filter((user) => asMs((user as any)?.createdAt) >= Date.now() - RANGE_MS['30d']).length
  const uploads = jobs30d.length
  const renders = completed30d.length
  const downloads = downloadSignals.length
  const subscribed = subscriptions.filter(
    (sub) =>
      String((sub as any)?.planTier || '').toLowerCase() !== 'free' &&
      asMs((sub as any)?.updatedAt) >= Date.now() - RANGE_MS['30d']
  ).length

  const massiveUploaderSignals = Array.from(
    jobs30d.reduce((map: Map<string, number>, job: any) => {
      const userId = String(job?.userId || '').trim()
      if (!userId) return map
      const sizeBytes = readVideoSizeBytesFromJob(job)
      if (sizeBytes < 500 * 1024 * 1024) return map
      map.set(userId, (map.get(userId) || 0) + 1)
      return map
    }, new Map<string, number>()).entries()
  )
    .map(([userId, count]) => ({ userId, count }))
    .filter((row) => row.count >= 2)
    .sort((a, b) => b.count - a.count)
    .slice(0, 12)

  const ipToUsers = sessions.reduce((map, session) => {
    const ip = parseIpForGeo((session as any)?.ip) || 'unknown'
    const userId = String((session as any)?.userId || '').trim()
    if (!userId) return map
    const bucket = map.get(ip) || new Set<string>()
    bucket.add(userId)
    map.set(ip, bucket)
    return map
  }, new Map<string, Set<string>>())
  const multipleAccountsSameIp = Array.from(ipToUsers.entries())
    .filter(([ip, usersSet]) => ip !== 'unknown' && usersSet.size > 1)
    .map(([ip, usersSet]) => ({ ip, accounts: usersSet.size }))
    .sort((a, b) => b.accounts - a.accounts)
    .slice(0, 20)
  const stripeFraudFlags = stripe30d
    .filter((event) => /dispute|fraud/i.test(String(event?.type || '')))
    .slice(0, 20)
    .map((event) => ({
      eventId: event.eventId,
      type: event.type,
      createdAt: event.createdAt
    }))

  const abnormalUsageByUser = jobs24h.reduce((map: Map<string, number>, job: any) => {
    const userId = String(job?.userId || '').trim()
    if (!userId) return map
    map.set(userId, (map.get(userId) || 0) + 1)
    return map
  }, new Map<string, number>())
  const abnormalUsageUsers = Array.from(
    abnormalUsageByUser.entries()
  )
    .map(([userId, jobs]) => ({ userId, jobs }))
    .filter((row) => row.jobs >= 12)
    .sort((a, b) => b.jobs - a.jobs)
    .slice(0, 12)

  const hooksThisWeek = jobs7d
    .map((job: any) => {
      const analysis = (job?.analysis || {}) as Record<string, any>
      const text = String(analysis?.hook_text || '').trim()
      const score = Number(job?.retentionScore || 0)
      if (!text) return null
      return { text, score }
    })
    .filter((value): value is { text: string; score: number } => Boolean(value))
  const topHooksGenerated = Array.from(
    hooksThisWeek.reduce((map, row) => {
      const existing = map.get(row.text) || { text: row.text, uses: 0, avgScore: 0 }
      existing.uses += 1
      existing.avgScore += row.score
      map.set(row.text, existing)
      return map
    }, new Map<string, { text: string; uses: number; avgScore: number }>()).values()
  )
    .map((row) => ({
      text: row.text,
      uses: row.uses,
      avgScore: Number((row.avgScore / Math.max(1, row.uses)).toFixed(1))
    }))
    .sort((a, b) => b.avgScore - a.avgScore || b.uses - a.uses)
    .slice(0, 8)

  const emotionalPatternMap = completed30d.reduce((map: Map<string, { pattern: string; count: number; score: number }>, job: any) => {
    const analysis = (job?.analysis || {}) as Record<string, any>
    const key = String(
      analysis?.retentionStrategyProfile ||
      analysis?.retentionStrategy ||
      analysis?.style_profile ||
      'unknown'
    )
    const existing = map.get(key) || { pattern: key, count: 0, score: 0 }
    existing.count += 1
    existing.score += Number(job?.retentionScore || 0)
    map.set(key, existing)
    return map
  }, new Map<string, { pattern: string; count: number; score: number }>())
  const emotionalPatterns = Array.from(emotionalPatternMap.values())
    .map((row) => ({
      pattern: row.pattern,
      count: row.count,
      avgScore: Number((row.score / Math.max(1, row.count)).toFixed(1))
    }))
    .sort((a, b) => b.avgScore - a.avgScore)
    .slice(0, 8)

  const suspiciousActivityScore = Number(
    clamp(
      massiveUploaderSignals.length * 12 +
      multipleAccountsSameIp.length * 18 +
      requestSummary.tokenAbuseSignals.length * 20 +
      stripeFraudFlags.length * 25 +
      abnormalUsageUsers.length * 10,
      0,
      100
    ).toFixed(1)
  )

  const bestEmotionalPacingPattern = emotionalPatterns.length ? emotionalPatterns[0].pattern : 'insufficient_data'
  const scoreDistributionCurve = buildDistributionCurve(retentionScores)
  const aiAutoSuggestions = [
    avgRetentionScore < 70 ? 'Users prefer faster cuts and stronger first-3-second hooks.' : null,
    hookSuccessRate < 55 ? 'Hook success rate is low; test experimental hook logic in Feature Lab.' : null,
    dropOffPredictionAccuracy < 60 ? 'Prediction accuracy is weak; collect more explicit negative feedback labels.' : null,
    verticalCount > completed30d.length * 0.6
      ? 'Vertical mode dominates usage. Prioritize short-form pacing presets.'
      : null,
    failedReasons[0]?.count > 4
      ? `Top failure reason is ${failedReasons[0]?.reason}. Add a deterministic fallback path.`
      : null
  ].filter((value): value is string => Boolean(value))

  const allJobs = await prisma.job
    .findMany({
      orderBy: { createdAt: 'desc' },
      take: 5000
    })
    .catch(() => jobs30d)
  const exportedJobs = allJobs.filter((job) => String((job as any)?.status || '').toLowerCase() === 'completed')
  const totalMinutesProcessed = Number(
    (
      exportedJobs.reduce((sum, job) => sum + Math.max(0, Number((job as any)?.inputDurationSeconds || 0)), 0) /
      60
    ).toFixed(1)
  )
  const totalGbProcessed = Number(
    (
      exportedJobs.reduce((sum, job) => sum + readVideoSizeBytesFromJob(job), 0) /
      (1024 * 1024 * 1024)
    ).toFixed(2)
  )
  const estimatedHoursSaved = Number((totalMinutesProcessed * 0.58 / 60).toFixed(1))
  const mostViralGeneratedClip = exportedJobs
    .map((job: any) => ({
      jobId: String(job?.id || ''),
      score: Number(job?.retentionScore || 0),
      hook: String((job?.analysis as any)?.hook_text || '').trim() || null
    }))
    .sort((a, b) => b.score - a.score)[0] || null

  const predictedRetentionScores = completed30d.map((job) => inferPredictedRetention(job))
  const avgPredictedRetentionScore = predictedRetentionScores.length
    ? Number((predictedRetentionScores.reduce((sum, value) => sum + value, 0) / predictedRetentionScores.length).toFixed(1))
    : 0
  const hookStrengthScore = completed30d.length
    ? Number((completed30d.reduce((sum, job) => sum + inferHookScore(job), 0) / completed30d.length).toFixed(1))
    : 0
  const strongHookVideoPct = toPct(
    completed30d.filter((job) => inferHookScore(job) >= 72).length,
    Math.max(1, completed30d.length)
  )
  const avgFirst8SecEngagementScore = completed30d.length
    ? Number((completed30d.reduce((sum, job) => sum + inferFirst8SecEngagement(job), 0) / completed30d.length).toFixed(1))
    : 0
  const dropOffRiskPrediction = Number((100 - avgPredictedRetentionScore).toFixed(1))

  const emotionalIntensityGraph = completed30d
    .slice(-28)
    .map((job) => ({
      t: new Date(job?.updatedAt || job?.createdAt || Date.now()).toISOString(),
      v: inferEmotionalIntensity(job)
    }))

  const boringSegments = ['0-10%', '10-20%', '20-30%', '30-40%', '40-50%', '50-60%', '60-70%', '70-80%', '80-90%', '90-100%']
  const boringFallbackRisk = [0.18, 0.26, 0.38, 0.54, 0.62, 0.57, 0.46, 0.35, 0.26, 0.2]
  const boringBucketAccumulator = boringSegments.map((segment, index) => ({
    segment,
    index,
    riskSum: 0,
    samples: 0
  }))
  const brainBucketCount = 12
  const brainAccumulator = Array.from({ length: brainBucketCount }).map((_, index) => ({
    index,
    hookSum: 0,
    emotionalSpikeSum: 0,
    patternInterruptSum: 0,
    zoomBurstSum: 0,
    captionImpactSum: 0,
    predictedAttentionSum: 0,
    samples: 0
  }))
  let jobsWithTimelineSignals = 0

  for (const job of completed30d) {
    const duration = readJobDurationSeconds(job)
    if (!duration || duration <= 0) continue
    const predicted = inferPredictedRetention(job)
    const hookScore = inferHookScore(job)
    const emotionalIntensity = inferEmotionalIntensity(job)
    const feedbackSignals = readRetentionFeedbackSignals(job)
    const timelineSignals = readTimelineFeatureSignals(job)
    const hasAutoCaptions = Boolean((job as any)?.renderSettings?.autoCaptions)
    const timelineEvents = readTimelineEvents(job)
    const emotionMoments = parseEmotionMoments(job)
    const heatActivity = new Array(boringSegments.length).fill(0)
    const brainPatternCounts = new Array(brainBucketCount).fill(0)
    const brainZoomCounts = new Array(brainBucketCount).fill(0)
    const brainCaptionCounts = new Array(brainBucketCount).fill(0)
    const brainHookCounts = new Array(brainBucketCount).fill(0)
    const brainEmotionCounts = new Array(brainBucketCount).fill(0)

    for (const event of timelineEvents) {
      const weight = getTimelineEventWeight(event.type)
      const heatIndex = getBucketIndex(event.t, duration, boringSegments.length)
      if (heatIndex >= 0) heatActivity[heatIndex] += weight
      const brainIndex = getBucketIndex(event.t, duration, brainBucketCount)
      if (brainIndex < 0) continue
      if (event.type === 'pattern_interrupt' || event.type === 'auto_escalation') brainPatternCounts[brainIndex] += 1
      if (event.type === 'zoom') brainZoomCounts[brainIndex] += 1
      if (event.type === 'caption_emphasis') brainCaptionCounts[brainIndex] += 1
      if (event.type === 'hook') brainHookCounts[brainIndex] += 1
    }
    for (const moment of emotionMoments) {
      const heatIndex = getBucketIndex(moment, duration, boringSegments.length)
      if (heatIndex >= 0) heatActivity[heatIndex] += 0.42
      const brainIndex = getBucketIndex(moment, duration, brainBucketCount)
      if (brainIndex >= 0) brainEmotionCounts[brainIndex] += 1
    }

    const maxHeatActivity = Math.max(0.25, ...heatActivity)
    const lowRetentionFactor = clamp((68 - predicted) / 68, 0, 1)
    const feedbackPenalty =
      (feedbackSignals.completionPct !== null && feedbackSignals.completionPct < 48 ? 0.08 : 0) +
      (feedbackSignals.watchPct !== null && feedbackSignals.watchPct < 50 ? 0.06 : 0) +
      (feedbackSignals.hookHoldPct !== null && feedbackSignals.hookHoldPct < 45 ? 0.06 : 0)
    for (let index = 0; index < boringBucketAccumulator.length; index += 1) {
      const activityNorm = clamp(heatActivity[index] / maxHeatActivity, 0, 1)
      const middlePenalty = index >= 3 && index <= 6 ? 0.12 : 0
      const risk = clamp(
        0.58 - activityNorm * 0.44 + lowRetentionFactor * 0.33 + middlePenalty + feedbackPenalty,
        0.04,
        0.98
      )
      boringBucketAccumulator[index].riskSum += risk
      boringBucketAccumulator[index].samples += 1
    }

    const maxPattern = Math.max(1, ...brainPatternCounts)
    const maxZoom = Math.max(1, ...brainZoomCounts)
    const maxCaption = Math.max(1, ...brainCaptionCounts)
    const maxEmotion = Math.max(1, ...brainEmotionCounts)
    for (let index = 0; index < brainBucketCount; index += 1) {
      const progress = index / Math.max(1, brainBucketCount - 1)
      const hookPresence = brainHookCounts[index] > 0 ? 1 : 0
      const patternNorm = clamp(brainPatternCounts[index] / maxPattern, 0, 1)
      const zoomNorm = clamp(brainZoomCounts[index] / maxZoom, 0, 1)
      const captionNorm = clamp(brainCaptionCounts[index] / maxCaption, 0, 1)
      const emotionNorm = clamp(brainEmotionCounts[index] / maxEmotion, 0, 1)
      const styleEnergyBoost = timelineSignals.energySpikeDensityPer10s !== null
        ? clamp(timelineSignals.energySpikeDensityPer10s * 12, 0, 22)
        : 0
      const styleCaptionBoost = timelineSignals.captionRatePer10s !== null
        ? clamp(timelineSignals.captionRatePer10s * 10, 0, 18)
        : 0
      const stylePatternBoost = timelineSignals.patternSpacingSeconds !== null
        ? clamp((8 - timelineSignals.patternSpacingSeconds) * 3.2, 0, 16)
        : 0
      const hook = clamp(
        hookPresence
          ? hookScore * 1.04
          : hookScore * Math.max(0.18, 1 - progress * 1.34),
        0,
        100
      )
      const emotionalSpike = clamp(
        emotionalIntensity * (0.34 + emotionNorm * 0.44 + patternNorm * 0.16) + styleEnergyBoost,
        0,
        100
      )
      const patternInterrupt = clamp(
        22 + patternNorm * 58 + stylePatternBoost - progress * 8,
        0,
        100
      )
      const zoomBias = featureLab.zoomIntensityLevel === 'high'
        ? 20
        : featureLab.zoomIntensityLevel === 'medium'
          ? 13
          : 8
      const zoomBurst = clamp(18 + zoomNorm * 62 + zoomBias - progress * 5, 0, 100)
      const captionBase = hasAutoCaptions ? 26 : 11
      const captionImpact = clamp(captionBase + captionNorm * 60 + styleCaptionBoost - progress * 7, 0, 100)
      const predictedAttention = clamp(
        predicted * 0.34 +
          hook * 0.15 +
          emotionalSpike * 0.2 +
          patternInterrupt * 0.13 +
          zoomBurst * 0.08 +
          captionImpact * 0.1,
        0,
        100
      )
      brainAccumulator[index].hookSum += hook
      brainAccumulator[index].emotionalSpikeSum += emotionalSpike
      brainAccumulator[index].patternInterruptSum += patternInterrupt
      brainAccumulator[index].zoomBurstSum += zoomBurst
      brainAccumulator[index].captionImpactSum += captionImpact
      brainAccumulator[index].predictedAttentionSum += predictedAttention
      brainAccumulator[index].samples += 1
    }
    jobsWithTimelineSignals += 1
  }

  const boringFeedbackSignals = feedbackItems.filter((item) =>
    /(boring|slow|generic|not engaging|not exciting)/i.test(`${item.category || ''} ${item.note || ''}`)
  ).length
  const boringFeedbackBoost = boringFeedbackSignals > 0
    ? Math.min(0.34, boringFeedbackSignals / 30)
    : 0
  const boringSegmentHeatmap = boringBucketAccumulator.map((bucket) => {
    const measuredRisk = bucket.samples
      ? bucket.riskSum / bucket.samples
      : boringFallbackRisk[bucket.index] || 0.24
    const boost = bucket.index >= 3 && bucket.index <= 6 ? boringFeedbackBoost : boringFeedbackBoost * 0.35
    return {
      segment: bucket.segment,
      v: Number(clamp((measuredRisk + boost) * 100, 0, 100).toFixed(1))
    }
  })

  const retentionBrainMap = Array.from({ length: brainBucketCount }).map((_, index) => {
    const progress = index / Math.max(1, brainBucketCount - 1)
    const sampleCount = brainAccumulator[index]?.samples || 0
    const fallbackHook = clamp(96 - progress * 74 + hookStrengthScore * 0.09, 0, 100)
    const fallbackEmotional = clamp(18 + avgFirst8SecEngagementScore * 0.22 + Math.sin(progress * Math.PI * 2.4) * 16, 0, 100)
    const fallbackPattern = clamp(28 + (index % 3 === 0 ? 18 : 0) + avgFirst8SecEngagementScore * 0.12, 0, 100)
    const fallbackZoom = clamp(
      (featureLab.zoomIntensityLevel === 'high' ? 68 : featureLab.zoomIntensityLevel === 'medium' ? 52 : 38) +
      (index % 4 === 0 ? 9 : -2),
      0,
      100
    )
    const fallbackCaption = clamp((toPct(subtitlesCount, Math.max(1, completed30d.length)) * 0.85) + 12 - progress * 6, 0, 100)
    const hook = sampleCount ? brainAccumulator[index].hookSum / sampleCount : fallbackHook
    const emotionalSpike = sampleCount ? brainAccumulator[index].emotionalSpikeSum / sampleCount : fallbackEmotional
    const patternInterrupt = sampleCount ? brainAccumulator[index].patternInterruptSum / sampleCount : fallbackPattern
    const zoomBurst = sampleCount ? brainAccumulator[index].zoomBurstSum / sampleCount : fallbackZoom
    const captionImpact = sampleCount ? brainAccumulator[index].captionImpactSum / sampleCount : fallbackCaption
    const predictedAttention = sampleCount
      ? brainAccumulator[index].predictedAttentionSum / sampleCount
      : clamp(
          hook * 0.18 +
          emotionalSpike * 0.22 +
          patternInterrupt * 0.16 +
          zoomBurst * 0.18 +
          captionImpact * 0.26,
          0,
          100
        )
    return {
      t: `${Math.round(progress * 100)}%`,
      hook: Number(hook.toFixed(1)),
      emotionalSpike: Number(emotionalSpike.toFixed(1)),
      patternInterrupt: Number(patternInterrupt.toFixed(1)),
      zoomBurst: Number(zoomBurst.toFixed(1)),
      captionImpact: Number(captionImpact.toFixed(1)),
      predictedAttention: Number(predictedAttention.toFixed(1))
    }
  })

  const revenueVsRenderByDay = new Map<string, { revenueCents: number; renders: number }>()
  for (const event of stripe30d) {
    const key = dayIso(event?.createdAt || Date.now())
    const existing = revenueVsRenderByDay.get(key) || { revenueCents: 0, renders: 0 }
    const amount = Number(event?.amountCents || 0)
    const type = String(event?.type || '').toLowerCase()
    const delta = type === 'charge.refunded' ? -Math.abs(amount) : amount
    existing.revenueCents += Number.isFinite(delta) ? delta : 0
    revenueVsRenderByDay.set(key, existing)
  }
  for (const job of completed30d) {
    const key = dayIso(job?.updatedAt || job?.createdAt || Date.now())
    const existing = revenueVsRenderByDay.get(key) || { revenueCents: 0, renders: 0 }
    existing.renders += 1
    revenueVsRenderByDay.set(key, existing)
  }
  const revenueVsRenderUsage = Array.from(revenueVsRenderByDay.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .slice(-30)
    .map(([day, row]) => ({
      t: `${day}T00:00:00.000Z`,
      revenue: Number((row.revenueCents / 100).toFixed(2)),
      renders: row.renders
    }))

  const upgradeConversionRatePct = toPct(subscribed, Math.max(1, signups))
  const founderPlanSalesCount = founderAvailability.purchasedCount
  const founderPlanAutoRemoveAt = founderAvailability.maxPurchases

  const completed24hDurations = jobs24h
    .filter((job) => String(job?.status || '').toLowerCase() === 'completed')
    .map((job) => renderSeconds(job))
    .filter((value): value is number => value !== null)
  const avgRenderTime24h = completed24hDurations.length
    ? completed24hDurations.reduce((sum, value) => sum + value, 0) / completed24hDurations.length
    : avgRenderTimeSec
  const baselineRenderTime = renderDurations.length
    ? renderDurations.reduce((sum, value) => sum + value, 0) / renderDurations.length
    : avgRenderTimeSec
  const processingSpikeRatio = baselineRenderTime > 0 ? avgRenderTime24h / baselineRenderTime : 1
  const processingSpikeAlert = {
    active: processingSpikeRatio > 1.35,
    severity: processingSpikeRatio > 1.7 ? 'critical' : processingSpikeRatio > 1.35 ? 'elevated' : 'normal',
    ratio: Number(processingSpikeRatio.toFixed(2)),
    currentSec: Number(avgRenderTime24h.toFixed(2)),
    baselineSec: Number(baselineRenderTime.toFixed(2))
  }

  const storageCapacityGb = Number(process.env.ADMIN_STORAGE_CAP_GB || process.env.ADMIN_STORAGE_SOFT_LIMIT_GB || 1200)
  const storageUsagePct = storageCapacityGb > 0
    ? Number(clamp((storageUsage.gb / storageCapacityGb) * 100, 0, 100).toFixed(2))
    : 0
  const storageCostPerGb = Number(process.env.ADMIN_STORAGE_COST_PER_GB_USD || 0.023)
  const storageCostCurrent = Number((storageUsage.gb * storageCostPerGb).toFixed(2))
  const storageCostTrend = Array.from({ length: 12 }).map((_, index) => {
    const factor = 0.82 + index * 0.018
    return {
      t: new Date(Date.now() - (11 - index) * DAY_MS).toISOString(),
      v: Number((storageCostCurrent * factor).toFixed(2))
    }
  })

  const infraBaseBurnMonthly = Number(process.env.ADMIN_INFRA_BURN_MONTHLY_USD || process.env.ADMIN_INFRA_MONTHLY_COST_USD || 2600)
  const infraBurnRateMonthly = Number((infraBaseBurnMonthly + storageCostCurrent).toFixed(2))
  const costPerRenderEstimateUsd = Number((infraBurnRateMonthly / Math.max(1, renders)).toFixed(3))
  const costPerUserUsd = Number((infraBurnRateMonthly / Math.max(1, users.length)).toFixed(3))
  const profitMarginPct = Number(
    clamp((((mrr - infraBurnRateMonthly) / Math.max(1, mrr)) * 100), -400, 95).toFixed(1)
  )
  const runwayCashReserve = Number(process.env.ADMIN_CASH_RESERVE_USD || 120000)
  const runwayMonths = estimatedRunwayMonths({
    cashReserveUsd: runwayCashReserve,
    monthlyRevenueUsd: mrr,
    monthlyBurnUsd: infraBurnRateMonthly
  })

  const gpuUtilizationPct = Number(clamp(Number(process.env.ADMIN_GPU_UTILIZATION_PCT || 0), 0, 100).toFixed(1))

  const groupedErrors = groupErrorsBySignature(enrichedErrors)
  const commonErrorTypes = groupedErrors.slice(0, 8).map((row) => ({ type: row.type, count: row.count }))
  const aiFixSuggestion = groupedErrors.length
    ? `Top failure: ${groupedErrors[0].type}. Prioritize deterministic retries and stricter input validation before pipeline execution.`
    : 'No major recurring errors in the selected window.'

  const usersById = new Map(users.map((user: any) => [String(user?.id || ''), user]))
  const subsByUserId = new Map(subscriptions.map((sub: any) => [String(sub?.userId || ''), sub]))
  const topUsersByRenders = Array.from(
    jobs30d.reduce((map: Map<string, number>, job: any) => {
      const userId = String(job?.userId || '').trim()
      if (!userId) return map
      map.set(userId, (map.get(userId) || 0) + 1)
      return map
    }, new Map<string, number>()).entries()
  )
    .map(([userId, renderCount]) => {
      const user = usersById.get(userId)
      const sub = subsByUserId.get(userId)
      const planTier = String((sub as any)?.planTier || 'free').toLowerCase()
      const planLimit = Number((PLAN_CONFIG as any)?.[planTier]?.maxRendersPerMonth || PLAN_CONFIG.free.maxRendersPerMonth || 10)
      const usagePct = toPct(renderCount, Math.max(1, planLimit))
      return {
        userId,
        email: user?.email || null,
        renders: renderCount,
        planTier,
        usagePct
      }
    })
    .sort((a, b) => b.renders - a.renders)
    .slice(0, 15)

  const whaleDetector = topUsersByRenders
    .filter((row) => row.planTier !== 'founder' && row.usagePct >= 75)
    .slice(0, 10)
    .map((row) => ({
      userId: row.userId,
      email: row.email,
      planTier: row.planTier,
      usagePct: row.usagePct,
      upgradeLikelihood: Number(clamp(row.usagePct * 0.9 + (row.planTier === 'free' ? 8 : 16), 0, 100).toFixed(1))
    }))

  const averageWatchLengthSec = completed30d.length
    ? Number(
        (
          completed30d.reduce((sum, job) => {
            const inputDuration = Number((job as any)?.inputDurationSeconds || 0)
            const predictedRetention = inferPredictedRetention(job) / 100
            return sum + Math.max(0, inputDuration * predictedRetention)
          }, 0) / completed30d.length
        ).toFixed(1)
      )
    : 0

  const variantRetentionBase = avgPredictedRetentionScore || avgRetentionScore || 52
  const variantConversionBase = upgradeConversionRatePct || 3
  const experimentVariantPerformance = [
    {
      variant: 'hook_algorithm_v1',
      predictedRetention: Number(clamp(variantRetentionBase - 2.2, 0, 100).toFixed(1)),
      paidConversionPct: Number(clamp(variantConversionBase - 0.6, 0, 100).toFixed(2))
    },
    {
      variant: 'hook_algorithm_v2',
      predictedRetention: Number(
        clamp(
          variantRetentionBase + (featureLab.hookLogicMode === 'experimental' ? 3.1 : 1.4),
          0,
          100
        ).toFixed(1)
      ),
      paidConversionPct: Number(
        clamp(
          variantConversionBase + (featureLab.hookLogicMode === 'experimental' ? 1.2 : 0.5),
          0,
          100
        ).toFixed(2)
      )
    },
    {
      variant: featureLab.subtitleEngineMode === 'v2' ? 'subtitle_style_v2' : 'subtitle_style_v1',
      predictedRetention: Number(
        clamp(
          variantRetentionBase + (featureLab.subtitleEngineMode === 'v2' ? 1.5 : -0.4),
          0,
          100
        ).toFixed(1)
      ),
      paidConversionPct: Number(
        clamp(
          variantConversionBase + (featureLab.subtitleEngineMode === 'v2' ? 0.45 : 0),
          0,
          100
        ).toFixed(2)
      )
    }
  ]

  const qualityRenders = completed30d
    .slice()
    .sort((a, b) => asMs(b?.updatedAt || b?.createdAt) - asMs(a?.updatedAt || a?.createdAt))
    .slice(0, 36)
    .map((job: any) => {
      const hookScore = inferHookScore(job)
      const pacingScore = inferPacingScore(job)
      const storyCoherenceScore = inferStoryCoherenceScore(job)
      const viralityProbability = inferViralityScore(job)
      const emotionalSpikeMoments = parseEmotionMoments(job)
      const qualityScore = Number(
        clamp(
          hookScore * 0.24 + pacingScore * 0.24 + storyCoherenceScore * 0.28 + viralityProbability * 0.24,
          0,
          100
        ).toFixed(1)
      )
      return {
        jobId: String(job?.id || ''),
        userId: String(job?.userId || ''),
        createdAt: new Date(job?.updatedAt || job?.createdAt || Date.now()).toISOString(),
        hookScore,
        pacingScore,
        storyCoherenceScore,
        emotionalSpikeMoments,
        viralityProbability,
        qualityScore,
        isPremiumQuality: qualityScore >= 70
      }
    })

  const feedbackClusterRows = Array.from(
    feedbackItems.reduce((map, item) => {
      const key = String(item.category || 'unknown')
      const existing = map.get(key) || {
        category: key,
        count: 0,
        sentimentScore: 0
      }
      existing.count += 1
      const sentimentWeight = item.sentiment === 'positive' ? 1 : item.sentiment === 'request' ? 0.25 : item.sentiment === 'negative' ? -0.6 : -1
      existing.sentimentScore += sentimentWeight
      map.set(key, existing)
      return map
    }, new Map<string, { category: string; count: number; sentimentScore: number }>()).values()
  )
    .map((row) => ({
      cluster: row.category,
      count: row.count,
      sentimentTag: row.sentimentScore >= 0.4 ? 'positive' : row.sentimentScore <= -0.5 ? 'negative' : 'mixed'
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 12)

  const overallSentimentScore = feedbackItems.length
    ? Number(
        (
          feedbackItems.reduce((sum, item) => {
            if (item.sentiment === 'positive') return sum + 1
            if (item.sentiment === 'request') return sum + 0.25
            if (item.sentiment === 'negative') return sum - 0.6
            return sum - 1
          }, 0) / feedbackItems.length
        ).toFixed(2)
      )
    : 0
  const supportSummary = feedbackClusterRows.length
    ? `Most frequent: ${feedbackClusterRows[0].cluster}. Sentiment is ${feedbackClusterRows[0].sentimentTag}.`
    : 'No support feedback captured in this range.'

  const adminAccessLogs = await prisma.adminAudit
    .findMany({
      where: { action: { in: ['admin_access_granted', 'admin_access_denied'] } },
      orderBy: { createdAt: 'desc' },
      take: 60
    })
    .catch(() => [] as any[])
  const suspiciousLoginAttempts = adminAccessLogs.filter((log) =>
    String((log as any)?.action || '').toLowerCase().includes('denied')
  ).length
  const api429Count24h = Number(requestSummary.statusCounts['429'] || 0)
  const rateLimitAlerts = api429Count24h > 0
    ? [{ label: 'HTTP 429 responses (24h)', count: api429Count24h }]
    : []
  const tokenExpirationTracking = {
    nearingTimeoutSessions: sessions.filter((session) => Date.now() - asMs((session as any)?.lastSeen) > 45_000).length,
    staleSessions: sessions.filter((session) => Date.now() - asMs((session as any)?.lastSeen) > 110_000).length
  }

  const apiEvents30d = events30d
  const onboardingStarted = apiEvents30d.filter((event: any) =>
    /(onboarding|signup|register|create_account)/i.test(String(event?.eventName || ''))
  ).length || signups
  const uploadStarted = apiEvents30d.filter((event: any) =>
    /(upload_started|upload_start|file_selected|upload)/i.test(String(event?.eventName || ''))
  ).length || uploads
  const uploadCompleted = completed30d.length
  const trialingSubs = subscriptions.filter((sub) => /trial/i.test(String((sub as any)?.status || ''))).length
  const paidActiveSubs = activeSubscriptions.filter((sub) => String((sub as any)?.planTier || '').toLowerCase() !== 'free').length
  const trialToPaidConversionPct = trialingSubs > 0 ? toPct(paidActiveSubs, trialingSubs) : upgradeConversionRatePct
  const onboardingDropOff = [
    { step: 'Onboarding Started', count: onboardingStarted },
    { step: 'Upload Started', count: uploadStarted },
    { step: 'Render Completed', count: uploadCompleted },
    { step: 'Paid Upgrade', count: subscribed }
  ].map((row, index, arr) => ({
    ...row,
    dropOffPct: index === 0 ? 0 : Number((100 - toPct(row.count, Math.max(1, arr[index - 1].count))).toFixed(1))
  }))

  const pageHeatmapAnalyticsEntries = Array.from(
    apiEvents30d.reduce((map: Map<string, number>, event: any) => {
      const path = String(event?.pagePath || event?.eventName || '/')
      map.set(path, (map.get(path) || 0) + 1)
      return map
    }, new Map<string, number>()).entries()
  ) as Array<[string, number]>
  const pageHeatmapAnalytics = pageHeatmapAnalyticsEntries
    .map(([page, count]) => ({ page, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 16)

  const founderSalesByDay = Array.from(
    subscriptions.reduce((map: Map<string, number>, sub: any) => {
      if (String(sub?.planTier || '').toLowerCase() !== 'founder') return map
      const key = dayIso(sub?.updatedAt || Date.now())
      map.set(key, (map.get(key) || 0) + 1)
      return map
    }, new Map<string, number>()).entries()
  )
    .sort((a, b) => a[0].localeCompare(b[0]))
  const founderSoldRecent = founderSalesByDay.reduce((sum, [, count]) => sum + count, 0)
  let founderRunningSold = Math.max(0, founderAvailability.purchasedCount - founderSoldRecent)
  const founderPlanUrgencyGraph = founderSalesByDay.slice(-30).map(([day, sold]) => {
    founderRunningSold += sold
    return {
      t: `${day}T00:00:00.000Z`,
      remaining: Math.max(0, founderAvailability.maxPurchases - founderRunningSold),
      sold: founderRunningSold
    }
  })

  const queueScaleUpAt = Math.max(5, Number(process.env.ADMIN_QUEUE_SCALE_UP_AT || 18))
  const queueScaleDownAt = Math.max(1, Number(process.env.ADMIN_QUEUE_SCALE_DOWN_AT || 5))
  const suggestedWorkers = queueLength >= queueScaleUpAt ? Math.ceil(queueLength / 6) : 1
  const multiRegionEnabled = parseBool(process.env.MULTI_REGION_ENABLED, false)
  const cdnUrl = String(process.env.CDN_URL || '').trim()
  const cacheHitRatePct = Number(clamp(Number(process.env.ADMIN_CACHE_HIT_RATE_PCT || 88), 0, 100).toFixed(1))

  return {
    generatedAt: new Date().toISOString(),
    systemHealth: {
      cpuUsagePct: getCpuUsagePercent(),
      memoryUsage: {
        rssMb: toMb(memory.rss),
        heapUsedMb: toMb(memory.heapUsed),
        heapTotalMb: toMb(memory.heapTotal),
        systemUsedPct: Number((totalMem > 0 ? (memory.rss / totalMem) * 100 : 0).toFixed(1))
      },
      renderQueueLength: queueLength,
      failedJobs: failedReasons,
      workerStatus: {
        online: true,
        uptimeSeconds: Math.round(process.uptime()),
        status: queueLength > 0 ? 'online_busy' : 'online_idle'
      },
      r2StorageUsage: storageUsage,
      stripeWebhookStatus: {
        ok: webhookHealthy,
        lastEventAt: latestWebhook?.createdAt || null,
        events24h: webhookEvents24h.length
      }
    },
    liveUsers: {
      usersOnSite: activeUsersOnSite,
      usersRendering: currentlyRenderingUsers,
      usersExporting: currentlyExportingUsers,
      averageSessionMinutes: avgSessionMinutes,
      map: liveMap
    },
    revenue: {
      mrr,
      arrProjection: arr,
      churnRatePct: churnRate,
      ltv,
      cacEstimate,
      activeSubscriptionsByTier: activeByTier,
      upgradeConversionRatePct,
      founderPlanSalesCount,
      founderPlanAutoRemoveAt,
      founderPlanSoldOut: founderAvailability.soldOut,
      founderPlanRemainingSlots: founderAvailability.remaining,
      stripeBreakdown: {
        failedPayments: failedPayments.length,
        upcomingRenewals: activeSubscriptions.filter((sub) => {
          const endMs = asMs((sub as any)?.currentPeriodEnd)
          return endMs > Date.now() && endMs <= Date.now() + RANGE_MS['30d']
        }).length,
        refunds: refunds.length,
        revenueByPlan
      }
    },
    editorPerformance: {
      renderIntelligence: {
        averageRenderTimeSec: avgRenderTimeSec,
        averageFileSizeMb: avgFileSizeMb,
        averageRetentionScore: avgRetentionScore
      },
      featureUsage: {
        subtitlesPct: toPct(subtitlesCount, completed30d.length),
        hookDetectionPct: toPct(hookDetectedCount, completed30d.length),
        autoZoomPct: toPct(autoZoomCount, completed30d.length),
        verticalModePct: toPct(verticalCount, completed30d.length)
      },
      aiQuality: {
        averageUserRating,
        feedbackHeatmap,
        dropOffPredictionAccuracyPct: dropOffPredictionAccuracy,
        hookSuccessRatePct: hookSuccessRate
      }
    },
    errors: {
      backendErrors: enrichedErrors.filter((item) => item.endpoint !== 'frontend://browser').length,
      frontendJsErrors: enrichedErrors.filter((item) => item.endpoint === 'frontend://browser').length,
      failedUploads24h,
      failedWebhooks24h,
      authFailures24h: requestSummary.totals.authFailures,
      apiLatencySpikes: requestSummary.latencySpikes,
      items: enrichedErrors.slice(0, 120)
    },
    growth: {
      viralMetrics: {
        shareRatePct: toPct(shareEvents.length, Math.max(1, renders)),
        downloadRatePct: toPct(downloads, Math.max(1, renders)),
        returnIn24hPct: toPct(returningUsers24h, Math.max(1, usersByEventTime.size)),
        averageVideosPerUser: avgVideosPerUser
      },
      funnel: {
        visitor: visitors,
        signup: signups,
        upload: uploads,
        render: renders,
        download: downloads,
        subscribe: subscribed
      }
    },
    securityAbuse: {
      suspiciousActivityScore,
      massiveUploadUsers: massiveUploaderSignals,
      multipleAccountsFromSameIp: multipleAccountsSameIp,
      tokenAbuseSignals: requestSummary.tokenAbuseSignals,
      stripeFraudFlags,
      abnormalUsagePatterns: abnormalUsageUsers
    },
    featureLab: {
      controls: featureLab
    },
    aiBrain: {
      topPerformingHooksThisWeek: topHooksGenerated,
      emotionallyEffectiveCuts: emotionalPatterns,
      bestEmotionalPacingPattern,
      videoScoreDistributionCurve: scoreDistributionCurve,
      autoSuggestions: aiAutoSuggestions
    },
    founderEgo: {
      totalMinutesProcessed,
      totalVideosExported: exportedJobs.length,
      estimatedTimeSavedHours: estimatedHoursSaved,
      totalGbProcessed,
      mostViralGeneratedClip
    },
    aiIntelligenceDashboard: {
      retentionPredictionEngine: {
        avgPredictedRetentionScorePerRender: avgPredictedRetentionScore,
        hookStrengthScore,
        emotionalIntensityGraph,
        boringSegmentHeatmap,
        strongHooksPct: strongHookVideoPct,
        avgFirst8SecEngagementScore,
        dropOffRiskPredictionPct: dropOffRiskPrediction,
        timelineSignalCoveragePct: toPct(jobsWithTimelineSignals, Math.max(1, completed30d.length))
      },
      retentionBrainMap
    },
    revenueCommandCenter: {
      mrr,
      arrProjection: arr,
      founderPlanSalesCount,
      founderPlanAutoRemoveAt,
      founderPlanRemainingSlots: founderAvailability.remaining,
      founderPlanSoldOut: founderAvailability.soldOut,
      churnRatePct: churnRate,
      upgradeConversionRatePct,
      failedPaymentAlerts: failedPayments.length,
      stripeWebhookLogs: webhookEvents24h.slice(0, 20).map((event) => ({
        eventId: event.eventId,
        type: event.type,
        status: event.status || 'unknown',
        amount: Number((Number(event.amountCents || 0) / 100).toFixed(2)),
        currency: event.currency || 'USD',
        createdAt: event.createdAt
      })),
      revenueVsRenderUsage
    },
    renderInfrastructureMonitor: {
      activeJobsInQueue: queueLength,
      avgProcessingTimeSec: avgRenderTimeSec,
      failedRenders: failedReasons,
      workerHealth: {
        online: true,
        status: queueLength > 0 ? 'online_busy' : 'online_idle',
        uptimeSeconds: Math.round(process.uptime())
      },
      r2UploadStatus: {
        ok: failedUploads24h === 0,
        provider: storageUsage.provider,
        failedUploads24h,
        note: failedUploads24h > 0 ? 'Upload failures detected in the last 24h.' : 'Healthy'
      },
      storageUsage: {
        gb: storageUsage.gb,
        pct: storageUsagePct,
        objects: storageUsage.objects,
        estimated: storageUsage.estimated
      },
      costPerRenderEstimateUsd,
      cpuUtilizationPct: getCpuUsagePercent(),
      gpuUtilizationPct,
      processingTimeSpikeAlert: processingSpikeAlert
    },
    liveErrorTerminal: {
      backendLogCount24h: enrichedErrors.filter((item) => item.endpoint !== 'frontend://browser').length,
      frontendErrorCount24h: enrichedErrors.filter((item) => item.endpoint === 'frontend://browser').length,
      api401Count24h: requestSummary.totals.authFailures,
      api500Count24h: requestSummary.totals.serverErrors,
      mostCommonErrorTypes: commonErrorTypes,
      groupedErrors,
      fixSuggestion: aiFixSuggestion
    },
    userIntelligencePanel: {
      activeUsers: activeUsersOnSite,
      geoHeatmap: liveMap,
      planBreakdown: {
        free: Math.max(
          0,
          users.length -
            (activeByTier.starter || 0) -
            (activeByTier.creator || 0) -
            (activeByTier.studio || 0) -
            (activeByTier.founder || 0)
        ),
        starter: activeByTier.starter || 0,
        creator: activeByTier.creator || 0,
        studio: activeByTier.studio || 0,
        founder: activeByTier.founder || 0
      },
      topUsersByRenders,
      suspiciousActivityFlag: suspiciousActivityScore >= 65,
      abuseDetection: abnormalUsageUsers,
      averageWatchLengthSec,
      whaleDetector
    },
    experimentLab: {
      controls: featureLab,
      variantPerformance: experimentVariantPerformance
    },
    editorQualityAnalyzer: {
      renders: qualityRenders,
      lowQualityCount: qualityRenders.filter((row) => !row.isPremiumQuality).length
    },
    feedbackIntelligence: {
      clusters: feedbackClusterRows,
      topRequestedFeatures: requestedFeaturesTop.slice(0, 8),
      sentimentScore: overallSentimentScore,
      supportSummary,
      featureDemandHeatmap: feedbackHeatmap
    },
    costControlPanel: {
      costPerUserUsd,
      costPerRenderUsd: costPerRenderEstimateUsd,
      storageCostTrend,
      infrastructureBurnRateUsdMonthly: infraBurnRateMonthly,
      profitMarginPct,
      runwayMonths
    },
    securityPanel: {
      adminAccessLogs: adminAccessLogs.map((entry: any) => ({
        id: String(entry?.id || ''),
        actor: entry?.actor || null,
        action: entry?.action || null,
        reason: entry?.reason || null,
        createdAt: entry?.createdAt ? new Date(entry.createdAt).toISOString() : new Date().toISOString()
      })),
      suspiciousLoginAttempts,
      apiAbuseMonitor: requestSummary.tokenAbuseSignals,
      rateLimitMonitor: {
        alerts: rateLimitAlerts,
        status429Count24h: api429Count24h
      },
      tokenExpirationTracking,
      r2KeyUsageLog: {
        provider: storageUsage.provider,
        configured: r2.isConfigured,
        lastCheckAt: new Date().toISOString()
      },
      webhookVerificationStatus: {
        configured: Boolean(String(process.env.STRIPE_WEBHOOK_SECRET || '').trim()),
        healthy: webhookHealthy,
        lastEventAt: latestWebhook?.createdAt || null
      }
    },
    conversionIntelligence: {
      onboardingDropOff,
      uploadCompletionPct: toPct(uploadCompleted, Math.max(1, uploadStarted)),
      trialToPaidConversionPct,
      founderPlanUrgencyGraph,
      pageHeatmapAnalytics
    },
    futureScalingPanel: {
      multiRegionDeployEnabled: multiRegionEnabled,
      cdnHealth: {
        configured: Boolean(cdnUrl),
        url: cdnUrl || null,
        ok: Boolean(cdnUrl)
      },
      cacheHitRatePct,
      queueScalingThresholds: {
        scaleUpAt: queueScaleUpAt,
        scaleDownAt: queueScaleDownAt
      },
      autoScaleWorkerTriggers: {
        active: queueLength >= queueScaleUpAt,
        suggestedWorkers
      }
    },
    aiSelfImprovementPanel: {
      supported: true,
      defaultAnalyzeCount: 1000,
      quickSuggestions: aiAutoSuggestions.slice(0, 5)
    }
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

router.get('/auth-check', async (_req, res) => {
  res.json({
    ok: true,
    checkedAt: new Date().toISOString()
  })
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
  const enriched = await enrichErrorItems(items)
  res.json({ rangeMs, severity, total: enriched.length, items: enriched })
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

router.get('/bank/takeout/status', async (req: any, res) => {
  const actorKey = resolveAdminActorKey(req)
  const actor = String(req.user?.email || req.user?.id || '').trim() || null
  const memoryCooldown = cleanExpiredTakeoutCooldown(actorKey)
  const auditCooldown = await readTakeoutCooldownFromAudit(actor)
  const cooldown = [memoryCooldown, auditCooldown]
    .filter(Boolean)
    .sort((a: any, b: any) => Number(b?.nextAllowedAt || 0) - Number(a?.nextAllowedAt || 0))[0] || null
  const availableBalanceUsd = await readAvailableUsdBalance()
  const canTakeOut = isStripeEnabled() && Boolean(stripe) && (!cooldown || cooldown.nextAllowedAt <= Date.now())
  return res.json({
    maxAmountUsd: BANK_TAKEOUT_MAX_USD,
    cooldownMinutes: Math.round(BANK_TAKEOUT_COOLDOWN_MS / 60_000),
    currency: BANK_TAKEOUT_CURRENCY.toUpperCase(),
    stripeConfigured: isStripeEnabled() && Boolean(stripe),
    availableBalanceUsd,
    nextAllowedAt: cooldown ? new Date(cooldown.nextAllowedAt).toISOString() : null,
    lastTakeoutAt: cooldown?.lastTakeoutAt || null,
    lastAmountUsd: cooldown?.lastAmountUsd ?? null,
    lastPayoutId: cooldown?.lastPayoutId ?? null,
    canTakeOut,
    serverTime: new Date().toISOString()
  })
})

router.post('/bank/takeout', async (req: any, res) => {
  const actorKey = resolveAdminActorKey(req)
  const actor = String(req.user?.email || req.user?.id || '').trim() || null
  const amountUsd = parseUsdAmount(req.body?.amountUsd)
  if (amountUsd === null) {
    return res.status(400).json({
      error: 'invalid_amount',
      message: 'Enter a valid withdrawal amount in USD.'
    })
  }
  if (amountUsd > BANK_TAKEOUT_MAX_USD) {
    return res.status(400).json({
      error: 'amount_exceeds_limit',
      message: `Take out amount cannot exceed $${BANK_TAKEOUT_MAX_USD.toFixed(2)} every 10 minutes.`
    })
  }
  if (!isStripeEnabled() || !stripe) {
    return res.status(400).json({
      error: 'stripe_not_configured',
      message: 'Stripe is not enabled in this environment.'
    })
  }

  const memoryCooldown = cleanExpiredTakeoutCooldown(actorKey)
  const auditCooldown = await readTakeoutCooldownFromAudit(actor)
  const cooldown = [memoryCooldown, auditCooldown]
    .filter(Boolean)
    .sort((a: any, b: any) => Number(b?.nextAllowedAt || 0) - Number(a?.nextAllowedAt || 0))[0] || null
  if (cooldown && cooldown.nextAllowedAt > Date.now()) {
    return res.status(429).json({
      error: 'takeout_cooldown_active',
      message: 'Take out is limited to once every 10 minutes.',
      nextAllowedAt: new Date(cooldown.nextAllowedAt).toISOString()
    })
  }

  const availableBalanceUsd = await readAvailableUsdBalance()
  if (availableBalanceUsd !== null && amountUsd > availableBalanceUsd) {
    return res.status(400).json({
      error: 'insufficient_stripe_balance',
      message: `Requested $${amountUsd.toFixed(2)} exceeds available Stripe balance of $${availableBalanceUsd.toFixed(2)}.`,
      availableBalanceUsd
    })
  }

  const payoutAmountCents = Math.round(amountUsd * 100)
  try {
    const payout = await stripe.payouts.create({
      amount: payoutAmountCents,
      currency: BANK_TAKEOUT_CURRENCY,
      description: `Admin take out by ${req.user?.email || req.user?.id || 'unknown'}`
    })
    const now = Date.now()
    const nextAllowedAt = now + BANK_TAKEOUT_COOLDOWN_MS
    bankTakeoutCooldownByActor.set(actorKey, {
      nextAllowedAt,
      lastPayoutId: String(payout?.id || ''),
      lastAmountUsd: amountUsd,
      lastTakeoutAt: new Date(now).toISOString()
    })
    await auditAdminAction({
      actor: req.user?.email || req.user?.id || null,
      action: 'admin_bank_takeout',
      targetEmail: req.user?.email || null,
      planKey: String(payout?.id || 'unknown'),
      reason: `takeout_${amountUsd.toFixed(2)}_${BANK_TAKEOUT_CURRENCY}`
    })
    return res.json({
      ok: true,
      payoutId: payout?.id || null,
      status: payout?.status || 'pending',
      amountUsd,
      currency: BANK_TAKEOUT_CURRENCY.toUpperCase(),
      nextAllowedAt: new Date(nextAllowedAt).toISOString(),
      availableBalanceUsd: await readAvailableUsdBalance(),
      processedAt: new Date().toISOString()
    })
  } catch (err: any) {
    return res.status(502).json({
      error: 'stripe_takeout_failed',
      message: String(err?.message || 'Stripe payout failed')
    })
  }
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

router.post('/errors/:id/fix-now', async (req: any, res) => {
  const id = String(req.params?.id || '').trim()
  if (!id) {
    return res.status(400).json({ error: 'invalid_error_id' })
  }
  const allRecentErrors = await getAdminErrorLogs({ rangeMs: YEAR_MS, severity: null })
  const target = allRecentErrors.find((item) => String(item?.id || '') === id)
  if (!target) {
    return res.status(404).json({ error: 'error_not_found' })
  }
  const jobId = String((target as any)?.jobId || '').trim()
  if (!jobId) {
    return res.status(400).json({
      error: 'unsupported_error',
      message: 'This error has no linked job. Manual investigation required.'
    })
  }
  const job = await prisma.job.findUnique({ where: { id: jobId } }).catch(() => null)
  if (!job) {
    return res.status(404).json({ error: 'job_not_found' })
  }
  const normalizedStatus = String(job.status || '').toLowerCase()
  if (normalizedStatus !== 'failed' && normalizedStatus !== 'completed') {
    return res.status(409).json({
      error: 'job_not_retryable',
      message: 'Only failed/completed jobs can be re-queued from Fix Now.'
    })
  }
  const owner = await prisma.user.findUnique({ where: { id: String(job.userId) } }).catch(() => null)
  const priorityLevel = Number((job as any)?.priorityLevel ?? 2) || 2
  await updateJob(jobId, {
    status: 'queued',
    progress: 1,
    error: null,
    ...(normalizedStatus === 'completed' ? { outputPath: null } : {})
  })
  enqueuePipeline({
    jobId,
    user: {
      id: String(job.userId || ''),
      email: owner?.email || undefined
    },
    requestedQuality: (job as any)?.requestedQuality as any,
    requestId: req?.requestId,
    priorityLevel
  })
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_error_fix_now',
    targetEmail: owner?.email || String(job.userId || ''),
    planKey: jobId,
    reason: sanitizeReason(req.body?.reason) || `error:${id}`
  })
  return res.json({
    ok: true,
    errorId: id,
    jobId,
    queued: true
  })
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

router.get('/automation/prompts', async (_req, res) => {
  const items = await loadRecentPromptInboxEntries()
  return res.json({
    items,
    updatedAt: new Date().toISOString()
  })
})

router.post('/automation/prompts', async (req: any, res) => {
  const title = sanitizePromptTitle(req.body?.title)
  const prompt = sanitizePromptBody(req.body?.prompt)
  const targetPathRaw = String(req.body?.targetPath || '').trim()
  const createTargetFile = parseBool(req.body?.createTargetFile, true)
  const overwriteTargetFile = parseBool(req.body?.overwriteTargetFile, false)
  if (!prompt) {
    return res.status(400).json({
      error: 'invalid_prompt',
      message: 'Prompt text is required.'
    })
  }

  const id = `prompt_${Date.now()}_${crypto.randomUUID().slice(0, 8)}`
  const createdAt = new Date().toISOString()
  const slug = slugifyToken(title, 'vscode-prompt')
  const inboxFileName = `${Date.now()}-${slug}-${id.slice(-6)}.json`
  const inboxFilePath = path.join(ADMIN_PROMPT_INBOX_DIR, inboxFileName)

  await ensurePromptInboxDir()
  let targetPath: string | null = null
  if (targetPathRaw) {
    const resolved = resolveSafeProjectFile(targetPathRaw)
    if (!resolved) {
      return res.status(400).json({
        error: 'invalid_target_path',
        message: 'Target path must be project-relative.'
      })
    }
    targetPath = resolved
  }

  let createdTargetPath: string | null = null
  if (targetPath && createTargetFile) {
    try {
      await fs.access(targetPath)
      if (!overwriteTargetFile) {
        return res.status(409).json({
          error: 'target_exists',
          message: 'Target file already exists. Enable overwrite to replace it.',
          targetPath: toProjectRelativePath(targetPath)
        })
      }
    } catch {
      // target file does not exist yet.
    }
    await fs.mkdir(path.dirname(targetPath), { recursive: true })
    const generatedBody = [
      `/*`,
      ` Auto-generated from Admin Prompt Inbox`,
      ` Title: ${title}`,
      ` Created: ${createdAt}`,
      ` Actor: ${String(req.user?.email || req.user?.id || 'unknown')}`,
      `*/`,
      ``,
      `/**`,
      ` * Prompt`,
      ` */`,
      `${JSON.stringify(prompt, null, 2)}`,
      ``
    ].join('\n')
    await fs.writeFile(targetPath, generatedBody, 'utf8')
    createdTargetPath = toProjectRelativePath(targetPath)
  }

  const payload = {
    id,
    title,
    prompt,
    targetPath: createdTargetPath || (targetPath ? toProjectRelativePath(targetPath) : null),
    createdBy: String(req.user?.email || req.user?.id || '').trim() || null,
    createdAt,
    updatedAt: createdAt
  }
  await fs.writeFile(inboxFilePath, JSON.stringify(payload, null, 2), 'utf8')
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_automation_prompt_create',
    targetEmail: req.user?.email || null,
    planKey: payload.targetPath || null,
    reason: sanitizeReason(req.body?.reason) || title
  })
  return res.json({
    ok: true,
    item: {
      id: payload.id,
      title: payload.title,
      promptPreview: payload.prompt.slice(0, 320),
      targetPath: payload.targetPath,
      inboxPath: toProjectRelativePath(inboxFilePath),
      createdAt: payload.createdAt,
      createdBy: payload.createdBy
    },
    createdTargetPath
  })
})

router.post('/server/restart', async (req: any, res) => {
  const reason = sanitizeReason(req.body?.reason) || 'admin_restart_requested'
  const allowRestart = parseBool(process.env.ALLOW_ADMIN_SERVER_RESTART, false)
  const requestedAt = new Date().toISOString()
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_server_restart_request',
    targetEmail: req.user?.email || null,
    reason
  })
  if (!allowRestart) {
    return res.status(202).json({
      ok: true,
      requested: true,
      executed: false,
      requestedAt,
      message: 'Restart request recorded. Set ALLOW_ADMIN_SERVER_RESTART=true to enable automatic process restart.'
    })
  }

  setTimeout(() => {
    process.exit(0)
  }, 1250).unref()

  return res.json({
    ok: true,
    requested: true,
    executed: true,
    requestedAt,
    message: 'Restart signal issued. Process will exit shortly.'
  })
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
  const cpuUsagePct = getCpuUsagePercent()
  const queue = await countQueue()
  const failedJobs24h = (await getJobsSince(RANGE_MS['24h']))
    .filter((job) => String(job?.status || '').toLowerCase() === 'failed')
    .reduce((map: Map<string, number>, job: any) => {
      const reason = String(job?.error || 'unknown_failure')
      map.set(reason, (map.get(reason) || 0) + 1)
      return map
    }, new Map<string, number>())
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
      cpuUsagePct,
      nodeVersion: process.version,
      platform: `${os.platform()} ${os.release()}`,
      memory: {
        rss: memory.rss,
        heapUsed: memory.heapUsed,
        heapTotal: memory.heapTotal
      },
      failedJobs24h: Array.from(failedJobs24h.entries())
        .map(([reason, count]) => ({ reason, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 8)
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

router.get('/feature-lab', async (_req, res) => {
  const controls = await getFeatureLabControls()
  return res.json({
    controls,
    updatedAt: controls.updatedAt
  })
})

router.post('/feature-lab', async (req: any, res) => {
  const patch = req.body && typeof req.body === 'object' ? req.body : {}
  const controls = await updateFeatureLabControls(patch, req.user?.id || null)
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_feature_lab_update',
    targetEmail: req.user?.email || null,
    reason: sanitizeReason(req.body?.reason) || null
  })
  return res.json({
    ok: true,
    controls,
    updatedAt: controls.updatedAt
  })
})

router.get('/live-geo', async (_req, res) => {
  const sessions = getRealtimePresenceSessions()
  const geoHeatmap = await buildLiveGeoHeatmap(sessions)
  return res.json({
    activeUsers: getRealtimeActiveUsersCount(),
    geoHeatmap,
    updatedAt: new Date().toISOString()
  })
})

router.get('/command-center', async (_req, res) => {
  const payload = await buildCommandCenterPayload()
  return res.json(payload)
})

router.get('/analytics', async (_req, res) => {
  const [users, jobs, eventsRaw] = await Promise.all([
    getUsers(),
    prisma.job
      .findMany({
        select: { userId: true }
      })
      .catch(() => [] as any[]),
    prisma.siteAnalyticsEvent
      .findMany({
        select: {
          userId: true,
          sessionId: true,
          eventName: true,
          category: true,
          pagePath: true,
          createdAt: true
        },
        orderBy: { createdAt: 'asc' }
      })
      .catch(() => [] as any[])
  ])

  const events = Array.isArray(eventsRaw) ? eventsRaw : []
  const allTimeAccounts = users.length
  const trackedUsers = new Set<string>()
  const renderUsers = new Set<string>()
  const sessionMap = new Map<
    string,
    {
      startMs: number
      endMs: number
      events: number
      pageViewCount: number
      pageEvents: Array<{ ms: number; pagePath: string; userId: string | null }>
      pages: Set<string>
    }
  >()
  const pageMap = new Map<
    string,
    {
      views: number
      users: Set<string>
      dwellTotalSec: number
      dwellSamples: number
    }
  >()
  const dwellSamplesAllSec: number[] = []

  for (const job of jobs) {
    const userId = String((job as any)?.userId || '').trim()
    if (!userId) continue
    renderUsers.add(userId)
  }

  for (const event of events) {
    const userId = String((event as any)?.userId || '').trim()
    if (userId) trackedUsers.add(userId)

    const pagePathRaw = String((event as any)?.pagePath || '').trim()
    const pagePath = pagePathRaw || null
    const createdMs = asMs((event as any)?.createdAt)
    if (!createdMs) continue
    const isPageView = isImpressionEvent(event as any)

    if (pagePath && isPageView) {
      const page = pageMap.get(pagePath) || {
        views: 0,
        users: new Set<string>(),
        dwellTotalSec: 0,
        dwellSamples: 0
      }
      page.views += 1
      if (userId) page.users.add(userId)
      pageMap.set(pagePath, page)
    }

    const sessionId = String((event as any)?.sessionId || '').trim()
    if (!sessionId) continue
    const current = sessionMap.get(sessionId) || {
      startMs: createdMs,
      endMs: createdMs,
      events: 0,
      pageViewCount: 0,
      pageEvents: [] as Array<{ ms: number; pagePath: string; userId: string | null }>,
      pages: new Set<string>()
    }
    current.startMs = Math.min(current.startMs, createdMs)
    current.endMs = Math.max(current.endMs, createdMs)
    current.events += 1
    if (isPageView) current.pageViewCount += 1
    if (pagePath) {
      current.pageEvents.push({ ms: createdMs, pagePath, userId: userId || null })
      current.pages.add(pagePath)
    }
    sessionMap.set(sessionId, current)
  }

  for (const session of sessionMap.values()) {
    if (!session.pageEvents.length) continue
    const timeline = session.pageEvents.sort((a, b) => a.ms - b.ms)
    for (let index = 0; index < timeline.length; index += 1) {
      const current = timeline[index]
      const next = timeline[index + 1]
      const dwellSec = next && next.ms > current.ms
        ? clamp((next.ms - current.ms) / 1000, 1, 1800)
        : 20
      const page = pageMap.get(current.pagePath) || {
        views: 0,
        users: new Set<string>(),
        dwellTotalSec: 0,
        dwellSamples: 0
      }
      if (current.userId) page.users.add(current.userId)
      page.dwellTotalSec += dwellSec
      page.dwellSamples += 1
      pageMap.set(current.pagePath, page)
      dwellSamplesAllSec.push(dwellSec)
    }
  }

  const sessionDurationsSec = Array.from(sessionMap.values()).map((session) =>
    clamp((session.endMs - session.startMs) / 1000, 0, 8 * 60 * 60)
  )
  const totalPageViews = Array.from(pageMap.values()).reduce((sum, row) => sum + row.views, 0)
  const totalPagesVisitedAcrossSessions = Array.from(sessionMap.values()).reduce((sum, row) => sum + row.pages.size, 0)
  const totalPageViewSessions = Array.from(sessionMap.values()).filter((row) => row.pageViewCount > 0).length
  const bouncedSessions = Array.from(sessionMap.values()).filter((row) => row.pageViewCount <= 1).length

  const avgSessionMinutes = sessionDurationsSec.length
    ? toFixedNumber(sessionDurationsSec.reduce((sum, value) => sum + value, 0) / sessionDurationsSec.length / 60, 2)
    : 0
  const medianSessionMinutes = sessionDurationsSec.length
    ? toFixedNumber(medianOfNumbers(sessionDurationsSec) / 60, 2)
    : 0
  const avgTimeOnPageSeconds = dwellSamplesAllSec.length
    ? toFixedNumber(dwellSamplesAllSec.reduce((sum, value) => sum + value, 0) / dwellSamplesAllSec.length, 1)
    : 0
  const medianTimeOnPageSeconds = dwellSamplesAllSec.length
    ? toFixedNumber(medianOfNumbers(dwellSamplesAllSec), 1)
    : 0
  const avgPagesPerSession = totalPageViewSessions > 0
    ? toFixedNumber(totalPagesVisitedAcrossSessions / totalPageViewSessions, 2)
    : 0
  const avgEventsPerSession = sessionMap.size > 0
    ? toFixedNumber(events.length / sessionMap.size, 2)
    : 0
  const bounceRatePct = totalPageViewSessions > 0
    ? toFixedNumber((bouncedSessions / totalPageViewSessions) * 100, 1)
    : 0

  const pages = Array.from(pageMap.entries())
    .map(([pagePath, row]) => ({
      pagePath,
      views: row.views,
      uniqueUsers: row.users.size,
      avgTimeSeconds: row.dwellSamples > 0 ? toFixedNumber(row.dwellTotalSec / row.dwellSamples, 1) : 0,
      totalTimeMinutes: toFixedNumber(row.dwellTotalSec / 60, 2)
    }))
    .filter((row) => row.views > 0 || row.totalTimeMinutes > 0)

  const topPagesByViews = pages
    .slice()
    .sort((a, b) => b.views - a.views || b.uniqueUsers - a.uniqueUsers || b.avgTimeSeconds - a.avgTimeSeconds)
    .slice(0, 12)
  const topPagesByTime = pages
    .slice()
    .sort((a, b) => b.avgTimeSeconds - a.avgTimeSeconds || b.views - a.views)
    .slice(0, 12)

  return res.json({
    generatedAt: new Date().toISOString(),
    totals: {
      allTimeTrackedUsers: trackedUsers.size,
      allTimeAccounts,
      allTimeRenderUsers: renderUsers.size,
      allTimeEvents: events.length,
      allTimeSessions: sessionMap.size,
      allTimePageViews: totalPageViews
    },
    engagement: {
      avgSessionMinutes,
      medianSessionMinutes,
      avgTimeOnPageSeconds,
      medianTimeOnPageSeconds,
      avgPagesPerSession,
      avgEventsPerSession,
      bounceRatePct
    },
    topPagesByViews,
    topPagesByTime
  })
})

router.post('/ai-self-improvement', async (req: any, res) => {
  const analyzeCountRaw = Number.parseInt(String(req.body?.count ?? '1000'), 10)
  const analyzeCount = clamp(Number.isFinite(analyzeCountRaw) ? analyzeCountRaw : 1000, 100, 5000)
  const jobs = await prisma.job
    .findMany({
      orderBy: { createdAt: 'desc' },
      take: analyzeCount
    })
    .catch(() => [] as any[])
  const completed = jobs.filter((job) => String(job?.status || '').toLowerCase() === 'completed')
  const failed = jobs.filter((job) => String(job?.status || '').toLowerCase() === 'failed')
  const failureReasonEntries = Array.from(
    failed.reduce((map: Map<string, number>, job: any) => {
      const reason = String(job?.error || 'unknown_failure')
      map.set(reason, (map.get(reason) || 0) + 1)
      return map
    }, new Map<string, number>()).entries()
  ) as Array<[string, number]>
  const failureReasons: Array<{ reason: string; count: number }> = failureReasonEntries
    .map(([reason, count]) => ({ reason, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8)
  const lowQualityCount = completed
    .map((job) => {
      const hook = inferHookScore(job)
      const pacing = inferPacingScore(job)
      const story = inferStoryCoherenceScore(job)
      const viral = inferViralityScore(job)
      const quality = hook * 0.24 + pacing * 0.24 + story * 0.28 + viral * 0.24
      return quality < 70 ? 1 : 0
    })
    .reduce((sum, value) => sum + value, 0)
  const avgUploadToRenderSeconds = completed.length
    ? Number(
        (
          completed
            .map((job) => renderSeconds(job))
            .filter((value): value is number => value !== null)
            .reduce((sum, value) => sum + value, 0) /
          Math.max(1, completed.length)
        ).toFixed(2)
      )
    : 0
  const feedback = extractFeedback(jobs, RANGE_MS['30d'])
  const complaintTagEntries = Array.from(
    feedback
      .filter((item) => item.sentiment === 'negative' || item.sentiment === 'bug')
      .reduce((map, item) => {
        const key = String(item.category || 'unknown')
        map.set(key, (map.get(key) || 0) + 1)
        return map
      }, new Map<string, number>()).entries()
  ) as Array<[string, number]>
  const complaintTags: Array<{ tag: string; count: number }> = complaintTagEntries
    .map(([tag, count]) => ({ tag, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8)
  const suggestions = await aiSuggestions(failureReasons)
  const normalizedSuggestions = suggestions.slice(0, 5).map((item: any, index: number) => ({
    priority: Number(item?.priority || index + 1),
    title: String(item?.title || `Improve pipeline priority ${index + 1}`),
    expectedImpact: String(item?.expectedImpact || 'Improve render success rate and retention quality.'),
    difficulty: String(item?.difficulty || 'Medium')
  }))
  return res.json({
    analyzedRenders: jobs.length,
    completedRenders: completed.length,
    failedRenders: failed.length,
    lowQualityCount,
    averageUploadToRenderSeconds: avgUploadToRenderSeconds,
    topFailures: failureReasons,
    complaintTags,
    suggestions: normalizedSuggestions,
    generatedAt: new Date().toISOString()
  })
})

router.post('/founder-tools/grant-lifetime', async (req: any, res) => {
  const reason = sanitizeReason(req.body?.reason) || 'founder_lifetime_grant'
  const targetEmail = normalizeEmail(req.body?.email)
  let user = await resolveUserFromPayload({ userId: req.body?.userId, email: targetEmail })

  if (!user && targetEmail && isValidEmail(targetEmail)) {
    user = await prisma.user.create({
      data: {
        email: targetEmail,
        planStatus: 'active'
      }
    })
  }
  if (!user) return res.status(404).json({ error: 'user_not_found' })

  const lifetimeEnd = new Date('2099-12-31T23:59:59.000Z')
  const subscription = await prisma.subscription.upsert({
    where: { userId: user.id },
    create: {
      userId: user.id,
      stripeCustomerId: user.stripeCustomerId || null,
      stripeSubscriptionId: user.stripeSubscriptionId || null,
      status: 'active',
      planTier: 'founder',
      priceId: 'admin_lifetime_founder',
      currentPeriodEnd: lifetimeEnd,
      cancelAtPeriodEnd: false
    },
    update: {
      status: 'active',
      planTier: 'founder',
      priceId: 'admin_lifetime_founder',
      currentPeriodEnd: lifetimeEnd,
      cancelAtPeriodEnd: false
    }
  })
  await prisma.user.update({
    where: { id: user.id },
    data: {
      planStatus: 'active',
      currentPeriodEnd: lifetimeEnd
    }
  })
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_founder_grant_lifetime',
    targetEmail: user.email || null,
    planKey: user.id,
    reason
  })
  return res.json({
    ok: true,
    userId: user.id,
    email: user.email,
    planTier: subscription.planTier,
    currentPeriodEnd: subscription.currentPeriodEnd
  })
})

router.post('/founder-tools/reprocess-job', async (req: any, res) => {
  const jobId = String(req.body?.jobId || '').trim()
  if (!jobId) return res.status(400).json({ error: 'missing_job_id' })
  const job = await prisma.job.findUnique({ where: { id: jobId } }).catch(() => null)
  if (!job) return res.status(404).json({ error: 'job_not_found' })

  const owner = await prisma.user.findUnique({ where: { id: String(job.userId) } }).catch(() => null)
  await updateJob(jobId, {
    status: 'queued',
    progress: 1,
    error: null,
    outputPath: null
  })
  enqueuePipeline({
    jobId,
    user: {
      id: String(job.userId || ''),
      email: owner?.email || undefined
    },
    requestedQuality: (job as any)?.requestedQuality as any,
    requestId: req?.requestId,
    priorityLevel: Number((job as any)?.priorityLevel ?? 2) || 2
  })
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_founder_reprocess_job',
    targetEmail: owner?.email || String(job.userId || ''),
    planKey: jobId,
    reason: sanitizeReason(req.body?.reason) || 'founder_reprocess_job'
  })
  return res.json({
    ok: true,
    jobId,
    queued: true
  })
})

router.post('/founder-tools/kill-job', async (req: any, res) => {
  const jobId = String(req.body?.jobId || '').trim()
  if (!jobId) return res.status(400).json({ error: 'missing_job_id' })
  try {
    const result = await cancelJobById({
      jobId,
      reason: 'founder_force_kill'
    })
    await auditAdminAction({
      actor: req.user?.email || req.user?.id || null,
      action: 'admin_founder_kill_job',
      targetEmail: result.ownerUserId || null,
      planKey: result.id,
      reason: sanitizeReason(req.body?.reason) || 'founder_force_kill'
    })
    return res.json({
      ok: true,
      id: result.id,
      status: result.status,
      running: result.running,
      killedCount: result.killedCount
    })
  } catch (err: any) {
    const status = Number(err?.statusCode || 500)
    const code = String(err?.code || 'server_error')
    const message = String(err?.message || 'server_error')
    return res.status(status >= 500 ? 500 : status).json({ error: code, message })
  }
})

router.post('/founder-tools/refund-payment', async (req: any, res) => {
  const eventId = String(req.body?.eventId || '').trim()
  const reason = sanitizeReason(req.body?.reason) || 'requested_by_admin'
  if (!eventId) return res.status(400).json({ error: 'missing_event_id' })
  const events = await getStripeWebhookEvents({ rangeMs: YEAR_MS })
  const target = events.find((event) => String(event?.eventId || '') === eventId)
  if (!target) return res.status(404).json({ error: 'payment_event_not_found' })
  if (!isStripeEnabled() || !stripe) {
    return res.status(400).json({
      error: 'stripe_not_configured',
      message: 'Stripe is not enabled in this environment.'
    })
  }
  const payload = (target as any)?.payload || {}
  const chargeId = String(payload?.charge || payload?.id || '').trim()
  const paymentIntentId = String(payload?.payment_intent || '').trim()
  if (!chargeId && !paymentIntentId) {
    return res.status(400).json({
      error: 'refund_reference_missing',
      message: 'The selected event does not include charge/payment_intent identifiers.'
    })
  }
  try {
    const refund = paymentIntentId
      ? await stripe.refunds.create({ payment_intent: paymentIntentId, reason: 'requested_by_customer' })
      : await stripe.refunds.create({ charge: chargeId, reason: 'requested_by_customer' })
    await auditAdminAction({
      actor: req.user?.email || req.user?.id || null,
      action: 'admin_founder_refund_payment',
      targetEmail: target.userId || null,
      planKey: eventId,
      reason
    })
    return res.json({
      ok: true,
      refundId: refund?.id || null,
      status: refund?.status || 'pending',
      amount: Number((Number(refund?.amount || 0) / 100).toFixed(2)),
      currency: String(refund?.currency || target.currency || 'usd').toUpperCase()
    })
  } catch (err: any) {
    return res.status(502).json({
      error: 'stripe_refund_failed',
      message: String(err?.message || 'Stripe refund failed')
    })
  }
})

router.post('/founder-tools/simulate-webhook', async (req: any, res) => {
  const type = String(req.body?.type || 'invoice.paid').trim() || 'invoice.paid'
  const amountCentsRaw = Number.parseInt(String(req.body?.amountCents ?? '0'), 10)
  const amountCents = Number.isFinite(amountCentsRaw) ? Math.max(0, amountCentsRaw) : 0
  const currency = String(req.body?.currency || 'usd').trim().toLowerCase() || 'usd'
  const userId = String(req.body?.userId || '').trim() || undefined
  const status = String(req.body?.status || 'simulated').trim() || 'simulated'
  const fakeEvent = {
    id: `evt_sim_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`,
    type,
    created: Math.floor(Date.now() / 1000),
    data: {
      object: {
        id: `sim_${crypto.randomUUID().replace(/-/g, '').slice(0, 16)}`,
        amount_total: amountCents,
        amount_paid: amountCents,
        amount_due: amountCents,
        currency,
        status,
        metadata: userId ? { userId } : {}
      }
    }
  }
  const stored = await storeStripeWebhookEvent(fakeEvent as any)
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_founder_simulate_webhook',
    targetEmail: userId || null,
    planKey: type,
    reason: sanitizeReason(req.body?.reason) || 'manual_webhook_simulation'
  })
  return res.json({
    ok: true,
    simulated: stored
  })
})

router.post('/founder-tools/generate-test-user', async (req: any, res) => {
  const prefix = String(req.body?.prefix || 'internal-test').trim().toLowerCase().replace(/[^a-z0-9\-]/g, '')
  const base = prefix || 'internal-test'
  const email = `${base}+${Date.now()}-${crypto.randomUUID().slice(0, 6)}@autoeditor.internal`
  const planTier = parsePlanTier(req.body?.planTier) || 'free'
  const user = await prisma.user.create({
    data: {
      email,
      planStatus: planTier === 'free' ? 'free' : 'active'
    }
  })
  if (planTier !== 'free') {
    const periodEnd = new Date(Date.now() + 30 * DAY_MS)
    await prisma.subscription.upsert({
      where: { userId: user.id },
      create: {
        userId: user.id,
        stripeCustomerId: null,
        stripeSubscriptionId: null,
        status: 'active',
        planTier,
        priceId: `admin_test_${planTier}`,
        currentPeriodEnd: periodEnd,
        cancelAtPeriodEnd: true
      },
      update: {
        status: 'active',
        planTier,
        priceId: `admin_test_${planTier}`,
        currentPeriodEnd: periodEnd,
        cancelAtPeriodEnd: true
      }
    })
    await prisma.user.update({
      where: { id: user.id },
      data: {
        planStatus: 'active',
        currentPeriodEnd: periodEnd
      }
    })
  }
  await auditAdminAction({
    actor: req.user?.email || req.user?.id || null,
    action: 'admin_founder_generate_test_user',
    targetEmail: user.email || null,
    planKey: user.id,
    reason: sanitizeReason(req.body?.reason) || 'generate_test_user'
  })
  return res.json({
    ok: true,
    user: {
      id: user.id,
      email: user.email,
      planTier
    }
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
  const providerStatus = getWeeklyReportProviderStatus()
  if (!providerStatus.configured) {
    return res.status(400).json({
      error: 'weekly_report_provider_not_configured',
      message: 'Configure WEEKLY_REPORT_WEBHOOK_URL or RESEND_API_KEY before sending weekly reports.'
    })
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
    const message = String(err?.message || 'weekly report send failed')
    if (message === 'weekly_report_provider_not_configured' || message === 'weekly_report_email_provider_not_configured') {
      return res.status(400).json({
        error: 'weekly_report_provider_not_configured',
        message: 'Configure WEEKLY_REPORT_WEBHOOK_URL or RESEND_API_KEY before sending weekly reports.'
      })
    }
    return res.status(502).json({
      error: 'weekly_report_send_failed',
      message
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

  const intervalMs = parseStreamIntervalMs((req as any)?.query?.intervalMs)

  let closed = false
  const send = (eventName: string, payload: any) => {
    if (closed || res.writableEnded || (res as any).destroyed) return
    try {
      res.write(`event: ${eventName}\n`)
      res.write(`data: ${JSON.stringify(payload)}\n\n`)
    } catch {
      // best-effort stream write
    }
  }
  const realtimePayload = async () => {
    const [jobs, jobsInQueue, websiteImpressions5m, websiteImpressions24h] = await Promise.all([
      getJobsSince(RANGE_MS['24h']),
      countQueue(),
      getImpressionCountSince(5 * 60 * 1000),
      getImpressionCountSince(RANGE_MS['24h'])
    ])
    const activeUsers = getRealtimeActiveUsersCount()
    const sessions = getRealtimePresenceSessions()
    const sessionsByUser = sessions.reduce((map, session: any) => {
      const userId = String(session?.userId || '').trim()
      if (!userId) return map
      const existing = map.get(userId) || []
      existing.push(session)
      map.set(userId, existing)
      return map
    }, new Map<string, any[]>())
    const latestJobByUser = jobs.reduce((map, job: any) => {
      const userId = String(job?.userId || '').trim()
      if (!userId) return map
      const existing = map.get(userId)
      if (!existing || asMs(job?.updatedAt || job?.createdAt) > asMs(existing?.updatedAt || existing?.createdAt)) {
        map.set(userId, job)
      }
      return map
    }, new Map<string, any>())
    const usersRendering = Array.from(sessionsByUser.keys()).filter((userId) => {
      const latest = latestJobByUser.get(userId)
      return QUEUE_STATUSES.has(String(latest?.status || '').toLowerCase())
    }).length
    const exportWindowMs = 12 * 60 * 1000
    const usersExporting = Array.from(sessionsByUser.keys()).filter((userId) => {
      const latest = latestJobByUser.get(userId)
      const status = String(latest?.status || '').toLowerCase()
      const updatedMs = asMs(latest?.updatedAt || latest?.createdAt)
      return status === 'completed' && updatedMs > Date.now() - exportWindowMs
    }).length
    const averageSessionMinutes = sessions.length
      ? Number(
          (
            sessions.reduce((sum, session: any) => {
              const connectedMs = asMs(session?.connectedAt)
              if (!connectedMs) return sum
              return sum + Math.max(0, (Date.now() - connectedMs) / 60_000)
            }, 0) / sessions.length
          ).toFixed(1)
        )
      : 0
    return {
      activeUsers,
      connectedRealtimeClients: getConnectedRealtimeClientCount(),
      jobsInQueue,
      jobsFailed24h: jobs.filter((job) => String(job?.status || '').toLowerCase() === 'failed').length,
      websiteImpressions5m,
      websiteImpressions24h,
      liveUsers: {
        usersOnSite: activeUsers,
        usersRendering,
        usersExporting,
        averageSessionMinutes
      },
      t: new Date().toISOString()
    }
  }
  const sendRealtime = async () => {
    try {
      send('realtime', await realtimePayload())
    } catch (err: any) {
      send('stream_warning', {
        message: String(err?.message || 'realtime_payload_failed'),
        t: new Date().toISOString()
      })
    }
  }

  send('ready', { ok: true, intervalMs, t: new Date().toISOString() })
  await sendRealtime()

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

  const realtimeTimer = setInterval(() => {
    void sendRealtime()
  }, intervalMs)
  const keepaliveTimer = setInterval(() => {
    if (!closed && !res.writableEnded && !(res as any).destroyed) {
      res.write(':keepalive\n\n')
    }
  }, 15_000)
  realtimeTimer.unref()
  keepaliveTimer.unref()
  req.on('close', () => {
    closed = true
    clearInterval(realtimeTimer)
    clearInterval(keepaliveTimer)
    unsubscribe()
    res.end()
  })
})

export default router
