import os from 'os'
import type { PlanTier } from '../shared/planConfig'
import { PLAN_CONFIG } from '../shared/planConfig'
import { prisma } from '../db/prisma'
import { getUserPlan, isActiveSubscriptionStatus } from './plans'
import { isDevAccount } from '../lib/devAccounts'

const DAY_MS = 24 * 60 * 60 * 1000
const LIVE_STATS_CACHE_MS = 2_500
const ACTIVE_WINDOW_MS = 60_000
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

type ActiveSeriesPoint = {
  t: string
  v: number
}

type LiveStatsBuildContext = {
  activeUsers: number
  activeUsersSeries: ActiveSeriesPoint[]
  connectedClients: number
}

type LiveStatsUserContext = {
  userId: string
  email?: string | null
}

type LiveStatsAccess = {
  tier: PlanTier | string
  isPaid: boolean
  isDev: boolean
  advanced: boolean
}

type TrendingNicheRow = {
  label: string
  changePct: number
  direction: 'up' | 'down'
  volume: number
}

type SubscriptionBreakdownRow = {
  tier: string
  count: number
}

type RenderByTierRow = {
  tier: string
  renders: number
  minutes: number
}

type RecentJobRow = {
  id: string
  user: string
  status: string
  durationSec: number
  createdAt: string
}

type LiveStatsSnapshot = {
  generatedAt: string
  activeUsers: number
  activeUsersSeries: ActiveSeriesPoint[]
  rendersToday: {
    count: number
    minutesUsed: number
    byTier: RenderByTierRow[]
  }
  trendingNiches: TrendingNicheRow[]
  subscriptionMetrics: {
    totalSubs: number
    churnRatePct: number
    mrr: number
    byTier: SubscriptionBreakdownRow[]
  }
  serverLoad: {
    cpuPct: number
    ramPct: number
    rssMb: number
    heapUsedMb: number
    heapTotalMb: number
  }
  recentJobs: RecentJobRow[]
  upgradeSignals: {
    upgradedToday: number
    activeEditors: number
  }
  debug: {
    wsClients: number
    activeWindowMs: number
    dbOk: boolean
  }
}

export type LiveStatsPulse = {
  t: string
  activeUsers: number
  rendersToday: number
  upgradedToday: number
  cpuPct: number
  ramPct: number
}

export type LiveStatsResponse = {
  access: LiveStatsAccess
  snapshot: LiveStatsSnapshot
  pulse: LiveStatsPulse
  teaser: {
    locked: boolean
    message: string | null
    upgradeCta: string | null
  }
}

type LiveStatsBundle = {
  snapshot: LiveStatsSnapshot
  pulse: LiveStatsPulse
}

let cpuSample: { usage: NodeJS.CpuUsage; hrtime: bigint } | null = null
let cache: { at: number; value: LiveStatsBundle } | null = null
let inflight: Promise<LiveStatsBundle> | null = null

const asMs = (value: unknown) => {
  const ms = new Date(value as any).getTime()
  return Number.isFinite(ms) ? ms : 0
}

const toIso = (value: unknown) => {
  const ms = asMs(value)
  return ms > 0 ? new Date(ms).toISOString() : new Date().toISOString()
}

const toPct = (num: number, den: number) =>
  den > 0 ? Number(((num / den) * 100).toFixed(1)) : 0

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const toMb = (value: number) => Number((Math.max(0, Number(value || 0)) / (1024 * 1024)).toFixed(2))

const isObject = (value: unknown): value is Record<string, any> =>
  Boolean(value && typeof value === 'object' && !Array.isArray(value))

const canonicalTierOrder = ['free', 'starter', 'creator', 'studio', 'founder']

const normalizeTier = (value: unknown): string => {
  const normalized = String(value || 'free').trim().toLowerCase()
  return canonicalTierOrder.includes(normalized) ? normalized : 'free'
}

const renderSeconds = (job: any) => {
  const start = asMs(job?.createdAt)
  const end = asMs(job?.updatedAt)
  if (start > 0 && end > start) return (end - start) / 1000
  const fallback = Number(job?.inputDurationSeconds || 0)
  return Number.isFinite(fallback) && fallback > 0 ? fallback : 0
}

const formatNiche = (value: unknown) => {
  const raw = String(value || '')
    .trim()
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
  if (!raw) return null
  return raw
    .split(' ')
    .map((token) => token.slice(0, 1).toUpperCase() + token.slice(1).toLowerCase())
    .join(' ')
}

const inferNicheLabel = (job: any) => {
  const renderSettings = isObject(job?.renderSettings) ? job.renderSettings : {}
  const analysis = isObject(job?.analysis) ? job.analysis : {}
  const editorMode = String(renderSettings?.editorMode || '').trim().toLowerCase()
  const explicit =
    formatNiche(renderSettings?.contentNiche)
    || formatNiche(renderSettings?.niche)
    || formatNiche(renderSettings?.creatorNiche)
    || formatNiche(analysis?.contentNiche)
    || formatNiche(analysis?.niche)
    || formatNiche(analysis?.topic)
  if (explicit) return explicit
  if (editorMode === 'savage-roast') return 'Gen Z Pranks'
  if (editorMode === 'ultra') return 'Ultra Binge Mode'
  if (editorMode === 'retention-king') return 'Retention King'
  if (editorMode === 'gaming') return 'Gaming Clips'
  if (editorMode === 'sports') return 'Sports Highlights'
  if (editorMode === 'education') return 'Study Hacks'
  if (editorMode === 'podcast') return 'Podcast Snippets'
  if (editorMode === 'vlog') return 'Lifestyle Vlogs'
  if (editorMode === 'reaction') return 'Reaction Memes'
  if (editorMode === 'commentary') return 'Story Commentary'
  const platform = String(renderSettings?.retentionTargetPlatform || '').trim().toLowerCase()
  if (platform.includes('tiktok')) return 'TikTok Challenges'
  if (platform.includes('instagram')) return 'Reels Trends'
  if (platform.includes('youtube')) return 'YouTube Stories'
  return 'General Edits'
}

const toUserTierMap = (subscriptions: any[]) => {
  const map = new Map<string, string>()
  for (const sub of subscriptions) {
    const userId = String(sub?.userId || '').trim()
    if (!userId) continue
    const status = String(sub?.status || '').toLowerCase()
    if (!isActiveSubscriptionStatus(status)) continue
    map.set(userId, normalizeTier(sub?.planTier))
  }
  return map
}

const sumMinutesForJobs = (jobs: any[]) => {
  const seconds = jobs.reduce((sum, job) => sum + Math.max(0, Number(job?.inputDurationSeconds || 0)), 0)
  return Number((seconds / 60).toFixed(1))
}

const getCpuUsagePercent = () => {
  const now = process.hrtime.bigint()
  const usage = process.cpuUsage()
  const cpuCount = Math.max(1, os.cpus().length)
  if (!cpuSample) {
    cpuSample = { usage, hrtime: now }
    return 0
  }
  const elapsedMs = Number(now - cpuSample.hrtime) / 1_000_000
  const delta = process.cpuUsage(cpuSample.usage)
  cpuSample = { usage, hrtime: now }
  if (!Number.isFinite(elapsedMs) || elapsedMs <= 0) return 0
  const cpuMs = (Number(delta.user || 0) + Number(delta.system || 0)) / 1000
  const pct = (cpuMs / elapsedMs) * (100 / cpuCount)
  return Number(clamp(pct, 0, 100).toFixed(1))
}

const resolveAccess = async (ctx: LiveStatsUserContext): Promise<LiveStatsAccess> => {
  try {
    const plan = await getUserPlan(ctx.userId)
    const tier = normalizeTier(plan?.tier || 'free')
    const isDev = isDevAccount(ctx.userId, ctx.email || null)
    const isPaid = tier !== 'free'
    return {
      tier,
      isPaid,
      isDev,
      advanced: isPaid || isDev
    }
  } catch {
    const isDev = isDevAccount(ctx.userId, ctx.email || null)
    return {
      tier: 'free',
      isPaid: false,
      isDev,
      advanced: isDev
    }
  }
}

const buildTrending = (jobs48h: any[]) => {
  const now = Date.now()
  const currentStart = now - DAY_MS
  const previousStart = now - 2 * DAY_MS
  const current = new Map<string, number>()
  const previous = new Map<string, number>()

  for (const job of jobs48h) {
    const createdMs = asMs(job?.createdAt)
    if (!createdMs || createdMs < previousStart) continue
    const label = inferNicheLabel(job)
    if (createdMs >= currentStart) {
      current.set(label, (current.get(label) || 0) + 1)
    } else {
      previous.set(label, (previous.get(label) || 0) + 1)
    }
  }

  const allLabels = new Set([...current.keys(), ...previous.keys()])
  return Array.from(allLabels.values())
    .map((label) => {
      const nowCount = current.get(label) || 0
      const prevCount = previous.get(label) || 0
      const changeRaw = prevCount > 0 ? ((nowCount - prevCount) / prevCount) * 100 : nowCount > 0 ? 100 : 0
      const changePct = Number(clamp(changeRaw, -999, 999).toFixed(1))
      return {
        label,
        changePct,
        direction: changePct >= 0 ? 'up' : 'down',
        volume: nowCount
      } as TrendingNicheRow
    })
    .sort((a, b) => {
      if (b.volume !== a.volume) return b.volume - a.volume
      return Math.abs(b.changePct) - Math.abs(a.changePct)
    })
    .slice(0, 5)
}

const buildServerLoad = () => {
  const mem = process.memoryUsage()
  const totalMem = os.totalmem()
  const freeMem = os.freemem()
  const ramPct = totalMem > 0 ? Number((((totalMem - freeMem) / totalMem) * 100).toFixed(1)) : 0
  return {
    cpuPct: getCpuUsagePercent(),
    ramPct: Number(clamp(ramPct, 0, 100).toFixed(1)),
    rssMb: toMb(mem.rss),
    heapUsedMb: toMb(mem.heapUsed),
    heapTotalMb: toMb(mem.heapTotal)
  }
}

const fetchRecentJobUsers = async (jobs: any[]) => {
  const ids = Array.from(
    new Set(
      jobs.map((job) => String(job?.userId || '').trim()).filter(Boolean)
    )
  )
  const byId = new Map<string, string>()
  await Promise.all(
    ids.map(async (id) => {
      try {
        const user = await prisma.user.findUnique({ where: { id } })
        if (user?.email) {
          byId.set(id, String(user.email))
        }
      } catch {
        // best effort
      }
    })
  )
  return byId
}

const maskTeaserSnapshot = (snapshot: LiveStatsSnapshot): LiveStatsSnapshot => {
  const teaserRecent = snapshot.recentJobs.slice(0, 4).map((job) => ({
    ...job,
    user: 'Upgrade Required',
    durationSec: Math.max(0, Math.round(job.durationSec)),
    status: job.status === 'completed' ? 'completed' : 'processing'
  }))
  return {
    ...snapshot,
    rendersToday: {
      ...snapshot.rendersToday,
      byTier: snapshot.rendersToday.byTier.map((row) => ({ ...row, minutes: 0 }))
    },
    trendingNiches: snapshot.trendingNiches.map((item) => ({
      ...item,
      changePct: Math.max(0, Math.min(99.9, Math.abs(item.changePct))),
      direction: 'up'
    })),
    subscriptionMetrics: {
      ...snapshot.subscriptionMetrics,
      churnRatePct: 0,
      mrr: 0
    },
    recentJobs: teaserRecent,
    debug: {
      ...snapshot.debug,
      dbOk: false
    }
  }
}

const buildBundleUncached = async (ctx: LiveStatsBuildContext): Promise<LiveStatsBundle> => {
  const now = Date.now()
  const dayStart = new Date(now)
  dayStart.setHours(0, 0, 0, 0)
  const last30d = new Date(now - 30 * DAY_MS)
  const last48h = new Date(now - 2 * DAY_MS)
  const last24h = now - DAY_MS

  const [jobsTodayRaw, jobs48hRaw, recentJobsRaw, subscriptionsRaw, siteEvents24h] = await Promise.all([
    prisma.job.findMany({
      where: { createdAt: { gte: dayStart } },
      orderBy: { createdAt: 'desc' },
      take: 1200
    }).catch(() => [] as any[]),
    prisma.job.findMany({
      where: { createdAt: { gte: last48h } },
      orderBy: { createdAt: 'desc' },
      take: 2500
    }).catch(() => [] as any[]),
    prisma.job.findMany({
      orderBy: { createdAt: 'desc' },
      take: 12
    }).catch(() => [] as any[]),
    prisma.subscription.findMany({
      orderBy: { updatedAt: 'desc' }
    }).catch(() => [] as any[]),
    prisma.siteAnalyticsEvent.count({
      where: { createdAt: { gte: new Date(last24h) }, eventName: 'editor_page_view' }
    }).catch(() => 0)
  ])

  const jobsToday = Array.isArray(jobsTodayRaw) ? jobsTodayRaw : []
  const jobs48h = Array.isArray(jobs48hRaw) ? jobs48hRaw : []
  const recentJobs = Array.isArray(recentJobsRaw) ? recentJobsRaw : []
  const subscriptions = Array.isArray(subscriptionsRaw) ? subscriptionsRaw : []

  const tierByUser = toUserTierMap(subscriptions)
  const completedToday = jobsToday.filter((job) => String(job?.status || '').toLowerCase() === 'completed')
  const renderByTierMap = new Map<string, { renders: number; minutes: number }>()
  for (const tier of canonicalTierOrder) {
    renderByTierMap.set(tier, { renders: 0, minutes: 0 })
  }
  for (const job of completedToday) {
    const tier = tierByUser.get(String(job?.userId || '')) || 'free'
    const row = renderByTierMap.get(tier) || { renders: 0, minutes: 0 }
    row.renders += 1
    row.minutes += Math.max(0, Number(job?.inputDurationSeconds || 0) / 60)
    renderByTierMap.set(tier, row)
  }

  const rendersByTier = canonicalTierOrder.map((tier) => {
    const row = renderByTierMap.get(tier) || { renders: 0, minutes: 0 }
    return {
      tier,
      renders: row.renders,
      minutes: Number(row.minutes.toFixed(1))
    } as RenderByTierRow
  })

  const activeSubs = subscriptions.filter((sub) => isActiveSubscriptionStatus(String(sub?.status || '').toLowerCase()))
  const canceledLast30d = subscriptions.filter((sub) => {
    const status = String(sub?.status || '').toLowerCase()
    if (status !== 'canceled') return false
    return asMs(sub?.updatedAt) >= asMs(last30d)
  }).length
  const byTierMap = new Map<string, number>()
  let mrr = 0
  for (const sub of activeSubs) {
    const tier = normalizeTier(sub?.planTier)
    byTierMap.set(tier, (byTierMap.get(tier) || 0) + 1)
    const plan = PLAN_CONFIG[tier as PlanTier]
    mrr += Number(plan?.priceMonthly || 0)
  }
  const subsByTier = canonicalTierOrder
    .map((tier) => ({ tier, count: byTierMap.get(tier) || 0 }))
    .filter((row) => row.count > 0)

  const queueDepth = jobsToday.filter((job) => QUEUE_STATUSES.has(String(job?.status || '').toLowerCase())).length
  const recentUserMap = await fetchRecentJobUsers(recentJobs)
  const serverLoad = buildServerLoad()

  const snapshot: LiveStatsSnapshot = {
    generatedAt: new Date().toISOString(),
    activeUsers: Math.max(0, Number(ctx.activeUsers || 0)),
    activeUsersSeries: (ctx.activeUsersSeries || [])
      .filter((point) => asMs(point?.t) >= now - DAY_MS)
      .map((point) => ({
        t: toIso(point?.t),
        v: Math.max(0, Number(point?.v || 0))
      }))
      .slice(-24 * 60),
    rendersToday: {
      count: completedToday.length,
      minutesUsed: sumMinutesForJobs(completedToday),
      byTier: rendersByTier
    },
    trendingNiches: buildTrending(jobs48h),
    subscriptionMetrics: {
      totalSubs: activeSubs.length,
      churnRatePct: toPct(canceledLast30d, activeSubs.length + canceledLast30d),
      mrr: Number(mrr.toFixed(2)),
      byTier: subsByTier
    },
    serverLoad,
    recentJobs: recentJobs.map((job) => {
      const userId = String(job?.userId || '')
      const email = recentUserMap.get(userId) || userId || 'unknown'
      return {
        id: String(job?.id || ''),
        user: email,
        status: String(job?.status || 'queued'),
        durationSec: Number(renderSeconds(job).toFixed(1)),
        createdAt: toIso(job?.createdAt)
      } as RecentJobRow
    }),
    upgradeSignals: {
      upgradedToday: subscriptions.filter((sub) => {
        const status = String(sub?.status || '').toLowerCase()
        const tier = normalizeTier(sub?.planTier)
        return isActiveSubscriptionStatus(status) && tier !== 'free' && asMs(sub?.updatedAt) >= asMs(dayStart)
      }).length,
      activeEditors: Math.max(0, Number(siteEvents24h || 0))
    },
    debug: {
      wsClients: Math.max(0, Number(ctx.connectedClients || 0)),
      activeWindowMs: ACTIVE_WINDOW_MS,
      dbOk: true
    }
  }

  const pulse: LiveStatsPulse = {
    t: snapshot.generatedAt,
    activeUsers: snapshot.activeUsers,
    rendersToday: snapshot.rendersToday.count,
    upgradedToday: snapshot.upgradeSignals.upgradedToday,
    cpuPct: snapshot.serverLoad.cpuPct,
    ramPct: snapshot.serverLoad.ramPct
  }

  if (!snapshot.trendingNiches.length) {
    snapshot.trendingNiches = [
      { label: 'Gen Z Pranks', changePct: 20, direction: 'up', volume: 0 },
      { label: 'Story Commentary', changePct: 14, direction: 'up', volume: 0 },
      { label: 'Gaming Clips', changePct: 9, direction: 'up', volume: 0 },
      { label: 'Study Hacks', changePct: 6, direction: 'up', volume: 0 },
      { label: 'Podcast Snippets', changePct: 3, direction: 'up', volume: 0 }
    ]
  }

  if (!snapshot.subscriptionMetrics.byTier.length) {
    snapshot.subscriptionMetrics.byTier = canonicalTierOrder.map((tier) => ({ tier, count: 0 }))
  }

  if (!snapshot.activeUsersSeries.length) {
    snapshot.activeUsersSeries = [{ t: snapshot.generatedAt, v: snapshot.activeUsers }]
  }

  if (!snapshot.rendersToday.byTier.some((row) => row.renders > 0)) {
    snapshot.rendersToday.byTier = canonicalTierOrder.map((tier) => ({
      tier,
      renders: 0,
      minutes: 0
    }))
  }

  if (!snapshot.recentJobs.length) {
    snapshot.recentJobs = [
      {
        id: 'pending',
        user: 'No jobs yet',
        status: queueDepth > 0 ? 'queued' : 'idle',
        durationSec: 0,
        createdAt: snapshot.generatedAt
      }
    ]
  }

  return { snapshot, pulse }
}

const getBaseBundle = async (ctx: LiveStatsBuildContext): Promise<LiveStatsBundle> => {
  const now = Date.now()
  if (cache && now - cache.at < LIVE_STATS_CACHE_MS) {
    return cache.value
  }
  if (inflight) return inflight
  inflight = buildBundleUncached(ctx)
    .then((value) => {
      cache = { at: Date.now(), value }
      return value
    })
    .finally(() => {
      inflight = null
    })
  return inflight
}

export const buildLiveStatsResponse = async ({
  user,
  context
}: {
  user: LiveStatsUserContext
  context: LiveStatsBuildContext
}): Promise<LiveStatsResponse> => {
  const [bundle, access] = await Promise.all([
    getBaseBundle(context),
    resolveAccess(user)
  ])

  const snapshot = access.advanced ? bundle.snapshot : maskTeaserSnapshot(bundle.snapshot)

  return {
    access,
    snapshot,
    pulse: bundle.pulse,
    teaser: {
      locked: !access.advanced,
      message: access.advanced ? null : 'Upgrade to unlock full real-time metrics, deep drill-downs, and precision trends.',
      upgradeCta: access.advanced ? null : '/pricing'
    }
  }
}

export const buildPublicLivePulse = async (context: LiveStatsBuildContext): Promise<LiveStatsPulse> => {
  const bundle = await getBaseBundle(context)
  return bundle.pulse
}
