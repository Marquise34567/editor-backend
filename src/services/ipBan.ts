import net from 'net'
import { prisma } from '../db/prisma'

type IpBanRow = {
  ip: string
  reason: string | null
  createdBy: string | null
  active: boolean
  expiresAt: Date | string | null
  createdAt: Date | string
  updatedAt?: Date | string
}

const CACHE_TTL_MS = 30_000
let cacheLoadedAt = 0
let cachedActiveBans = new Map<string, IpBanRow>()

const nowMs = () => Date.now()

const toMs = (value: unknown) => {
  if (!value) return 0
  const ms = new Date(value as any).getTime()
  return Number.isFinite(ms) ? ms : 0
}

const isActiveBan = (row: IpBanRow | null | undefined) => {
  if (!row || !row.active) return false
  const expiresMs = toMs(row.expiresAt)
  if (!expiresMs) return true
  return expiresMs > nowMs()
}

const normalizeIPv4Port = (raw: string) => {
  const match = /^(\d+\.\d+\.\d+\.\d+):\d+$/.exec(raw)
  return match ? match[1] : raw
}

export const normalizeIpAddress = (value: unknown) => {
  const raw = String(value || '').trim().toLowerCase()
  if (!raw) return null
  let normalized = raw
  if (normalized.startsWith('::ffff:')) {
    normalized = normalized.slice('::ffff:'.length)
  }
  if (normalized.startsWith('[') && normalized.endsWith(']')) {
    normalized = normalized.slice(1, -1)
  }
  normalized = normalizeIPv4Port(normalized)
  const ipVersion = net.isIP(normalized)
  if (!ipVersion) return null
  return normalized
}

export const getRequestIpAddress = (req: any) => {
  const forwarded = String(req?.headers?.['x-forwarded-for'] || '')
    .split(',')[0]
    ?.trim()
  return normalizeIpAddress(forwarded || req?.ip || req?.socket?.remoteAddress || null)
}

const hydrateCacheFromRows = (rows: any[]) => {
  const next = new Map<string, IpBanRow>()
  for (const row of rows) {
    const ip = normalizeIpAddress(row?.ip)
    if (!ip) continue
    const candidate: IpBanRow = {
      ip,
      reason: row?.reason ? String(row.reason) : null,
      createdBy: row?.createdBy ? String(row.createdBy) : null,
      active: Boolean(row?.active ?? true),
      expiresAt: row?.expiresAt || null,
      createdAt: row?.createdAt || new Date().toISOString(),
      updatedAt: row?.updatedAt || null
    }
    if (isActiveBan(candidate)) {
      next.set(ip, candidate)
    }
  }
  cachedActiveBans = next
  cacheLoadedAt = nowMs()
}

const refreshBanCache = async (force = false) => {
  if (!force && cacheLoadedAt > 0 && nowMs() - cacheLoadedAt <= CACHE_TTL_MS) return
  try {
    const rows = await prisma.bannedIp.findMany({
      where: { active: true }
    })
    hydrateCacheFromRows(Array.isArray(rows) ? rows : [])
  } catch {
    // Ignore DB errors and keep existing cache snapshot.
    cacheLoadedAt = nowMs()
  }
}

export const getActiveIpBan = async (ipRaw: unknown) => {
  const ip = normalizeIpAddress(ipRaw)
  if (!ip) return null
  await refreshBanCache()
  const cached = cachedActiveBans.get(ip)
  if (cached && isActiveBan(cached)) return cached
  return null
}

export const listIpBans = async () => {
  try {
    const rows = await prisma.bannedIp.findMany({ orderBy: { createdAt: 'desc' } })
    return Array.isArray(rows)
      ? rows.map((row: any) => ({
          ip: normalizeIpAddress(row?.ip) || String(row?.ip || ''),
          reason: row?.reason ? String(row.reason) : null,
          createdBy: row?.createdBy ? String(row.createdBy) : null,
          active: Boolean(row?.active ?? true),
          expiresAt: row?.expiresAt || null,
          createdAt: row?.createdAt || null,
          updatedAt: row?.updatedAt || null
        }))
      : []
  } catch {
    return []
  }
}

export const banIpAddress = async ({
  ip,
  reason,
  createdBy,
  expiresAt
}: {
  ip: string
  reason?: string | null
  createdBy?: string | null
  expiresAt?: Date | null
}) => {
  const normalizedIp = normalizeIpAddress(ip)
  if (!normalizedIp) {
    const err: any = new Error('invalid_ip')
    err.code = 'invalid_ip'
    throw err
  }
  const record = await prisma.bannedIp.upsert({
    where: { ip: normalizedIp },
    create: {
      ip: normalizedIp,
      reason: reason || null,
      createdBy: createdBy || null,
      active: true,
      expiresAt: expiresAt || null
    },
    update: {
      reason: reason || null,
      createdBy: createdBy || null,
      active: true,
      expiresAt: expiresAt || null
    }
  })
  cacheLoadedAt = 0
  await refreshBanCache(true)
  return record
}

export const unbanIpAddress = async (ip: string) => {
  const normalizedIp = normalizeIpAddress(ip)
  if (!normalizedIp) return null
  try {
    const deleted = await prisma.bannedIp.delete({ where: { ip: normalizedIp } })
    cacheLoadedAt = 0
    await refreshBanCache(true)
    return deleted
  } catch {
    return null
  }
}

export const warmIpBanCache = async () => {
  await refreshBanCache(true)
}
