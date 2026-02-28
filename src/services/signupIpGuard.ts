import crypto from 'crypto'
import { prisma } from '../db/prisma'
import { normalizeIpAddress } from './ipBan'

type SignupIpGuardDecision = {
  allowed: boolean
  code: 'ok' | 'ip_signup_limit_reached'
}

const SIGNUP_IP_HASH_PEPPER = String(
  process.env.SIGNUP_IP_HASH_PEPPER ||
    process.env.IP_HASH_SECRET ||
    'autoeditor-signup-ip-guard-v1'
)

const inMemorySignupIpLocks = new Map<string, { email: string | null; createdAt: string }>()
let infraEnsured = false
let lastEnsureAttemptAt = 0

const canRunRawSql = () =>
  typeof (prisma as any)?.$executeRawUnsafe === 'function' &&
  typeof (prisma as any)?.$queryRawUnsafe === 'function'

const hashSignupIpAddress = (ipRaw: unknown) => {
  const normalized = normalizeIpAddress(ipRaw)
  if (!normalized) return null
  return crypto
    .createHash('sha256')
    .update(`${SIGNUP_IP_HASH_PEPPER}|${normalized}`)
    .digest('hex')
}

const ensureSignupIpGuardInfra = async () => {
  const now = Date.now()
  if (infraEnsured) return true
  if (now - lastEnsureAttemptAt < 10_000) return false
  lastEnsureAttemptAt = now
  if (!canRunRawSql()) return false
  try {
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS signup_ip_locks (
        ip_hash TEXT PRIMARY KEY,
        first_email TEXT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    infraEnsured = true
    return true
  } catch (error) {
    console.warn('ensureSignupIpGuardInfra failed; using in-memory fallback', error)
    return false
  }
}

export const canCreateSignupFromIp = async (ipRaw: unknown): Promise<SignupIpGuardDecision> => {
  const ipHash = hashSignupIpAddress(ipRaw)
  if (!ipHash) return { allowed: true, code: 'ok' }

  if (canRunRawSql()) {
    try {
      await ensureSignupIpGuardInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        'SELECT ip_hash FROM signup_ip_locks WHERE ip_hash = $1 LIMIT 1',
        ipHash
      )
      if (Array.isArray(rows) && rows.length > 0) {
        return { allowed: false, code: 'ip_signup_limit_reached' }
      }
      return { allowed: true, code: 'ok' }
    } catch (error) {
      console.warn('canCreateSignupFromIp raw sql failed; using in-memory fallback', error)
    }
  }

  return inMemorySignupIpLocks.has(ipHash)
    ? { allowed: false, code: 'ip_signup_limit_reached' }
    : { allowed: true, code: 'ok' }
}

export const claimSignupIp = async ({
  ip,
  email,
}: {
  ip: unknown
  email?: string | null
}): Promise<SignupIpGuardDecision> => {
  const ipHash = hashSignupIpAddress(ip)
  if (!ipHash) return { allowed: true, code: 'ok' }
  const normalizedEmail = email ? String(email).trim().toLowerCase().slice(0, 320) : null

  if (canRunRawSql()) {
    try {
      await ensureSignupIpGuardInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        `
          INSERT INTO signup_ip_locks (ip_hash, first_email, created_at, updated_at)
          VALUES ($1, $2, NOW(), NOW())
          ON CONFLICT (ip_hash) DO NOTHING
          RETURNING ip_hash
        `,
        ipHash,
        normalizedEmail
      )
      if (Array.isArray(rows) && rows.length > 0) {
        return { allowed: true, code: 'ok' }
      }
      return { allowed: false, code: 'ip_signup_limit_reached' }
    } catch (error) {
      console.warn('claimSignupIp raw sql failed; using in-memory fallback', error)
    }
  }

  if (inMemorySignupIpLocks.has(ipHash)) {
    return { allowed: false, code: 'ip_signup_limit_reached' }
  }
  inMemorySignupIpLocks.set(ipHash, {
    email: normalizedEmail,
    createdAt: new Date().toISOString(),
  })
  return { allowed: true, code: 'ok' }
}
