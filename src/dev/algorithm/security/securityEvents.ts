import crypto from 'crypto'
import { prisma } from '../../../db/prisma'

const canRunRawSql = () =>
  typeof (prisma as any)?.$executeRawUnsafe === 'function' &&
  typeof (prisma as any)?.$queryRawUnsafe === 'function'

let infraEnsured = false
const inMemoryEvents: Array<{
  id: string
  created_at: string
  type: string
  meta: Record<string, unknown>
}> = []

const ensureInfra = async () => {
  if (!canRunRawSql() || infraEnsured) return
  await (prisma as any).$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS security_events (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      type TEXT NOT NULL,
      meta JSONB NOT NULL
    )
  `)
  await (prisma as any).$executeRawUnsafe(
    'CREATE INDEX IF NOT EXISTS idx_security_events_created_at ON security_events (created_at DESC)'
  )
  await (prisma as any).$executeRawUnsafe(
    'CREATE INDEX IF NOT EXISTS idx_security_events_type ON security_events (type)'
  )
  infraEnsured = true
}

export const recordSecurityEvent = async ({
  type,
  meta
}: {
  type: string
  meta: Record<string, unknown>
}) => {
  const payloadType = String(type || '').trim().slice(0, 120) || 'unknown'
  const payloadMeta = meta && typeof meta === 'object' ? meta : { value: String(meta) }

  if (!canRunRawSql()) {
    inMemoryEvents.unshift({
      id: crypto.randomUUID(),
      created_at: new Date().toISOString(),
      type: payloadType,
      meta: payloadMeta
    })
    if (inMemoryEvents.length > 500) inMemoryEvents.length = 500
    return
  }

  try {
    await ensureInfra()
    await (prisma as any).$executeRawUnsafe(
      'INSERT INTO security_events (id, type, meta) VALUES (gen_random_uuid()::text, $1, $2::jsonb)',
      payloadType,
      JSON.stringify(payloadMeta)
    )
  } catch {
    // best-effort logging only
  }
}
