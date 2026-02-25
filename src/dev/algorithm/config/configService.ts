import crypto from 'crypto'
import { prisma } from '../../../db/prisma'
import { ALGORITHM_PRESET_TEMPLATES, DEFAULT_ALGORITHM_PARAMS, getDefaultPreset } from '../presets'
import { AlgorithmConfigParams, AlgorithmConfigVersion, algorithmConfigParamsSchema } from '../types'

const canRunRawSql = () =>
  typeof (prisma as any)?.$executeRawUnsafe === 'function' &&
  typeof (prisma as any)?.$queryRawUnsafe === 'function'

let infraEnsured = false
let loaded = false
let cache: AlgorithmConfigVersion[] = []

const ensureInfra = async () => {
  if (!canRunRawSql() || infraEnsured) return
  await (prisma as any).$executeRawUnsafe('CREATE EXTENSION IF NOT EXISTS pgcrypto')
  await (prisma as any).$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS editor_config_versions (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_by_user_id TEXT NULL,
      preset_name TEXT NULL,
      params JSONB NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT FALSE,
      note TEXT NULL
    )
  `)
  await (prisma as any).$executeRawUnsafe(
    'CREATE INDEX IF NOT EXISTS idx_editor_config_versions_created_at ON editor_config_versions (created_at DESC)'
  )
  await (prisma as any).$executeRawUnsafe(
    'CREATE INDEX IF NOT EXISTS idx_editor_config_versions_active_created_at ON editor_config_versions (is_active, created_at DESC)'
  )
  await (prisma as any).$executeRawUnsafe('ALTER TABLE jobs ADD COLUMN IF NOT EXISTS config_version_id TEXT')
  await (prisma as any).$executeRawUnsafe(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'jobs_config_version_id_fkey'
      ) THEN
        ALTER TABLE jobs
          ADD CONSTRAINT jobs_config_version_id_fkey
          FOREIGN KEY (config_version_id) REFERENCES editor_config_versions(id)
          ON DELETE SET NULL;
      END IF;
    END $$;
  `)
  await (prisma as any).$executeRawUnsafe(
    'CREATE INDEX IF NOT EXISTS idx_jobs_config_version_id ON jobs (config_version_id)'
  )
  infraEnsured = true
}

const normalizeVersionRow = (row: any): AlgorithmConfigVersion => ({
  id: String(row?.id || ''),
  created_at: row?.created_at ? new Date(row.created_at).toISOString() : new Date().toISOString(),
  created_by_user_id: row?.created_by_user_id ? String(row.created_by_user_id) : null,
  preset_name: row?.preset_name ? String(row.preset_name) : null,
  params: algorithmConfigParamsSchema.parse({
    ...DEFAULT_ALGORITHM_PARAMS,
    ...((row?.params as Record<string, unknown>) || {})
  }),
  is_active: Boolean(row?.is_active),
  note: row?.note ? String(row.note) : null
})

const loadFromDb = async () => {
  if (!canRunRawSql()) {
    loaded = true
    return
  }
  await ensureInfra()
  const rows = await (prisma as any).$queryRawUnsafe(`
    SELECT id, created_at, created_by_user_id, preset_name, params, is_active, note
    FROM editor_config_versions
    ORDER BY created_at DESC
    LIMIT 200
  `)
  cache = Array.isArray(rows) ? rows.map(normalizeVersionRow) : []
  loaded = true
}

const ensureLoaded = async () => {
  if (!loaded) await loadFromDb()
}

const insertConfigVersionInMemory = async ({
  createdByUserId,
  presetName,
  params,
  isActive,
  note
}: {
  createdByUserId?: string | null
  presetName?: string | null
  params: AlgorithmConfigParams
  isActive?: boolean
  note?: string | null
}) => {
  if (isActive) {
    cache = cache.map((row) => ({ ...row, is_active: false }))
  }
  const row: AlgorithmConfigVersion = {
    id: crypto.randomUUID(),
    created_at: new Date().toISOString(),
    created_by_user_id: createdByUserId || null,
    preset_name: presetName || null,
    params,
    is_active: Boolean(isActive),
    note: note || null
  }
  cache = [row, ...cache]
  return row
}

const ensureDefaultExists = async () => {
  await ensureLoaded()
  if (cache.length > 0) {
    const hasActive = cache.some((row) => row.is_active)
    if (!hasActive) {
      cache = cache.map((row, index) => ({ ...row, is_active: index === 0 }))
      if (canRunRawSql()) {
        await (prisma as any).$executeRawUnsafe('UPDATE editor_config_versions SET is_active = FALSE')
        await (prisma as any).$executeRawUnsafe(
          'UPDATE editor_config_versions SET is_active = TRUE WHERE id = $1',
          cache[0].id
        )
      }
    }
    return
  }

  const defaultPreset = getDefaultPreset()
  const params = algorithmConfigParamsSchema.parse(defaultPreset?.params || DEFAULT_ALGORITHM_PARAMS)

  if (!canRunRawSql()) {
    await insertConfigVersionInMemory({
      createdByUserId: null,
      presetName: defaultPreset?.name || 'Premium Creator Mode',
      params,
      isActive: true,
      note: 'Auto-created default algorithm config.'
    })
    return
  }

  await ensureInfra()
  const rows = await (prisma as any).$queryRawUnsafe(
    `
      INSERT INTO editor_config_versions (id, created_by_user_id, preset_name, params, is_active, note)
      VALUES (gen_random_uuid()::text, NULL, $1, $2::jsonb, TRUE, $3)
      RETURNING id, created_at, created_by_user_id, preset_name, params, is_active, note
    `,
    defaultPreset?.name || 'Premium Creator Mode',
    JSON.stringify(params),
    'Auto-created default algorithm config.'
  )
  const inserted = Array.isArray(rows) && rows[0] ? normalizeVersionRow(rows[0]) : null
  cache = inserted ? [inserted] : []
}

export const listAlgorithmPresets = () => ALGORITHM_PRESET_TEMPLATES

export const listConfigVersions = async (limit = 40): Promise<AlgorithmConfigVersion[]> => {
  await ensureDefaultExists()
  const safeLimit = clampLimit(limit, 1, 200)
  if (!canRunRawSql()) return cache.slice(0, safeLimit)
  const rows = await (prisma as any).$queryRawUnsafe(
    `
      SELECT id, created_at, created_by_user_id, preset_name, params, is_active, note
      FROM editor_config_versions
      ORDER BY created_at DESC
      LIMIT $1
    `,
    safeLimit
  )
  const next = Array.isArray(rows) ? rows.map(normalizeVersionRow) : []
  cache = next
  return next
}

const clampLimit = (value: number, min: number, max: number) => {
  const parsed = Math.round(Number(value || min))
  if (!Number.isFinite(parsed)) return min
  return Math.max(min, Math.min(max, parsed))
}

export const getConfigVersionById = async (id: string): Promise<AlgorithmConfigVersion | null> => {
  await ensureDefaultExists()
  const target = String(id || '').trim()
  if (!target) return null
  const fromCache = cache.find((row) => row.id === target)
  if (fromCache) return fromCache
  if (!canRunRawSql()) return null
  const rows = await (prisma as any).$queryRawUnsafe(
    `
      SELECT id, created_at, created_by_user_id, preset_name, params, is_active, note
      FROM editor_config_versions
      WHERE id = $1
      LIMIT 1
    `,
    target
  )
  const row = Array.isArray(rows) && rows[0] ? normalizeVersionRow(rows[0]) : null
  if (row) {
    cache = [row, ...cache.filter((entry) => entry.id !== row.id)]
  }
  return row
}

export const getActiveConfigVersion = async (): Promise<AlgorithmConfigVersion> => {
  await ensureDefaultExists()
  const fromCache = cache.find((row) => row.is_active)
  if (fromCache) return fromCache

  if (!canRunRawSql()) {
    const first = cache[0]
    if (!first) throw new Error('algorithm_config_unavailable')
    first.is_active = true
    return first
  }

  const rows = await (prisma as any).$queryRawUnsafe(`
    SELECT id, created_at, created_by_user_id, preset_name, params, is_active, note
    FROM editor_config_versions
    WHERE is_active = TRUE
    ORDER BY created_at DESC
    LIMIT 1
  `)
  const active = Array.isArray(rows) && rows[0] ? normalizeVersionRow(rows[0]) : cache[0]
  if (!active) throw new Error('algorithm_config_unavailable')
  cache = [active, ...cache.filter((entry) => entry.id !== active.id)]
  return active
}

export const activateConfigVersion = async (id: string): Promise<AlgorithmConfigVersion | null> => {
  await ensureDefaultExists()
  const target = String(id || '').trim()
  if (!target) return null

  if (!canRunRawSql()) {
    let matched = false
    cache = cache.map((entry) => {
      const next = { ...entry, is_active: entry.id === target }
      if (next.is_active) matched = true
      return next
    })
    return matched ? cache.find((entry) => entry.id === target) || null : null
  }

  await ensureInfra()
  await (prisma as any).$executeRawUnsafe('UPDATE editor_config_versions SET is_active = FALSE WHERE is_active = TRUE')
  const rows = await (prisma as any).$queryRawUnsafe(
    `
      UPDATE editor_config_versions
      SET is_active = TRUE
      WHERE id = $1
      RETURNING id, created_at, created_by_user_id, preset_name, params, is_active, note
    `,
    target
  )
  const activated = Array.isArray(rows) && rows[0] ? normalizeVersionRow(rows[0]) : null
  if (!activated) return null
  cache = [activated, ...cache.filter((entry) => entry.id !== activated.id).map((entry) => ({ ...entry, is_active: false }))]
  return activated
}

export const createConfigVersion = async ({
  createdByUserId,
  presetName,
  params,
  activate,
  note
}: {
  createdByUserId?: string | null
  presetName?: string | null
  params: AlgorithmConfigParams
  activate?: boolean
  note?: string | null
}): Promise<AlgorithmConfigVersion> => {
  await ensureDefaultExists()
  const validated = algorithmConfigParamsSchema.parse(params)

  if (!canRunRawSql()) {
    const inserted = await insertConfigVersionInMemory({
      createdByUserId,
      presetName,
      params: validated,
      isActive: Boolean(activate),
      note
    })
    if (activate) return inserted
    return inserted
  }

  await ensureInfra()
  if (activate) {
    await (prisma as any).$executeRawUnsafe('UPDATE editor_config_versions SET is_active = FALSE WHERE is_active = TRUE')
  }

  const rows = await (prisma as any).$queryRawUnsafe(
    `
      INSERT INTO editor_config_versions (id, created_by_user_id, preset_name, params, is_active, note)
      VALUES (gen_random_uuid()::text, $1, $2, $3::jsonb, $4, $5)
      RETURNING id, created_at, created_by_user_id, preset_name, params, is_active, note
    `,
    createdByUserId || null,
    presetName || null,
    JSON.stringify(validated),
    Boolean(activate),
    note || null
  )

  const inserted = Array.isArray(rows) && rows[0] ? normalizeVersionRow(rows[0]) : null
  if (!inserted) throw new Error('config_create_failed')

  cache = [
    inserted,
    ...cache
      .filter((entry) => entry.id !== inserted.id)
      .map((entry) => ({ ...entry, is_active: activate ? false : entry.is_active }))
  ]

  if (activate) {
    await (prisma as any).$executeRawUnsafe(
      'UPDATE jobs SET config_version_id = $1 WHERE status IN ($2, $3, $4, $5)',
      inserted.id,
      'queued',
      'uploading',
      'analyzing',
      'rendering'
    )
  }

  return inserted
}

export const rollbackConfigVersion = async (): Promise<AlgorithmConfigVersion | null> => {
  const versions = await listConfigVersions(10)
  if (!versions.length) return null
  const active = versions.find((row) => row.is_active) || versions[0]
  const previous = versions.find((row) => row.id !== active.id)
  if (!previous) return active
  return activateConfigVersion(previous.id)
}

export const parseConfigParams = (value: unknown): AlgorithmConfigParams =>
  algorithmConfigParamsSchema.parse({
    ...DEFAULT_ALGORITHM_PARAMS,
    ...((value as Record<string, unknown>) || {})
  })
