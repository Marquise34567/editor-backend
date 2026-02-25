import { prisma } from '../db/prisma'

export type WatermarkOverride = 'auto' | 'force_on' | 'force_off'
export type HookLogicMode = 'stable' | 'experimental'
export type SubtitleEngineMode = 'v1' | 'v2'
export type RetentionAlgorithmMode = 'adaptive_v3' | 'emotional_focus' | 'safe_mode'
export type ZoomIntensityLevel = 'low' | 'medium' | 'high'
export type RetentionModelVariant = 'v1' | 'v2'

export type FeatureLabControls = {
  hookLogicMode: HookLogicMode
  subtitleEngineMode: SubtitleEngineMode
  maxUploadSizeMb: number
  aiIntensity: number
  watermarkOverride: WatermarkOverride
  retentionAlgorithmMode: RetentionAlgorithmMode
  zoomIntensityLevel: ZoomIntensityLevel
  emotionalDetectionThreshold: number
  retentionModelVariant: RetentionModelVariant
  updatedAt: string
  updatedBy: string | null
}

const DEFAULT_CONTROLS: FeatureLabControls = {
  hookLogicMode: 'stable',
  subtitleEngineMode: 'v1',
  maxUploadSizeMb: 2048,
  aiIntensity: 1,
  watermarkOverride: 'auto',
  retentionAlgorithmMode: 'adaptive_v3',
  zoomIntensityLevel: 'medium',
  emotionalDetectionThreshold: 0.55,
  retentionModelVariant: 'v1',
  updatedAt: new Date(0).toISOString(),
  updatedBy: null
}

const RUNTIME_CONTROL_ROW_ID = 'global'
const MIN_UPLOAD_MB = 50
const MAX_UPLOAD_MB = 8192
const MIN_AI_INTENSITY = 0.4
const MAX_AI_INTENSITY = 2
const MIN_EMOTIONAL_THRESHOLD = 0.1
const MAX_EMOTIONAL_THRESHOLD = 1

let controlsCache: FeatureLabControls = {
  ...DEFAULT_CONTROLS,
  updatedAt: new Date().toISOString()
}
let controlsLoaded = false
let controlsInfraEnsured = false

const canRunRawSql = () =>
  typeof (prisma as any)?.$executeRawUnsafe === 'function' &&
  typeof (prisma as any)?.$queryRawUnsafe === 'function'

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const parseUploadLimitMb = (value: unknown, fallback: number) => {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.round(clamp(parsed, MIN_UPLOAD_MB, MAX_UPLOAD_MB))
}

const parseAiIntensity = (value: unknown, fallback: number) => {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Number(clamp(parsed, MIN_AI_INTENSITY, MAX_AI_INTENSITY).toFixed(2))
}

const parseEmotionalThreshold = (value: unknown, fallback: number) => {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Number(clamp(parsed, MIN_EMOTIONAL_THRESHOLD, MAX_EMOTIONAL_THRESHOLD).toFixed(2))
}

const normalizeEnum = <T extends string>(
  value: unknown,
  allowed: readonly T[],
  fallback: T
): T => {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return fallback
  const hit = allowed.find((candidate) => candidate === normalized)
  return hit || fallback
}

const parseControls = (value: unknown, existing: FeatureLabControls): FeatureLabControls => {
  const payload = value && typeof value === 'object' ? (value as Record<string, unknown>) : {}
  const now = new Date().toISOString()
  return {
    hookLogicMode: normalizeEnum(payload.hookLogicMode, ['stable', 'experimental'], existing.hookLogicMode),
    subtitleEngineMode: normalizeEnum(payload.subtitleEngineMode, ['v1', 'v2'], existing.subtitleEngineMode),
    maxUploadSizeMb: parseUploadLimitMb(payload.maxUploadSizeMb, existing.maxUploadSizeMb),
    aiIntensity: parseAiIntensity(payload.aiIntensity, existing.aiIntensity),
    watermarkOverride: normalizeEnum(payload.watermarkOverride, ['auto', 'force_on', 'force_off'], existing.watermarkOverride),
    retentionAlgorithmMode: normalizeEnum(
      payload.retentionAlgorithmMode,
      ['adaptive_v3', 'emotional_focus', 'safe_mode'],
      existing.retentionAlgorithmMode
    ),
    zoomIntensityLevel: normalizeEnum(payload.zoomIntensityLevel, ['low', 'medium', 'high'], existing.zoomIntensityLevel),
    emotionalDetectionThreshold: parseEmotionalThreshold(
      payload.emotionalDetectionThreshold,
      existing.emotionalDetectionThreshold
    ),
    retentionModelVariant: normalizeEnum(payload.retentionModelVariant, ['v1', 'v2'], existing.retentionModelVariant),
    updatedAt: typeof payload.updatedAt === 'string' && payload.updatedAt.trim() ? payload.updatedAt : now,
    updatedBy: typeof payload.updatedBy === 'string' && payload.updatedBy.trim() ? payload.updatedBy : existing.updatedBy
  }
}

const ensureControlsInfra = async () => {
  if (controlsInfraEnsured || !canRunRawSql()) return
  try {
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS admin_runtime_controls (
        id TEXT PRIMARY KEY,
        controls JSONB NOT NULL DEFAULT '{}'::jsonb,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    controlsInfraEnsured = true
  } catch {
    // fallback to in-memory mode
  }
}

const saveControlsToDb = async () => {
  if (!canRunRawSql()) return
  try {
    await ensureControlsInfra()
    await (prisma as any).$executeRawUnsafe(
      `
        INSERT INTO admin_runtime_controls (id, controls, updated_at)
        VALUES ($1, $2::jsonb, $3)
        ON CONFLICT (id) DO UPDATE
        SET controls = EXCLUDED.controls,
            updated_at = EXCLUDED.updated_at
      `,
      RUNTIME_CONTROL_ROW_ID,
      JSON.stringify(controlsCache),
      controlsCache.updatedAt
    )
  } catch {
    // fallback to in-memory mode
  }
}

const loadControlsFromDb = async () => {
  if (!canRunRawSql()) {
    controlsLoaded = true
    return
  }
  try {
    await ensureControlsInfra()
    const rows = await (prisma as any).$queryRawUnsafe(
      `
        SELECT controls, updated_at AS "updatedAt"
        FROM admin_runtime_controls
        WHERE id = $1
        LIMIT 1
      `,
      RUNTIME_CONTROL_ROW_ID
    )
    const row = Array.isArray(rows) && rows.length ? rows[0] : null
    if (row) {
      const controls = parseControls((row as any).controls || {}, controlsCache)
      controls.updatedAt = (row as any).updatedAt
        ? new Date((row as any).updatedAt).toISOString()
        : controls.updatedAt
      controlsCache = controls
    }
  } catch {
    // fallback to in-memory mode
  } finally {
    controlsLoaded = true
  }
}

export const getFeatureLabControls = async (): Promise<FeatureLabControls> => {
  if (!controlsLoaded) {
    await loadControlsFromDb()
  }
  return { ...controlsCache }
}

export const getCachedFeatureLabControls = (): FeatureLabControls => ({ ...controlsCache })

export const updateFeatureLabControls = async (
  patch: Partial<FeatureLabControls> | Record<string, unknown>,
  actor?: string | null
): Promise<FeatureLabControls> => {
  const current = await getFeatureLabControls()
  const next = parseControls(
    {
      ...current,
      ...(patch || {}),
      updatedAt: new Date().toISOString(),
      updatedBy: actor || null
    },
    current
  )
  controlsCache = next
  await saveControlsToDb()
  return { ...controlsCache }
}

export const getMaxUploadSizeBytes = () => {
  const mb = parseUploadLimitMb(controlsCache.maxUploadSizeMb, DEFAULT_CONTROLS.maxUploadSizeMb)
  return Math.round(mb * 1024 * 1024)
}

export const applyWatermarkOverride = (defaultEnabled: boolean, override?: WatermarkOverride | null) => {
  const mode = override || controlsCache.watermarkOverride
  if (mode === 'force_on') return true
  if (mode === 'force_off') return false
  return defaultEnabled
}
