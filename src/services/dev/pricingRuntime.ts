import { z } from 'zod'
import { prisma } from '../../db/prisma'
import { PLAN_CONFIG, PLAN_TIERS, type PlanTier } from '../../shared/planConfig'

export type RuntimePricingFeature = {
  key: string
  label: string
  enabled: boolean
}

export type RuntimePlanPricing = {
  tier: PlanTier
  name: string
  description: string
  priceMonthly: number
  annualPrice: number | null
  priceLabel: string
  badge: 'popular' | 'founder' | null
  visible: boolean
  features: RuntimePricingFeature[]
}

export type RuntimePricingConfig = {
  plans: Record<PlanTier, RuntimePlanPricing>
  updatedAt: string
  updatedBy: string | null
  version: number
}

export type RuntimePlanPatch = Partial<{
  name: string
  description: string
  priceMonthly: number
  annualPrice: number | null
  badge: 'popular' | 'founder' | null
  visible: boolean
  features: RuntimePricingFeature[]
}>

export type RuntimePricingPatch = Partial<Record<PlanTier, RuntimePlanPatch>>

const RUNTIME_PRICING_ROW_ID = 'global'
const FEATURE_LIMIT = 40
const MAX_PRICE = 100_000

const canRunRawSql = () =>
  typeof (prisma as any)?.$executeRawUnsafe === 'function' &&
  typeof (prisma as any)?.$queryRawUnsafe === 'function'

const planTierSchema = z.enum(PLAN_TIERS as [PlanTier, ...PlanTier[]])

const runtimeFeatureSchema = z
  .object({
    key: z
      .string()
      .trim()
      .min(1)
      .max(80)
      .regex(/^[a-z0-9:_-]+$/i),
    label: z.string().trim().min(1).max(120),
    enabled: z.boolean()
  })
  .strict()

const runtimePlanPatchSchema = z
  .object({
    name: z.string().trim().min(1).max(60).optional(),
    description: z.string().trim().min(1).max(200).optional(),
    priceMonthly: z.number().min(0).max(MAX_PRICE).optional(),
    annualPrice: z.number().min(0).max(MAX_PRICE).nullable().optional(),
    badge: z.union([z.literal('popular'), z.literal('founder'), z.null()]).optional(),
    visible: z.boolean().optional(),
    features: z.array(runtimeFeatureSchema).max(FEATURE_LIMIT).optional()
  })
  .strict()

const runtimePricingPatchSchema = z
  .object({
    free: runtimePlanPatchSchema.optional(),
    starter: runtimePlanPatchSchema.optional(),
    creator: runtimePlanPatchSchema.optional(),
    studio: runtimePlanPatchSchema.optional(),
    founder: runtimePlanPatchSchema.optional()
  })
  .strict()

const slugFeature = (label: string) =>
  label
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 80)

const toMoneyLabel = (value: number) => `$${Math.max(0, Math.round(value)).toString()}`

const normalizeFeature = (feature: RuntimePricingFeature): RuntimePricingFeature => {
  const safeLabel = String(feature.label || '').trim().slice(0, 120) || 'Feature'
  const safeKey = slugFeature(String(feature.key || '').trim()) || slugFeature(safeLabel) || `feature_${Date.now()}`
  return {
    key: safeKey,
    label: safeLabel,
    enabled: Boolean(feature.enabled)
  }
}

const defaultRuntimePricing = (): RuntimePricingConfig => {
  const plans = PLAN_TIERS.reduce((acc, tier) => {
    const plan = PLAN_CONFIG[tier]
    const features = Array.isArray(plan.features) ? plan.features : []
    acc[tier] = {
      tier,
      name: String(plan.name || tier),
      description: String(plan.description || ''),
      priceMonthly: Number.isFinite(Number(plan.priceMonthly)) ? Number(plan.priceMonthly) : 0,
      annualPrice: Number.isFinite(Number(plan.priceMonthly)) ? Number(plan.priceMonthly) * 12 : null,
      priceLabel: String(plan.priceLabel || toMoneyLabel(Number(plan.priceMonthly || 0))),
      badge: plan.badge || null,
      visible: true,
      features: features
        .slice(0, FEATURE_LIMIT)
        .map((label) => normalizeFeature({ key: slugFeature(label), label, enabled: true }))
    }
    return acc
  }, {} as Record<PlanTier, RuntimePlanPricing>)
  return {
    plans,
    updatedAt: new Date(0).toISOString(),
    updatedBy: null,
    version: 1
  }
}

let runtimePricingCache: RuntimePricingConfig = defaultRuntimePricing()
let runtimePricingLoaded = false
let runtimePricingInfraEnsured = false

const coerceTier = (value: unknown): PlanTier | null => {
  const parsed = planTierSchema.safeParse(String(value || '').trim().toLowerCase())
  return parsed.success ? parsed.data : null
}

const normalizeRuntimePricing = (value: unknown): RuntimePricingConfig => {
  const fallback = defaultRuntimePricing()
  if (!value || typeof value !== 'object') return fallback
  const payload = value as Record<string, unknown>
  const plansPayload = payload.plans && typeof payload.plans === 'object' ? (payload.plans as Record<string, unknown>) : {}

  const plans = PLAN_TIERS.reduce((acc, tier) => {
    const base = fallback.plans[tier]
    const row = plansPayload[tier] && typeof plansPayload[tier] === 'object'
      ? (plansPayload[tier] as Record<string, unknown>)
      : {}
    const rawFeatures = Array.isArray(row.features) ? row.features : base.features
    const features = rawFeatures
      .map((entry) => {
        if (!entry || typeof entry !== 'object') return null
        const parsed = runtimeFeatureSchema.safeParse(entry)
        if (!parsed.success) return null
        return normalizeFeature(parsed.data)
      })
      .filter((entry): entry is RuntimePricingFeature => Boolean(entry))
      .slice(0, FEATURE_LIMIT)
    acc[tier] = {
      tier,
      name: String(row.name || base.name).trim().slice(0, 60) || base.name,
      description: String(row.description || base.description).trim().slice(0, 200),
      priceMonthly: Number.isFinite(Number(row.priceMonthly)) ? Math.max(0, Number(row.priceMonthly)) : base.priceMonthly,
      annualPrice:
        row.annualPrice === null
          ? null
          : Number.isFinite(Number(row.annualPrice))
          ? Math.max(0, Number(row.annualPrice))
          : base.annualPrice,
      priceLabel: String(row.priceLabel || toMoneyLabel(Number(row.priceMonthly ?? base.priceMonthly))),
      badge:
        row.badge === 'popular' || row.badge === 'founder'
          ? (row.badge as 'popular' | 'founder')
          : row.badge === null
          ? null
          : base.badge,
      visible: typeof row.visible === 'boolean' ? row.visible : base.visible,
      features: features.length ? features : base.features
    }
    return acc
  }, {} as Record<PlanTier, RuntimePlanPricing>)

  return {
    plans,
    updatedAt:
      typeof payload.updatedAt === 'string' && payload.updatedAt.trim()
        ? payload.updatedAt
        : fallback.updatedAt,
    updatedBy:
      typeof payload.updatedBy === 'string' && payload.updatedBy.trim()
        ? payload.updatedBy.trim().slice(0, 140)
        : null,
    version: Number.isFinite(Number(payload.version)) ? Math.max(1, Math.round(Number(payload.version))) : fallback.version
  }
}

const ensureRuntimePricingInfra = async () => {
  if (runtimePricingInfraEnsured || !canRunRawSql()) return
  await (prisma as any).$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS pricing_runtime_config (
      id TEXT PRIMARY KEY,
      config JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_by TEXT NULL
    )
  `)
  runtimePricingInfraEnsured = true
}

const loadRuntimePricingFromDb = async () => {
  if (!canRunRawSql()) {
    runtimePricingLoaded = true
    return
  }
  try {
    await ensureRuntimePricingInfra()
    const rows = await (prisma as any).$queryRawUnsafe(
      `
        SELECT config, updated_at AS "updatedAt", updated_by AS "updatedBy"
        FROM pricing_runtime_config
        WHERE id = $1
        LIMIT 1
      `,
      RUNTIME_PRICING_ROW_ID
    )
    const row = Array.isArray(rows) && rows.length ? rows[0] : null
    if (row) {
      const normalized = normalizeRuntimePricing((row as any).config || {})
      normalized.updatedAt = (row as any).updatedAt
        ? new Date((row as any).updatedAt).toISOString()
        : normalized.updatedAt
      normalized.updatedBy = (row as any).updatedBy ? String((row as any).updatedBy) : normalized.updatedBy
      runtimePricingCache = normalized
    }
  } catch {
    // in-memory fallback
  } finally {
    runtimePricingLoaded = true
  }
}

const saveRuntimePricingToDb = async () => {
  if (!canRunRawSql()) return
  try {
    await ensureRuntimePricingInfra()
    await (prisma as any).$executeRawUnsafe(
      `
        INSERT INTO pricing_runtime_config (id, config, updated_at, updated_by)
        VALUES ($1, $2::jsonb, $3, $4)
        ON CONFLICT (id) DO UPDATE
        SET config = EXCLUDED.config,
            updated_at = EXCLUDED.updated_at,
            updated_by = EXCLUDED.updated_by
      `,
      RUNTIME_PRICING_ROW_ID,
      JSON.stringify(runtimePricingCache),
      runtimePricingCache.updatedAt,
      runtimePricingCache.updatedBy || null
    )
  } catch {
    // in-memory fallback
  }
}

export const getRuntimePricingConfig = async (): Promise<RuntimePricingConfig> => {
  if (!runtimePricingLoaded) {
    await loadRuntimePricingFromDb()
  }
  return JSON.parse(JSON.stringify(runtimePricingCache)) as RuntimePricingConfig
}

export const getRuntimePricingConfigSnapshot = (): RuntimePricingConfig =>
  JSON.parse(JSON.stringify(runtimePricingCache)) as RuntimePricingConfig

export const validateRuntimePricingPatch = (input: unknown) => runtimePricingPatchSchema.parse(input)

export const updateRuntimePricingConfig = async ({
  patch,
  actor
}: {
  patch: RuntimePricingPatch
  actor?: string | null
}): Promise<RuntimePricingConfig> => {
  const current = await getRuntimePricingConfig()
  const validated = runtimePricingPatchSchema.parse(patch)

  const nextPlans = { ...current.plans }
  for (const [tierRaw, tierPatch] of Object.entries(validated)) {
    const tier = coerceTier(tierRaw)
    if (!tier || !tierPatch) continue
    const existing = nextPlans[tier]
    const mergedFeatures = Array.isArray(tierPatch.features)
      ? tierPatch.features.map(normalizeFeature).slice(0, FEATURE_LIMIT)
      : existing.features
    const nextPrice = typeof tierPatch.priceMonthly === 'number' ? Math.max(0, tierPatch.priceMonthly) : existing.priceMonthly
    nextPlans[tier] = {
      ...existing,
      name: typeof tierPatch.name === 'string' ? tierPatch.name.trim().slice(0, 60) : existing.name,
      description:
        typeof tierPatch.description === 'string'
          ? tierPatch.description.trim().slice(0, 200)
          : existing.description,
      priceMonthly: nextPrice,
      annualPrice:
        tierPatch.annualPrice === null
          ? null
          : typeof tierPatch.annualPrice === 'number'
          ? Math.max(0, tierPatch.annualPrice)
          : existing.annualPrice,
      badge:
        tierPatch.badge === 'popular' || tierPatch.badge === 'founder' || tierPatch.badge === null
          ? tierPatch.badge
          : existing.badge,
      visible: typeof tierPatch.visible === 'boolean' ? tierPatch.visible : existing.visible,
      priceLabel: toMoneyLabel(nextPrice),
      features: mergedFeatures
    }
  }

  runtimePricingCache = {
    plans: nextPlans,
    updatedAt: new Date().toISOString(),
    updatedBy: actor ? String(actor).slice(0, 140) : null,
    version: current.version + 1
  }
  await saveRuntimePricingToDb()
  return getRuntimePricingConfig()
}

export const getPublicRuntimePricing = async () => {
  const runtime = await getRuntimePricingConfig()
  return {
    updatedAt: runtime.updatedAt,
    version: runtime.version,
    plans: PLAN_TIERS.reduce((acc, tier) => {
      const row = runtime.plans[tier]
      acc[tier] = {
        tier,
        name: row.name,
        description: row.description,
        priceMonthly: row.priceMonthly,
        annualPrice: row.annualPrice,
        priceLabel: row.priceLabel,
        badge: row.badge,
        visible: row.visible,
        features: row.features.filter((feature) => feature.enabled)
      }
      return acc
    }, {} as Record<PlanTier, Omit<RuntimePlanPricing, 'features'> & { features: RuntimePricingFeature[] }>)
  }
}
