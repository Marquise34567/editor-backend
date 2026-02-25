import { z } from 'zod'
import { EDITOR_CONFIG_VERSION, EDITOR_ENGINE_VERSION, EDITOR_RETENTION_CONFIG } from './editorConfig'

type NormalizedHookCandidate = {
  start: number
  duration: number
  score: number
  auditScore: number
  auditPassed: boolean
  text: string
  reason: string
  synthetic: boolean
}

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value))
const clamp01 = (value: number) => clamp(value, 0, 1)

const toFiniteNumber = (value: unknown): number | null => {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

const HookCandidateSchema = z.object({
  start: z.any(),
  duration: z.any(),
  score: z.any().optional(),
  auditScore: z.any().optional(),
  auditPassed: z.any().optional(),
  text: z.any().optional(),
  reason: z.any().optional(),
  synthetic: z.any().optional()
}).passthrough()

const AnalysisSchema = z.object({
  hook_start_time: z.any().optional(),
  hook_end_time: z.any().optional(),
  hook_score: z.any().optional(),
  hook_audit_score: z.any().optional(),
  hook_variants: z.array(HookCandidateSchema).optional(),
  hook_candidates: z.array(HookCandidateSchema).optional(),
  preferred_hook: HookCandidateSchema.nullable().optional(),
  retention_attempts: z.array(z.any()).optional(),
  pipelineSteps: z.record(z.any()).optional(),
  metadata_summary: z.record(z.any()).optional()
}).passthrough()

const normalizeHookCandidate = (raw: unknown): NormalizedHookCandidate | null => {
  const parsed = HookCandidateSchema.safeParse(raw)
  if (!parsed.success) return null
  const item = parsed.data
  const start = toFiniteNumber(item.start)
  const duration = toFiniteNumber(item.duration)
  if (start === null || duration === null || duration <= 0) return null
  const score = toFiniteNumber(item.score)
  const auditScore = toFiniteNumber(item.auditScore)
  return {
    start: Number(start.toFixed(3)),
    duration: Number(duration.toFixed(3)),
    score: Number(clamp01(score ?? 0).toFixed(4)),
    auditScore: Number(clamp01(auditScore ?? score ?? 0).toFixed(4)),
    auditPassed: Boolean(item.auditPassed),
    text: typeof item.text === 'string' ? item.text.trim() : '',
    reason: typeof item.reason === 'string' ? item.reason.trim() : '',
    synthetic: Boolean(item.synthetic)
  }
}

const normalizeHookCandidates = (raw: unknown, limit: number): NormalizedHookCandidate[] => {
  if (!Array.isArray(raw)) return []
  const out: NormalizedHookCandidate[] = []
  const seen = new Set<string>()
  for (const entry of raw) {
    const candidate = normalizeHookCandidate(entry)
    if (!candidate) continue
    const key = `${candidate.start.toFixed(3)}:${candidate.duration.toFixed(3)}`
    if (seen.has(key)) continue
    seen.add(key)
    out.push(candidate)
    if (out.length >= limit) break
  }
  return out
}

const sanitizeHookWindow = ({
  start,
  end,
  fallback
}: {
  start: unknown
  end: unknown
  fallback?: NormalizedHookCandidate | null
}) => {
  const fromStart = toFiniteNumber(start)
  const fromEnd = toFiniteNumber(end)
  const fallbackStart = fallback ? fallback.start : null
  const fallbackEnd = fallback ? fallback.start + fallback.duration : null
  const startValue = fromStart ?? fallbackStart
  const endValue = fromEnd ?? fallbackEnd
  if (startValue === null || endValue === null) {
    return { hookStart: null, hookEnd: null }
  }
  const safeStart = Number(Math.max(0, startValue).toFixed(3))
  const minHookLen = EDITOR_RETENTION_CONFIG.hookMin
  const maxHookLen = EDITOR_RETENTION_CONFIG.hookMax
  const boundedEnd = clamp(endValue, safeStart + minHookLen * 0.2, safeStart + maxHookLen)
  if (!Number.isFinite(boundedEnd) || boundedEnd <= safeStart) {
    return { hookStart: null, hookEnd: null }
  }
  return {
    hookStart: safeStart,
    hookEnd: Number(boundedEnd.toFixed(3))
  }
}

export const normalizeAnalysisPayload = (raw: unknown) => {
  if (raw === null || raw === undefined) return raw
  const objectRaw = raw && typeof raw === 'object' && !Array.isArray(raw)
    ? (raw as Record<string, any>)
    : {}
  const parsed = AnalysisSchema.safeParse(objectRaw)
  const base = parsed.success ? parsed.data : objectRaw
  const hookVariants = normalizeHookCandidates(
    base.hook_variants ?? base.hook_candidates,
    Math.max(8, EDITOR_RETENTION_CONFIG.hookSelectionMaxCandidates * 3)
  )
  const hookCandidates = normalizeHookCandidates(
    base.hook_candidates ?? base.hook_variants,
    Math.max(8, EDITOR_RETENTION_CONFIG.hookSelectionMaxCandidates * 4)
  )
  const preferredHook = normalizeHookCandidate(base.preferred_hook ?? null)
  const { hookStart, hookEnd } = sanitizeHookWindow({
    start: base.hook_start_time,
    end: base.hook_end_time,
    fallback: preferredHook || hookVariants[0] || hookCandidates[0] || null
  })
  const hasHookWindow = hookStart !== null && hookEnd !== null && hookEnd > hookStart
  const hookScoreRaw = toFiniteNumber(base.hook_score)
  const hookAuditScoreRaw = toFiniteNumber(base.hook_audit_score)
  const fallbackScore = preferredHook?.score ?? hookVariants[0]?.score ?? hookCandidates[0]?.score ?? null
  const fallbackAudit = preferredHook?.auditScore ?? hookVariants[0]?.auditScore ?? hookCandidates[0]?.auditScore ?? fallbackScore
  const hookScore = hasHookWindow
    ? Number(clamp01(hookScoreRaw ?? fallbackScore ?? 0).toFixed(4))
    : null
  const hookAuditScore = hasHookWindow
    ? Number(clamp01(hookAuditScoreRaw ?? fallbackAudit ?? hookScore ?? 0).toFixed(4))
    : null
  const safeRetentionAttempts = Array.isArray(base.retention_attempts)
    ? base.retention_attempts.slice(0, 20)
    : base.retention_attempts
  return {
    ...base,
    hook_start_time: hasHookWindow ? hookStart : null,
    hook_end_time: hasHookWindow ? hookEnd : null,
    hook_score: hookScore,
    hook_audit_score: hookAuditScore,
    hook_variants: hookVariants,
    hook_candidates: hookCandidates.length ? hookCandidates : hookVariants,
    preferred_hook: preferredHook ?? (base.preferred_hook ?? null),
    retention_attempts: safeRetentionAttempts,
    editor_engine_version: EDITOR_ENGINE_VERSION,
    editor_config_version: EDITOR_CONFIG_VERSION,
    editor_last_updated_at: new Date().toISOString()
  }
}
