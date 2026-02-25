import express from 'express'
import fs from 'fs'
import os from 'os'
import path from 'path'
import crypto from 'crypto'
import { AsyncLocalStorage } from 'async_hooks'
import { spawn, spawnSync } from 'child_process'
import { prisma } from '../db/prisma'
import { supabaseAdmin } from '../supabaseClient'
import r2 from '../lib/r2'
import { clampQualityForTier, normalizeQuality, type ExportQuality } from '../lib/gating'
import { getOrCreateUser } from '../services/users'
import { getUserPlan } from '../services/plans'
import { getUsageForMonth, incrementUsageForMonth } from '../services/usage'
import { getRenderUsageForMonth, incrementRenderUsage } from '../services/renderUsage'
import { PLAN_CONFIG, getMonthKey, isPaidTier, type PlanTier } from '../shared/planConfig'
import { broadcastJobUpdate } from '../realtime'
import { FFMPEG_PATH, FFPROBE_PATH, formatCommand } from '../lib/ffmpeg'
import { isDevAccount } from '../lib/devAccounts'
import {
  EDITOR_RETENTION_CONFIG,
  EMOTIONAL_NICHE_TUNING,
  EMOTIONAL_STYLE_TUNING,
  isJobStatusTransitionAllowed,
  normalizeJobStatus
} from '../lib/editorConfig'
import { normalizeAnalysisPayload } from '../lib/analysisNormalizer'
import {
  getPlanFeatures,
  getRequiredPlanForAdvancedEffects,
  getRequiredPlanForAutoZoom,
  getRequiredPlanForQuality,
  getRequiredPlanForSubtitlePreset,
  getRequiredPlanForRenders,
  isSubtitlePresetAllowed
} from '../lib/planFeatures'
import {
  DEFAULT_SUBTITLE_PRESET,
  normalizeSubtitlePreset,
  parseSubtitleStyleConfig,
  type SubtitleFontId
} from '../shared/subtitlePresets'
import {
  PLATFORM_EDIT_PROFILES,
  parsePlatformProfile,
  type PlatformProfileId
} from '../shared/platformProfiles'
import {
  applyAutoEscalationGuarantee,
  buildEditDecisionTimeline,
  extractTimelineFeatures,
  resolveRuntimeStyleProfile,
  type AutoEscalationEvent,
  type EditDecisionTimeline,
  type RetentionBehaviorStyleProfile,
  type StyleArchetypeBlend,
  type TimelineFeatureSnapshot
} from '../lib/multiStyleRetention'

const router = express.Router()

const INPUT_BUCKET = process.env.SUPABASE_BUCKET_INPUT || process.env.SUPABASE_BUCKET_UPLOADS || 'uploads'
const OUTPUT_BUCKET = process.env.SUPABASE_BUCKET_OUTPUT || process.env.SUPABASE_BUCKET_OUTPUTS || 'outputs'
const FFMPEG_LOG_LIMIT = 10_000_000

type FfmpegRunResult = {
  exitCode: number | null
  stdout: string
  stderr: string
}

const bucketChecks: Record<string, Promise<void> | null> = {}
const RETRY_BASE_DELAY_MS = 350

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const withRetries = async <T>(label: string, attempts: number, run: () => Promise<T>): Promise<T> => {
  let lastError: any = null
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await run()
    } catch (error: any) {
      lastError = error
      if (attempt >= attempts) break
      const waitMs = RETRY_BASE_DELAY_MS * attempt
      console.warn(`${label} failed (attempt ${attempt}/${attempts}), retrying in ${waitMs}ms`, error?.message || error)
      await delay(waitMs)
    }
  }
  throw lastError
}

const ensureBucket = async (name: string, isPublic: boolean) => {
  if (bucketChecks[name]) return bucketChecks[name]
  bucketChecks[name] = (async () => {
    try {
      const existing = await supabaseAdmin.storage.getBucket(name)
      if (existing?.data) return
      if (existing?.error) {
        const created = await supabaseAdmin.storage.createBucket(name, { public: isPublic })
        if (created.error) throw created.error
      }
    } catch (e) {
      console.warn(`ensureBucket failed for ${name}:`, e)
      // don't block job creation if storage is temporarily unavailable
    }
  })()
  return bucketChecks[name]
}

const downloadObjectToFile = async ({ key, destPath }: { key: string; destPath: string }) => {
  if (r2.isConfigured) {
    try {
      await withRetries(`r2_download:${key}`, 3, async () => {
        await r2.getObjectToFile({ Key: key, destPath })
      })
      return
    } catch (err) {
      console.warn('R2 download failed, trying Supabase fallback', err)
    }
  }
  await withRetries(`supabase_download:${key}`, 3, async () => {
    const { data, error } = await supabaseAdmin.storage.from(INPUT_BUCKET).download(key)
    if (error || !data) throw error || new Error('download_failed')
    const bytes = Buffer.from(await data.arrayBuffer())
    fs.writeFileSync(destPath, bytes)
  })
}

const uploadBufferToOutput = async ({ key, body, contentType }: { key: string; body: Buffer; contentType?: string }) => {
  if (r2.isConfigured) {
    await withRetries(`r2_upload:${key}`, 3, async () => {
      await r2.uploadBuffer({ Key: key, Body: body, ContentType: contentType })
    })
    return
  }
  await withRetries(`supabase_upload:${key}`, 3, async () => {
    const { error } = await supabaseAdmin.storage
      .from(OUTPUT_BUCKET)
      .upload(key, body, { contentType: contentType || 'application/octet-stream', upsert: true })
    if (error) throw error
  })
}

const uploadFileToOutput = async ({ key, filePath, contentType }: { key: string; filePath: string; contentType?: string }) => {
  if (r2.isConfigured) {
    await withRetries(`r2_upload_file:${key}`, 3, async () => {
      await r2.uploadFile({ Key: key, filePath, ContentType: contentType })
    })
    return
  }
  await withRetries(`supabase_upload_file:${key}`, 3, async () => {
    const body = fs.readFileSync(filePath)
    const { error } = await supabaseAdmin.storage
      .from(OUTPUT_BUCKET)
      .upload(key, body, { contentType: contentType || 'application/octet-stream', upsert: true })
    if (error) throw error
  })
}

const getSignedOutputUrl = async ({ key, expiresIn }: { key: string; expiresIn: number }) => {
  if (r2.isConfigured) return r2.getPresignedGetUrl({ Key: key, expiresIn })
  const { data, error } = await supabaseAdmin.storage.from(OUTPUT_BUCKET).createSignedUrl(key, expiresIn)
  if (error || !data?.signedUrl) throw error || new Error('signed_url_failed')
  return data.signedUrl
}

const getSignedInputUrl = async ({ key, expiresIn }: { key: string; expiresIn: number }) => {
  if (r2.isConfigured) return r2.getPresignedGetUrl({ Key: key, expiresIn })
  const { data, error } = await supabaseAdmin.storage.from(INPUT_BUCKET).createSignedUrl(key, expiresIn)
  if (error || !data?.signedUrl) throw error || new Error('signed_url_failed')
  return data.signedUrl
}

const deleteOutputObject = async (key: string) => {
  if (r2.isConfigured) {
    await r2.deleteObject({ Key: key })
    return
  }
  const { error } = await supabaseAdmin.storage.from(OUTPUT_BUCKET).remove([key])
  if (error) throw error
}

const hasFfmpeg = () => {
  try {
    const result = spawnSync(FFMPEG_PATH, ['-version'], { stdio: 'ignore' })
    return result.status === 0
  } catch (e) {
    return false
  }
}

const hasFfprobe = () => {
  try {
    const result = spawnSync(FFPROBE_PATH, ['-version'], { stdio: 'ignore' })
    return result.status === 0
  } catch (e) {
    return false
  }
}

const pipelineJobContext = new AsyncLocalStorage<{ jobId: string }>()
const canceledPipelineJobIds = new Set<string>()
const ffmpegProcessesByJobId = new Map<string, Set<ReturnType<typeof spawn>>>()

class JobCanceledError extends Error {
  jobId?: string
  constructor(jobId?: string) {
    super('job_canceled')
    this.name = 'JobCanceledError'
    this.jobId = jobId
  }
}

class HookGateError extends Error {
  reason: string
  details: any
  constructor(reason: string, details?: any) {
    super('FAILED_HOOK')
    this.name = 'HookGateError'
    this.reason = reason
    this.details = details ?? null
  }
}

class QualityGateError extends Error {
  reason: string
  details: any
  constructor(reason: string, details?: any) {
    super('FAILED_QUALITY_GATE')
    this.name = 'QualityGateError'
    this.reason = reason
    this.details = details ?? null
  }
}

const getPipelineJobId = () => pipelineJobContext.getStore()?.jobId

const isPipelineCanceled = (jobId?: string | null) => {
  if (!jobId) return false
  return canceledPipelineJobIds.has(jobId)
}

const markPipelineCanceled = (jobId: string) => {
  if (!jobId) return
  canceledPipelineJobIds.add(jobId)
}

const clearPipelineCanceled = (jobId: string) => {
  if (!jobId) return
  canceledPipelineJobIds.delete(jobId)
}

const killJobFfmpegProcesses = (jobId: string) => {
  const processes = ffmpegProcessesByJobId.get(jobId)
  if (!processes || processes.size === 0) return 0
  let killed = 0
  for (const proc of Array.from(processes)) {
    if (!proc || proc.killed) continue
    try {
      proc.kill('SIGKILL')
      killed += 1
    } catch (e) {
      // ignore
    }
  }
  return killed
}

const runFfmpegProcess = (args: string[]) => {
  return new Promise<FfmpegRunResult>((resolve, reject) => {
    const proc = spawn(FFMPEG_PATH, args, { stdio: 'pipe' })
    const jobId = getPipelineJobId()
    if (jobId) {
      let set = ffmpegProcessesByJobId.get(jobId)
      if (!set) {
        set = new Set()
        ffmpegProcessesByJobId.set(jobId, set)
      }
      set.add(proc)
      if (isPipelineCanceled(jobId)) {
        try {
          proc.kill('SIGKILL')
        } catch (e) {
          // ignore
        }
      }
    }
    let stdout = ''
    let stderr = ''
    let finished = false
    const cleanup = () => {
      if (finished) return
      finished = true
      if (!jobId) return
      const set = ffmpegProcessesByJobId.get(jobId)
      if (!set) return
      set.delete(proc)
      if (set.size === 0) ffmpegProcessesByJobId.delete(jobId)
    }
    proc.stdout.on('data', (data) => {
      if (stdout.length >= FFMPEG_LOG_LIMIT) return
      stdout += data.toString()
    })
    proc.stderr.on('data', (data) => {
      if (stderr.length >= FFMPEG_LOG_LIMIT) return
      stderr += data.toString()
    })
    proc.on('error', (err) => {
      cleanup()
      reject(err)
    })
    proc.on('close', (exitCode) => {
      cleanup()
      resolve({ exitCode, stdout, stderr })
    })
  })
}

const runFfmpeg = async (args: string[]) => {
  const jobId = getPipelineJobId()
  if (isPipelineCanceled(jobId)) {
    throw new JobCanceledError(jobId || undefined)
  }
  const result = await runFfmpegProcess(args)
  if (result.exitCode === 0) {
    if (isPipelineCanceled(jobId)) throw new JobCanceledError(jobId || undefined)
    return result
  }
  if (isPipelineCanceled(jobId)) {
    throw new JobCanceledError(jobId || undefined)
  }
  const err: any = new Error(`ffmpeg_failed_${result.exitCode}`)
  err.exitCode = result.exitCode
  err.stdout = result.stdout
  err.stderr = result.stderr
  err.command = formatFfmpegCommand(args)
  throw err
}

const formatFfmpegCommand = (args: string[]) => {
  return formatCommand(FFMPEG_PATH, args)
}

const formatFfmpegFailure = (err: any) => {
  const message = err?.message ? String(err.message) : 'ffmpeg_failed'
  const exitCode = err?.exitCode !== undefined && err?.exitCode !== null ? `exit=${err.exitCode}` : 'exit=unknown'
  const stderr = err?.stderr ? String(err.stderr).trim() : ''
  const stdout = err?.stdout ? String(err.stdout).trim() : ''
  const detail = [stderr, stdout].filter(Boolean).join('\n')
  const combined = `${message} (${exitCode})${detail ? `\n${detail}` : ''}`
  return combined.length > 3500 ? combined.slice(0, 3500) : combined
}

const safeUnlink = (filePath?: string | null) => {
  if (!filePath) return
  try {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath)
  } catch (e) {
    // ignore
  }
}

export const updateJob = async (
  jobId: string,
  data: any,
  opts?: {
    expectedUpdatedAt?: Date | string | null
  }
) => {
  const nextData = data && typeof data === 'object' ? { ...data } : {}
  const hasStatusPatch = Object.prototype.hasOwnProperty.call(nextData, 'status')
  const hasAnalysisPatch = Object.prototype.hasOwnProperty.call(nextData, 'analysis')
  let expectedUpdatedAt: Date | null = null

  if (opts?.expectedUpdatedAt) {
    const parsedExpected = new Date(opts.expectedUpdatedAt)
    if (Number.isFinite(parsedExpected.getTime())) expectedUpdatedAt = parsedExpected
  }

  if (hasStatusPatch) {
    const normalizedStatus = normalizeJobStatus(nextData.status)
    if (!normalizedStatus) {
      throw new Error(`invalid_job_status:${String(nextData.status || '')}`)
    }
    const existing = await prisma.job.findUnique({
      where: { id: jobId },
      select: { status: true }
    })
    if (!existing) throw new Error('job_not_found')
    const currentStatus = normalizeJobStatus(existing.status)
    if (
      currentStatus &&
      EDITOR_RETENTION_CONFIG.enforceStatusTransitions &&
      !isJobStatusTransitionAllowed(currentStatus, normalizedStatus)
    ) {
      throw new Error(`invalid_status_transition:${currentStatus}->${normalizedStatus}`)
    }
    nextData.status = normalizedStatus
  }

  if (hasAnalysisPatch && nextData.analysis !== null && nextData.analysis !== undefined) {
    nextData.analysis = normalizeAnalysisPayload(nextData.analysis)
  }

  let updated: any
  if (expectedUpdatedAt) {
    const result = await prisma.job.updateMany({
      where: { id: jobId, updatedAt: expectedUpdatedAt },
      data: nextData
    })
    if (result.count === 0) {
      const conflict: any = new Error('job_update_conflict')
      conflict.code = 'job_update_conflict'
      throw conflict
    }
    updated = await prisma.job.findUnique({ where: { id: jobId } })
    if (!updated) throw new Error('job_not_found')
  } else {
    updated = await prisma.job.update({ where: { id: jobId }, data: nextData })
  }

  broadcastJobUpdate(updated.userId, { job: updated })
  return updated
}

const getDurationSeconds = (filePath: string) => {
  try {
    if (hasFfprobe()) {
      const result = spawnSync(
        FFPROBE_PATH,
        ['-v', 'error', '-show_entries', 'format=duration', '-of', 'default=nk=1:nw=1', filePath],
        { encoding: 'utf8' }
      )
      if (result.status === 0) {
        const value = String(result.stdout || '').trim()
        const parsed = Number.parseFloat(value)
        if (Number.isFinite(parsed) && parsed > 0) return parsed
      }
    }
  } catch (e) {
    // ignore and fall back to ffmpeg
  }
  if (!hasFfmpeg()) return null
  try {
    const result = spawnSync(FFMPEG_PATH, ['-hide_banner', '-i', filePath], { encoding: 'utf8' })
    const output = `${result.stderr || ''}\n${result.stdout || ''}`
    const match = output.match(/Duration:\s*([0-9]+):([0-9]+):([0-9.]+)/)
    if (!match) return null
    const hours = Number.parseInt(match[1], 10)
    const minutes = Number.parseInt(match[2], 10)
    const seconds = Number.parseFloat(match[3])
    if (!Number.isFinite(hours) || !Number.isFinite(minutes) || !Number.isFinite(seconds)) return null
    const total = hours * 3600 + minutes * 60 + seconds
    return Number.isFinite(total) && total > 0 ? total : null
  } catch (e) {
    return null
  }
}

const toMinutes = (seconds?: number | null) => {
  if (!seconds || seconds <= 0) return 0
  return Math.ceil(seconds / 60)
}

const getTargetDimensions = (quality?: ExportQuality | null) => {
  if (quality === '4k') return { width: 3840, height: 2160 }
  if (quality === '1080p') return { width: 1920, height: 1080 }
  if (quality === '720p') return { width: 1280, height: 720 }
  return { width: 1280, height: 720 }
}

type TimeRange = { start: number; end: number }
type AudioStreamProfile = {
  channels: number
  channelLayout: string | null
  sampleRate: number | null
  bitRate: number | null
}
type FaceSample = {
  time: number
  presence: number
  intensity?: number
  faceCount?: number
  centerX?: number
  centerY?: number
}
type SegmentTransitionStyle = 'jump' | 'smooth'
type Segment = {
  start: number
  end: number
  speed?: number
  zoom?: number
  brightness?: number
  emphasize?: boolean
  audioGain?: number
  faceFocusX?: number
  faceFocusY?: number
  transitionStyle?: SegmentTransitionStyle
  soundFxLevel?: number
}
type WebcamCrop = { x: number; y: number; width: number; height: number }
type HorizontalFitMode = 'cover' | 'contain'
type HorizontalModeOutput = 'quality' | 'source' | { width: number; height: number }
type HorizontalModeSettings = {
  output: HorizontalModeOutput
  fit: HorizontalFitMode
}
type VerticalFitMode = 'cover' | 'contain'
type VerticalLayoutMode = 'stacked' | 'single'
type VerticalWebcamCrop = { x: number; y: number; w: number; h: number }
type VerticalModeSettings = {
  enabled: boolean
  output: { width: number; height: number }
  layout: VerticalLayoutMode
  webcamCrop: VerticalWebcamCrop | null
  topHeightPx: number | null
  bottomFit: VerticalFitMode
}
type RenderMode = 'horizontal' | 'vertical'
type RenderConfig = {
  mode: RenderMode
  verticalClipCount: number
  horizontalMode: HorizontalModeSettings
  verticalMode: VerticalModeSettings | null
}
type EngagementWindow = {
  time: number
  audioEnergy: number
  speechIntensity: number
  motionScore: number
  facePresence: number
  faceIntensity?: number
  faceCenterX?: number
  faceCenterY?: number
  textDensity: number
  textConfidence?: number
  sceneChangeRate: number
  actionSpike?: number
  visualImpact?: number
  emotionalSpike: number
  vocalExcitement: number
  emotionIntensity: number
  audioVariance?: number
  keywordIntensity?: number
  curiosityTrigger?: number
  fillerDensity?: number
  boredomScore?: number
  hookScore?: number
  narrativeProgress?: number
  patternInterrupt?: boolean
  score: number
}
type RetentionAggressionLevel = 'low' | 'medium' | 'high' | 'viral'
type RetentionStrategyProfile = 'safe' | 'balanced' | 'viral'
type RetentionContentFormat = 'youtube_long' | 'tiktok_short' | 'podcast_clip'
type RetentionTargetPlatform = 'auto' | 'tiktok' | 'instagram_reels' | 'youtube'
type PlatformProfile = PlatformProfileId
type PipelineStepStatus = 'pending' | 'running' | 'completed' | 'failed'
type RetentionPipelineStep =
  | 'TRANSCRIBE'
  | 'FRAME_ANALYSIS'
  | 'BEST_MOMENT_SCORING'
  | 'HOOK_SELECT_AND_AUDIT'
  | 'TIMELINE_REORDER'
  | 'PACING_AND_INTERRUPTS'
  | 'STORY_QUALITY_GATE'
  // Backward-compatible pipeline step keys kept for existing rows.
  | 'HOOK_SCORING'
  | 'BOREDOM_SCORING'
  | 'STORY_REORDER'
  | 'PACING_ENFORCEMENT'
  | 'RENDER_FINAL'
  | 'RETENTION_SCORE'
type PipelineStepState = {
  status: PipelineStepStatus
  attempts: number
  retries: number
  startedAt?: string
  completedAt?: string
  lastError?: string | null
  meta?: any
}
type TranscriptCue = {
  start: number
  end: number
  text: string
  keywordIntensity: number
  curiosityTrigger: number
  fillerDensity: number
}
type HookAuditResult = {
  passed: boolean
  auditScore: number
  understandable: boolean
  curiosity: boolean
  payoff: boolean
  reasons: string[]
}
type HookCandidate = {
  start: number
  duration: number
  score: number
  auditScore: number
  auditPassed: boolean
  text: string
  reason: string
  synthetic?: boolean
}
type QualityGateThresholds = {
  hook_strength: number
  emotional_pull: number
  pacing_score: number
  retention_score: number
}
type RetentionRetryStrategy = 'BASELINE' | 'HOOK_FIRST' | 'EMOTION_FIRST' | 'PACING_FIRST' | 'RESCUE_MODE'
type RetentionJudgeReport = {
  retention_score: number
  hook_strength: number
  pacing_score: number
  clarity_score: number
  emotional_pull: number
  content_format?: RetentionContentFormat
  target_platform?: RetentionTargetPlatform
  strategy_profile?: RetentionStrategyProfile
  why_keep_watching: string[]
  what_is_generic: string[]
  required_fixes: {
    stronger_hook: boolean
    raise_emotion: boolean
    improve_pacing: boolean
    increase_interrupts: boolean
  }
  applied_thresholds: QualityGateThresholds
  gate_mode: 'strict' | 'adaptive'
  passed: boolean
}
type RetentionAttemptRecord = {
  attempt: number
  strategy: RetentionRetryStrategy
  judge: RetentionJudgeReport
  hook: HookCandidate
  patternInterruptCount: number
  patternInterruptDensity: number
  boredomRemovalRatio: number
  predictedRetention?: number
  variantScore?: number
}
type HookSelectionDecision = {
  candidate: HookCandidate
  confidence: number
  threshold: number
  usedFallback: boolean
  reason: string | null
}
type HookCalibrationWeights = {
  candidateScore: number
  auditScore: number
  energy: number
  curiosity: number
  emotionalSpike: number
}
type HookCalibrationProfile = {
  enabled: boolean
  sampleSize: number
  averageOutcome: number
  earlyDropRate: number
  platformFeedbackShare: number
  dominantStyle: ContentStyle | null
  weights: HookCalibrationWeights
  strategyBias: Partial<Record<RetentionRetryStrategy, number>>
  reasons: string[]
  updatedAt: string
}
type CreatorFeedbackCategory = 'bad_hook' | 'too_fast' | 'too_generic' | 'great_edit'
type RetentionFeedbackPayload = {
  watchPercent: number | null
  hookHoldPercent: number | null
  completionPercent: number | null
  rewatchRate: number | null
  first30Retention: number | null
  avgViewDurationSeconds: number | null
  clickThroughRate: number | null
  sharesPerView: number | null
  likesPerView: number | null
  commentsPerView: number | null
  manualScore: number | null
  source: string | null
  sourceType: 'platform' | 'internal'
  notes: string | null
  submittedAt: string
}
type CreatorFeedbackPayload = {
  category: CreatorFeedbackCategory
  source: string | null
  notes: string | null
  manualScore: number | null
  submittedAt: string
}
type EditPlan = {
  hook: HookCandidate
  segments: Segment[]
  silences: TimeRange[]
  removedSegments: TimeRange[]
  compressedSegments: TimeRange[]
  engagementWindows: EngagementWindow[]
  hookCandidates?: HookCandidate[]
  boredomRanges?: TimeRange[]
  patternInterruptCount?: number
  patternInterruptDensity?: number
  boredomRemovedRatio?: number
  storyReorderMap?: Array<{ sourceStart: number; sourceEnd: number; orderedIndex: number }>
  hookFailureReason?: string | null
  transcriptSignals?: {
    cueCount: number
    hasTranscript: boolean
  }
  styleProfile?: ContentStyleProfile
  nicheProfile?: VideoNicheProfile | null
  beatAnchors?: number[]
  emotionalBeatAnchors?: number[]
  emotionalBeatCutCount?: number
  emotionalLeadTrimmedSeconds?: number
  emotionalTuning?: EmotionalTuningProfile
  hookVariants?: HookCandidate[]
  hookCalibration?: HookCalibrationProfile | null
  styleArchetypeBlend?: StyleArchetypeBlend | null
  behaviorStyleProfile?: RetentionBehaviorStyleProfile | null
  autoEscalationEvents?: AutoEscalationEvent[]
  editDecisionTimeline?: EditDecisionTimeline | null
  styleFeatureSnapshot?: TimelineFeatureSnapshot | null
}
type EditOptions = {
  autoHookMove: boolean
  removeBoring: boolean
  onlyCuts: boolean
  smartZoom: boolean
  jumpCuts: boolean
  transitions: boolean
  soundFx: boolean
  emotionalBoost: boolean
  aggressiveMode: boolean
  autoCaptions: boolean
  musicDuck: boolean
  subtitleStyle?: string | null
  autoZoomMax: number
  retentionAggressionLevel: RetentionAggressionLevel
  retentionStrategyProfile: RetentionStrategyProfile
  preferredHookCandidate?: HookCandidate | null
  styleArchetypeBlend?: Partial<StyleArchetypeBlend> | null
  fastMode?: boolean
}
type ContentStyle = 'reaction' | 'vlog' | 'tutorial' | 'gaming' | 'story'
type ContentStyleProfile = {
  style: ContentStyle
  confidence: number
  rationale: string[]
  tempoBias: number
  interruptBias: number
  hookBias: number
}
type PacingNiche = 'high_energy' | 'education' | 'talking_head' | 'story'
type PacingProfile = {
  niche: PacingNiche
  minLen: number
  maxLen: number
  earlyTarget: number
  middleTarget: number
  lateTarget: number
  jitter: number
  speedCap: number
}
type VideoNicheProfile = {
  niche: PacingNiche
  confidence: number
  rationale: string[]
  styleAlignment: ContentStyle | null
  metrics: {
    avgSpeech: number
    avgScene: number
    avgEmotion: number
    spikeRatio: number
    transcriptCueCount: number
    durationSeconds: number
  }
}

const HOOK_MIN = EDITOR_RETENTION_CONFIG.hookMin
const HOOK_MAX = EDITOR_RETENTION_CONFIG.hookMax
const HOOK_RELOCATE_MIN_START = 6
const HOOK_RELOCATE_SCORE_TOLERANCE = 0.06
const HOOK_SELECTION_MATCH_START_TOLERANCE_SEC = EDITOR_RETENTION_CONFIG.hookSelectionMatchStartToleranceSec
const HOOK_SELECTION_MATCH_DURATION_TOLERANCE_SEC = EDITOR_RETENTION_CONFIG.hookSelectionMatchDurationToleranceSec
const HOOK_SELECTION_WAIT_MS = EDITOR_RETENTION_CONFIG.hookSelectionWaitMs
const HOOK_SELECTION_POLL_MS = EDITOR_RETENTION_CONFIG.hookSelectionPollMs
const CUT_MIN = EDITOR_RETENTION_CONFIG.cutMin
const CUT_MAX = EDITOR_RETENTION_CONFIG.cutMax
const PACE_MIN = EDITOR_RETENTION_CONFIG.cutMin
const PACE_MAX = EDITOR_RETENTION_CONFIG.cutMax
const CUT_GUARD_SEC = 0.35
const CUT_LEN_PATTERN = [5.2, 6.1, 5.8, 7.1]
const CUT_GAP_PATTERN = [1.3, 1.8, 1.5, 1.2]
const MAX_CUT_RATIO = 0.68
const AGGRESSIVE_MAX_CUT_RATIO = 0.74
const AGGRESSIVE_CUT_GAP_MULTIPLIER = 0.78
const ZOOM_HARD_MAX = 1.15
const ZOOM_MAX_DURATION_RATIO = 0.1
const ZOOM_EASE_SEC = 0.2
const STITCH_FADE_SEC = 0.08
const JUMPCUT_FADE_SEC = 0.012
const MIN_RENDER_SEGMENT_SECONDS = 0.08
const MERGE_ADJACENT_SEGMENT_GAP_SEC = 0.06
const FILTER_TIME_DECIMALS = 3
const MAX_RENDER_SEGMENTS = (() => {
  const envValue = Number(process.env.MAX_RENDER_SEGMENTS || 180)
  return Number.isFinite(envValue) && envValue > 0 ? Math.round(envValue) : 180
})()
const LONG_FORM_RUNTIME_THRESHOLD_SECONDS = (() => {
  const envValue = Number(process.env.LONG_FORM_RUNTIME_THRESHOLD_SECONDS || 95)
  return Number.isFinite(envValue) && envValue >= 45 ? Number(envValue.toFixed(2)) : 95
})()
const LONG_FORM_CONTEXT_WINDOW_SECONDS = (() => {
  const envValue = Number(process.env.LONG_FORM_CONTEXT_WINDOW_SECONDS || 18)
  return Number.isFinite(envValue) && envValue >= 6 ? Number(envValue.toFixed(2)) : 18
})()
const LONG_FORM_MIN_CONTEXT_SECONDS = (() => {
  const envValue = Number(process.env.LONG_FORM_MIN_CONTEXT_SECONDS || 2.2)
  return Number.isFinite(envValue) && envValue >= 0.8 ? Number(envValue.toFixed(2)) : 2.2
})()
const PLATFORM_MAX_CUTS_PER_10_SECONDS: Record<RetentionTargetPlatform, { horizontal: number; vertical: number }> = {
  auto: { horizontal: 2.8, vertical: 4.8 },
  tiktok: { horizontal: 3, vertical: 5.4 },
  instagram_reels: { horizontal: 2.9, vertical: 5 },
  youtube: { horizontal: 2.6, vertical: 4.4 }
}
const FILTER_COMPLEX_SCRIPT_THRESHOLD = (() => {
  const envValue = Number(process.env.FILTER_COMPLEX_SCRIPT_THRESHOLD || 16_000)
  return Number.isFinite(envValue) && envValue > 2_000 ? Math.round(envValue) : 16_000
})()
const SILENCE_DB = -30
const SILENCE_MIN = 0.8
const SILENCE_KEEP_PADDING_SEC = 0.2
const HOOK_ANALYZE_MAX = (() => {
  const envValue = Number(process.env.HOOK_ANALYZE_MAX_SECONDS || 1800)
  return Number.isFinite(envValue) && envValue >= 45 ? Math.round(envValue) : 1800
})()
const ANALYSIS_SKIP_PROXY = /^(1|true|yes)$/i.test(String(process.env.ANALYSIS_SKIP_PROXY || '').trim())
const ANALYSIS_PROXY_WIDTH = (() => {
  const envValue = Number(process.env.ANALYSIS_PROXY_WIDTH || 960)
  return Number.isFinite(envValue) && envValue >= 320 ? Math.round(envValue) : 960
})()
const ANALYSIS_PROXY_HEIGHT = (() => {
  const envValue = Number(process.env.ANALYSIS_PROXY_HEIGHT || 540)
  return Number.isFinite(envValue) && envValue >= 180 ? Math.round(envValue) : 540
})()
const ANALYSIS_FRAME_FPS = (() => {
  const envValue = Number(process.env.ANALYSIS_FRAME_FPS || 2)
  return Number.isFinite(envValue) && envValue >= 0.25 && envValue <= 8
    ? Number(envValue.toFixed(2))
    : 2
})()
const ANALYSIS_FRAME_SCALE_WIDTH = (() => {
  const envValue = Number(process.env.ANALYSIS_FRAME_SCALE_WIDTH || 360)
  return Number.isFinite(envValue) && envValue >= 160 ? Math.round(envValue) : 360
})()
const ANALYSIS_DISABLE_FACE_DETECTION = /^(1|true|yes)$/i.test(String(process.env.ANALYSIS_DISABLE_FACE_DETECTION || '').trim())
const ANALYSIS_DISABLE_TEXT_DENSITY = /^(1|true|yes)$/i.test(String(process.env.ANALYSIS_DISABLE_TEXT_DENSITY || '').trim())
const ANALYSIS_DISABLE_EMOTION_MODEL = /^(1|true|yes)$/i.test(String(process.env.ANALYSIS_DISABLE_EMOTION_MODEL || '').trim())
const RENDER_FILTER_THREADS = (() => {
  const envValue = Number(process.env.FFMPEG_FILTER_THREADS || 1)
  return Number.isFinite(envValue) && envValue >= 1 ? Math.round(envValue) : 1
})()
const SCENE_THRESHOLD = 0.45
const STRATEGIST_HOOK_WINDOW_SEC = 35
const STRATEGIST_LATE_HOOK_PENALTY_SEC = 55
const MAX_VERTICAL_CLIPS = 3
const LONG_FORM_RESCUE_MIN_DURATION = 120
const LONG_FORM_MIN_EDIT_RATIO = 0.035
const LONG_FORM_MIN_EDIT_SECONDS = 20
const LONG_FORM_MAX_EDIT_SECONDS = 140
const MIN_EDIT_IMPACT_RATIO_SHORT = 0.035
const MIN_EDIT_IMPACT_RATIO_LONG = 0.06
const FREE_MONTHLY_RENDER_LIMIT = PLAN_CONFIG.free.maxRendersPerMonth || 10
const MIN_WEBCAM_CROP_RATIO = 0.03
const DEFAULT_VERTICAL_OUTPUT_WIDTH = 1080
const DEFAULT_VERTICAL_OUTPUT_HEIGHT = 1920
const DEFAULT_VERTICAL_TOP_HEIGHT_PCT = 0.4
const HOOK_SELECTION_MAX_CANDIDATES = EDITOR_RETENTION_CONFIG.hookSelectionMaxCandidates
const HOOK_CALIBRATION_LOOKBACK_JOBS = (() => {
  const envValue = Number(process.env.HOOK_CALIBRATION_LOOKBACK_JOBS || 24)
  return Number.isFinite(envValue) && envValue > 2 ? Math.round(envValue) : 24
})()
const HOOK_CALIBRATION_MIN_SAMPLES = 3
const DEFAULT_HOOK_FACEOFF_WEIGHTS: HookCalibrationWeights = {
  candidateScore: 0.4,
  auditScore: 0.24,
  energy: 0.16,
  curiosity: 0.1,
  emotionalSpike: 0.1
}
const RETENTION_VARIANT_STRATEGIES: RetentionRetryStrategy[] = ['BASELINE', 'HOOK_FIRST', 'EMOTION_FIRST', 'PACING_FIRST']
const CREATOR_FEEDBACK_CATEGORIES: CreatorFeedbackCategory[] = ['bad_hook', 'too_fast', 'too_generic', 'great_edit']
const CREATOR_FEEDBACK_SIGNAL_MAP: Record<CreatorFeedbackCategory, {
  manualScore: number
  watchPercent: number
  hookHoldPercent: number
  completionPercent: number
  rewatchRate: number
}> = {
  bad_hook: {
    manualScore: 42,
    watchPercent: 0.44,
    hookHoldPercent: 0.34,
    completionPercent: 0.28,
    rewatchRate: 0.04
  },
  too_fast: {
    manualScore: 56,
    watchPercent: 0.52,
    hookHoldPercent: 0.51,
    completionPercent: 0.43,
    rewatchRate: 0.06
  },
  too_generic: {
    manualScore: 53,
    watchPercent: 0.5,
    hookHoldPercent: 0.46,
    completionPercent: 0.4,
    rewatchRate: 0.05
  },
  great_edit: {
    manualScore: 92,
    watchPercent: 0.82,
    hookHoldPercent: 0.86,
    completionPercent: 0.72,
    rewatchRate: 0.18
  }
}
const MAX_QUALITY_GATE_RETRIES = 3
const QUALITY_GATE_THRESHOLDS: QualityGateThresholds = {
  hook_strength: 80,
  emotional_pull: 70,
  pacing_score: 70,
  retention_score: 75
}
const QUALITY_GATE_THRESHOLD_FLOORS: QualityGateThresholds = {
  hook_strength: 64,
  emotional_pull: 55,
  pacing_score: 62,
  retention_score: 58
}
const RESCUE_RENDER_MINIMUMS = {
  retention_score: 44,
  hook_strength: 52,
  pacing_score: 50
}
const LEVEL_HOOK_THRESHOLD_BASE: Record<RetentionAggressionLevel, number> = {
  low: 0.62,
  medium: 0.68,
  high: 0.74,
  viral: 0.8
}
const LEVEL_HOOK_THRESHOLD_FLOOR: Record<RetentionAggressionLevel, number> = {
  low: 0.46,
  medium: 0.5,
  high: 0.56,
  viral: 0.62
}
const LEVEL_QUALITY_THRESHOLD_OFFSET: Record<RetentionAggressionLevel, number> = {
  low: -6,
  medium: 0,
  high: 4,
  viral: 8
}
const STRATEGY_TO_AGGRESSION: Record<RetentionStrategyProfile, RetentionAggressionLevel> = {
  safe: 'low',
  balanced: 'medium',
  viral: 'viral'
}
const AGGRESSION_TO_STRATEGY: Record<RetentionAggressionLevel, RetentionStrategyProfile> = {
  low: 'safe',
  medium: 'balanced',
  high: 'balanced',
  viral: 'viral'
}
type RetentionStyleReferencePreset = {
  referenceAnchors: string[]
  autoHookMove: boolean
  removeBoring: boolean
  smartZoom: boolean
  jumpCuts: boolean
  transitions: boolean
  soundFx: boolean
  emotionalBoost: boolean
  aggressiveMode: boolean
  musicDuck: boolean
}
const RETENTION_STYLE_REFERENCE_PRESETS: Record<RetentionStrategyProfile, RetentionStyleReferencePreset> = {
  viral: {
    referenceAnchors: ['high_stakes_challenge', 'energetic_vlog'],
    autoHookMove: true,
    removeBoring: true,
    smartZoom: true,
    jumpCuts: true,
    transitions: true,
    soundFx: true,
    emotionalBoost: true,
    aggressiveMode: true,
    musicDuck: true
  },
  balanced: {
    referenceAnchors: ['longform_reaction_commentary', 'cinematic_lifestyle_archive', 'energetic_vlog'],
    autoHookMove: true,
    removeBoring: true,
    smartZoom: false,
    jumpCuts: true,
    transitions: false,
    soundFx: false,
    emotionalBoost: false,
    aggressiveMode: false,
    musicDuck: true
  },
  safe: {
    referenceAnchors: ['longform_reaction_commentary', 'cinematic_lifestyle_archive'],
    autoHookMove: true,
    removeBoring: true,
    smartZoom: false,
    jumpCuts: false,
    transitions: false,
    soundFx: false,
    emotionalBoost: false,
    aggressiveMode: false,
    musicDuck: true
  }
}
const FORMAT_SCORE_WEIGHTS: Record<RetentionContentFormat, {
  hook: number
  consistency: number
  pacing: number
  boredomRemoval: number
  emotionalSpikeDensity: number
  interruptDensity: number
  subtitle: number
  audio: number
  targetSegmentSeconds: number
  interruptIntervalSeconds: number
}> = {
  youtube_long: {
    hook: 0.22,
    consistency: 0.18,
    pacing: 0.16,
    boredomRemoval: 0.16,
    emotionalSpikeDensity: 0.12,
    interruptDensity: 0.08,
    subtitle: 0.04,
    audio: 0.04,
    targetSegmentSeconds: 4.2,
    interruptIntervalSeconds: 6
  },
  tiktok_short: {
    hook: 0.3,
    consistency: 0.12,
    pacing: 0.2,
    boredomRemoval: 0.1,
    emotionalSpikeDensity: 0.13,
    interruptDensity: 0.1,
    subtitle: 0.03,
    audio: 0.02,
    targetSegmentSeconds: 2.8,
    interruptIntervalSeconds: 4
  },
  podcast_clip: {
    hook: 0.2,
    consistency: 0.2,
    pacing: 0.12,
    boredomRemoval: 0.15,
    emotionalSpikeDensity: 0.14,
    interruptDensity: 0.06,
    subtitle: 0.06,
    audio: 0.07,
    targetSegmentSeconds: 4.8,
    interruptIntervalSeconds: 7
  }
}
const PLATFORM_SCORE_TUNING: Record<RetentionTargetPlatform, {
  segmentScale: number
  interruptScale: number
}> = {
  auto: {
    segmentScale: 1,
    interruptScale: 1
  },
  tiktok: {
    segmentScale: 0.9,
    interruptScale: 0.82
  },
  instagram_reels: {
    segmentScale: 0.92,
    interruptScale: 0.86
  },
  youtube: {
    segmentScale: 1.08,
    interruptScale: 1.12
  }
}
const FORMAT_QUALITY_GATE_OFFSET: Record<RetentionContentFormat, Partial<QualityGateThresholds>> = {
  youtube_long: {
    hook_strength: 0,
    emotional_pull: 0,
    pacing_score: 0,
    retention_score: 0
  },
  tiktok_short: {
    hook_strength: 3,
    emotional_pull: 2,
    pacing_score: 4,
    retention_score: 2
  },
  podcast_clip: {
    hook_strength: -2,
    emotional_pull: 0,
    pacing_score: -3,
    retention_score: -2
  }
}
const PLATFORM_QUALITY_GATE_OFFSET: Record<RetentionTargetPlatform, Partial<QualityGateThresholds>> = {
  auto: {
    hook_strength: 0,
    emotional_pull: 0,
    pacing_score: 0,
    retention_score: 0
  },
  tiktok: {
    hook_strength: 3,
    emotional_pull: 1,
    pacing_score: 3,
    retention_score: 2
  },
  instagram_reels: {
    hook_strength: 2,
    emotional_pull: 2,
    pacing_score: 2,
    retention_score: 2
  },
  youtube: {
    hook_strength: 0,
    emotional_pull: 0,
    pacing_score: 0,
    retention_score: 0
  }
}
const RETENTION_PIPELINE_STEPS: RetentionPipelineStep[] = [
  'TRANSCRIBE',
  'FRAME_ANALYSIS',
  'BEST_MOMENT_SCORING',
  'HOOK_SELECT_AND_AUDIT',
  'TIMELINE_REORDER',
  'PACING_AND_INTERRUPTS',
  'STORY_QUALITY_GATE',
  // Backward-compatible keys kept for old records.
  'HOOK_SCORING',
  'BOREDOM_SCORING',
  'STORY_REORDER',
  'PACING_ENFORCEMENT',
  'RENDER_FINAL',
  'RETENTION_SCORE'
]
const RETENTION_KEYWORDS = [
  'crazy',
  'insane',
  'secret',
  'money',
  'profit',
  'revenue',
  'million',
  'minutes',
  'days',
  'hours',
  'results',
  'mistake',
  'warning',
  'proof',
  'trick',
  'changed everything',
  'messed up',
  'watch this'
]
const CURIOSITY_PHRASES = [
  "you won't believe",
  'watch this',
  "here's the trick",
  'i messed up',
  'here is why',
  'what happened next',
  'before i reveal',
  'most people miss',
  'the truth is',
  'this changed everything',
  'what happens next',
  'wait for it'
]
const FILLER_WORDS = [
  'um',
  'uh',
  'like',
  'you know',
  'basically',
  'literally',
  'kind of',
  'sort of'
]
const REACTION_STYLE_KEYWORDS = [
  'reaction',
  'reacting',
  'no way',
  'oh my god',
  'omg',
  'wtf',
  'you guys',
  'chat',
  'live stream'
]
const VLOG_STYLE_KEYWORDS = [
  'vlog',
  'day in the life',
  'today',
  'this morning',
  'come with me',
  'we went',
  'my day'
]
const TUTORIAL_STYLE_KEYWORDS = [
  'how to',
  'step',
  'tutorial',
  'guide',
  'tip',
  'lesson',
  'first',
  'next',
  'finally'
]
const GAMING_STYLE_KEYWORDS = [
  'game',
  'gaming',
  'match',
  'ranked',
  'clutch',
  'boss',
  'level',
  'fps',
  'controller'
]
const RETENTION_AGGRESSION_PRESET: Record<RetentionAggressionLevel, {
  cutMultiplier: number
  hookRelocateBias: number
  patternIntervalMin: number
  patternIntervalMax: number
  zoomBoost: number
  boredomThreshold: number
}> = {
  low: {
    cutMultiplier: 0.85,
    hookRelocateBias: 0.9,
    patternIntervalMin: 7,
    patternIntervalMax: 9,
    zoomBoost: 0.85,
    boredomThreshold: 0.68
  },
  medium: {
    cutMultiplier: 1,
    hookRelocateBias: 1,
    patternIntervalMin: 6,
    patternIntervalMax: 8,
    zoomBoost: 1,
    boredomThreshold: 0.62
  },
  high: {
    cutMultiplier: 1.15,
    hookRelocateBias: 1.1,
    patternIntervalMin: 5,
    patternIntervalMax: 7,
    zoomBoost: 1.15,
    boredomThreshold: 0.57
  },
  viral: {
    cutMultiplier: 1.32,
    hookRelocateBias: 1.2,
    patternIntervalMin: 3,
    patternIntervalMax: 6,
    zoomBoost: 1.35,
    boredomThreshold: 0.52
  }
}
const DEFAULT_EDIT_OPTIONS: EditOptions = {
  autoHookMove: true,
  removeBoring: true,
  onlyCuts: false,
  smartZoom: true,
  jumpCuts: true,
  transitions: true,
  soundFx: true,
  emotionalBoost: true,
  aggressiveMode: false,
  autoCaptions: false,
  musicDuck: true,
  subtitleStyle: DEFAULT_SUBTITLE_PRESET,
  autoZoomMax: 1.1,
  retentionAggressionLevel: 'medium',
  retentionStrategyProfile: 'balanced'
}

const applyRetentionStyleReferencePreset = ({
  options,
  strategy,
  allowAdvancedEffects
}: {
  options: EditOptions
  strategy: RetentionStrategyProfile
  allowAdvancedEffects: boolean
}): EditOptions => {
  const preset = RETENTION_STYLE_REFERENCE_PRESETS[strategy]
  if (!preset || options.onlyCuts) {
    return {
      ...options,
      retentionStrategyProfile: strategy,
      aggressiveMode: options.onlyCuts ? false : options.aggressiveMode
    }
  }
  return {
    ...options,
    retentionStrategyProfile: strategy,
    autoHookMove: preset.autoHookMove,
    removeBoring: preset.removeBoring,
    smartZoom: preset.smartZoom,
    jumpCuts: preset.jumpCuts,
    transitions: preset.transitions,
    soundFx: preset.soundFx,
    musicDuck: preset.musicDuck,
    emotionalBoost: allowAdvancedEffects ? preset.emotionalBoost : false,
    aggressiveMode: allowAdvancedEffects ? preset.aggressiveMode : false
  }
}

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value))
type EmotionalTuningProfile = {
  thresholdOffset: number
  spacingMultiplier: number
  leadTrimMultiplier: number
  splitLenBias: number
  openLoopBoost: number
  curiosityBoost: number
  contextPenaltyMultiplier: number
}

const resolveEmotionalTuningProfile = ({
  styleProfile,
  nicheProfile,
  aggressionLevel
}: {
  styleProfile?: ContentStyleProfile | null
  nicheProfile?: VideoNicheProfile | null
  aggressionLevel?: RetentionAggressionLevel
}): EmotionalTuningProfile => {
  const nicheKey = nicheProfile?.niche || 'story'
  const styleKey = styleProfile?.style || 'story'
  const base = EMOTIONAL_NICHE_TUNING[nicheKey] || EMOTIONAL_NICHE_TUNING.story
  const style = EMOTIONAL_STYLE_TUNING[styleKey] || EMOTIONAL_STYLE_TUNING.story
  const styleConfidence = clamp(Number(styleProfile?.confidence ?? 0.55), 0, 1)
  const nicheConfidence = clamp(Number(nicheProfile?.confidence ?? 0.55), 0, 1)
  const styleWeight = 0.34 + styleConfidence * 0.46
  const nicheWeight = 0.4 + nicheConfidence * 0.5
  let thresholdShiftByAggression = 0
  if (aggressionLevel === 'low') thresholdShiftByAggression = 0.04
  else if (aggressionLevel === 'medium') thresholdShiftByAggression = 0
  else if (aggressionLevel === 'high') thresholdShiftByAggression = -0.02
  else if (aggressionLevel === 'viral') thresholdShiftByAggression = -0.05

  return {
    thresholdOffset: Number(clamp(
      base.thresholdOffset * nicheWeight +
      style.thresholdOffset * styleWeight +
      thresholdShiftByAggression,
      -0.22,
      0.22
    ).toFixed(4)),
    spacingMultiplier: Number(clamp(
      base.spacingMultiplier * (1 + (style.spacingMultiplier - 1) * styleWeight),
      0.7,
      1.45
    ).toFixed(4)),
    leadTrimMultiplier: Number(clamp(
      base.leadTrimMultiplier * (1 + (style.leadTrimMultiplier - 1) * styleWeight),
      0.58,
      1.48
    ).toFixed(4)),
    splitLenBias: Number(clamp(
      base.splitLenBias * (1 + (style.splitLenBias - 1) * styleWeight),
      0.72,
      1.55
    ).toFixed(4)),
    openLoopBoost: Number(clamp(
      base.openLoopBoost * (1 + (style.openLoopBoost - 1) * styleWeight),
      0.75,
      1.45
    ).toFixed(4)),
    curiosityBoost: Number(clamp(
      base.curiosityBoost * (1 + (style.curiosityBoost - 1) * styleWeight),
      0.8,
      1.5
    ).toFixed(4)),
    contextPenaltyMultiplier: Number(clamp(
      base.contextPenaltyMultiplier * (1 + (style.contextPenaltyMultiplier - 1) * styleWeight),
      0.8,
      1.35
    ).toFixed(4))
  }
}

const roundForFilter = (value: number, decimals: number = FILTER_TIME_DECIMALS) => {
  if (!Number.isFinite(value)) return 0
  return Number(value.toFixed(decimals))
}
const toFilterNumber = (value: number, decimals: number = FILTER_TIME_DECIMALS) => String(roundForFilter(value, decimals))
const toIsoNow = () => new Date().toISOString()
const truncateErrorText = (value: unknown, max = 360) => {
  const text = String(value || '').trim()
  if (!text) return null
  return text.length > max ? text.slice(0, max) : text
}

const getDefaultCrfForQuality = (quality: ExportQuality) => {
  if (quality === '4k') return 18
  if (quality === '1080p') return 20
  return 22
}

const resolveAudioSampleRate = (value: unknown, fallback: number) => {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  const rounded = Math.round(parsed)
  if (rounded < 8_000 || rounded > 192_000) return fallback
  return rounded
}

const parseAudioBitrateKbps = (value: unknown): number | null => {
  const raw = String(value || '').trim().toLowerCase()
  if (!raw) return null
  if (raw === 'auto' || raw === 'default') return null
  const match = raw.match(/^(\d+(?:\.\d+)?)([km]?)$/i)
  if (!match) return null
  let numeric = Number(match[1])
  if (!Number.isFinite(numeric) || numeric <= 0) return null
  const unit = match[2]
  if (unit === 'm') numeric *= 1000
  if (unit !== 'k' && numeric > 1000) numeric /= 1000
  if (!Number.isFinite(numeric) || numeric <= 0) return null
  return Math.round(numeric)
}

const resolveAudioBitrateArg = (value: unknown, fallbackKbps: number) => {
  const parsed = parseAudioBitrateKbps(value)
  const fallback = Number.isFinite(fallbackKbps) && fallbackKbps > 0 ? Math.round(fallbackKbps) : 192
  const selected = parsed !== null && Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
  const clamped = Math.round(clamp(selected, 160, 320))
  return `${clamped}k`
}

const parseRetentionAggressionLevel = (value?: any): RetentionAggressionLevel => {
  const raw = String(value || '').trim().toLowerCase()
  if (raw === 'safe') return 'low'
  if (raw === 'balanced') return 'medium'
  if (raw === 'low') return 'low'
  if (raw === 'high') return 'high'
  if (raw === 'viral' || raw === 'max') return 'viral'
  return 'medium'
}

const parseRetentionStrategyProfile = (value?: any): RetentionStrategyProfile => {
  const raw = String(value || '').trim().toLowerCase()
  if (raw === 'safe') return 'safe'
  if (raw === 'viral' || raw === 'max') return 'viral'
  if (raw === 'balanced' || raw === 'default' || raw === 'normal') return 'balanced'
  // Backward-compatible mapping from aggression labels.
  if (raw === 'low') return 'safe'
  if (raw === 'high' || raw === 'medium') return 'balanced'
  return 'balanced'
}

const parseRetentionTargetPlatform = (value?: any): RetentionTargetPlatform => {
  return parsePlatformProfile(value, 'auto') as RetentionTargetPlatform
}

const strategyFromAggressionLevel = (level: RetentionAggressionLevel): RetentionStrategyProfile => (
  AGGRESSION_TO_STRATEGY[level] || 'balanced'
)

const isAggressiveRetentionLevel = (level: RetentionAggressionLevel) => level === 'high' || level === 'viral'

const parseRenderMode = (value?: any): RenderMode => {
  const raw = String(value || '').trim().toLowerCase()
  if (raw === 'vertical') return 'vertical'
  if (raw === 'horizontal' || raw === 'standard') return 'horizontal'
  return 'horizontal'
}

const parseHorizontalFitMode = (value?: any, fallback: HorizontalFitMode = 'contain'): HorizontalFitMode => {
  const raw = String(value || '').trim().toLowerCase()
  if (raw === 'cover') return 'cover'
  if (raw === 'contain') return 'contain'
  return fallback
}

const parseHorizontalOutput = (value?: any): HorizontalModeOutput => {
  if (value === null || value === undefined || value === '' || value === 'quality') return 'quality'
  const raw = String(value || '').trim().toLowerCase()
  if (raw === 'source') return 'source'
  if (raw && raw !== '[object object]') return 'quality'
  if (!value || typeof value !== 'object') return 'quality'
  const width = Number((value as any).width)
  const height = Number((value as any).height)
  if (!Number.isFinite(width) || !Number.isFinite(height)) return 'quality'
  if (width <= 0 || height <= 0) return 'quality'
  return {
    width: Math.round(clamp(width, 240, 4320)),
    height: Math.round(clamp(height, 240, 7680))
  }
}

const parseHorizontalModeSettings = (value?: any): Partial<HorizontalModeSettings> | null => {
  if (!value || typeof value !== 'object') return null
  return {
    output: parseHorizontalOutput((value as any).output),
    fit: parseHorizontalFitMode((value as any).fit, 'contain')
  }
}

const defaultHorizontalModeSettings = (): HorizontalModeSettings => ({
  output: 'quality',
  fit: 'contain'
})

const buildHorizontalModeSettings = (value?: any): HorizontalModeSettings => {
  const defaults = defaultHorizontalModeSettings()
  const parsed = parseHorizontalModeSettings(value)
  return {
    ...defaults,
    ...(parsed || {})
  }
}

const parseVerticalFitMode = (value?: any, fallback: VerticalFitMode = 'cover'): VerticalFitMode => {
  const raw = String(value || '').trim().toLowerCase()
  if (raw === 'contain') return 'contain'
  if (raw === 'cover') return 'cover'
  return fallback
}

const parseVerticalLayoutMode = (value?: any, fallback: VerticalLayoutMode = 'stacked'): VerticalLayoutMode => {
  const raw = String(value || '').trim().toLowerCase()
  if (raw === 'single' || raw === 'source' || raw === 'original') return 'single'
  if (raw === 'stacked') return 'stacked'
  return fallback
}

const parseVerticalClipCount = (value?: any) => {
  const parsed = Number.parseInt(String(value ?? ''), 10)
  if (!Number.isFinite(parsed)) return 1
  return clamp(parsed, 1, MAX_VERTICAL_CLIPS)
}

const parseBooleanFlag = (value: any): boolean | null => {
  if (value === true) return true
  if (value === false) return false
  if (value === null || value === undefined) return null
  const raw = String(value).trim().toLowerCase()
  if (!raw) return null
  if (raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on') return true
  if (raw === '0' || raw === 'false' || raw === 'no' || raw === 'off') return false
  return null
}

const parseWebcamCrop = (value?: any): WebcamCrop | null => {
  if (!value || typeof value !== 'object') return null
  const x = Number((value as any).x)
  const y = Number((value as any).y)
  const width = Number((value as any).width ?? (value as any).w)
  const height = Number((value as any).height ?? (value as any).h)
  if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(width) || !Number.isFinite(height)) return null
  const clampedX = clamp(x, 0, 1 - MIN_WEBCAM_CROP_RATIO)
  const clampedY = clamp(y, 0, 1 - MIN_WEBCAM_CROP_RATIO)
  const maxWidth = 1 - clampedX
  const maxHeight = 1 - clampedY
  const clampedWidth = clamp(width, MIN_WEBCAM_CROP_RATIO, maxWidth)
  const clampedHeight = clamp(height, MIN_WEBCAM_CROP_RATIO, maxHeight)
  if (clampedWidth <= 0 || clampedHeight <= 0) return null
  return {
    x: Number(clampedX.toFixed(4)),
    y: Number(clampedY.toFixed(4)),
    width: Number(clampedWidth.toFixed(4)),
    height: Number(clampedHeight.toFixed(4))
  }
}

const parseVerticalWebcamCrop = (value?: any): VerticalWebcamCrop | null => {
  if (!value || typeof value !== 'object') return null
  const x = Number((value as any).x)
  const y = Number((value as any).y)
  const w = Number((value as any).w ?? (value as any).width)
  const h = Number((value as any).h ?? (value as any).height)
  if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(w) || !Number.isFinite(h)) return null
  if (w <= 0 || h <= 0) return null
  return {
    x: Number(x.toFixed(4)),
    y: Number(y.toFixed(4)),
    w: Number(w.toFixed(4)),
    h: Number(h.toFixed(4))
  }
}

const parseVerticalOutput = (value?: any) => {
  if (!value || typeof value !== 'object') {
    return { width: DEFAULT_VERTICAL_OUTPUT_WIDTH, height: DEFAULT_VERTICAL_OUTPUT_HEIGHT }
  }
  const width = Number((value as any).width)
  const height = Number((value as any).height)
  if (!Number.isFinite(width) || !Number.isFinite(height)) {
    return { width: DEFAULT_VERTICAL_OUTPUT_WIDTH, height: DEFAULT_VERTICAL_OUTPUT_HEIGHT }
  }
  return {
    width: Math.round(clamp(width, 240, 4320)),
    height: Math.round(clamp(height, 426, 7680))
  }
}

const parseVerticalModeSettings = (value?: any): Partial<VerticalModeSettings> | null => {
  if (!value || typeof value !== 'object') return null
  const topHeightPxRaw = Number((value as any).topHeightPx)
  return {
    enabled: (value as any).enabled !== false,
    output: parseVerticalOutput((value as any).output),
    layout: parseVerticalLayoutMode((value as any).layout, 'stacked'),
    webcamCrop: parseVerticalWebcamCrop((value as any).webcamCrop),
    topHeightPx: Number.isFinite(topHeightPxRaw) && topHeightPxRaw > 0 ? Math.round(topHeightPxRaw) : null,
    bottomFit: parseVerticalFitMode((value as any).bottomFit, 'cover')
  }
}

const legacyWebcamCropToVerticalCrop = (value: WebcamCrop | null): VerticalWebcamCrop | null => {
  if (!value) return null
  return {
    x: Number(value.x.toFixed(4)),
    y: Number(value.y.toFixed(4)),
    w: Number(value.width.toFixed(4)),
    h: Number(value.height.toFixed(4))
  }
}

const defaultVerticalModeSettings = (): VerticalModeSettings => ({
  enabled: true,
  output: { width: DEFAULT_VERTICAL_OUTPUT_WIDTH, height: DEFAULT_VERTICAL_OUTPUT_HEIGHT },
  layout: 'stacked',
  webcamCrop: null,
  topHeightPx: Math.round(DEFAULT_VERTICAL_OUTPUT_HEIGHT * DEFAULT_VERTICAL_TOP_HEIGHT_PCT),
  bottomFit: 'cover'
})

const buildVerticalModeSettings = ({
  value,
  legacyCrop
}: {
  value?: any
  legacyCrop?: VerticalWebcamCrop | null
}): VerticalModeSettings => {
  const parsed = parseVerticalModeSettings(value)
  const defaults = defaultVerticalModeSettings()
  const merged: VerticalModeSettings = {
    ...defaults,
    ...(parsed || {}),
    output: {
      ...defaults.output,
      ...((parsed?.output as any) || {})
    }
  }
  if (!merged.webcamCrop && legacyCrop) {
    merged.webcamCrop = legacyCrop
  }
  return merged
}

const parseLegacyVerticalCrop = (value?: any): VerticalWebcamCrop | null => {
  const absolute = parseVerticalWebcamCrop(value)
  if (absolute) return absolute
  const normalized = parseWebcamCrop(value)
  return legacyWebcamCropToVerticalCrop(normalized)
}

const parseRenderConfigFromRequest = (body?: any): RenderConfig => {
  const hasExplicitMode = body?.renderMode !== undefined || body?.mode !== undefined
  const explicitMode = parseRenderMode(body?.renderMode || body?.mode)
  const legacyVerticalMode = body?.verticalMode?.enabled === true
  const mode: RenderMode = hasExplicitMode ? explicitMode : (legacyVerticalMode ? 'vertical' : 'horizontal')
  const horizontalMode = buildHorizontalModeSettings(body?.horizontalMode)
  if (mode !== 'vertical') {
    return { mode: 'horizontal', verticalClipCount: 1, horizontalMode, verticalMode: null }
  }
  const legacyCrop = parseLegacyVerticalCrop(body?.webcamCrop)
  return {
    mode: 'vertical',
    verticalClipCount: parseVerticalClipCount(body?.verticalClipCount),
    horizontalMode,
    verticalMode: buildVerticalModeSettings({
      value: body?.verticalMode,
      legacyCrop
    })
  }
}

const parseRenderConfigFromAnalysis = (analysis?: any, renderSettings?: any): RenderConfig => {
  const modeSource = renderSettings?.renderMode ?? analysis?.renderMode
  const hasExplicitMode = modeSource !== undefined && modeSource !== null && String(modeSource).trim().length > 0
  const mode: RenderMode = hasExplicitMode
    ? parseRenderMode(modeSource)
    : (renderSettings?.verticalMode?.enabled === true || analysis?.verticalMode?.enabled === true ? 'vertical' : 'horizontal')
  const horizontalMode = buildHorizontalModeSettings(renderSettings?.horizontalMode ?? analysis?.horizontalMode)
  if (mode !== 'vertical') {
    return { mode: 'horizontal', verticalClipCount: 1, horizontalMode, verticalMode: null }
  }
  const legacyCrop = parseLegacyVerticalCrop(analysis?.vertical?.webcamCrop)
  return {
    mode: 'vertical',
    verticalClipCount: parseVerticalClipCount(
      renderSettings?.verticalClipCount ?? analysis?.verticalClipCount ?? analysis?.vertical?.clipCount
    ),
    horizontalMode,
    verticalMode: buildVerticalModeSettings({
      value: renderSettings?.verticalMode ?? analysis?.verticalMode ?? analysis?.vertical?.mode,
      legacyCrop
    })
  }
}

const getRetentionStrategyFromPayload = (payload?: any) => {
  if (!payload || typeof payload !== 'object') return DEFAULT_EDIT_OPTIONS.retentionStrategyProfile
  const explicitStrategy = (payload as any).retentionStrategyProfile ?? (payload as any).retentionStrategy
  if (explicitStrategy !== undefined && explicitStrategy !== null && String(explicitStrategy).trim().length > 0) {
    return parseRetentionStrategyProfile(explicitStrategy)
  }
  const legacyAggressionValue =
    (payload as any).retentionLevel ??
    (payload as any).retentionAggressionLevel ??
    (payload as any).aggressionLevel
  if (legacyAggressionValue !== undefined && legacyAggressionValue !== null && String(legacyAggressionValue).trim().length > 0) {
    return parseRetentionStrategyProfile(legacyAggressionValue)
  }
  return DEFAULT_EDIT_OPTIONS.retentionStrategyProfile
}

const getRetentionAggressionFromPayload = (payload?: any) => {
  if (!payload || typeof payload !== 'object') return DEFAULT_EDIT_OPTIONS.retentionAggressionLevel
  const explicitAggression =
    (payload as any).retentionLevel ??
    (payload as any).retentionAggressionLevel ??
    (payload as any).aggressionLevel
  if (explicitAggression !== undefined && explicitAggression !== null && String(explicitAggression).trim().length > 0) {
    return parseRetentionAggressionLevel(explicitAggression)
  }
  const strategy = getRetentionStrategyFromPayload(payload)
  return STRATEGY_TO_AGGRESSION[strategy] || DEFAULT_EDIT_OPTIONS.retentionAggressionLevel
}

const getOnlyCutsFromPayload = (payload?: any): boolean | null => {
  if (!payload || typeof payload !== 'object') return null
  return (
    parseBooleanFlag((payload as any).onlyCuts) ??
    parseBooleanFlag((payload as any).onlyHookAndCut) ??
    parseBooleanFlag((payload as any).hookAndCutOnly) ??
    null
  )
}

const getAutoCaptionsFromPayload = (payload?: any): boolean | null => {
  if (!payload || typeof payload !== 'object') return null
  const nested = (payload as any).subtitles
  return (
    parseBooleanFlag((payload as any).autoCaptions) ??
    parseBooleanFlag((payload as any).auto_captions) ??
    parseBooleanFlag(nested?.enabled) ??
    null
  )
}

const getSubtitleStyleFromPayload = (payload?: any): string | null => {
  if (!payload || typeof payload !== 'object') return null
  const nested = (payload as any).subtitles
  const candidate =
    (payload as any).subtitleStyle ??
    (payload as any).subtitle_style ??
    nested?.style ??
    nested?.subtitleStyle ??
    nested?.subtitle_style ??
    nested?.preset
  if (candidate === undefined || candidate === null) return null
  const text = String(candidate).trim()
  if (!text) return null
  return text.slice(0, 320)
}

const STYLE_ARCHETYPE_KEYS: Array<keyof StyleArchetypeBlend> = [
  'high_stakes_challenge',
  'longform_reaction_commentary',
  'cinematic_lifestyle_archive',
  'energetic_vlog'
]

const parseStyleArchetypeBlendFromPayload = (payload?: any): Partial<StyleArchetypeBlend> | null => {
  if (!payload || typeof payload !== 'object') return null
  const rawBlend =
    (payload as any).styleArchetypeBlendOverride ??
    (payload as any).style_archetype_blend_override ??
    (payload as any).styleArchetypeBlend ??
    (payload as any).styleMix ??
    (payload as any).style_mix
  if (!rawBlend || typeof rawBlend !== 'object') return null
  const blend: Partial<StyleArchetypeBlend> = {}
  let hasValue = false
  for (const key of STYLE_ARCHETYPE_KEYS) {
    const value = Number((rawBlend as any)[key])
    if (!Number.isFinite(value) || value <= 0) continue
    blend[key] = Number(value.toFixed(6))
    hasValue = true
  }
  return hasValue ? blend : null
}

const hasRetentionTargetPlatformOverride = (payload?: any) => Boolean(
  payload &&
  (
    payload?.retentionTargetPlatform !== undefined ||
    payload?.retention_target_platform !== undefined ||
    payload?.retentionPlatform !== undefined ||
    payload?.targetPlatform !== undefined ||
    payload?.platform !== undefined
  )
)

const getRetentionTargetPlatformFromPayload = (payload?: any): RetentionTargetPlatform => {
  if (!payload || typeof payload !== 'object') return 'auto'
  const explicit = (
    (payload as any).retentionTargetPlatform ??
    (payload as any).retention_target_platform ??
    (payload as any).retentionPlatform ??
    (payload as any).targetPlatform ??
    (payload as any).platform
  )
  return parseRetentionTargetPlatform(explicit)
}

const hasPlatformProfileOverride = (payload?: any) => Boolean(
  payload &&
  (
    payload?.platformProfile !== undefined ||
    payload?.platform_profile !== undefined ||
    payload?.editProfile !== undefined
  )
)

const getPlatformProfileFromPayload = (payload?: any, fallback: PlatformProfile = 'auto'): PlatformProfile => {
  if (!payload || typeof payload !== 'object') return fallback
  const explicit = (
    (payload as any).platformProfile ??
    (payload as any).platform_profile ??
    (payload as any).editProfile
  )
  if (explicit !== undefined && explicit !== null && String(explicit).trim().length > 0) {
    return parsePlatformProfile(explicit, fallback)
  }
  return parsePlatformProfile(getRetentionTargetPlatformFromPayload(payload), fallback)
}

const getRetentionAggressionFromJob = (job?: any) => {
  const analysis = job?.analysis as any
  const settings = (job as any)?.renderSettings as any
  const explicitAggression =
    settings?.retentionLevel ??
    settings?.retentionAggressionLevel ??
    analysis?.retentionLevel ??
    analysis?.retentionAggressionLevel
  if (explicitAggression !== undefined && explicitAggression !== null && String(explicitAggression).trim().length > 0) {
    return parseRetentionAggressionLevel(explicitAggression)
  }
  const strategy = parseRetentionStrategyProfile(
    settings?.retentionStrategyProfile ??
    settings?.retentionStrategy ??
    analysis?.retentionStrategyProfile ??
    analysis?.retentionStrategy
  )
  return STRATEGY_TO_AGGRESSION[strategy] || DEFAULT_EDIT_OPTIONS.retentionAggressionLevel
}

const getOnlyCutsFromJob = (job?: any): boolean | null => {
  const analysis = job?.analysis as any
  const settings = (job as any)?.renderSettings as any
  return (
    parseBooleanFlag(settings?.onlyCuts) ??
    parseBooleanFlag(settings?.onlyHookAndCut) ??
    parseBooleanFlag(analysis?.onlyCuts) ??
    parseBooleanFlag(analysis?.onlyHookAndCut) ??
    null
  )
}

const getRetentionStrategyFromJob = (job?: any): RetentionStrategyProfile => {
  const analysis = job?.analysis as any
  const settings = (job as any)?.renderSettings as any
  const strategyRaw =
    settings?.retentionStrategyProfile ??
    settings?.retentionStrategy ??
    analysis?.retentionStrategyProfile ??
    analysis?.retentionStrategy
  if (strategyRaw !== undefined && strategyRaw !== null && String(strategyRaw).trim().length > 0) {
    return parseRetentionStrategyProfile(strategyRaw)
  }
  return strategyFromAggressionLevel(getRetentionAggressionFromJob(job))
}

const getRetentionTargetPlatformFromJob = (job?: any): RetentionTargetPlatform => {
  const analysis = job?.analysis as any
  const settings = (job as any)?.renderSettings as any
  const source =
    settings?.retentionTargetPlatform ??
    settings?.retention_target_platform ??
    settings?.retentionPlatform ??
    settings?.targetPlatform ??
    analysis?.retentionTargetPlatform ??
    analysis?.retention_target_platform ??
    analysis?.retentionPlatform ??
    analysis?.targetPlatform ??
    analysis?.platform
  return parseRetentionTargetPlatform(source)
}

const getPlatformProfileFromJob = (job?: any): PlatformProfile => {
  const analysis = job?.analysis as any
  const settings = (job as any)?.renderSettings as any
  const retentionFallback = parsePlatformProfile(
    settings?.retentionTargetPlatform ??
    settings?.retention_target_platform ??
    settings?.retentionPlatform ??
    settings?.targetPlatform ??
    analysis?.retentionTargetPlatform ??
    analysis?.retention_target_platform ??
    analysis?.retentionPlatform ??
    analysis?.targetPlatform ??
    analysis?.platform,
    'auto'
  )
  const source =
    settings?.platformProfile ??
    settings?.platform_profile ??
    analysis?.platformProfile ??
    analysis?.platform_profile
  return parsePlatformProfile(source, retentionFallback)
}

const buildRetentionPlatformFromPayload = ({
  payload,
  fallbackPlatform
}: {
  payload?: any
  fallbackPlatform: RetentionTargetPlatform
}) => {
  const hasOverride = hasRetentionTargetPlatformOverride(payload)
  const targetPlatform = hasOverride
    ? getRetentionTargetPlatformFromPayload(payload)
    : fallbackPlatform
  return { targetPlatform, hasOverride }
}

const buildRetentionTuningFromPayload = ({
  payload,
  fallbackAggression,
  fallbackStrategy
}: {
  payload?: any
  fallbackAggression: RetentionAggressionLevel
  fallbackStrategy: RetentionStrategyProfile
}) => {
  const hasAggressionOverride = Boolean(
    payload &&
    (
      payload?.retentionLevel !== undefined ||
      payload?.retentionAggressionLevel !== undefined ||
      payload?.aggressionLevel !== undefined
    )
  )
  const hasStrategyOverride = Boolean(
    payload &&
    (
      payload?.retentionStrategyProfile !== undefined ||
      payload?.retentionStrategy !== undefined
    )
  )
  const strategy = hasStrategyOverride
    ? getRetentionStrategyFromPayload(payload)
    : fallbackStrategy
  const aggression = hasAggressionOverride
    ? getRetentionAggressionFromPayload(payload)
    : (hasStrategyOverride
      ? (STRATEGY_TO_AGGRESSION[strategy] || fallbackAggression)
      : fallbackAggression)
  return { aggression, strategy, hasAggressionOverride, hasStrategyOverride }
}

const buildPersistedRenderSettings = (
  renderConfig: RenderConfig,
  opts?: {
    retentionAggressionLevel?: RetentionAggressionLevel | null
    retentionStrategyProfile?: RetentionStrategyProfile | null
    retentionTargetPlatform?: RetentionTargetPlatform | null
    platformProfile?: PlatformProfile | null
    onlyCuts?: boolean | null
  }
) => {
  const retentionLevel = parseRetentionAggressionLevel(
    opts?.retentionAggressionLevel || DEFAULT_EDIT_OPTIONS.retentionAggressionLevel
  )
  const retentionStrategy = parseRetentionStrategyProfile(
    opts?.retentionStrategyProfile || strategyFromAggressionLevel(retentionLevel)
  )
  const retentionTargetPlatform = parseRetentionTargetPlatform(opts?.retentionTargetPlatform || 'auto')
  const platformProfile = parsePlatformProfile(opts?.platformProfile || retentionTargetPlatform, 'auto')
  const onlyCuts = typeof opts?.onlyCuts === 'boolean' ? opts.onlyCuts : null
  return {
    renderMode: renderConfig.mode,
    horizontalMode: renderConfig.horizontalMode,
    verticalClipCount: renderConfig.mode === 'vertical' ? renderConfig.verticalClipCount : 1,
    verticalMode: renderConfig.mode === 'vertical' ? renderConfig.verticalMode : null,
    retentionAggressionLevel: retentionLevel,
    retentionLevel,
    retentionStrategyProfile: retentionStrategy,
    retentionStrategy: retentionStrategy,
    retentionTargetPlatform,
    retention_target_platform: retentionTargetPlatform,
    targetPlatform: retentionTargetPlatform,
    platformProfile,
    platform_profile: platformProfile,
    ...(onlyCuts === null ? {} : { onlyCuts, onlyHookAndCut: onlyCuts })
  }
}

const buildPersistedRenderAnalysis = ({
  existing,
  renderConfig,
  outputPaths,
  onlyCuts,
  retentionTargetPlatform,
  platformProfile
}: {
  existing?: any
  renderConfig: RenderConfig
  outputPaths?: string[] | null
  onlyCuts?: boolean | null
  retentionTargetPlatform?: RetentionTargetPlatform | null
  platformProfile?: PlatformProfile | null
}) => {
  const resolvedOnlyCuts = typeof onlyCuts === 'boolean'
    ? onlyCuts
    : (
      parseBooleanFlag((existing as any)?.onlyCuts) ??
      parseBooleanFlag((existing as any)?.onlyHookAndCut) ??
      null
    )
  const verticalCrop = renderConfig.verticalMode?.webcamCrop
    ? {
        x: renderConfig.verticalMode.webcamCrop.x,
        y: renderConfig.verticalMode.webcamCrop.y,
        width: renderConfig.verticalMode.webcamCrop.w,
        height: renderConfig.verticalMode.webcamCrop.h
      }
    : null
  const payload: Record<string, any> = {
    ...(existing || {}),
    metadata_version: Number.isFinite(Number((existing as any)?.metadata_version))
      ? Number((existing as any).metadata_version)
      : 2,
    renderMode: renderConfig.mode,
    horizontalMode: renderConfig.horizontalMode,
    verticalMode: renderConfig.mode === 'vertical' ? renderConfig.verticalMode : null,
    verticalClipCount: renderConfig.mode === 'vertical' ? renderConfig.verticalClipCount : 1,
    // Legacy fields kept for backward-compatible clients.
    vertical: renderConfig.mode === 'vertical'
      ? {
          clipCount: outputPaths?.length ?? renderConfig.verticalClipCount,
          webcamCrop: verticalCrop,
          mode: renderConfig.verticalMode
        }
      : null,
    verticalOutputPaths: renderConfig.mode === 'vertical' ? (outputPaths || []) : null
  }
  const resolvedTargetPlatform = retentionTargetPlatform
    ? parseRetentionTargetPlatform(retentionTargetPlatform)
    : parseRetentionTargetPlatform(
        (existing as any)?.retentionTargetPlatform ??
        (existing as any)?.retention_target_platform ??
        (existing as any)?.retentionPlatform ??
        (existing as any)?.targetPlatform ??
        (existing as any)?.platform
      )
  payload.retentionTargetPlatform = resolvedTargetPlatform
  payload.retention_target_platform = resolvedTargetPlatform
  payload.retentionPlatform = resolvedTargetPlatform
  payload.targetPlatform = resolvedTargetPlatform
  payload.platform = resolvedTargetPlatform
  const resolvedPlatformProfile = platformProfile
    ? parsePlatformProfile(platformProfile, parsePlatformProfile(resolvedTargetPlatform, 'auto'))
    : parsePlatformProfile(
        (existing as any)?.platformProfile ??
        (existing as any)?.platform_profile ??
        resolvedTargetPlatform,
        'auto'
      )
  payload.platformProfile = resolvedPlatformProfile
  payload.platform_profile = resolvedPlatformProfile
  if (resolvedOnlyCuts !== null) {
    payload.onlyCuts = resolvedOnlyCuts
    payload.onlyHookAndCut = resolvedOnlyCuts
  }
  return normalizeAnalysisPayload(payload)
}

const normalizePipelineStepMap = (raw?: any): Record<RetentionPipelineStep, PipelineStepState> => {
  const source = raw && typeof raw === 'object' ? raw : {}
  const out = {} as Record<RetentionPipelineStep, PipelineStepState>
  for (const step of RETENTION_PIPELINE_STEPS) {
    const entry = source[step] && typeof source[step] === 'object' ? source[step] : {}
    const statusRaw = String(entry.status || '').toLowerCase()
    const status: PipelineStepStatus =
      statusRaw === 'running' || statusRaw === 'completed' || statusRaw === 'failed' ? (statusRaw as PipelineStepStatus) : 'pending'
    out[step] = {
      status,
      attempts: Number.isFinite(Number(entry.attempts)) ? Math.max(0, Number(entry.attempts)) : 0,
      retries: Number.isFinite(Number(entry.retries)) ? Math.max(0, Number(entry.retries)) : 0,
      startedAt: entry.startedAt ? String(entry.startedAt) : undefined,
      completedAt: entry.completedAt ? String(entry.completedAt) : undefined,
      lastError: entry.lastError ? String(entry.lastError) : null,
      meta: entry.meta ?? null
    }
  }
  return out
}

const updatePipelineStepState = async (
  jobId: string,
  step: RetentionPipelineStep,
  patch: Partial<PipelineStepState>
) => {
  const current = await prisma.job.findUnique({ where: { id: jobId }, select: { analysis: true } })
  const analysis = ((current?.analysis as any) || {}) as Record<string, any>
  const steps = normalizePipelineStepMap(analysis.pipelineSteps)
  const prev = steps[step]
  steps[step] = {
    ...prev,
    ...patch,
    attempts: patch.attempts ?? prev.attempts,
    retries: patch.retries ?? prev.retries
  }
  const nextAnalysis = {
    ...analysis,
    pipelineSteps: steps,
    pipelineUpdatedAt: toIsoNow()
  }
  await updateJob(jobId, { analysis: nextAnalysis })
  return nextAnalysis
}

const runRetentionStep = async <T>({
  jobId,
  step,
  maxRetries = 1,
  statusUpdate,
  run,
  summarize
}: {
  jobId: string
  step: RetentionPipelineStep
  maxRetries?: number
  statusUpdate?: { status?: string; progress?: number }
  run: (attempt: number) => Promise<T>
  summarize?: (result: T) => any
}) => {
  let attempt = 0
  while (attempt <= maxRetries) {
    attempt += 1
    await updatePipelineStepState(jobId, step, {
      status: 'running',
      attempts: attempt,
      startedAt: toIsoNow(),
      completedAt: undefined,
      lastError: null
    })
    if (statusUpdate) {
      await updateJob(jobId, statusUpdate)
    }
    try {
      const result = await run(attempt)
      await updatePipelineStepState(jobId, step, {
        status: 'completed',
        completedAt: toIsoNow(),
        meta: summarize ? summarize(result) : null
      })
      return result
    } catch (err: any) {
      const retryCount = Math.min(maxRetries, attempt)
      await updatePipelineStepState(jobId, step, {
        status: 'failed',
        retries: retryCount,
        completedAt: toIsoNow(),
        lastError: truncateErrorText(err?.message || err) || 'step_failed'
      })
      if (attempt > maxRetries) throw err
    }
  }
  throw new Error(`pipeline_step_failed:${step}`)
}

const resolveHorizontalTargetDimensions = ({
  horizontalMode,
  qualityTarget,
  sourceWidth,
  sourceHeight
}: {
  horizontalMode: HorizontalModeSettings
  qualityTarget: { width: number; height: number }
  sourceWidth?: number | null
  sourceHeight?: number | null
}) => {
  const modeOutput = horizontalMode.output
  if (modeOutput === 'source') {
    if (Number.isFinite(sourceWidth) && Number.isFinite(sourceHeight) && sourceWidth && sourceHeight) {
      return {
        width: Math.round(clamp(Number(sourceWidth), 240, 4320)),
        height: Math.round(clamp(Number(sourceHeight), 240, 7680))
      }
    }
    return qualityTarget
  }
  if (modeOutput && typeof modeOutput === 'object') {
    return {
      width: Math.round(clamp(modeOutput.width, 240, 4320)),
      height: Math.round(clamp(modeOutput.height, 240, 7680))
    }
  }
  return qualityTarget
}

const getVerticalOutputPathsFromAnalysis = (analysis?: any) => {
  const raw = analysis?.verticalOutputPaths
  if (!Array.isArray(raw)) return [] as string[]
  return raw
    .map((value: any) => String(value || '').trim())
    .filter((value: string) => value.length > 0)
}

const getOutputPathsForJob = (job: any) => {
  const analysis = job?.analysis as any
  const verticalPaths = getVerticalOutputPathsFromAnalysis(analysis)
  if (verticalPaths.length > 0) return verticalPaths
  if (job?.outputPath) return [job.outputPath]
  return [] as string[]
}

const resolveLocalOutputPathForJob = (job: any, clipIndex = 0) => {
  const localOutDir = path.join(process.cwd(), 'outputs', job.userId, job.id)
  const renderConfig = parseRenderConfigFromAnalysis(job.analysis as any, (job as any)?.renderSettings)
  if (renderConfig.mode === 'vertical') {
    return path.join(localOutDir, `vertical-clip-${clipIndex + 1}.mp4`)
  }
  return path.join(localOutDir, 'output.mp4')
}

const getLocalOutputFileInfo = (job: any, clipIndex = 0) => {
  const filePath = resolveLocalOutputPathForJob(job, clipIndex)
  if (!fs.existsSync(filePath)) return null
  try {
    const stats = fs.statSync(filePath)
    if (!stats.isFile() || stats.size <= 0) return null
    return { filePath, size: stats.size }
  } catch {
    return null
  }
}

const buildAbsoluteApiUrl = (req: any, pathname: string) => {
  const forwardedProtoRaw = req.headers?.['x-forwarded-proto']
  const forwardedProto = Array.isArray(forwardedProtoRaw)
    ? String(forwardedProtoRaw[0] || '')
    : String(forwardedProtoRaw || '')
  const protocol = forwardedProto
    ? forwardedProto.split(',')[0].trim()
    : (req.protocol || 'http')
  const host = req.get?.('host') || req.headers?.host
  if (!host) return pathname
  return `${protocol}://${host}${pathname}`
}

const buildLocalOutputFallbackUrl = (req: any, jobId: string, clipIndex: number) => {
  const clip = Math.max(1, clipIndex + 1)
  const query = clip > 1 ? `?clip=${clip}` : ''
  return buildAbsoluteApiUrl(req, `/api/jobs/${jobId}/local-output${query}`)
}

const resolveOutputUrlWithLocalFallback = async ({
  req,
  job,
  outputPath,
  clipIndex,
  expiresIn = 60 * 10
}: {
  req: any
  job: any
  outputPath: string
  clipIndex: number
  expiresIn?: number
}) => {
  try {
    await ensureBucket(OUTPUT_BUCKET, false)
    const signed = await getSignedOutputUrl({ key: outputPath, expiresIn })
    return {
      url: signed,
      source: 'remote' as const
    }
  } catch (error) {
    const local = getLocalOutputFileInfo(job, clipIndex)
    if (local) {
      return {
        url: buildLocalOutputFallbackUrl(req, job.id, clipIndex),
        source: 'local' as const
      }
    }
    throw error
  }
}

const normalizePercentMetric = (value: any) => {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return null
  if (numeric >= 0 && numeric <= 1) return Number(clamp01(numeric).toFixed(4))
  return Number(clamp01(numeric / 100).toFixed(4))
}

const normalizeScore100 = (value: any) => {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return null
  const score = numeric <= 10 ? numeric * 10 : numeric
  return Number(clamp(score, 0, 100).toFixed(2))
}

const normalizeDurationSeconds = (value: any) => {
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric < 0) return null
  return Number(clamp(numeric, 0, 60 * 60 * 8).toFixed(3))
}

const resolveFeedbackSource = (raw: any) => {
  const source = typeof raw === 'string' && raw.trim()
    ? raw.trim().toLowerCase().slice(0, 64)
    : ''
  if (!source) return { source: null as string | null, isPlatform: false }
  const platform = /(youtube|tiktok|instagram|reels|shorts|analytics|platform|meta)/.test(source)
  return {
    source,
    isPlatform: platform
  }
}

const parseRetentionFeedbackPayload = (payload: any): RetentionFeedbackPayload | null => {
  if (!payload || typeof payload !== 'object') return null
  const watchPercent = normalizePercentMetric(
    payload.watchPercent ?? payload.watch_percent ?? payload.avgWatchPercent ?? payload.averageWatchPercent
  )
  const hookHoldPercent = normalizePercentMetric(
    payload.hookHoldPercent ?? payload.hook_hold_percent ?? payload.first8sRetention ?? payload.hookRetention
  )
  const completionPercent = normalizePercentMetric(
    payload.completionPercent ?? payload.completion_percent ?? payload.finishRate
  )
  const rewatchRate = normalizePercentMetric(
    payload.rewatchRate ?? payload.rewatch_rate ?? payload.loopRate
  )
  const first30Retention = normalizePercentMetric(
    payload.first30Retention ?? payload.first30_retention ?? payload.firstThirtyRetention
  )
  const avgViewDurationSeconds = normalizeDurationSeconds(
    payload.avgViewDurationSec ?? payload.avg_view_duration_seconds ?? payload.averageViewDurationSec
  )
  const clickThroughRate = normalizePercentMetric(
    payload.clickThroughRate ?? payload.click_through_rate ?? payload.ctr
  )
  const sharesPerView = normalizePercentMetric(
    payload.sharesPerView ?? payload.shares_per_view
  )
  const likesPerView = normalizePercentMetric(
    payload.likesPerView ?? payload.likes_per_view
  )
  const commentsPerView = normalizePercentMetric(
    payload.commentsPerView ?? payload.comments_per_view
  )
  const manualScore = normalizeScore100(
    payload.manualScore ?? payload.manual_score ?? payload.creatorScore ?? payload.editorScore
  )
  const sourceMeta = resolveFeedbackSource(
    payload.source ?? payload.sourceType ?? payload.source_type
  )
  const source = sourceMeta.source
  const sourceTypeRaw = typeof payload.sourceType === 'string' ? payload.sourceType.toLowerCase() : ''
  const sourceType = sourceTypeRaw === 'platform' || sourceMeta.isPlatform
    ? 'platform'
    : 'internal'
  const notes = typeof payload.notes === 'string' && payload.notes.trim()
    ? payload.notes.trim().slice(0, 400)
    : null
  const hasSignal =
    watchPercent !== null ||
    hookHoldPercent !== null ||
    completionPercent !== null ||
    rewatchRate !== null ||
    first30Retention !== null ||
    avgViewDurationSeconds !== null ||
    clickThroughRate !== null ||
    sharesPerView !== null ||
    likesPerView !== null ||
    commentsPerView !== null ||
    manualScore !== null
  if (!hasSignal) return null
  return {
    watchPercent,
    hookHoldPercent,
    completionPercent,
    rewatchRate,
    first30Retention,
    avgViewDurationSeconds,
    clickThroughRate,
    sharesPerView,
    likesPerView,
    commentsPerView,
    manualScore,
    source,
    sourceType,
    notes,
    submittedAt: toIsoNow()
  }
}

const parseCreatorFeedbackPayload = (payload: any): CreatorFeedbackPayload | null => {
  if (!payload || typeof payload !== 'object') return null
  const categoryRaw = String(payload.category || payload.feedback || '').trim().toLowerCase()
  if (!CREATOR_FEEDBACK_CATEGORIES.includes(categoryRaw as CreatorFeedbackCategory)) {
    return null
  }
  const sourceMeta = resolveFeedbackSource(payload.source ?? 'creator_feedback')
  const notes = typeof payload.notes === 'string' && payload.notes.trim()
    ? payload.notes.trim().slice(0, 400)
    : null
  const manualScore = normalizeScore100(payload.manualScore ?? payload.manual_score)
  return {
    category: categoryRaw as CreatorFeedbackCategory,
    source: sourceMeta.source || 'creator_feedback',
    notes,
    manualScore,
    submittedAt: toIsoNow()
  }
}

const buildRetentionFeedbackFromCreatorPayload = (
  payload: CreatorFeedbackPayload
): RetentionFeedbackPayload => {
  const mapped = CREATOR_FEEDBACK_SIGNAL_MAP[payload.category]
  const manualScore = payload.manualScore ?? mapped.manualScore
  return {
    watchPercent: mapped.watchPercent,
    hookHoldPercent: mapped.hookHoldPercent,
    completionPercent: mapped.completionPercent,
    rewatchRate: mapped.rewatchRate,
    first30Retention: null,
    avgViewDurationSeconds: null,
    clickThroughRate: null,
    sharesPerView: null,
    likesPerView: null,
    commentsPerView: null,
    manualScore,
    source: payload.source || `creator_feedback:${payload.category}`,
    sourceType: 'internal',
    notes: payload.notes,
    submittedAt: payload.submittedAt
  }
}

const getRetentionFeedbackFromAnalysis = (analysis: any) => {
  const feedback = analysis?.retention_feedback
  if (!feedback || typeof feedback !== 'object') return null
  return {
    watchPercent: normalizePercentMetric(feedback.watchPercent ?? feedback.watch_percent),
    hookHoldPercent: normalizePercentMetric(feedback.hookHoldPercent ?? feedback.hook_hold_percent ?? feedback.first8sRetention),
    completionPercent: normalizePercentMetric(feedback.completionPercent ?? feedback.completion_percent),
    rewatchRate: normalizePercentMetric(feedback.rewatchRate ?? feedback.rewatch_rate),
    manualScore: normalizeScore100(feedback.manualScore ?? feedback.manual_score),
    first30Retention: normalizePercentMetric(feedback.first30Retention ?? feedback.first30_retention),
    avgViewDurationSeconds: normalizeDurationSeconds(feedback.avgViewDurationSec ?? feedback.avg_view_duration_seconds),
    clickThroughRate: normalizePercentMetric(feedback.clickThroughRate ?? feedback.click_through_rate ?? feedback.ctr),
    sharesPerView: normalizePercentMetric(feedback.sharesPerView ?? feedback.shares_per_view),
    likesPerView: normalizePercentMetric(feedback.likesPerView ?? feedback.likes_per_view),
    commentsPerView: normalizePercentMetric(feedback.commentsPerView ?? feedback.comments_per_view),
    sourceType: String(feedback.sourceType || '').toLowerCase() === 'platform' ? 'platform' : 'internal'
  }
}

const persistRetentionFeedbackForJob = async ({
  job,
  feedback,
  analysisPatch
}: {
  job: any
  feedback: RetentionFeedbackPayload
  analysisPatch?: Record<string, any>
}) => {
  const existingAnalysis = (job.analysis as any) || {}
  const feedbackHistoryRaw = Array.isArray(existingAnalysis?.retention_feedback_history)
    ? existingAnalysis.retention_feedback_history
    : []
  const feedbackHistory = [
    ...feedbackHistoryRaw.slice(-39),
    feedback
  ]
  const nextAnalysis = {
    ...existingAnalysis,
    ...(analysisPatch || {}),
    retention_feedback: feedback,
    retention_feedback_history: feedbackHistory,
    retention_feedback_updated_at: toIsoNow()
  }
  await updateJob(job.id, { analysis: nextAnalysis })
  return nextAnalysis
}

const normalizeHookCalibrationWeights = (weights: HookCalibrationWeights): HookCalibrationWeights => {
  const bounded = {
    candidateScore: clamp(weights.candidateScore, 0.05, 0.7),
    auditScore: clamp(weights.auditScore, 0.05, 0.7),
    energy: clamp(weights.energy, 0.05, 0.7),
    curiosity: clamp(weights.curiosity, 0.05, 0.7),
    emotionalSpike: clamp(weights.emotionalSpike, 0.05, 0.7)
  }
  const total = bounded.candidateScore + bounded.auditScore + bounded.energy + bounded.curiosity + bounded.emotionalSpike
  if (!Number.isFinite(total) || total <= 0) return { ...DEFAULT_HOOK_FACEOFF_WEIGHTS }
  return {
    candidateScore: Number((bounded.candidateScore / total).toFixed(4)),
    auditScore: Number((bounded.auditScore / total).toFixed(4)),
    energy: Number((bounded.energy / total).toFixed(4)),
    curiosity: Number((bounded.curiosity / total).toFixed(4)),
    emotionalSpike: Number((bounded.emotionalSpike / total).toFixed(4))
  }
}

const buildDefaultHookCalibrationProfile = (reason: string, sampleSize = 0): HookCalibrationProfile => ({
  enabled: false,
  sampleSize,
  averageOutcome: 0,
  earlyDropRate: 0,
  platformFeedbackShare: 0,
  dominantStyle: null,
  weights: { ...DEFAULT_HOOK_FACEOFF_WEIGHTS },
  strategyBias: {},
  reasons: [reason],
  updatedAt: toIsoNow()
})

const computeFeedbackOutcomeSignal = (entry: {
  analysis?: any
  retentionScore?: number | null
}) => {
  const analysis = entry.analysis || {}
  const feedback = getRetentionFeedbackFromAnalysis(analysis)
  const modelRetentionRaw = Number(
    analysis?.retention_score ??
    analysis?.retentionScore ??
    entry.retentionScore ??
    0
  )
  const modelRetention = Number.isFinite(modelRetentionRaw) && modelRetentionRaw > 0
    ? clamp01(modelRetentionRaw / 100)
    : null
  const watch = feedback?.watchPercent ?? null
  const hookHold = feedback?.hookHoldPercent ?? null
  const completion = feedback?.completionPercent ?? null
  const manualScore = feedback?.manualScore !== null && feedback?.manualScore !== undefined
    ? clamp01((feedback?.manualScore ?? 0) / 100)
    : null
  const rewatch = feedback?.rewatchRate ?? null
  const first30Retention = feedback?.first30Retention ?? null
  const clickThroughRate = feedback?.clickThroughRate ?? null
  const sharesPerView = feedback?.sharesPerView ?? null
  const likesPerView = feedback?.likesPerView ?? null
  const commentsPerView = feedback?.commentsPerView ?? null
  const platformBoost = [
    clickThroughRate,
    sharesPerView,
    likesPerView,
    commentsPerView
  ].filter((value): value is number => value !== null && Number.isFinite(value))

  const weightedSignals = [
    { value: watch, weight: 0.28 },
    { value: hookHold, weight: 0.21 },
    { value: completion, weight: 0.12 },
    { value: first30Retention, weight: 0.14 },
    { value: manualScore, weight: 0.08 },
    { value: rewatch, weight: 0.05 },
    { value: modelRetention, weight: 0.08 },
    { value: platformBoost.length ? clamp01(platformBoost.reduce((sum, value) => sum + Number(value), 0) / platformBoost.length) : null, weight: 0.04 }
  ].filter((signal) => signal.value !== null && Number.isFinite(signal.value))
  if (!weightedSignals.length) {
    return {
      outcome: null as number | null,
      hookHoldProxy: null as number | null,
      isPlatform: false
    }
  }
  const signalWeight = weightedSignals.reduce((sum, signal) => sum + signal.weight, 0)
  if (signalWeight <= 0) {
    return {
      outcome: null as number | null,
      hookHoldProxy: null as number | null,
      isPlatform: false
    }
  }
  const outcome = clamp01(
    weightedSignals.reduce((sum, signal) => sum + (signal.value as number) * signal.weight, 0) / signalWeight
  )
  const hookHoldProxy = hookHold ?? watch ?? first30Retention ?? completion ?? outcome
  return {
    outcome: Number(outcome.toFixed(4)),
    hookHoldProxy: Number(clamp01(hookHoldProxy).toFixed(4)),
    isPlatform: feedback?.sourceType === 'platform'
  }
}

const computeStrategyBiasFromHistory = (
  strategyOutcomes: Partial<Record<RetentionRetryStrategy, { total: number; count: number }>>,
  averageOutcome: number
) => {
  const bias: Partial<Record<RetentionRetryStrategy, number>> = {}
  for (const strategy of RETENTION_VARIANT_STRATEGIES) {
    const stats = strategyOutcomes[strategy]
    if (!stats || stats.count < 2) continue
    const strategyMean = stats.total / stats.count
    const delta = clamp((strategyMean - averageOutcome) * 100, -12, 12)
    bias[strategy] = Number(delta.toFixed(2))
  }
  return bias
}

const computeHookCalibrationProfileFromHistory = (
  entries: Array<{ analysis?: any; retentionScore?: number | null }>
): HookCalibrationProfile => {
  if (!entries.length) return buildDefaultHookCalibrationProfile('No completed jobs found for calibration.', 0)

  let sampleSize = 0
  let totalOutcome = 0
  let earlyDropCount = 0
  let lowEmotionCount = 0
  let lowPacingCount = 0
  let platformFeedbackCount = 0
  const strategyOutcomes: Partial<Record<RetentionRetryStrategy, { total: number; count: number }>> = {}
  const styleCounts: Record<ContentStyle, number> = {
    reaction: 0,
    vlog: 0,
    tutorial: 0,
    gaming: 0,
    story: 0
  }

  for (const entry of entries) {
    const analysis = entry.analysis || {}
    const signal = computeFeedbackOutcomeSignal(entry)
    if (signal.outcome === null) continue
    const outcome = signal.outcome
    sampleSize += 1
    totalOutcome += outcome
    if (signal.isPlatform) platformFeedbackCount += 1

    const hookHoldProxy = signal.hookHoldProxy ?? outcome
    if (hookHoldProxy < 0.55) earlyDropCount += 1
    const emotionalPull = Number(analysis?.retention_judge?.emotional_pull)
    if (Number.isFinite(emotionalPull) && emotionalPull < QUALITY_GATE_THRESHOLDS.emotional_pull) lowEmotionCount += 1
    const pacingScore = Number(analysis?.retention_judge?.pacing_score)
    if (Number.isFinite(pacingScore) && pacingScore < QUALITY_GATE_THRESHOLDS.pacing_score) lowPacingCount += 1
    const strategyRaw = String(analysis?.selected_strategy || '').toUpperCase()
    if (RETENTION_VARIANT_STRATEGIES.includes(strategyRaw as RetentionRetryStrategy)) {
      const strategy = strategyRaw as RetentionRetryStrategy
      const prev = strategyOutcomes[strategy] || { total: 0, count: 0 }
      strategyOutcomes[strategy] = {
        total: prev.total + outcome,
        count: prev.count + 1
      }
    }
    const style = analysis?.style_profile?.style
    if (style && Object.prototype.hasOwnProperty.call(styleCounts, style)) {
      styleCounts[style as ContentStyle] += 1
    }
  }

  if (sampleSize < HOOK_CALIBRATION_MIN_SAMPLES) {
    return buildDefaultHookCalibrationProfile(
      `Need at least ${HOOK_CALIBRATION_MIN_SAMPLES} jobs with retention feedback before adaptive hook tuning.`,
      sampleSize
    )
  }

  const averageOutcome = totalOutcome / sampleSize
  const earlyDropRate = earlyDropCount / sampleSize
  const lowEmotionRate = lowEmotionCount / sampleSize
  const lowPacingRate = lowPacingCount / sampleSize
  const platformFeedbackShare = platformFeedbackCount / sampleSize
  const reasons: string[] = []
  let weights: HookCalibrationWeights = { ...DEFAULT_HOOK_FACEOFF_WEIGHTS }

  if (earlyDropRate > 0.32) {
    weights = {
      ...weights,
      candidateScore: weights.candidateScore - 0.09,
      auditScore: weights.auditScore + 0.04,
      curiosity: weights.curiosity + 0.08,
      energy: weights.energy - 0.02,
      emotionalSpike: weights.emotionalSpike - 0.01
    }
    reasons.push('Early drop-off was high; increasing hook curiosity and audit strictness.')
  }
  if (lowEmotionRate > 0.28) {
    weights = {
      ...weights,
      candidateScore: weights.candidateScore - 0.05,
      energy: weights.energy + 0.02,
      emotionalSpike: weights.emotionalSpike + 0.06
    }
    reasons.push('Emotional pull underperformed; biasing toward emotional spikes and energetic delivery.')
  }
  if (lowPacingRate > 0.34) {
    weights = {
      ...weights,
      candidateScore: weights.candidateScore + 0.02,
      auditScore: weights.auditScore - 0.01,
      energy: weights.energy + 0.03
    }
    reasons.push('Pacing scores were weak; slightly prioritizing energetic flow in hook faceoff.')
  }
  if (averageOutcome < 0.58) {
    weights = {
      ...weights,
      candidateScore: weights.candidateScore - 0.05,
      auditScore: weights.auditScore + 0.04,
      curiosity: weights.curiosity + 0.03
    }
    reasons.push('Overall retention trended low; applying safer, curiosity-first weighting.')
  } else if (averageOutcome > 0.78) {
    weights = {
      ...weights,
      candidateScore: weights.candidateScore + 0.03,
      auditScore: weights.auditScore + 0.01,
      curiosity: weights.curiosity - 0.01
    }
    reasons.push('Retention trended high; preserving strong base scoring while keeping quality checks.')
  }

  const dominantStyleEntry = (Object.entries(styleCounts) as Array<[ContentStyle, number]>)
    .sort((a, b) => b[1] - a[1])[0]
  const dominantStyle = dominantStyleEntry && dominantStyleEntry[1] > 0 ? dominantStyleEntry[0] : null
  if (dominantStyle === 'tutorial') {
    weights = {
      ...weights,
      auditScore: weights.auditScore + 0.02,
      energy: weights.energy - 0.02
    }
    reasons.push('Tutorial-heavy feedback favors clarity and context-complete hooks.')
  } else if (dominantStyle === 'reaction' || dominantStyle === 'gaming') {
    weights = {
      ...weights,
      candidateScore: weights.candidateScore - 0.02,
      energy: weights.energy + 0.03,
      curiosity: weights.curiosity + 0.02
    }
    reasons.push('High-energy catalog detected; favoring momentum and curiosity for opening beats.')
  }

  if (!reasons.length) reasons.push('Using baseline hook faceoff weights from neutral feedback trend.')
  return {
    enabled: true,
    sampleSize,
    averageOutcome: Number(averageOutcome.toFixed(4)),
    earlyDropRate: Number(earlyDropRate.toFixed(4)),
    platformFeedbackShare: Number(platformFeedbackShare.toFixed(4)),
    dominantStyle,
    weights: normalizeHookCalibrationWeights(weights),
    strategyBias: computeStrategyBiasFromHistory(strategyOutcomes, averageOutcome),
    reasons,
    updatedAt: toIsoNow()
  }
}

const loadHookCalibrationProfile = async (userId: string) => {
  if (!userId) return buildDefaultHookCalibrationProfile('Missing user context for calibration.', 0)
  try {
    const jobs = await prisma.job.findMany({
      where: { userId, status: 'completed' },
      orderBy: { updatedAt: 'desc' },
      take: HOOK_CALIBRATION_LOOKBACK_JOBS,
      select: {
        analysis: true,
        retentionScore: true
      }
    })
    return computeHookCalibrationProfileFromHistory(
      jobs.map((job) => ({
        analysis: job.analysis,
        retentionScore: job.retentionScore
      }))
    )
  } catch (error) {
    console.warn('hook calibration load failed', error)
    return buildDefaultHookCalibrationProfile('Failed to load calibration data; using baseline weights.', 0)
  }
}

const predictVariantRetention = ({
  strategy,
  judge,
  hook,
  hookCalibration,
  styleProfile
}: {
  strategy: RetentionRetryStrategy
  judge: RetentionJudgeReport
  hook: HookCandidate
  hookCalibration?: HookCalibrationProfile | null
  styleProfile?: ContentStyleProfile | null
}) => {
  const calibration = hookCalibration && hookCalibration.enabled ? hookCalibration : null
  const base = (
    0.38 * judge.retention_score +
    0.24 * judge.hook_strength +
    0.18 * judge.emotional_pull +
    0.12 * judge.pacing_score +
    0.08 * judge.clarity_score
  )
  const strategyBias = calibration?.strategyBias?.[strategy] ?? 0
  const confidenceScale = calibration
    ? clamp(0.84 + calibration.sampleSize / Math.max(1, HOOK_CALIBRATION_LOOKBACK_JOBS * 2), 0.84, 1.18)
    : 1
  let styleBias = 0
  if (styleProfile?.style === 'tutorial' && strategy === 'HOOK_FIRST') styleBias -= 2.5
  if ((styleProfile?.style === 'reaction' || styleProfile?.style === 'gaming') && strategy === 'EMOTION_FIRST') styleBias += 2.2
  if (styleProfile?.style === 'vlog' && strategy === 'PACING_FIRST') styleBias += 1.2
  if (styleProfile?.style === 'story' && strategy === 'BASELINE') styleBias += 1

  const hookConfidence = clamp01(0.64 * hook.score + 0.36 * hook.auditScore)
  const projected = clamp(
    base * confidenceScale +
    strategyBias +
    styleBias +
    (hookConfidence - 0.5) * 12,
    0,
    100
  )
  return Number(projected.toFixed(2))
}

const buildRenderLimitPayload = (
  plan: { maxRendersPerMonth?: number | null },
  usage?: { rendersCount?: number | null }
) => {
  const maxRenders = plan.maxRendersPerMonth ?? null
  const rendersUsed = typeof usage?.rendersCount === 'number' ? usage.rendersCount : 0
  const rendersRemaining = maxRenders === null ? null : Math.max(0, maxRenders - rendersUsed)
  return {
    error: 'RENDER_LIMIT_REACHED',
    message: 'Monthly render limit reached. Upgrade to continue.',
    rendersRemaining,
    maxRendersPerMonth: maxRenders,
    rendersUsed
  }
}

type RenderLimitViolation = {
  code: 'RENDER_LIMIT_REACHED'
  payload: Record<string, any>
  requiredPlan: PlanTier
}

type RenderLimitCheckArgs = {
  userId: string
  email?: string | null
  tier: PlanTier
  plan: { maxRendersPerMonth?: number | null }
  renderMode: RenderMode
  allowAtLimitForFree?: boolean
}

const getRenderLimitViolation = async ({
  userId,
  email,
  tier,
  plan,
  renderMode: _renderMode,
  allowAtLimitForFree = false
}: RenderLimitCheckArgs): Promise<RenderLimitViolation | null> => {
  if (isDevAccount(userId, email)) return null
  const requiredPlan = getRequiredPlanForRenders(tier)

  if (tier === 'free') {
    const monthKey = getMonthKey()
    const standardLimit = typeof plan.maxRendersPerMonth === 'number' ? plan.maxRendersPerMonth : FREE_MONTHLY_RENDER_LIMIT
    const standardUsage = await getRenderUsageForMonth(userId, monthKey)
    const used = standardUsage?.rendersCount ?? 0
    const limitHit = allowAtLimitForFree
      ? used > standardLimit
      : used >= standardLimit
    if (limitHit) {
      return {
        code: 'RENDER_LIMIT_REACHED',
        payload: buildRenderLimitPayload({ maxRendersPerMonth: standardLimit }, { rendersCount: used }),
        requiredPlan
      }
    }
    return null
  }

  const monthKey = getMonthKey()
  const renderUsage = await getRenderUsageForMonth(userId, monthKey)
  const maxRenders = plan.maxRendersPerMonth
  if (maxRenders !== null && maxRenders !== undefined && (renderUsage?.rendersCount ?? 0) >= maxRenders) {
    return {
      code: 'RENDER_LIMIT_REACHED',
      payload: buildRenderLimitPayload(plan, renderUsage),
      requiredPlan
    }
  }
  return null
}

const runFfmpegCapture = async (args: string[]) => {
  const result = await runFfmpeg(args)
  return [result.stderr, result.stdout].filter(Boolean).join('\n')
}

const hasAudioStream = (filePath: string) => {
  try {
    if (hasFfprobe()) {
      const result = spawnSync(
        FFPROBE_PATH,
        ['-v', 'error', '-select_streams', 'a:0', '-show_entries', 'stream=codec_type', '-of', 'csv=p=0', filePath],
        { encoding: 'utf8' }
      )
      if (result.status !== 0) return false
      return String(result.stdout || '').trim().length > 0
    }
  } catch (e) {
    // ignore and fall back to ffmpeg
  }
  if (!hasFfmpeg()) return false
  try {
    const result = spawnSync(FFMPEG_PATH, ['-hide_banner', '-i', filePath], { encoding: 'utf8' })
    const output = `${result.stderr || ''}\n${result.stdout || ''}`
    return /Audio:\s/i.test(output)
  } catch (e) {
    return false
  }
}

let cachedFfmpegFilterCatalog: string | null = null
const hasFfmpegFilter = (name: string) => {
  if (!name || !hasFfmpeg()) return false
  try {
    if (cachedFfmpegFilterCatalog === null) {
      const result = spawnSync(FFMPEG_PATH, ['-hide_banner', '-filters'], { encoding: 'utf8' })
      if (result.status !== 0) return false
      cachedFfmpegFilterCatalog = String(result.stdout || '')
    }
    const pattern = new RegExp(`\\b${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i')
    return pattern.test(cachedFfmpegFilterCatalog)
  } catch (e) {
    return false
  }
}

const probeAudioStream = (filePath: string): AudioStreamProfile | null => {
  if (!hasFfprobe()) return null
  try {
    const result = spawnSync(
      FFPROBE_PATH,
      [
        '-v',
        'error',
        '-select_streams',
        'a:0',
        '-show_entries',
        'stream=channels,channel_layout,sample_rate,bit_rate',
        '-of',
        'json',
        filePath
      ],
      { encoding: 'utf8' }
    )
    if (result.status !== 0) return null
    const parsed = JSON.parse(String(result.stdout || '{}'))
    const stream = Array.isArray(parsed?.streams) ? parsed.streams[0] : null
    if (!stream) return null
    const channels = Number(stream.channels)
    const sampleRateRaw = Number(stream.sample_rate)
    const bitRateRaw = Number(stream.bit_rate)
    return {
      channels: Number.isFinite(channels) && channels > 0 ? Math.round(channels) : 2,
      channelLayout: typeof stream.channel_layout === 'string' && stream.channel_layout.trim()
        ? String(stream.channel_layout).trim().toLowerCase()
        : null,
      sampleRate: Number.isFinite(sampleRateRaw) && sampleRateRaw > 0 ? Math.round(sampleRateRaw) : null,
      bitRate: Number.isFinite(bitRateRaw) && bitRateRaw > 0 ? Math.round(bitRateRaw) : null
    }
  } catch (e) {
    return null
  }
}

const probeVideoStream = (filePath: string) => {
  if (!hasFfprobe()) return null
  try {
    const result = spawnSync(
      FFPROBE_PATH,
      ['-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=width,height,sample_aspect_ratio,r_frame_rate', '-of', 'json', filePath],
      { encoding: 'utf8' }
    )
    if (result.status !== 0) return null
    const parsed = JSON.parse(String(result.stdout || '{}'))
    const stream = Array.isArray(parsed?.streams) ? parsed.streams[0] : null
    if (!stream) return null
    return {
      width: stream.width,
      height: stream.height,
      sampleAspectRatio: stream.sample_aspect_ratio,
      frameRate: stream.r_frame_rate
    }
  } catch (e) {
    return null
  }
}

const detectAudioEnergy = async (filePath: string, durationSeconds: number) => {
  if (!hasFfmpeg() || !hasAudioStream(filePath)) return [] as { time: number; rms: number }[]
  const analyzeSeconds = Math.min(HOOK_ANALYZE_MAX, durationSeconds || HOOK_ANALYZE_MAX)
  const args = [
    '-hide_banner',
    '-nostdin',
    '-i', filePath,
    '-t', String(analyzeSeconds),
    '-af', 'astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level',
    '-f', 'null',
    '-'
  ]
  const output = await runFfmpegCapture(args)
  const lines = output.split(/\r?\n/)
  const sampleMap = new Map<number, number>()
  for (const line of lines) {
    if (!line.includes('lavfi.astats.Overall.RMS_level')) continue
    const timeMatch = line.match(/pts_time:([0-9.]+)/)
    const rmsMatch = line.match(/lavfi\.astats\.Overall\.RMS_level=([0-9.\-]+)/)
    if (!timeMatch || !rmsMatch) continue
    const time = Number.parseFloat(timeMatch[1])
    const rms = Number.parseFloat(rmsMatch[1])
    if (!Number.isFinite(time) || !Number.isFinite(rms)) continue
    const bucket = Math.floor(time)
    const prev = sampleMap.get(bucket)
    if (prev === undefined || rms > prev) sampleMap.set(bucket, rms)
  }
  return Array.from(sampleMap.entries())
    .map(([time, rms]) => ({ time, rms }))
    .sort((a, b) => a.time - b.time)
}

const detectSceneChanges = async (filePath: string, durationSeconds: number) => {
  if (!hasFfmpeg()) return [] as number[]
  const analyzeSeconds = Math.min(HOOK_ANALYZE_MAX, durationSeconds || HOOK_ANALYZE_MAX)
  const args = [
    '-hide_banner',
    '-nostdin',
    '-i', filePath,
    '-t', String(analyzeSeconds),
    '-vf', `select='gt(scene,${SCENE_THRESHOLD})',showinfo`,
    '-f', 'null',
    '-'
  ]
  const output = await runFfmpegCapture(args)
  const times = new Set<number>()
  for (const line of output.split(/\r?\n/)) {
    if (!line.includes('pts_time:')) continue
    const match = line.match(/pts_time:([0-9.]+)/)
    if (match) {
      const time = Number.parseFloat(match[1])
      if (Number.isFinite(time)) times.add(time)
    }
  }
  return Array.from(times.values()).sort((a, b) => a - b)
}

const extractFramesEveryHalfSecond = async (filePath: string, outDir: string, durationSeconds: number) => {
  if (!hasFfmpeg()) return [] as string[]
  const analyzeSeconds = Math.min(HOOK_ANALYZE_MAX, durationSeconds || HOOK_ANALYZE_MAX)
  fs.mkdirSync(outDir, { recursive: true })
  const framePattern = path.join(outDir, 'frame-%06d.jpg')
  const frameFilter = `fps=${ANALYSIS_FRAME_FPS},scale=${ANALYSIS_FRAME_SCALE_WIDTH}:-1:flags=lanczos`
  const args = [
    '-hide_banner',
    '-nostdin',
    '-y',
    '-i',
    filePath,
    '-t',
    String(analyzeSeconds),
    '-vf',
    frameFilter,
    '-q:v',
    '7',
    framePattern
  ]
  try {
    await runFfmpeg(args)
  } catch (err) {
    return [] as string[]
  }
  try {
    return fs.readdirSync(outDir)
      .filter((name) => name.toLowerCase().endsWith('.jpg'))
      .map((name) => path.join(outDir, name))
      .sort()
  } catch (e) {
    return [] as string[]
  }
}

const normalizeEnergy = (rmsDb: number) => {
  if (!Number.isFinite(rmsDb)) return 0
  const clamped = Math.min(0, Math.max(-60, rmsDb))
  return (clamped + 60) / 60
}

const clamp01 = (value: number) => Math.max(0, Math.min(1, value))

const getHookCandidateConfidence = (hook: Pick<HookCandidate, 'score' | 'auditScore'>) => {
  return clamp01(0.7 * (hook.score ?? 0) + 0.3 * (hook.auditScore ?? 0))
}

const computeContentSignalStrength = (windows: EngagementWindow[]) => {
  if (!windows.length) return 0.42
  const totals = windows.reduce((acc, window) => ({
    speech: acc.speech + window.speechIntensity,
    motion: acc.motion + window.motionScore,
    emotion: acc.emotion + window.emotionIntensity,
    vocal: acc.vocal + window.vocalExcitement,
    variance: acc.variance + (window.audioVariance ?? 0),
    spikes: acc.spikes + (window.emotionalSpike > 0 || window.emotionIntensity > 0.66 ? 1 : 0)
  }), { speech: 0, motion: 0, emotion: 0, vocal: 0, variance: 0, spikes: 0 })
  const total = Math.max(1, windows.length)
  const base = clamp01(
    0.26 * (totals.emotion / total) +
    0.22 * (totals.vocal / total) +
    0.2 * (totals.speech / total) +
    0.17 * (totals.motion / total) +
    0.15 * (totals.variance / total)
  )
  const spikeDensity = totals.spikes / total
  return Number(clamp01(base * 0.88 + spikeDensity * 0.12).toFixed(4))
}

const countPhraseHits = (text: string, phrases: string[]) => {
  if (!text) return 0
  const normalized = text.toLowerCase()
  return phrases.reduce((sum, phrase) => (
    sum + (normalized.includes(phrase.toLowerCase()) ? 1 : 0)
  ), 0)
}

const inferContentStyleProfile = ({
  windows,
  transcriptCues,
  durationSeconds
}: {
  windows: EngagementWindow[]
  transcriptCues: TranscriptCue[]
  durationSeconds: number
}): ContentStyleProfile => {
  const transcriptText = transcriptCues.map((cue) => cue.text).join(' ').toLowerCase()
  const reactionHits = countPhraseHits(transcriptText, REACTION_STYLE_KEYWORDS)
  const vlogHits = countPhraseHits(transcriptText, VLOG_STYLE_KEYWORDS)
  const tutorialHits = countPhraseHits(transcriptText, TUTORIAL_STYLE_KEYWORDS)
  const gamingHits = countPhraseHits(transcriptText, GAMING_STYLE_KEYWORDS)
  const totalWindows = Math.max(1, windows.length)
  const avgSpeech = windows.reduce((sum, window) => sum + window.speechIntensity, 0) / totalWindows
  const avgScene = windows.reduce((sum, window) => sum + window.sceneChangeRate, 0) / totalWindows
  const avgEmotion = windows.reduce((sum, window) => sum + window.emotionIntensity, 0) / totalWindows
  const spikeRatio = windows.filter((window) => window.emotionalSpike > 0 || window.emotionIntensity > 0.72).length / totalWindows
  const longFormBias = durationSeconds > 120 ? 0.08 : 0

  const scores: Record<ContentStyle, number> = {
    reaction: clamp01(0.34 * clamp01(reactionHits / 3) + 0.24 * avgEmotion + 0.18 * spikeRatio + 0.12 * avgScene + 0.12 * computeContentSignalStrength(windows)),
    vlog: clamp01(0.34 * clamp01(vlogHits / 3) + 0.24 * avgSpeech + 0.18 * (1 - avgScene) + 0.14 * longFormBias + 0.1 * avgEmotion),
    tutorial: clamp01(0.42 * clamp01(tutorialHits / 4) + 0.28 * avgSpeech + 0.18 * (1 - spikeRatio) + 0.12 * (1 - avgScene)),
    gaming: clamp01(0.36 * clamp01(gamingHits / 3) + 0.24 * avgScene + 0.2 * spikeRatio + 0.2 * avgEmotion),
    story: clamp01(0.26 + 0.22 * avgEmotion + 0.2 * avgSpeech + 0.16 * clamp01(1 - Math.abs(avgScene - 0.34)) + 0.16 * clamp01(1 - Math.abs(spikeRatio - 0.18)))
  }
  const ranked = (Object.keys(scores) as ContentStyle[])
    .map((style) => ({ style, score: scores[style] }))
    .sort((a, b) => b.score - a.score)
  const selected = ranked[0]
  const runnerUp = ranked[1]
  const confidence = Number(clamp01(0.58 + (selected.score - (runnerUp?.score ?? 0)) * 0.9).toFixed(4))
  const rationale: string[] = []
  if (selected.style === 'reaction') rationale.push('Emotion spikes and reaction-style language dominate.')
  if (selected.style === 'vlog') rationale.push('Conversational flow and day-style narration indicate vlog pacing.')
  if (selected.style === 'tutorial') rationale.push('Instructional language favors clarity-first pacing.')
  if (selected.style === 'gaming') rationale.push('High scene churn and action terms indicate gaming cadence.')
  if (selected.style === 'story') rationale.push('Balanced narrative signals favor story pacing.')
  if (!rationale.length) rationale.push('Default story profile selected from mixed signals.')

  const biasByStyle: Record<ContentStyle, Pick<ContentStyleProfile, 'tempoBias' | 'interruptBias' | 'hookBias'>> = {
    reaction: { tempoBias: -0.58, interruptBias: 0.28, hookBias: 0.1 },
    vlog: { tempoBias: -0.2, interruptBias: 0.08, hookBias: 0.04 },
    tutorial: { tempoBias: 0.35, interruptBias: -0.12, hookBias: -0.04 },
    gaming: { tempoBias: -0.52, interruptBias: 0.24, hookBias: 0.08 },
    story: { tempoBias: -0.06, interruptBias: 0.04, hookBias: 0.02 }
  }
  return {
    style: selected.style,
    confidence,
    rationale,
    ...biasByStyle[selected.style]
  }
}

const getStyleAdjustedAggressionLevel = (
  baseLevel: RetentionAggressionLevel,
  styleProfile?: ContentStyleProfile | null
): RetentionAggressionLevel => {
  if (!styleProfile) return baseLevel
  const confidence = clamp01(styleProfile.confidence)
  const ranking: RetentionAggressionLevel[] = ['low', 'medium', 'high', 'viral']
  let index = ranking.indexOf(baseLevel)
  if (index < 0) index = 1
  if ((styleProfile.style === 'reaction' || styleProfile.style === 'gaming') && confidence >= 0.6) {
    index = Math.min(ranking.length - 1, index + 1)
  } else if (styleProfile.style === 'tutorial' && confidence >= 0.58) {
    index = Math.max(0, index - 1)
  }
  return ranking[index]
}

const applyStyleToPacingProfile = (
  profile: PacingProfile,
  styleProfile?: ContentStyleProfile | null,
  aggressiveMode = false
) => {
  if (!styleProfile) return profile
  const weight = clamp01(styleProfile.confidence)
  const tempoShift = styleProfile.tempoBias * (0.65 + 0.35 * weight)
  const interruptShift = styleProfile.interruptBias * (0.7 + 0.3 * weight)
  const adjustedMin = clamp(profile.minLen + tempoShift, CUT_MIN, CUT_MAX - 0.4)
  const adjustedMaxLower = Math.min(CUT_MAX - 0.1, Math.max(CUT_MIN + 0.5, adjustedMin + 0.6))
  const adjustedMax = clamp(profile.maxLen + tempoShift * 1.25, adjustedMaxLower, CUT_MAX)
  const adjustedJitter = clamp(profile.jitter + interruptShift * 0.16, 0.12, 0.52)
  const adjustedSpeedCap = clamp(
    profile.speedCap + (aggressiveMode ? 0.03 : 0) - tempoShift * 0.06 + interruptShift * 0.05,
    1.18,
    1.58
  )
  return {
    ...profile,
    minLen: Number(adjustedMin.toFixed(2)),
    maxLen: Number(adjustedMax.toFixed(2)),
    earlyTarget: Number(clamp(profile.earlyTarget + tempoShift, adjustedMin, adjustedMax).toFixed(2)),
    middleTarget: Number(clamp(profile.middleTarget + tempoShift, adjustedMin, adjustedMax).toFixed(2)),
    lateTarget: Number(clamp(profile.lateTarget + tempoShift, adjustedMin, adjustedMax).toFixed(2)),
    jitter: Number(adjustedJitter.toFixed(3)),
    speedCap: Number(adjustedSpeedCap.toFixed(3))
  }
}

const applyBehaviorStyleProfileToPacingProfile = (
  profile: PacingProfile,
  behaviorStyleProfile?: RetentionBehaviorStyleProfile | null
) => {
  if (!behaviorStyleProfile) return profile
  const targetInterval = clamp(behaviorStyleProfile.avgCutInterval, CUT_MIN, CUT_MAX)
  const maxCutIntervalShift = 1.1
  const centerShift = clamp(targetInterval - profile.middleTarget, -maxCutIntervalShift, maxCutIntervalShift)
  const adjustedMin = clamp(profile.minLen + centerShift * 0.55, CUT_MIN, CUT_MAX - 0.3)
  const adjustedMax = clamp(profile.maxLen + centerShift * 0.65, adjustedMin + 0.4, CUT_MAX)
  const interruptBias = clamp(0.9 - behaviorStyleProfile.patternInterruptInterval / 10, -0.2, 0.32)
  const jitter = clamp(profile.jitter + interruptBias * 0.14, 0.12, 0.56)
  const speedCapShift = behaviorStyleProfile.energyEscalationCurve === 'aggressive'
    ? 0.08
    : behaviorStyleProfile.energyEscalationCurve === 'steady'
      ? 0.03
      : -0.02
  const speedCap = clamp(profile.speedCap + speedCapShift, 1.15, 1.62)
  return {
    ...profile,
    minLen: Number(adjustedMin.toFixed(2)),
    maxLen: Number(adjustedMax.toFixed(2)),
    earlyTarget: Number(clamp(profile.earlyTarget + centerShift, adjustedMin, adjustedMax).toFixed(2)),
    middleTarget: Number(clamp(profile.middleTarget + centerShift, adjustedMin, adjustedMax).toFixed(2)),
    lateTarget: Number(clamp(profile.lateTarget + centerShift * 0.85, adjustedMin, adjustedMax).toFixed(2)),
    jitter: Number(jitter.toFixed(3)),
    speedCap: Number(speedCap.toFixed(3))
  }
}

const buildEnergySamplesFromWindows = (windows: EngagementWindow[]) => {
  return windows
    .map((window) => ({
      t: Number((window.time + 0.5).toFixed(3)),
      value: Number(clamp01(
        0.52 * window.score +
        0.2 * window.emotionIntensity +
        0.14 * window.speechIntensity +
        0.08 * window.vocalExcitement +
        0.06 * (window.curiosityTrigger ?? 0)
      ).toFixed(4))
    }))
    .filter((sample) => Number.isFinite(sample.t) && Number.isFinite(sample.value))
}

const detectRhythmAnchors = ({
  windows,
  durationSeconds,
  styleProfile
}: {
  windows: EngagementWindow[]
  durationSeconds: number
  styleProfile?: ContentStyleProfile | null
}) => {
  if (!windows.length) return [] as number[]
  const pulse = windows.map((window) => (
    0.3 * window.audioEnergy +
    0.2 * (window.audioVariance ?? 0) +
    0.17 * window.vocalExcitement +
    0.11 * window.sceneChangeRate +
    0.12 * window.emotionIntensity +
    0.06 * (window.curiosityTrigger ?? 0) +
    0.04 * (window.actionSpike ?? 0)
  ))
  const meanPulse = pulse.reduce((sum, value) => sum + value, 0) / pulse.length
  const variance = pulse.reduce((sum, value) => sum + (value - meanPulse) ** 2, 0) / pulse.length
  const std = Math.sqrt(Math.max(0, variance))
  const threshold = meanPulse + std * 0.14
  const spacing = styleProfile?.style === 'tutorial' ? 0.9 : styleProfile?.style === 'vlog' ? 0.75 : 0.58
  const anchors: number[] = []
  for (let index = 1; index < windows.length - 1; index += 1) {
    const prev = pulse[index - 1]
    const curr = pulse[index]
    const next = pulse[index + 1]
    if (curr < prev || curr < next || curr < threshold) continue
    const candidate = Number(clamp(windows[index].time + 0.5, 0, Math.max(0, durationSeconds)).toFixed(3))
    if (!anchors.length || Math.abs(candidate - anchors[anchors.length - 1]) >= spacing) {
      anchors.push(candidate)
    }
  }
  if (!anchors.length) {
    anchors.push(Number(clamp(durationSeconds * 0.2, 0, durationSeconds).toFixed(3)))
    anchors.push(Number(clamp(durationSeconds * 0.5, 0, durationSeconds).toFixed(3)))
    anchors.push(Number(clamp(durationSeconds * 0.8, 0, durationSeconds).toFixed(3)))
  }
  return anchors
}

const detectEmotionalBeatAnchors = ({
  windows,
  durationSeconds,
  styleProfile,
  nicheProfile,
  aggressionLevel
}: {
  windows: EngagementWindow[]
  durationSeconds: number
  styleProfile?: ContentStyleProfile | null
  nicheProfile?: VideoNicheProfile | null
  aggressionLevel?: RetentionAggressionLevel
}) => {
  if (!windows.length) return [] as number[]
  const tuning = resolveEmotionalTuningProfile({
    styleProfile,
    nicheProfile,
    aggressionLevel
  })
  const beatStrength = (window: EngagementWindow) => clamp01(
    0.34 * window.emotionIntensity +
    0.2 * window.vocalExcitement +
    0.18 * (window.hookScore ?? window.score) +
    0.12 * (window.curiosityTrigger ?? 0) +
    0.1 * (window.actionSpike ?? 0) +
    0.06 * window.sceneChangeRate
  )
  const spacingByStyle = styleProfile?.style === 'tutorial'
    ? 1.22
    : styleProfile?.style === 'vlog'
      ? 1.04
      : 0.88
  const minSpacing = Math.max(
    spacingByStyle * tuning.spacingMultiplier,
    EDITOR_RETENTION_CONFIG.emotionalBeatSpacingSec * tuning.spacingMultiplier
  )
  const baseThreshold = clamp(
    EDITOR_RETENTION_CONFIG.emotionalBeatThreshold + tuning.thresholdOffset,
    0.45,
    0.94
  )
  const candidates: Array<{ time: number; score: number }> = []
  for (let index = 1; index < windows.length - 1; index += 1) {
    const prev = beatStrength(windows[index - 1])
    const curr = beatStrength(windows[index])
    const next = beatStrength(windows[index + 1])
    if (curr < baseThreshold || curr < prev || curr < next) continue
    candidates.push({
      time: Number(clamp(windows[index].time + 0.45, 0, Math.max(0, durationSeconds)).toFixed(3)),
      score: curr
    })
  }
  if (!candidates.length) return []
  const selected: number[] = []
  for (const candidate of candidates.sort((a, b) => b.score - a.score || a.time - b.time)) {
    const tooClose = selected.some((time) => Math.abs(time - candidate.time) < minSpacing)
    if (tooClose) continue
    selected.push(candidate.time)
    if (selected.length >= 20) break
  }
  return selected.slice().sort((a, b) => a - b)
}

const mergeBeatAnchorSets = ({
  rhythmAnchors,
  emotionalAnchors,
  durationSeconds
}: {
  rhythmAnchors: number[]
  emotionalAnchors: number[]
  durationSeconds: number
}) => {
  const raw = [...rhythmAnchors, ...emotionalAnchors]
    .filter((value) => Number.isFinite(value))
    .map((value) => Number(clamp(value, 0, Math.max(0, durationSeconds)).toFixed(3)))
    .sort((a, b) => a - b)
  if (!raw.length) return [] as number[]
  const minSpacing = clamp(EDITOR_RETENTION_CONFIG.emotionalBeatSpacingSec * 0.45, 0.24, 0.8)
  const merged: number[] = []
  for (const anchor of raw) {
    const prev = merged[merged.length - 1]
    if (!Number.isFinite(prev)) {
      merged.push(anchor)
      continue
    }
    if (Math.abs(anchor - prev) < minSpacing) {
      merged[merged.length - 1] = Number(((prev + anchor) / 2).toFixed(3))
      continue
    }
    merged.push(anchor)
  }
  return merged
}

const applyEmotionalBeatCuts = ({
  segments,
  windows,
  aggressionLevel,
  hookRange,
  styleProfile,
  nicheProfile
}: {
  segments: Segment[]
  windows: EngagementWindow[]
  aggressionLevel: RetentionAggressionLevel
  hookRange: TimeRange | null
  styleProfile?: ContentStyleProfile | null
  nicheProfile?: VideoNicheProfile | null
}) => {
  if (!segments.length || !windows.length) {
    return {
      segments: segments.map((segment) => ({ ...segment })),
      anchors: [] as number[],
      cutCount: 0,
      trimmedSeconds: 0
    }
  }
  const tuning = resolveEmotionalTuningProfile({
    styleProfile,
    nicheProfile,
    aggressionLevel
  })
  const beatStrength = (window: EngagementWindow) => clamp01(
    0.34 * window.emotionIntensity +
    0.2 * window.vocalExcitement +
    0.18 * (window.hookScore ?? window.score) +
    0.12 * (window.curiosityTrigger ?? 0) +
    0.1 * (window.actionSpike ?? 0) +
    0.06 * window.sceneChangeRate
  )
  const thresholdOffsetByAggression: Record<RetentionAggressionLevel, number> = {
    low: 0.05,
    medium: 0.02,
    high: -0.01,
    viral: -0.04
  }
  const beatThreshold = clamp(
    EDITOR_RETENTION_CONFIG.emotionalBeatThreshold + thresholdOffsetByAggression[aggressionLevel] + tuning.thresholdOffset,
    0.44,
    0.95
  )
  const targetLeadByStyle =
    styleProfile?.style === 'tutorial'
      ? 0.92
      : styleProfile?.style === 'vlog'
        ? 0.72
      : aggressionLevel === 'high' || aggressionLevel === 'viral'
          ? 0.46
          : 0.58
  const targetLeadSeconds = clamp(targetLeadByStyle / Math.max(0.68, tuning.leadTrimMultiplier), 0.28, 1.25)
  const maxLeadTrim = clamp(
    (
      EDITOR_RETENTION_CONFIG.emotionalLeadTrimSec * tuning.leadTrimMultiplier +
      (aggressionLevel === 'viral' ? 0.35 : aggressionLevel === 'high' ? 0.2 : 0)
    ),
    0.5,
    4
  )
  const output: Segment[] = []
  const anchors: number[] = []
  let cutCount = 0
  let trimmedSeconds = 0

  for (const sourceSegment of segments) {
    if (sourceSegment.end - sourceSegment.start <= 0.3) continue
    if (hookRange && overlapsRange(sourceSegment, hookRange)) {
      output.push({ ...sourceSegment })
      continue
    }
    const relevant = windows.filter((window) => window.time >= sourceSegment.start && window.time < sourceSegment.end)
    if (relevant.length < 3) {
      output.push({ ...sourceSegment })
      continue
    }

    const scored = relevant.map((window, index) => ({
      window,
      index,
      score: beatStrength(window)
    }))
    const localPeaks = scored.filter((entry) => {
      const prev = scored[Math.max(0, entry.index - 1)]?.score ?? entry.score
      const next = scored[Math.min(scored.length - 1, entry.index + 1)]?.score ?? entry.score
      return entry.score >= beatThreshold && entry.score >= prev && entry.score >= next
    })
    const strongestPeak = (localPeaks.length ? localPeaks : scored)
      .slice()
      .sort((a, b) => b.score - a.score || a.window.time - b.window.time)[0]

    let adjusted = { ...sourceSegment }
    if (strongestPeak) {
      const leadIn = strongestPeak.window.time - adjusted.start
      if (leadIn > targetLeadSeconds + 0.3 && adjusted.end - adjusted.start > 2.1) {
        const preScore = averageWindowMetric(
          windows,
          adjusted.start,
          Math.min(adjusted.end, strongestPeak.window.time),
          (window) => window.score
        )
        const beatScore = averageWindowMetric(
          windows,
          strongestPeak.window.time,
          Math.min(adjusted.end, strongestPeak.window.time + 1.2),
          (window) => (
            0.5 * (window.hookScore ?? window.score) +
            0.28 * window.emotionIntensity +
            0.22 * window.vocalExcitement
          )
        )
        const trimGateMultiplier = clamp(0.86 + tuning.contextPenaltyMultiplier * 0.08, 0.8, 1)
        if (preScore < beatScore * trimGateMultiplier && preScore < 0.6) {
          const trimAmount = clamp(
            leadIn - targetLeadSeconds,
            0.22,
            Math.min(maxLeadTrim, Math.max(0.22, adjusted.end - adjusted.start - 1.05))
          )
          adjusted.start = Number((adjusted.start + trimAmount).toFixed(3))
          trimmedSeconds += trimAmount
          cutCount += 1
        }
      }
    }

    const adjustedDuration = adjusted.end - adjusted.start
    const fallbackSplitPool = (
      !localPeaks.length && strongestPeak && strongestPeak.score >= Math.max(0.48, beatThreshold * 0.82)
    )
      ? [strongestPeak]
      : []
    const splitCandidate = [...localPeaks, ...fallbackSplitPool]
      .filter((entry) => (
        entry.window.time > adjusted.start + 0.82 &&
        entry.window.time < adjusted.end - 0.82
      ))
      .sort((a, b) => b.score - a.score || a.window.time - b.window.time)[0]

    const splitDurationThreshold = CUT_MAX + 0.55 * tuning.splitLenBias
    if (adjustedDuration > splitDurationThreshold && splitCandidate) {
      const splitAt = Number(clamp(splitCandidate.window.time + 0.12, adjusted.start + 0.75, adjusted.end - 0.75).toFixed(3))
      const first = { ...adjusted, end: splitAt }
      const second = { ...adjusted, start: splitAt }
      if (first.end - first.start > 0.24 && second.end - second.start > 0.24) {
        output.push(first)
        output.push(second)
        anchors.push(splitAt)
        cutCount += 1
        continue
      }
    }

    if (adjusted.end - adjusted.start > 0.2) output.push(adjusted)
  }

  const normalized: Segment[] = []
  for (const segment of output.slice().sort((a, b) => a.start - b.start)) {
    const prev = normalized[normalized.length - 1]
    let start = segment.start
    const end = segment.end
    if (prev && start < prev.end - 0.02) {
      start = Number(prev.end.toFixed(3))
    }
    if (end - start <= 0.18) continue
    normalized.push({
      ...segment,
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3))
    })
  }

  const dedupedAnchors = anchors
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b)
    .filter((value, index, list) => index === 0 || Math.abs(value - list[index - 1]) >= 0.2)

  return {
    segments: normalized.length ? normalized : segments.map((segment) => ({ ...segment })),
    anchors: dedupedAnchors,
    cutCount,
    trimmedSeconds: Number(trimmedSeconds.toFixed(3))
  }
}

const alignSegmentsToRhythm = ({
  segments,
  durationSeconds,
  anchors,
  styleProfile
}: {
  segments: Segment[]
  durationSeconds: number
  anchors: number[]
  styleProfile?: ContentStyleProfile | null
}) => {
  if (segments.length <= 1 || !anchors.length) return segments.map((segment) => ({ ...segment }))
  const tolerance = styleProfile?.style === 'tutorial' ? 0.14 : styleProfile?.style === 'vlog' ? 0.18 : 0.24
  const minSegmentDuration = styleProfile?.style === 'tutorial' ? 0.3 : 0.24
  const sortedAnchors = anchors.slice().sort((a, b) => a - b)
  const boundaries = segments.map((segment) => segment.start)
  boundaries.push(segments[segments.length - 1].end)

  for (let index = 1; index < boundaries.length - 1; index += 1) {
    const boundary = boundaries[index]
    let nearest = boundary
    let nearestDistance = Number.POSITIVE_INFINITY
    for (const anchor of sortedAnchors) {
      const distance = Math.abs(anchor - boundary)
      if (distance < nearestDistance) {
        nearest = anchor
        nearestDistance = distance
      }
    }
    if (nearestDistance <= tolerance) {
      boundaries[index] = nearest
    }
  }
  boundaries[0] = clamp(boundaries[0], 0, Math.max(0, durationSeconds))
  boundaries[boundaries.length - 1] = clamp(boundaries[boundaries.length - 1], 0, Math.max(0, durationSeconds))
  for (let index = 1; index < boundaries.length; index += 1) {
    if (boundaries[index] < boundaries[index - 1] + minSegmentDuration) {
      boundaries[index] = boundaries[index - 1] + minSegmentDuration
    }
  }
  for (let index = boundaries.length - 2; index >= 0; index -= 1) {
    if (boundaries[index] > boundaries[index + 1] - minSegmentDuration) {
      boundaries[index] = boundaries[index + 1] - minSegmentDuration
    }
  }
  const rebuilt: Segment[] = []
  for (let index = 0; index < segments.length; index += 1) {
    const start = Number(clamp(boundaries[index], 0, durationSeconds).toFixed(3))
    const end = Number(clamp(boundaries[index + 1], start + 0.05, durationSeconds).toFixed(3))
    if (end - start < minSegmentDuration * 0.6) {
      if (rebuilt.length) {
        rebuilt[rebuilt.length - 1].end = end
      }
      continue
    }
    rebuilt.push({
      ...segments[index],
      start,
      end
    })
  }
  return rebuilt.length ? rebuilt : segments.map((segment) => ({ ...segment }))
}

const resolveHookScoreThreshold = ({
  aggressionLevel,
  hasTranscript,
  signalStrength
}: {
  aggressionLevel: RetentionAggressionLevel
  hasTranscript: boolean
  signalStrength: number
}) => {
  let threshold = LEVEL_HOOK_THRESHOLD_BASE[aggressionLevel]
  if (!hasTranscript) threshold -= 0.11
  if (signalStrength < 0.42) threshold -= 0.08
  else if (signalStrength < 0.52) threshold -= 0.05
  else if (signalStrength > 0.76) threshold += 0.02
  const floor = LEVEL_HOOK_THRESHOLD_FLOOR[aggressionLevel]
  return Number(clamp(threshold, floor, 0.9).toFixed(3))
}

const normalizeQualityGateThresholds = (thresholds?: Partial<QualityGateThresholds> | null): QualityGateThresholds => {
  return {
    hook_strength: Math.round(clamp(Number(thresholds?.hook_strength ?? QUALITY_GATE_THRESHOLDS.hook_strength), 45, 98)),
    emotional_pull: Math.round(clamp(Number(thresholds?.emotional_pull ?? QUALITY_GATE_THRESHOLDS.emotional_pull), 40, 96)),
    pacing_score: Math.round(clamp(Number(thresholds?.pacing_score ?? QUALITY_GATE_THRESHOLDS.pacing_score), 45, 96)),
    retention_score: Math.round(clamp(Number(thresholds?.retention_score ?? QUALITY_GATE_THRESHOLDS.retention_score), 45, 98))
  }
}

const resolveQualityGateThresholds = ({
  aggressionLevel,
  hasTranscript,
  signalStrength,
  contentFormat = 'youtube_long',
  targetPlatform = 'auto',
  feedbackOffset = 0
}: {
  aggressionLevel: RetentionAggressionLevel
  hasTranscript: boolean
  signalStrength: number
  contentFormat?: RetentionContentFormat
  targetPlatform?: RetentionTargetPlatform
  feedbackOffset?: number
}): QualityGateThresholds => {
  const levelOffset = LEVEL_QUALITY_THRESHOLD_OFFSET[aggressionLevel]
  const transcriptOffset = hasTranscript ? 0 : -8
  const lowSignalPenalty = signalStrength < 0.42 ? -7 : signalStrength < 0.52 ? -4 : 0
  const highSignalBoost = signalStrength > 0.74 ? 2 : 0
  const formatOffset = FORMAT_QUALITY_GATE_OFFSET[contentFormat] || FORMAT_QUALITY_GATE_OFFSET.youtube_long
  const platformOffset = PLATFORM_QUALITY_GATE_OFFSET[parseRetentionTargetPlatform(targetPlatform)] || PLATFORM_QUALITY_GATE_OFFSET.auto
  const telemetryOffset = Math.round(clamp(Number(feedbackOffset || 0), -4, 4))
  const baseOffset = levelOffset + transcriptOffset + lowSignalPenalty + highSignalBoost
  return normalizeQualityGateThresholds({
    hook_strength: clamp(
      QUALITY_GATE_THRESHOLDS.hook_strength +
      baseOffset +
      Number(formatOffset.hook_strength || 0) +
      Number(platformOffset.hook_strength || 0) +
      telemetryOffset,
      QUALITY_GATE_THRESHOLD_FLOORS.hook_strength,
      96
    ),
    emotional_pull: clamp(
      QUALITY_GATE_THRESHOLDS.emotional_pull +
      baseOffset +
      Number(formatOffset.emotional_pull || 0) +
      Number(platformOffset.emotional_pull || 0) +
      Math.round(telemetryOffset * 0.8),
      QUALITY_GATE_THRESHOLD_FLOORS.emotional_pull,
      94
    ),
    pacing_score: clamp(
      QUALITY_GATE_THRESHOLDS.pacing_score +
      Math.round(baseOffset * 0.6) +
      Number(formatOffset.pacing_score || 0) +
      Number(platformOffset.pacing_score || 0) +
      Math.round(telemetryOffset * 0.6),
      QUALITY_GATE_THRESHOLD_FLOORS.pacing_score,
      94
    ),
    retention_score: clamp(
      QUALITY_GATE_THRESHOLDS.retention_score +
      Math.round(baseOffset * 0.85) +
      Number(formatOffset.retention_score || 0) +
      Number(platformOffset.retention_score || 0) +
      telemetryOffset,
      QUALITY_GATE_THRESHOLD_FLOORS.retention_score,
      95
    )
  })
}

const maybeAllowQualityGateOverride = ({
  judge,
  thresholds,
  hasTranscript,
  signalStrength
}: {
  judge: RetentionJudgeReport
  thresholds: QualityGateThresholds
  hasTranscript: boolean
  signalStrength: number
}) => {
  if (hasTranscript && signalStrength >= 0.5) return null
  const hookBuffer = hasTranscript ? 8 : 14
  const emotionBuffer = hasTranscript ? 10 : 16
  const pacingBuffer = 8
  const retentionBuffer = 10
  const hookOk = judge.hook_strength >= Math.max(QUALITY_GATE_THRESHOLD_FLOORS.hook_strength, thresholds.hook_strength - hookBuffer)
  const emotionOk = judge.emotional_pull >= Math.max(QUALITY_GATE_THRESHOLD_FLOORS.emotional_pull, thresholds.emotional_pull - emotionBuffer)
  const pacingOk = judge.pacing_score >= Math.max(QUALITY_GATE_THRESHOLD_FLOORS.pacing_score, thresholds.pacing_score - pacingBuffer)
  const retentionOk = judge.retention_score >= Math.max(QUALITY_GATE_THRESHOLD_FLOORS.retention_score, thresholds.retention_score - retentionBuffer)
  if (hookOk && emotionOk && pacingOk && retentionOk && judge.retention_score >= RETENTION_RENDER_THRESHOLD) {
    if (!hasTranscript) {
      return 'Quality gate override: transcript unavailable, accepted strongest non-verbal retention cut.'
    }
    if (signalStrength < 0.45) {
      return 'Quality gate override: low-signal vlog/reaction profile passed adaptive floor after retries.'
    }
  }
  return null
}

const shouldForceRescueRender = (judge: RetentionJudgeReport) => {
  return (
    judge.retention_score >= RESCUE_RENDER_MINIMUMS.retention_score &&
    judge.hook_strength >= RESCUE_RENDER_MINIMUMS.hook_strength &&
    judge.pacing_score >= RESCUE_RENDER_MINIMUMS.pacing_score
  )
}

const selectRenderableHookCandidate = ({
  candidates,
  aggressionLevel,
  hasTranscript,
  signalStrength
}: {
  candidates: HookCandidate[]
  aggressionLevel: RetentionAggressionLevel
  hasTranscript: boolean
  signalStrength: number
}): HookSelectionDecision | null => {
  const deduped = candidates.filter((candidate, index) => (
    candidate &&
    Number.isFinite(candidate.start) &&
    Number.isFinite(candidate.duration) &&
    candidate.duration >= HOOK_MIN &&
    candidate.duration <= HOOK_MAX &&
    candidates.findIndex((entry) => (
      Math.abs((entry?.start ?? -9999) - candidate.start) < 0.01 &&
      Math.abs((entry?.duration ?? -9999) - candidate.duration) < 0.01
    )) === index
  ))
  if (!deduped.length) return null
  const ranked = deduped
    .map((candidate) => ({ candidate, confidence: getHookCandidateConfidence(candidate) }))
    .sort((a, b) => b.confidence - a.confidence || b.candidate.score - a.candidate.score)
  const threshold = resolveHookScoreThreshold({ aggressionLevel, hasTranscript, signalStrength })
  const strict = ranked.find((entry) => entry.candidate.auditPassed && entry.confidence >= threshold)
  if (strict) {
    return {
      candidate: strict.candidate,
      confidence: strict.confidence,
      threshold,
      usedFallback: false,
      reason: null
    }
  }
  const relaxedThreshold = clamp(
    threshold - (hasTranscript ? 0.08 : 0.16) - (signalStrength < 0.5 ? 0.04 : 0),
    0.4,
    threshold
  )
  const relaxed = ranked.find((entry) => (
    entry.confidence >= relaxedThreshold &&
    (entry.candidate.auditPassed || !hasTranscript || signalStrength < 0.48)
  ))
  if (relaxed) {
    return {
      candidate: relaxed.candidate,
      confidence: relaxed.confidence,
      threshold,
      usedFallback: true,
      reason: hasTranscript
        ? 'Hook fallback selected after strict audit miss, using strongest near-threshold candidate.'
        : 'Hook fallback selected from strongest non-verbal peak due missing transcript context.'
    }
  }
  return null
}

const averageWindowMetric = (
  windows: EngagementWindow[],
  start: number,
  end: number,
  metric: (window: EngagementWindow) => number
) => {
  const relevant = windows.filter((window) => window.time >= start && window.time < end)
  if (!relevant.length) return 0
  return relevant.reduce((sum, window) => sum + metric(window), 0) / relevant.length
}

const tightenHookRangeForRetention = ({
  range,
  windows,
  silences,
  durationSeconds
}: {
  range: TimeRange
  windows: EngagementWindow[]
  silences: TimeRange[]
  durationSeconds: number
}) => {
  if (!Number.isFinite(range.start) || !Number.isFinite(range.end) || range.end - range.start <= 0.2) {
    return {
      start: Number(clamp(range.start || 0, 0, Math.max(0, durationSeconds - HOOK_MIN)).toFixed(3)),
      end: Number(clamp((range.start || 0) + HOOK_MIN, HOOK_MIN, durationSeconds).toFixed(3))
    }
  }
  let start = clamp(range.start, 0, Math.max(0, durationSeconds - 0.2))
  let end = clamp(range.end, start + 0.2, durationSeconds)
  const minHookSeconds = clamp(HOOK_MIN - 1.4, 3.2, HOOK_MIN)
  const probeSeconds = 0.62
  const stepSeconds = 0.22
  const maxHeadTrim = 1.7
  const maxTailTrim = 1.3
  let headTrimmed = 0
  let tailTrimmed = 0

  while (headTrimmed + stepSeconds <= maxHeadTrim && end - (start + stepSeconds) >= minHookSeconds) {
    const probeRange: TimeRange = { start, end: Math.min(end, start + probeSeconds) }
    const hookSignal = averageWindowMetric(windows, probeRange.start, probeRange.end, (window) => window.hookScore ?? window.score)
    const speech = averageWindowMetric(windows, probeRange.start, probeRange.end, (window) => window.speechIntensity)
    const energy = averageWindowMetric(windows, probeRange.start, probeRange.end, (window) => window.audioEnergy)
    const silenceRatio = getSilenceCoverageRatio(probeRange, silences)
    const weakLead = silenceRatio >= 0.28 || (hookSignal < 0.44 && speech < 0.5 && energy < 0.46)
    if (!weakLead) break
    start += stepSeconds
    headTrimmed += stepSeconds
  }

  while (tailTrimmed + stepSeconds <= maxTailTrim && (end - stepSeconds) - start >= minHookSeconds) {
    const probeRange: TimeRange = { start: Math.max(start, end - probeSeconds), end }
    const hookSignal = averageWindowMetric(windows, probeRange.start, probeRange.end, (window) => window.hookScore ?? window.score)
    const speech = averageWindowMetric(windows, probeRange.start, probeRange.end, (window) => window.speechIntensity)
    const energy = averageWindowMetric(windows, probeRange.start, probeRange.end, (window) => window.audioEnergy)
    const silenceRatio = getSilenceCoverageRatio(probeRange, silences)
    const weakTail = silenceRatio >= 0.32 || (hookSignal < 0.42 && speech < 0.46 && energy < 0.43)
    if (!weakTail) break
    end -= stepSeconds
    tailTrimmed += stepSeconds
  }

  if (end - start < minHookSeconds) {
    const needed = minHookSeconds - (end - start)
    const extendTail = Math.min(needed, Math.max(0, durationSeconds - end))
    end += extendTail
    const remaining = needed - extendTail
    if (remaining > 0) start = Math.max(0, start - remaining)
  }

  end = Math.min(durationSeconds, start + Math.min(HOOK_MAX, Math.max(minHookSeconds, end - start)))
  start = Math.max(0, Math.min(start, end - minHookSeconds))
  return {
    start: Number(start.toFixed(3)),
    end: Number(end.toFixed(3))
  }
}

const buildFallbackHookCandidateFromStorySegments = ({
  segments,
  windows,
  silences,
  durationSeconds
}: {
  segments: Segment[]
  windows: EngagementWindow[]
  silences: TimeRange[]
  durationSeconds: number
}) => {
  const scored = segments
    .map((segment) => {
      const start = clamp(segment.start, 0, Math.max(0, durationSeconds - 0.5))
      const maxEnd = Math.min(durationSeconds, segment.end)
      const end = Math.min(maxEnd, start + HOOK_MAX)
      const duration = end - start
      if (duration < Math.max(3.2, HOOK_MIN - 1.4)) return null
      const hookSignal = averageWindowMetric(windows, start, end, (window) => window.hookScore ?? window.score)
      const speech = averageWindowMetric(windows, start, end, (window) => window.speechIntensity)
      const emotion = averageWindowMetric(windows, start, end, (window) => window.emotionIntensity)
      const vocal = averageWindowMetric(windows, start, end, (window) => window.vocalExcitement)
      const scene = averageWindowMetric(windows, start, end, (window) => window.sceneChangeRate)
      const silenceRatio = getSilenceCoverageRatio({ start, end }, silences)
      const earlyBias = 1 - clamp01(start / Math.max(45, durationSeconds * 0.4))
      const score = clamp01(
        0.42 * hookSignal +
        0.2 * speech +
        0.14 * emotion +
        0.08 * vocal +
        0.08 * scene +
        0.08 * earlyBias -
        0.28 * silenceRatio
      )
      return {
        start,
        end,
        score
      }
    })
    .filter((entry): entry is { start: number; end: number; score: number } => Boolean(entry))
    .sort((a, b) => b.score - a.score || a.start - b.start)

  const best = scored[0]
  if (!best) return null
  const tightened = tightenHookRangeForRetention({
    range: { start: best.start, end: best.end },
    windows,
    silences,
    durationSeconds
  })
  const duration = Math.max(0.2, tightened.end - tightened.start)
  const confidence = clamp01(Math.max(best.score, 0.44))
  return {
    start: Number(tightened.start.toFixed(3)),
    duration: Number(duration.toFixed(3)),
    score: Number(confidence.toFixed(4)),
    auditScore: Number(clamp01(confidence - 0.02).toFixed(4)),
    auditPassed: false,
    text: '',
    reason: 'Fallback hook selected from the strongest low-silence segment after weak candidate pool.',
    synthetic: true
  } as HookCandidate
}

const pickRetentionSplitPoint = (
  start: number,
  end: number,
  minLen: number,
  maxLen: number,
  windows: EngagementWindow[]
) => {
  const searchStart = start + minLen
  const searchEnd = Math.min(start + maxLen, end - minLen)
  if (searchEnd <= searchStart) return Math.min(end, start + maxLen)
  const ideal = Math.min(searchEnd, Math.max(searchStart, start + (minLen + maxLen) / 2))
  let best = ideal
  let bestScore = Number.POSITIVE_INFINITY
  for (let split = searchStart; split <= searchEnd; split += 0.5) {
    const before = averageWindowMetric(
      windows,
      Math.max(start, split - 2),
      split,
      (window) => 0.72 * window.score + 0.18 * window.speechIntensity + 0.1 * window.vocalExcitement
    )
    const valley = averageWindowMetric(
      windows,
      Math.max(start, split - 1),
      Math.min(end, split + 1),
      (window) => window.score
    )
    const after = averageWindowMetric(
      windows,
      split,
      Math.min(end, split + 2),
      (window) => 0.7 * window.score + 0.2 * window.vocalExcitement + 0.1 * window.speechIntensity
    )
    const momentumBoost = Math.max(0, after - before) * 0.35
    const distancePenalty = Math.abs(split - ideal) * 0.02
    const score = valley + distancePenalty - momentumBoost
    if (score < bestScore) {
      bestScore = score
      best = split
    }
  }
  return Number(best.toFixed(2))
}

const enforceSegmentLengths = (
  segments: Segment[],
  minLen: number,
  maxLen: number,
  windows: EngagementWindow[]
) => {
  const normalized: Segment[] = []
  for (const seg of segments) {
    let cursor = seg.start
    const end = seg.end
    while (end - cursor > maxLen + 0.05) {
      const split = pickRetentionSplitPoint(cursor, end, minLen, maxLen, windows)
      const safeSplit = Number.isFinite(split) ? split : cursor + maxLen
      if (safeSplit <= cursor + 0.25 || safeSplit >= end - 0.25) break
      normalized.push({ ...seg, start: cursor, end: safeSplit })
      cursor = safeSplit
    }
    if (end - cursor > 0.2) {
      normalized.push({ ...seg, start: cursor, end })
    }
  }

  const merged: Segment[] = []
  for (const seg of normalized) {
    const len = seg.end - seg.start
    if (len >= minLen || merged.length === 0) {
      merged.push({ ...seg })
      continue
    }
    const prev = merged[merged.length - 1]
    const prevLen = prev.end - prev.start
    if (prevLen + len <= maxLen + 0.25) {
      prev.end = seg.end
      continue
    }
    const needed = Math.min(minLen - len, Math.max(0, prevLen - minLen))
    if (needed > 0.1) {
      prev.end -= needed
      merged.push({ ...seg, start: seg.start - needed, end: seg.end })
    } else {
      merged.push({ ...seg })
    }
  }

  return merged.filter((seg) => seg.end - seg.start > 0.25)
}

const refineSegmentsForRetention = (
  segments: Segment[],
  windows: EngagementWindow[],
  minLen: number,
  maxLen: number
) => {
  const refined = segments
    .map((segment) => {
      let start = segment.start
      let end = segment.end
      for (let pass = 0; pass < 5; pass += 1) {
        const duration = end - start
        if (duration <= minLen + 0.2) break
        const segmentScore = averageWindowMetric(
          windows,
          start,
          end,
          (window) => 0.7 * window.score + 0.2 * window.speechIntensity + 0.1 * window.vocalExcitement
        )
        // If a segment already has strong retention signals, keep it mostly intact.
        if (segmentScore >= 0.64) break
        const headScore = averageWindowMetric(
          windows,
          start,
          Math.min(end, start + 1.5),
          (window) => 0.7 * window.score + 0.3 * window.speechIntensity
        )
        const tailScore = averageWindowMetric(
          windows,
          Math.max(start, end - 1.5),
          end,
          (window) => 0.7 * window.score + 0.3 * window.speechIntensity
        )
        let trimmed = false
        if (headScore < segmentScore * 0.76 && end - (start + 1) >= minLen) {
          start += 1
          trimmed = true
        }
        if (tailScore < segmentScore * 0.76 && (end - 1) - start >= minLen) {
          end -= 1
          trimmed = true
        }
        if (!trimmed) break
      }
      return { ...segment, start, end }
    })
    .filter((segment) => segment.end - segment.start > 0.25)

  return enforceSegmentLengths(refined, minLen, maxLen, windows)
}

const buildEngagementWindows = (
  durationSeconds: number,
  energySamples: { time: number; rms: number }[],
  sceneChanges: number[],
  faceSamples: FaceSample[] = [],
  textSamples: { time: number; density: number; confidence?: number }[] = [],
  emotionSamples: { time: number; intensity: number }[] = []
): EngagementWindow[] => {
  const totalSeconds = Math.max(0, Math.floor(durationSeconds))
  const energyBySecond = new Array(totalSeconds).fill(0)
  for (const sample of energySamples) {
    if (sample.time < 0 || sample.time >= totalSeconds) continue
    const idx = Math.floor(sample.time)
    energyBySecond[idx] = Math.max(energyBySecond[idx], normalizeEnergy(sample.rms))
  }
  const meanEnergy = energyBySecond.length
    ? energyBySecond.reduce((sum, v) => sum + v, 0) / energyBySecond.length
    : 0
  const variance = energyBySecond.length
    ? energyBySecond.reduce((sum, v) => sum + (v - meanEnergy) ** 2, 0) / energyBySecond.length
    : 0
  const std = Math.sqrt(variance)

  const sceneChangesBySecond = new Array(totalSeconds).fill(0)
  for (const change of sceneChanges) {
    const idx = Math.floor(change)
    if (idx >= 0 && idx < totalSeconds) sceneChangesBySecond[idx] += 1
  }

  const faceBySecond = new Array(totalSeconds).fill(0)
  const faceIntensityBySecond = new Array(totalSeconds).fill(0)
  const faceCenterXSumBySecond = new Array(totalSeconds).fill(0)
  const faceCenterYSumBySecond = new Array(totalSeconds).fill(0)
  const faceCenterWeightBySecond = new Array(totalSeconds).fill(0)
  for (const sample of faceSamples) {
    if (sample.time < 0 || sample.time >= totalSeconds) continue
    const idx = Math.floor(sample.time)
    const value = Number.isFinite(sample.presence) ? Math.max(0, Math.min(1, sample.presence)) : 0
    faceBySecond[idx] = Math.max(faceBySecond[idx], value)
    const intensity = Number.isFinite(sample.intensity) ? clamp01(sample.intensity ?? 0) : value
    faceIntensityBySecond[idx] = Math.max(faceIntensityBySecond[idx], intensity)
    if (Number.isFinite(sample.centerX) && Number.isFinite(sample.centerY)) {
      const weight = Math.max(0.15, intensity || value || 0.15)
      faceCenterWeightBySecond[idx] += weight
      faceCenterXSumBySecond[idx] += clamp01(Number(sample.centerX)) * weight
      faceCenterYSumBySecond[idx] += clamp01(Number(sample.centerY)) * weight
    }
  }

  const textBySecond = new Array(totalSeconds).fill(0)
  const textConfidenceBySecond = new Array(totalSeconds).fill(0)
  for (const sample of textSamples) {
    if (sample.time < 0 || sample.time >= totalSeconds) continue
    const idx = Math.floor(sample.time)
    const value = Number.isFinite(sample.density) ? Math.max(0, Math.min(1, sample.density)) : 0
    textBySecond[idx] = Math.max(textBySecond[idx], value)
    const confidence = Number.isFinite(sample.confidence) ? clamp01(sample.confidence ?? 0) : (value > 0 ? 0.6 : 0)
    textConfidenceBySecond[idx] = Math.max(textConfidenceBySecond[idx], confidence)
  }

  const emotionBySecond = new Array(totalSeconds).fill(0)
  for (const sample of emotionSamples) {
    if (sample.time < 0 || sample.time >= totalSeconds) continue
    const idx = Math.floor(sample.time)
    const value = Number.isFinite(sample.intensity) ? clamp01(sample.intensity) : 0
    emotionBySecond[idx] = Math.max(emotionBySecond[idx], value)
  }

  const windows: EngagementWindow[] = []
  for (let i = 0; i < totalSeconds; i += 1) {
    const audioEnergy = energyBySecond[i]
    const speechIntensity = Math.min(1, Math.abs(audioEnergy - meanEnergy) / (std || 0.15))
    const sceneChangeRate = Math.min(1, sceneChangesBySecond[i])
    const prevScene = i > 0 ? Math.min(1, sceneChangesBySecond[i - 1]) : sceneChangeRate
    const motionDelta = Math.abs(sceneChangeRate - prevScene)
    const audioDelta = i > 0 ? Math.abs(audioEnergy - energyBySecond[i - 1]) : 0
    const actionSpike = clamp01(
      0.5 * sceneChangeRate +
      0.28 * motionDelta +
      0.22 * clamp01(audioDelta * 2)
    )
    const motionScore = clamp01(0.64 * sceneChangeRate + 0.36 * actionSpike)
    const facePresence = faceBySecond[i] || 0
    const faceIntensity = Math.max(facePresence, faceIntensityBySecond[i] || 0)
    const faceCenterWeight = faceCenterWeightBySecond[i] || 0
    const faceCenterX = faceCenterWeight > 0
      ? clamp01(faceCenterXSumBySecond[i] / faceCenterWeight)
      : undefined
    const faceCenterY = faceCenterWeight > 0
      ? clamp01(faceCenterYSumBySecond[i] / faceCenterWeight)
      : undefined
    const textDensity = textBySecond[i] || 0
    const textConfidence = textConfidenceBySecond[i] || 0
    const emotionalSpike = audioEnergy > meanEnergy + std * 1.5 ? 1 : 0
    const vocalExcitement = Math.min(1, Math.max(0, (audioEnergy - meanEnergy) / (std + 0.05)))
    const modelEmotion = emotionBySecond[i] || 0
    const visualImpact = clamp01(
      0.36 * motionScore +
      0.26 * faceIntensity +
      0.2 * textDensity +
      0.18 * actionSpike
    )
    const emotionIntensity = Math.min(
      1,
      0.38 * speechIntensity +
      0.18 * vocalExcitement +
      0.14 * emotionalSpike +
      0.18 * modelEmotion +
      0.12 * faceIntensity
    )
    const baseScore =
      0.2 * audioEnergy +
      0.2 * speechIntensity +
      0.14 * motionScore +
      0.12 * facePresence +
      0.15 * emotionIntensity +
      0.09 * textDensity +
      0.06 * vocalExcitement +
      0.04 * visualImpact
    const hookPotential =
      0.25 * vocalExcitement +
      0.22 * emotionIntensity +
      0.18 * actionSpike +
      0.14 * sceneChangeRate +
      0.12 * speechIntensity +
      0.09 * textDensity
    const introBias = i < 20 ? 0.06 : i < 40 ? 0.03 : 0
    const score = clamp01(baseScore * 0.82 + hookPotential * 0.18 + introBias)
    windows.push({
      time: i,
      audioEnergy,
      speechIntensity,
      motionScore,
      facePresence,
      faceIntensity,
      faceCenterX: faceCenterX === undefined ? undefined : Number(faceCenterX.toFixed(4)),
      faceCenterY: faceCenterY === undefined ? undefined : Number(faceCenterY.toFixed(4)),
      textDensity,
      textConfidence,
      sceneChangeRate,
      actionSpike,
      visualImpact,
      emotionalSpike,
      vocalExcitement,
      emotionIntensity,
      score
    })
  }
  return windows
}

const parseSrtTimestamp = (raw: string) => {
  const match = String(raw || '').trim().match(/(\d+):(\d+):(\d+)[,.](\d+)/)
  if (!match) return null
  const hh = Number(match[1])
  const mm = Number(match[2])
  const ss = Number(match[3])
  const ms = Number(match[4].padEnd(3, '0').slice(0, 3))
  if (![hh, mm, ss, ms].every(Number.isFinite)) return null
  return hh * 3600 + mm * 60 + ss + ms / 1000
}

const formatSrtTimestamp = (seconds: number) => {
  const safe = Math.max(0, Number(seconds) || 0)
  const totalMs = Math.floor(safe * 1000)
  const hh = Math.floor(totalMs / 3_600_000)
  const mm = Math.floor((totalMs % 3_600_000) / 60_000)
  const ss = Math.floor((totalMs % 60_000) / 1_000)
  const ms = totalMs % 1_000
  return `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}:${String(ss).padStart(2, '0')},${String(ms).padStart(3, '0')}`
}

const remapTranscriptCuesToEditedTimeline = (cues: TranscriptCue[], segments: Segment[]) => {
  if (!cues.length || !segments.length) return [] as TranscriptCue[]
  type TimelineSegment = {
    sourceStart: number
    sourceEnd: number
    speed: number
    outputStart: number
    outputEnd: number
  }
  let outputCursor = 0
  const timeline = segments
    .map((segment) => {
      const sourceStart = Number(segment.start)
      const sourceEnd = Number(segment.end)
      const speed = Number(segment.speed) > 0 ? Number(segment.speed) : 1
      if (!Number.isFinite(sourceStart) || !Number.isFinite(sourceEnd) || sourceEnd <= sourceStart) return null
      const outputDuration = (sourceEnd - sourceStart) / speed
      if (!Number.isFinite(outputDuration) || outputDuration <= 0) return null
      const item: TimelineSegment = {
        sourceStart,
        sourceEnd,
        speed,
        outputStart: outputCursor,
        outputEnd: outputCursor + outputDuration
      }
      outputCursor = item.outputEnd
      return item
    })
    .filter((item): item is TimelineSegment => Boolean(item))
  if (!timeline.length) return [] as TranscriptCue[]

  const remapped: TranscriptCue[] = []
  for (const cue of cues) {
    const cueStart = Number(cue.start)
    const cueEnd = Number(cue.end)
    if (!Number.isFinite(cueStart) || !Number.isFinite(cueEnd) || cueEnd <= cueStart) continue
    for (const segment of timeline) {
      const overlapStart = Math.max(cueStart, segment.sourceStart)
      const overlapEnd = Math.min(cueEnd, segment.sourceEnd)
      if (overlapEnd - overlapStart <= 0.01) continue
      const mappedStart = segment.outputStart + (overlapStart - segment.sourceStart) / segment.speed
      const mappedEnd = segment.outputStart + (overlapEnd - segment.sourceStart) / segment.speed
      if (!Number.isFinite(mappedStart) || !Number.isFinite(mappedEnd) || mappedEnd - mappedStart <= 0.01) continue
      remapped.push({
        ...cue,
        start: Number(mappedStart.toFixed(3)),
        end: Number(mappedEnd.toFixed(3))
      })
    }
  }
  if (!remapped.length) return [] as TranscriptCue[]

  const sorted = remapped.sort((a, b) => a.start - b.start || a.end - b.end)
  const merged: TranscriptCue[] = []
  for (const cue of sorted) {
    const previous = merged.length ? merged[merged.length - 1] : null
    if (
      previous &&
      previous.text === cue.text &&
      cue.start - previous.end <= 0.08
    ) {
      previous.end = Number(Math.max(previous.end, cue.end).toFixed(3))
      continue
    }
    merged.push({ ...cue })
  }
  return merged
}

const writeTranscriptCuesToSrt = (cues: TranscriptCue[], outputPath: string) => {
  if (!cues.length) return null
  const blocks: string[] = []
  let index = 1
  for (const cue of cues) {
    const start = Number(cue.start)
    const end = Number(cue.end)
    const text = String(cue.text || '').trim()
    if (!text || !Number.isFinite(start) || !Number.isFinite(end) || end - start <= 0.01) continue
    blocks.push(
      `${index}\n${formatSrtTimestamp(start)} --> ${formatSrtTimestamp(end)}\n${text}\n`
    )
    index += 1
  }
  if (index === 1) return null
  fs.writeFileSync(outputPath, blocks.join('\n'))
  return outputPath
}

const scoreTranscriptSignals = (text: string) => {
  const normalized = String(text || '').toLowerCase().replace(/\s+/g, ' ').trim()
  if (!normalized) {
    return { keywordIntensity: 0, curiosityTrigger: 0, fillerDensity: 0 }
  }
  const tokens = normalized.split(/\s+/).filter(Boolean)
  const tokenCount = Math.max(1, tokens.length)
  const keywordHits = RETENTION_KEYWORDS.reduce((sum, keyword) => (
    sum + (normalized.includes(keyword) ? 1 : 0)
  ), 0)
  const curiosityHits = CURIOSITY_PHRASES.reduce((sum, phrase) => (
    sum + (normalized.includes(phrase) ? 1 : 0)
  ), 0)
  const fillerMatches = FILLER_WORDS.reduce((sum, phrase) => {
    const pattern = new RegExp(`\\b${phrase.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'g')
    const matches = normalized.match(pattern)
    return sum + (matches ? matches.length : 0)
  }, 0)
  const numericCue = /\b\d+([.,]\d+)?\b/.test(normalized) ? 0.2 : 0
  return {
    keywordIntensity: clamp01(keywordHits / 3 + numericCue),
    curiosityTrigger: clamp01(curiosityHits / 2),
    fillerDensity: clamp01(fillerMatches / tokenCount)
  }
}

const parseTranscriptCues = (srtPath: string | null) => {
  if (!srtPath || !fs.existsSync(srtPath)) return [] as TranscriptCue[]
  const content = String(fs.readFileSync(srtPath, 'utf8') || '')
  const blocks = content.split(/\r?\n\r?\n/)
  const cues: TranscriptCue[] = []
  for (const block of blocks) {
    const lines = block.split(/\r?\n/).map((line) => line.trim()).filter(Boolean)
    if (lines.length < 2) continue
    const timingLine = lines.find((line) => line.includes('-->'))
    if (!timingLine) continue
    const [startRaw, endRaw] = timingLine.split('-->').map((line) => line.trim())
    const start = parseSrtTimestamp(startRaw)
    const end = parseSrtTimestamp(endRaw)
    if (start === null || end === null || !Number.isFinite(start) || !Number.isFinite(end) || end <= start) continue
    const textLines = lines.filter((line) => !line.includes('-->') && !/^\d+$/.test(line))
    const text = textLines.join(' ').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim()
    if (!text) continue
    const scored = scoreTranscriptSignals(text)
    cues.push({
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      text,
      ...scored
    })
  }
  return cues.sort((a, b) => a.start - b.start)
}

const buildTranscriptSignalBuckets = (durationSeconds: number, cues: TranscriptCue[]) => {
  const totalSeconds = Math.max(0, Math.ceil(durationSeconds))
  const buckets = new Array(totalSeconds).fill(null).map(() => ({
    keywordIntensity: 0,
    curiosityTrigger: 0,
    fillerDensity: 0,
    novelty: 0
  }))
  let previousText = ''
  for (const cue of cues) {
    const start = Math.max(0, Math.floor(cue.start))
    const end = Math.max(start + 1, Math.ceil(cue.end))
    const novelty = previousText && cue.text
      ? (previousText === cue.text ? 0 : 1)
      : 0.6
    previousText = cue.text
    for (let second = start; second < end && second < buckets.length; second += 1) {
      buckets[second].keywordIntensity = Math.max(buckets[second].keywordIntensity, cue.keywordIntensity)
      buckets[second].curiosityTrigger = Math.max(buckets[second].curiosityTrigger, cue.curiosityTrigger)
      buckets[second].fillerDensity = Math.max(buckets[second].fillerDensity, cue.fillerDensity)
      buckets[second].novelty = Math.max(buckets[second].novelty, novelty)
    }
  }
  return buckets
}

const enrichWindowsWithCognitiveScores = ({
  windows,
  durationSeconds,
  silences,
  transcriptCues
}: {
  windows: EngagementWindow[]
  durationSeconds: number
  silences: TimeRange[]
  transcriptCues: TranscriptCue[]
}) => {
  if (!windows.length) return windows
  const transcriptBuckets = buildTranscriptSignalBuckets(durationSeconds, transcriptCues)
  const isSilentAt = (time: number) => {
    const windowEnd = time + 1
    return silences.some((range) => time < range.end && windowEnd > range.start)
  }
  return windows.map((window, idx) => {
    const prev = idx > 0 ? windows[idx - 1] : null
    const next = idx + 1 < windows.length ? windows[idx + 1] : null
    const transcript = transcriptBuckets[Math.max(0, Math.floor(window.time))] || {
      keywordIntensity: 0,
      curiosityTrigger: 0,
      fillerDensity: 0,
      novelty: 0
    }
    const audioVariance = clamp01(Math.abs(window.audioEnergy - (prev?.audioEnergy ?? window.audioEnergy)) * 1.6)
    const motionVariance = clamp01(Math.abs(window.motionScore - (prev?.motionScore ?? window.motionScore)) * 1.8)
    const repetitiveBackground = clamp01(1 - (0.6 * window.motionScore + 0.4 * motionVariance))
    const lowNarrativeProgress = clamp01(1 - (0.55 * transcript.novelty + 0.45 * transcript.curiosityTrigger))
    const silencePenalty = isSilentAt(window.time) ? 1 : 0
    const boredomScore = clamp01(
      0.21 * (1 - audioVariance) +
      0.19 * silencePenalty +
      0.18 * repetitiveBackground +
      0.15 * (1 - window.facePresence) +
      0.13 * lowNarrativeProgress +
      0.14 * transcript.fillerDensity
    )
    const hookScore = clamp01(
      0.14 * audioVariance +
      0.13 * window.speechIntensity +
      0.12 * window.emotionIntensity +
      0.1 * window.motionScore +
      0.08 * window.textDensity +
      0.11 * transcript.keywordIntensity +
      0.11 * transcript.curiosityTrigger +
      0.09 * window.facePresence +
      0.06 * (window.faceIntensity ?? window.facePresence) +
      0.06 * (window.actionSpike ?? 0)
    )
    return {
      ...window,
      audioVariance,
      keywordIntensity: transcript.keywordIntensity,
      curiosityTrigger: transcript.curiosityTrigger,
      fillerDensity: transcript.fillerDensity,
      narrativeProgress: transcript.novelty,
      boredomScore,
      hookScore
    }
  })
}

const overlapsRange = (a: TimeRange, b: TimeRange) => a.start < b.end && a.end > b.start
const getRangeOverlapSeconds = (a: TimeRange, b: TimeRange) => {
  const start = Math.max(a.start, b.start)
  const end = Math.min(a.end, b.end)
  return Math.max(0, end - start)
}
const getSilenceCoverageRatio = (range: TimeRange, silences: TimeRange[]) => {
  const duration = Math.max(0.001, range.end - range.start)
  const overlap = silences.reduce((sum, silence) => sum + getRangeOverlapSeconds(range, silence), 0)
  return clamp01(overlap / duration)
}

const evaluateHookContextDependency = (start: number, end: number, transcriptCues: TranscriptCue[]) => {
  const relevant = transcriptCues.filter((cue) => cue.end > start && cue.start < end)
  if (!relevant.length) return 0.08
  const mergedText = relevant.map((cue) => cue.text).join(' ').toLowerCase()
  let penalty = 0
  if (/^(and|but|so|then|because|this|that|it|they|we)\b/.test(mergedText)) penalty += 0.18
  if (/\b(as i said|like i said|earlier|before)\b/.test(mergedText)) penalty += 0.15
  if (/\b(he|she|they|it)\b/.test(mergedText) && !/\b(i|you)\b/.test(mergedText)) penalty += 0.06
  if (!/[.!?]["']?$/.test(mergedText.trim())) penalty += 0.08
  if (mergedText.split(/\s+/).length < 5) penalty += 0.06
  return clamp01(penalty)
}

const alignHookToSentenceBoundaries = (
  start: number,
  end: number,
  transcriptCues: TranscriptCue[],
  durationSeconds: number
) => {
  const clampRange = (rawStart: number, rawEnd: number) => {
    let clampedStart = clamp(rawStart, 0, Math.max(0, durationSeconds - HOOK_MIN))
    let clampedEnd = clamp(rawEnd, clampedStart + HOOK_MIN, durationSeconds)
    if (clampedEnd - clampedStart > HOOK_MAX) clampedEnd = clampedStart + HOOK_MAX
    if (clampedEnd > durationSeconds) {
      clampedEnd = durationSeconds
      clampedStart = Math.max(0, clampedEnd - HOOK_MAX)
    }
    if (clampedEnd - clampedStart < HOOK_MIN) {
      const needed = HOOK_MIN - (clampedEnd - clampedStart)
      clampedStart = Math.max(0, clampedStart - needed)
    }
    return {
      start: Number(clampedStart.toFixed(3)),
      end: Number(clampedEnd.toFixed(3))
    }
  }
  if (!transcriptCues.length) return clampRange(start, end)
  const overlapping = transcriptCues.filter((cue) => cue.end > start && cue.start < end)
  if (!overlapping.length) return clampRange(start, end)
  let alignedStart = overlapping[0].start
  let alignedEnd = overlapping[overlapping.length - 1].end
  const firstText = (overlapping[0]?.text || '').trim().toLowerCase()
  if (/^(and|but|so|then|because|this|that|it|they|we)\b/.test(firstText)) {
    alignedStart = Math.max(0, alignedStart - 0.8)
  }
  while (alignedEnd - alignedStart < HOOK_MIN) {
    const nextCue = transcriptCues.find((cue) => cue.start >= alignedEnd - 0.02)
    if (!nextCue) break
    alignedEnd = nextCue.end
  }
  while (alignedEnd - alignedStart > HOOK_MAX) {
    const fallbackEnd = alignedStart + HOOK_MAX
    if (fallbackEnd < alignedEnd) {
      alignedEnd = fallbackEnd
      break
    }
  }
  return clampRange(alignedStart, alignedEnd)
}

const extractHookText = (start: number, end: number, transcriptCues: TranscriptCue[]) => {
  return transcriptCues
    .filter((cue) => cue.end > start && cue.start < end)
    .map((cue) => cue.text)
    .join(' ')
    .replace(/\s+/g, ' ')
    .trim()
}

const scoreHookOpenLoopSignal = (text: string) => {
  const normalized = String(text || '').trim().toLowerCase()
  if (!normalized) return 0
  let score = 0
  if (/\?/.test(normalized)) score += 0.18
  if (/\b(wait|watch|see|before|after|until|why|how|what happens|you won't)\b/.test(normalized)) score += 0.22
  if (/\b(reveal|result|truth|mistake|secret|warning|proof|finally)\b/.test(normalized)) score += 0.2
  if (/\b(but|then|and then|so)\b/.test(normalized)) score += 0.08
  if (/\b(i|you|we)\b/.test(normalized)) score += 0.05
  return clamp01(score)
}

const runHookAudit = ({
  start,
  end,
  transcriptCues,
  windows
}: {
  start: number
  end: number
  transcriptCues: TranscriptCue[]
  windows: EngagementWindow[]
}): HookAuditResult => {
  const text = extractHookText(start, end, transcriptCues).toLowerCase()
  const words = text ? text.split(/\s+/).filter(Boolean) : []
  const contextPenalty = evaluateHookContextDependency(start, end, transcriptCues)
  const transcriptSignals = scoreTranscriptSignals(text)
  const curiositySignal = clamp01(
    averageWindowMetric(windows, start, end, (window) => (window.curiosityTrigger ?? 0)) * 0.55 +
    transcriptSignals.curiosityTrigger * 0.45
  )
  const emotionalSignal = clamp01(
    averageWindowMetric(windows, start, end, (window) => window.emotionIntensity) * 0.42 +
    averageWindowMetric(windows, start, end, (window) => window.vocalExcitement) * 0.22 +
    averageWindowMetric(windows, start, end, (window) => window.speechIntensity) * 0.12 +
    averageWindowMetric(windows, start, end, (window) => window.motionScore) * 0.12 +
    averageWindowMetric(windows, start, end, (window) => window.actionSpike ?? 0) * 0.12
  )
  const nonVerbalClarity = clamp01(
    0.34 * emotionalSignal +
    0.2 * averageWindowMetric(windows, start, end, (window) => window.motionScore) +
    0.18 * averageWindowMetric(windows, start, end, (window) => window.speechIntensity) +
    0.14 * averageWindowMetric(windows, start, end, (window) => window.vocalExcitement) +
    0.14 * averageWindowMetric(windows, start, end, (window) => window.visualImpact ?? 0)
  )
  const hasTranscriptSupport = words.length >= 3
  const understandableByTranscript = words.length >= 5 && contextPenalty <= 0.34
  const understandableBySignal = nonVerbalClarity >= 0.52 && contextPenalty <= 0.55
  const understandable = hasTranscriptSupport
    ? (understandableByTranscript || (words.length >= 3 && understandableBySignal))
    : understandableBySignal
  const curiosity = (
    curiositySignal >= 0.3 ||
    transcriptSignals.keywordIntensity >= 0.36 ||
    /\?/.test(text) ||
    (!hasTranscriptSupport && emotionalSignal >= 0.58)
  )
  const payoff = (
    emotionalSignal >= 0.5 ||
    /\b(changed|reveal|result|mistake|warning|proof|won|lost)\b/.test(text) ||
    (!hasTranscriptSupport && nonVerbalClarity >= 0.64)
  )
  const auditScore = hasTranscriptSupport
    ? clamp01(
        0.34 * (understandable ? 1 : 0) +
        0.28 * (curiosity ? 1 : 0) +
        0.28 * (payoff ? 1 : 0) +
        0.1 * (1 - contextPenalty)
      )
    : clamp01(
        0.32 * (understandable ? 1 : 0) +
        0.3 * (curiosity ? 1 : 0) +
        0.28 * (payoff ? 1 : 0) +
        0.1 * (1 - contextPenalty)
      )
  const passThreshold = hasTranscriptSupport ? 0.72 : 0.58
  const reasons: string[] = []
  if (!understandable) reasons.push(hasTranscriptSupport ? 'Not understandable in isolation' : 'Non-verbal hook beat is too context-dependent')
  if (!curiosity) reasons.push(hasTranscriptSupport ? 'Does not trigger curiosity strongly enough' : 'Visual/audio peak does not trigger enough curiosity')
  if (!payoff) reasons.push(hasTranscriptSupport ? 'Payoff signal is weak' : 'Peak moment does not imply a clear payoff')
  if (contextPenalty > (hasTranscriptSupport ? 0.34 : 0.55)) reasons.push('Requires too much prior context')
  return {
    passed: understandable && curiosity && payoff && auditScore >= passThreshold,
    auditScore: Number(auditScore.toFixed(4)),
    understandable,
    curiosity,
    payoff,
    reasons
  }
}

const buildSyntheticHookCandidate = ({
  durationSeconds,
  segments,
  windows,
  transcriptCues
}: {
  durationSeconds: number
  segments: TimeRange[]
  windows: EngagementWindow[]
  transcriptCues: TranscriptCue[]
}): HookCandidate | null => {
  if (!windows.length || durationSeconds <= 0) return null
  const payoffWindow = windows
    .slice()
    .sort((a, b) => {
      const aScore = 0.38 * (a.hookScore ?? a.score) + 0.28 * a.emotionIntensity + 0.2 * a.vocalExcitement + 0.14 * (a.time / Math.max(1, durationSeconds))
      const bScore = 0.38 * (b.hookScore ?? b.score) + 0.28 * b.emotionIntensity + 0.2 * b.vocalExcitement + 0.14 * (b.time / Math.max(1, durationSeconds))
      return bScore - aScore
    })
    .find((window) => window.time >= Math.min(12, durationSeconds * 0.1)) || windows[0]
  if (!payoffWindow) return null
  const targetDuration = clamp(6.2, HOOK_MIN, HOOK_MAX)
  const tentativeStart = clamp(payoffWindow.time - targetDuration * 0.62, 0, Math.max(0, durationSeconds - targetDuration))
  const aligned = alignHookToSentenceBoundaries(
    tentativeStart,
    tentativeStart + targetDuration,
    transcriptCues,
    durationSeconds
  )
  if (!isRangeCoveredBySegments(aligned.start, aligned.end, segments)) return null
  const firstAudit = runHookAudit({
    start: aligned.start,
    end: aligned.end,
    transcriptCues,
    windows
  })
  let finalStart = aligned.start
  let finalEnd = aligned.end
  let finalAudit = firstAudit
  if (!firstAudit.passed) {
    const teaserStart = Math.max(0, aligned.start - 0.8)
    const repaired = alignHookToSentenceBoundaries(teaserStart, teaserStart + targetDuration, transcriptCues, durationSeconds)
    if (isRangeCoveredBySegments(repaired.start, repaired.end, segments)) {
      finalStart = repaired.start
      finalEnd = repaired.end
      finalAudit = runHookAudit({
        start: finalStart,
        end: finalEnd,
        transcriptCues,
        windows
      })
    }
  }
  const text = extractHookText(finalStart, finalEnd, transcriptCues)
  const score = clamp01(
    averageWindowMetric(windows, finalStart, finalEnd, (window) => (window.hookScore ?? window.score)) * 0.66 +
    finalAudit.auditScore * 0.34
  )
  return {
    start: Number(finalStart.toFixed(3)),
    duration: Number((finalEnd - finalStart).toFixed(3)),
    score: Number(score.toFixed(4)),
    auditScore: finalAudit.auditScore,
    auditPassed: finalAudit.passed,
    text,
    reason: finalAudit.passed
      ? 'Synthetic hook built from strongest payoff moment with teaser context.'
      : `Synthetic hook failed audit: ${finalAudit.reasons.join('; ') || 'unknown reason'}`,
    synthetic: true
  }
}

const buildHookPartitions = (durationSeconds: number) => {
  const total = Math.max(0, durationSeconds || 0)
  if (total <= 0) return [] as TimeRange[]
  const maxByLength = Math.max(1, Math.floor(total / HOOK_MIN))
  let partitionCount = 4
  if (total < 42) partitionCount = 3
  else if (total >= 180 && total < 360) partitionCount = 6
  else if (total >= 360) partitionCount = 8
  partitionCount = Math.max(1, Math.min(partitionCount, maxByLength))
  const chunk = total / partitionCount
  const partitions: TimeRange[] = []
  for (let index = 0; index < partitionCount; index += 1) {
    const start = Number((index * chunk).toFixed(3))
    const end = Number((index === partitionCount - 1 ? total : (index + 1) * chunk).toFixed(3))
    if (end - start >= Math.min(2, HOOK_MIN * 0.4)) {
      partitions.push({ start, end })
    }
  }
  return partitions.length ? partitions : [{ start: 0, end: total }]
}

const scoreHookFaceoffCandidate = ({
  candidate,
  windows,
  hookCalibration
}: {
  candidate: HookCandidate
  windows: EngagementWindow[]
  hookCalibration?: HookCalibrationProfile | null
}) => {
  const computeEmotionalHookPull = (start: number, end: number) => {
    const duration = Math.max(0.5, end - start)
    const earlyEnd = start + duration * 0.38
    const lateStart = start + duration * 0.62
    const earlyEmotion = averageWindowMetric(windows, start, earlyEnd, (window) => (
      0.5 * window.emotionIntensity +
      0.25 * window.vocalExcitement +
      0.25 * (window.curiosityTrigger ?? 0)
    ))
    const lateEmotion = averageWindowMetric(windows, lateStart, end, (window) => (
      0.54 * window.emotionIntensity +
      0.24 * window.vocalExcitement +
      0.12 * (window.actionSpike ?? 0) +
      0.1 * window.sceneChangeRate
    ))
    const peakEmotion = averageWindowMetric(windows, start, end, (window) => (
      0.48 * window.emotionIntensity +
      0.2 * window.vocalExcitement +
      0.14 * (window.actionSpike ?? 0) +
      0.1 * (window.curiosityTrigger ?? 0) +
      0.08 * window.motionScore
    ))
    const emotionalRamp = clamp01((lateEmotion - earlyEmotion) * 0.9 + 0.5)
    return clamp01(
      0.5 * peakEmotion +
      0.32 * emotionalRamp +
      0.18 * clamp01(Math.max(0, lateEmotion - earlyEmotion) * 1.35)
    )
  }
  const weights = normalizeHookCalibrationWeights(
    hookCalibration?.weights || DEFAULT_HOOK_FACEOFF_WEIGHTS
  )
  const start = candidate.start
  const end = candidate.start + candidate.duration
  const energy = averageWindowMetric(windows, start, end, (window) => (
    0.38 * window.speechIntensity +
    0.32 * window.vocalExcitement +
    0.3 * (window.audioVariance ?? 0)
  ))
  const curiosity = averageWindowMetric(windows, start, end, (window) => (
    0.6 * (window.curiosityTrigger ?? 0) +
    0.4 * (window.keywordIntensity ?? 0)
  ))
  const emotionalPull = computeEmotionalHookPull(start, end)
  const emotionalSpike = averageWindowMetric(windows, start, end, (window) => (
    0.48 * window.emotionIntensity +
    0.16 * window.motionScore +
    0.16 * (window.actionSpike ?? 0) +
    0.2 * emotionalPull
  ))
  const faceoffScore = clamp01(
    weights.candidateScore * candidate.score +
    weights.auditScore * candidate.auditScore +
    weights.energy * energy +
    weights.curiosity * curiosity +
    weights.emotionalSpike * emotionalSpike +
    0.12 * emotionalPull
  )
  return Number(faceoffScore.toFixed(4))
}

const pickTopHookCandidates = ({
  durationSeconds,
  segments,
  windows,
  transcriptCues,
  hookCalibration,
  styleProfile,
  nicheProfile,
  aggressionLevel
}: {
  durationSeconds: number
  segments: TimeRange[]
  windows: EngagementWindow[]
  transcriptCues: TranscriptCue[]
  hookCalibration?: HookCalibrationProfile | null
  styleProfile?: ContentStyleProfile | null
  nicheProfile?: VideoNicheProfile | null
  aggressionLevel?: RetentionAggressionLevel
}) => {
  const emotionalTuning = resolveEmotionalTuningProfile({
    styleProfile,
    nicheProfile,
    aggressionLevel
  })
  const starts = new Set<number>()
  segments.forEach((segment) => starts.add(Number(segment.start.toFixed(2))))
  windows
    .slice()
    .sort((a, b) => (
      (b.hookScore ?? b.score) - (a.hookScore ?? a.score) ||
      b.emotionIntensity - a.emotionIntensity
    ))
    .slice(0, 42)
    .forEach((window) => starts.add(Math.max(0, Number((window.time - 1).toFixed(2)))))
  for (let second = 0; second <= Math.max(0, Math.floor(durationSeconds - HOOK_MIN)); second += 1) {
    starts.add(second)
  }
  const candidateDurations = [8, 7, 6, 5]
  const evaluated: HookCandidate[] = []
  for (const rawStart of starts) {
    for (const duration of candidateDurations) {
      const end = rawStart + duration
      if (end > durationSeconds) continue
      if (!isRangeCoveredBySegments(rawStart, end, segments)) continue
      const aligned = alignHookToSentenceBoundaries(rawStart, end, transcriptCues, durationSeconds)
      if (!isRangeCoveredBySegments(aligned.start, aligned.end, segments)) continue
      const baseHookScore = averageWindowMetric(windows, aligned.start, aligned.end, (window) => window.hookScore ?? window.score)
      const speechImpact = averageWindowMetric(windows, aligned.start, aligned.end, (window) => (
        0.42 * window.speechIntensity +
        0.32 * (window.audioVariance ?? 0) +
        0.26 * window.vocalExcitement
      ))
      const transcriptImpact = averageWindowMetric(windows, aligned.start, aligned.end, (window) => (
        0.52 * (window.keywordIntensity ?? 0) +
        0.48 * (window.curiosityTrigger ?? 0)
      ))
      const visualImpact = averageWindowMetric(windows, aligned.start, aligned.end, (window) => (
        0.45 * window.motionScore +
        0.3 * window.facePresence +
        0.25 * window.textDensity
      ))
      const emotionImpact = averageWindowMetric(windows, aligned.start, aligned.end, (window) => window.emotionIntensity)
      const emotionalHookPull = averageWindowMetric(windows, aligned.start, aligned.end, (window) => (
        0.42 * window.emotionIntensity +
        0.2 * window.vocalExcitement +
        0.18 * (window.actionSpike ?? 0) +
        0.1 * (window.curiosityTrigger ?? 0) +
        0.1 * window.sceneChangeRate
      ))
      const durationSecondsActual = aligned.end - aligned.start
      const durationAlignment = clamp01(1 - (Math.abs(durationSecondsActual - 8) / 3))
      const contextPenalty = evaluateHookContextDependency(aligned.start, aligned.end, transcriptCues)
      const hookText = extractHookText(aligned.start, aligned.end, transcriptCues)
      const openLoopSignal = clamp01(scoreHookOpenLoopSignal(hookText) * emotionalTuning.openLoopBoost)
      const curiosityAcceleration = clamp01(
        averageWindowMetric(windows, aligned.start, aligned.start + durationSecondsActual * 0.45, (window) => (window.curiosityTrigger ?? 0)) * 0.55 +
        averageWindowMetric(windows, aligned.start + durationSecondsActual * 0.55, aligned.end, (window) => (window.hookScore ?? window.score)) * 0.45
      )
      const boostedCuriosityAcceleration = clamp01(curiosityAcceleration * emotionalTuning.curiosityBoost)
      const tunedContextPenalty = clamp01(contextPenalty * emotionalTuning.contextPenaltyMultiplier)
      const audit = runHookAudit({
        start: aligned.start,
        end: aligned.end,
        transcriptCues,
        windows
      })
      const totalScore = clamp01(
        0.2 * baseHookScore +
        0.16 * speechImpact +
        0.13 * transcriptImpact +
        0.11 * visualImpact +
        0.14 * emotionImpact +
        0.14 * emotionalHookPull +
        0.06 * boostedCuriosityAcceleration +
        0.06 * openLoopSignal +
        0.06 * durationAlignment +
        0.16 * audit.auditScore -
        0.18 * tunedContextPenalty
      )
      evaluated.push({
        start: aligned.start,
        duration: Number((aligned.end - aligned.start).toFixed(3)),
        score: Number(totalScore.toFixed(4)),
        auditScore: audit.auditScore,
        auditPassed: audit.passed,
        text: hookText,
        reason: audit.passed ? 'Best-moment candidate passed hook audit.' : audit.reasons.join('; ')
      })
    }
  }
  const rankedEvaluated = evaluated
    .slice()
    .sort((a, b) => b.score - a.score || a.start - b.start)
  const uniqueTop: HookCandidate[] = []
  for (const candidate of rankedEvaluated) {
    const tooClose = uniqueTop.some((entry) => Math.abs(entry.start - candidate.start) < 1.3)
    if (tooClose) continue
    uniqueTop.push(candidate)
    if (uniqueTop.length >= Math.max(HOOK_SELECTION_MAX_CANDIDATES * 3, 12)) break
  }
  if (!uniqueTop.length) {
    const synthetic = buildSyntheticHookCandidate({ durationSeconds, segments, windows, transcriptCues })
    if (synthetic) {
      return {
        selected: synthetic,
        topCandidates: [synthetic],
        hookFailureReason: synthetic.auditPassed ? null : synthetic.reason
      }
    }
    return {
      selected: {
        start: 0,
        duration: Number(Math.min(HOOK_MAX, Math.max(HOOK_MIN, durationSeconds || HOOK_MIN)).toFixed(3)),
        score: 0,
        auditScore: 0,
        auditPassed: false,
        text: '',
        reason: 'No valid hook candidate found across timeline.'
      },
      topCandidates: [],
      hookFailureReason: 'No valid hook candidate found across timeline.'
    }
  }
  const partitions = buildHookPartitions(durationSeconds)
  const partitionWinners: HookCandidate[] = []
  for (let index = 0; index < partitions.length; index += 1) {
    const partition = partitions[index]
    const partitionPool = uniqueTop.filter((candidate) => {
      const center = candidate.start + candidate.duration / 2
      return center >= partition.start && center < partition.end
    })
    if (!partitionPool.length) continue
    const nearEightPool = partitionPool.filter((candidate) => candidate.duration >= 7.4)
    const pool = nearEightPool.length ? nearEightPool : partitionPool
    const best = pool
      .slice()
      .sort((a, b) => (
        scoreHookFaceoffCandidate({ candidate: b, windows, hookCalibration }) - scoreHookFaceoffCandidate({ candidate: a, windows, hookCalibration }) ||
        b.score - a.score ||
        a.start - b.start
      ))[0]
    partitionWinners.push({
      ...best,
      reason: `${best.reason} Chosen as top 8s candidate for section ${index + 1}/${partitions.length}.`
    })
  }
  const faceoffPool = partitionWinners.length ? partitionWinners : uniqueTop.slice(0, Math.max(4, HOOK_SELECTION_MAX_CANDIDATES))
  const faceoffRanked = faceoffPool
    .map((candidate) => ({
      candidate,
      faceoffScore: scoreHookFaceoffCandidate({ candidate, windows, hookCalibration })
    }))
    .sort((a, b) => (
      b.faceoffScore - a.faceoffScore ||
      b.candidate.score - a.candidate.score ||
      a.candidate.start - b.candidate.start
    ))
  let selected = (faceoffRanked.find((entry) => entry.candidate.auditPassed) || faceoffRanked[0]).candidate
  if (hookCalibration?.enabled && hookCalibration.sampleSize >= HOOK_CALIBRATION_MIN_SAMPLES) {
    selected = {
      ...selected,
      reason: `${selected.reason} Adaptive hook weights used from ${hookCalibration.sampleSize} feedback samples.`
    }
  }
  let hookFailureReason: string | null = null
  if (!selected.auditPassed) {
    const synthetic = buildSyntheticHookCandidate({ durationSeconds, segments, windows, transcriptCues })
    if (synthetic && synthetic.auditPassed) {
      selected = synthetic
      uniqueTop.push(synthetic)
    } else {
      hookFailureReason = synthetic?.reason || `No hook candidate passed audit. Best candidate reason: ${selected.reason}`
      if (synthetic) uniqueTop.push(synthetic)
    }
  }
  const finalRanked = [
    selected,
    ...faceoffRanked.map((entry) => entry.candidate),
    ...uniqueTop
  ]
  const dedupedFinal: HookCandidate[] = []
  for (const candidate of finalRanked) {
    const duplicate = dedupedFinal.some((entry) => (
      Math.abs(entry.start - candidate.start) < 0.01 &&
      Math.abs(entry.duration - candidate.duration) < 0.01
    ))
    if (duplicate) continue
    dedupedFinal.push(candidate)
    if (dedupedFinal.length >= HOOK_SELECTION_MAX_CANDIDATES) break
  }
  return { selected, topCandidates: dedupedFinal, hookFailureReason }
}

const buildBoredomRangesFromScores = (
  windows: EngagementWindow[],
  threshold: number,
  highThreshold: number
) => {
  const mild: TimeRange[] = []
  const severe: TimeRange[] = []
  let mildStart: number | null = null
  let severeStart: number | null = null
  for (let idx = 0; idx <= windows.length; idx += 1) {
    const window = idx < windows.length ? windows[idx] : null
    const boredom = window?.boredomScore ?? -1
    const mildFlag = boredom >= threshold
    const severeFlag = boredom >= highThreshold
    const time = window ? window.time : (windows[windows.length - 1]?.time ?? 0) + 1
    if (mildFlag && mildStart === null) mildStart = time
    if (!mildFlag && mildStart !== null) {
      mild.push({ start: mildStart, end: time })
      mildStart = null
    }
    if (severeFlag && severeStart === null) severeStart = time
    if (!severeFlag && severeStart !== null) {
      severe.push({ start: severeStart, end: time })
      severeStart = null
    }
  }
  return {
    mild: mergeRanges(mild.filter((range) => range.end - range.start >= 1)),
    severe: mergeRanges(severe.filter((range) => range.end - range.start >= 1.2))
  }
}

const applyBoredomModelToSegments = ({
  segments,
  windows,
  aggressionLevel,
  hookRange
}: {
  segments: Segment[]
  windows: EngagementWindow[]
  aggressionLevel: RetentionAggressionLevel
  hookRange: TimeRange | null
}) => {
  if (!segments.length || !windows.length) return { segments, removedRanges: [] as TimeRange[] }
  const preset = RETENTION_AGGRESSION_PRESET[aggressionLevel]
  const boredom = buildBoredomRangesFromScores(
    windows,
    preset.boredomThreshold,
    Math.min(0.92, preset.boredomThreshold + 0.16)
  )
  const protectedRanges: TimeRange[] = []
  if (hookRange) protectedRanges.push(hookRange)
  for (const window of windows) {
    const isEmotional = (window.emotionIntensity > 0.7 || window.emotionalSpike > 0) && (window.hookScore ?? window.score) > 0.58
    if (!isEmotional) continue
    protectedRanges.push({
      start: Math.max(0, window.time - 0.4),
      end: window.time + 1.2
    })
  }
  const safeSevereCuts = boredom.severe.filter((range) => !protectedRanges.some((guard) => overlapsRange(range, guard)))
  const afterCuts = safeSevereCuts.length ? subtractRanges(segments, safeSevereCuts) : segments.map((segment) => ({ ...segment }))
  const spedUp = afterCuts.map((segment) => {
    const overlapMild = boredom.mild.some((range) => overlapsRange(range, { start: segment.start, end: segment.end }))
    if (!overlapMild) return { ...segment }
    const baseSpeed = segment.speed && segment.speed > 0 ? segment.speed : 1
    const targetSpeed = clamp(baseSpeed * (1.12 * preset.cutMultiplier), 1, aggressionLevel === 'viral' ? 1.62 : 1.45)
    return { ...segment, speed: Number(targetSpeed.toFixed(3)) }
  })
  return {
    segments: spedUp.filter((segment) => segment.end - segment.start > 0.18),
    removedRanges: safeSevereCuts
  }
}

const injectPatternInterrupts = ({
  segments,
  durationSeconds,
  aggressionLevel,
  targetIntervalSeconds
}: {
  segments: Segment[]
  durationSeconds: number
  aggressionLevel: RetentionAggressionLevel
  targetIntervalSeconds?: number | null
}) => {
  const preset = RETENTION_AGGRESSION_PRESET[aggressionLevel]
  if (!segments.length || durationSeconds <= 0) return { segments, count: 0, density: 0 }
  const out = segments.map((segment) => ({ ...segment }))
  const runtimeSeconds = Math.max(0.1, computeEditedRuntimeSeconds(out))
  const preferredInterval = Number.isFinite(Number(targetIntervalSeconds))
    ? clamp(Number(targetIntervalSeconds), 3, 12)
    : runtimeSeconds <= 90
      ? 4
      : 6
  const requiredInterval = preferredInterval
  const minimumInterruptCount = Math.max(1, Math.ceil(runtimeSeconds / requiredInterval))
  const minInterval = Number.isFinite(Number(targetIntervalSeconds))
    ? clamp(Number(targetIntervalSeconds) * 0.8, 2.6, 11)
    : preset.patternIntervalMin
  const maxInterval = Number.isFinite(Number(targetIntervalSeconds))
    ? clamp(Number(targetIntervalSeconds) * 1.18, minInterval + 0.2, 13.5)
    : preset.patternIntervalMax
  let cursor = minInterval
  let count = 0
  while (cursor < durationSeconds) {
    const segment = out.find((item) => item.start <= cursor && item.end >= cursor)
    if (segment) {
      segment.zoom = Math.max(segment.zoom ?? 0, Number((0.03 * preset.zoomBoost).toFixed(3)))
      segment.brightness = Math.max(segment.brightness ?? 0, 0.02)
      ;(segment as any).emphasize = true
      count += 1
    }
    const interval = count % 2 === 0 ? maxInterval : minInterval
    cursor += interval
  }
  if (count < minimumInterruptCount) {
    const candidates = out
      .map((segment, idx) => {
        const speed = segment.speed && segment.speed > 0 ? segment.speed : 1
        const runtime = Math.max(0.1, (segment.end - segment.start) / speed)
        return { idx, segment, runtime }
      })
      .sort((a, b) => b.runtime - a.runtime)
    for (const candidate of candidates) {
      if (count >= minimumInterruptCount) break
      const seg = out[candidate.idx]
      const alreadyEmphasized = Boolean((seg as any).emphasize)
      if (!alreadyEmphasized) {
        seg.zoom = Math.max(seg.zoom ?? 0, Number((0.024 + 0.01 * preset.zoomBoost).toFixed(3)))
        seg.brightness = Math.max(seg.brightness ?? 0, 0.018)
        ;(seg as any).emphasize = true
        count += 1
      }
      // Low-energy speed ramp in rescue mode to avoid generic long drags.
      const baseSpeed = seg.speed && seg.speed > 0 ? seg.speed : 1
      if (baseSpeed < 1.15 && candidate.runtime >= 2.4) {
        seg.speed = Number(clamp(baseSpeed + 0.08, 1, 1.15).toFixed(3))
      }
    }
  }
  const density = Number((count / runtimeSeconds).toFixed(4))
  return { segments: out, count, density }
}

const enforceEndingSpike = ({
  segments,
  windows,
  durationSeconds
}: {
  segments: Segment[]
  windows: EngagementWindow[]
  durationSeconds: number
}) => {
  if (!segments.length || !windows.length || durationSeconds < 20) return segments
  const finalWindowStart = Math.max(0, durationSeconds - 5)
  const finalScore = averageWindowMetric(windows, finalWindowStart, durationSeconds, (window) => window.hookScore ?? window.score)
  const overall = averageWindowMetric(windows, 0, durationSeconds, (window) => window.hookScore ?? window.score)
  if (finalScore >= overall * 0.95) return segments
  const candidate = segments
    .map((segment) => ({
      segment,
      score: averageWindowMetric(windows, segment.start, segment.end, (window) => window.hookScore ?? window.score)
    }))
    .filter((entry) => entry.segment.end <= finalWindowStart)
    .sort((a, b) => b.score - a.score)[0]
  if (!candidate) return segments
  const maxTailLen = 5
  const tailStart = candidate.segment.start
  const tailEnd = Math.min(candidate.segment.end, tailStart + maxTailLen)
  const out = segments.slice()
  out.push({
    ...candidate.segment,
    start: Number(tailStart.toFixed(3)),
    end: Number(tailEnd.toFixed(3)),
    speed: candidate.segment.speed ?? 1
  })
  return out
}

const detectSilences = async (filePath: string, durationSeconds: number) => {
  if (!hasFfmpeg()) return [] as TimeRange[]
  const args = [
    '-hide_banner',
    '-nostdin',
    '-i', filePath,
    '-af', `silencedetect=noise=${SILENCE_DB}dB:d=${SILENCE_MIN}`,
    '-f', 'null',
    '-'
  ]
  const output = await runFfmpegCapture(args)
  const lines = output.split(/\r?\n/)
  const silences: TimeRange[] = []
  let currentStart: number | null = null
  for (const line of lines) {
    if (line.includes('silence_start:')) {
      const match = line.match(/silence_start:\s*([0-9.]+)/)
      if (match) currentStart = Number.parseFloat(match[1])
    }
    if (line.includes('silence_end:')) {
      const match = line.match(/silence_end:\s*([0-9.]+)/)
      if (match) {
        const end = Number.parseFloat(match[1])
        const start = currentStart ?? Math.max(0, end - SILENCE_MIN)
        silences.push({ start, end })
        currentStart = null
      }
    }
  }
  if (currentStart !== null && durationSeconds) {
    silences.push({ start: currentStart, end: durationSeconds })
  }
  return silences
}

let cachedFaceDetectFilter: boolean | null = null
const hasFaceDetectFilter = () => {
  if (cachedFaceDetectFilter !== null) return cachedFaceDetectFilter
  if (!hasFfmpeg()) {
    cachedFaceDetectFilter = false
    return cachedFaceDetectFilter
  }
  try {
    const result = spawnSync(FFMPEG_PATH, ['-hide_banner', '-filters'], { encoding: 'utf8' })
    if (result.status !== 0) {
      cachedFaceDetectFilter = false
      return cachedFaceDetectFilter
    }
    const output = String(result.stdout || '')
    cachedFaceDetectFilter = output.includes('facedetect')
    return cachedFaceDetectFilter
  } catch (e) {
    cachedFaceDetectFilter = false
    return cachedFaceDetectFilter
  }
}

const detectFacePresence = async (filePath: string, durationSeconds: number) => {
  if (!hasFfmpeg()) return [] as FaceSample[]
  if (!hasFaceDetectFilter()) return [] as FaceSample[]
  const sourceProbe = probeVideoStream(filePath)
  const sourceWidth = Number(sourceProbe?.width)
  const sourceHeight = Number(sourceProbe?.height)
  const hasSourceDimensions = Number.isFinite(sourceWidth) && Number.isFinite(sourceHeight) && sourceWidth > 0 && sourceHeight > 0
  const analyzeSeconds = Math.min(HOOK_ANALYZE_MAX, durationSeconds || HOOK_ANALYZE_MAX)
  const args = [
    '-hide_banner',
    '-nostdin',
    '-i', filePath,
    '-t', String(analyzeSeconds),
    '-vf', 'facedetect=mode=fast:scale=1,metadata=print',
    '-f', 'null',
    '-'
  ]
  const output = await runFfmpegCapture(args).catch(() => '')
  const lines = output.split(/\r?\n/)
  const sampleMap = new Map<number, {
    count: number
    maxArea: number
    centerWeight: number
    centerXWeightedSum: number
    centerYWeightedSum: number
  }>()
  for (const line of lines) {
    if (!line.includes('lavfi.facedetect')) continue
    const timeMatch = line.match(/pts_time:([0-9.]+)/)
    if (!timeMatch) continue
    const time = Number.parseFloat(timeMatch[1])
    if (!Number.isFinite(time)) continue
    const bucket = Math.floor(time)
    const width = Number.parseFloat((line.match(/lavfi\.facedetect\.w=([0-9.]+)/)?.[1] || '0'))
    const height = Number.parseFloat((line.match(/lavfi\.facedetect\.h=([0-9.]+)/)?.[1] || '0'))
    const area = Number.isFinite(width) && Number.isFinite(height) && width > 0 && height > 0
      ? width * height
      : 0
    const x = Number.parseFloat((line.match(/lavfi\.facedetect\.x=([0-9.]+)/)?.[1] || '0'))
    const y = Number.parseFloat((line.match(/lavfi\.facedetect\.y=([0-9.]+)/)?.[1] || '0'))
    const centerX = hasSourceDimensions && Number.isFinite(x) && Number.isFinite(width)
      ? clamp01((x + width / 2) / sourceWidth)
      : null
    const centerY = hasSourceDimensions && Number.isFinite(y) && Number.isFinite(height)
      ? clamp01((y + height / 2) / sourceHeight)
      : null
    const weight = area > 0 ? area : 1
    const prev = sampleMap.get(bucket) || {
      count: 0,
      maxArea: 0,
      centerWeight: 0,
      centerXWeightedSum: 0,
      centerYWeightedSum: 0
    }
    const centerWeight = centerX === null || centerY === null
      ? prev.centerWeight
      : prev.centerWeight + weight
    const centerXWeightedSum = centerX === null
      ? prev.centerXWeightedSum
      : prev.centerXWeightedSum + centerX * weight
    const centerYWeightedSum = centerY === null
      ? prev.centerYWeightedSum
      : prev.centerYWeightedSum + centerY * weight
    sampleMap.set(bucket, {
      count: prev.count + 1,
      maxArea: Math.max(prev.maxArea, area),
      centerWeight,
      centerXWeightedSum,
      centerYWeightedSum
    })
  }
  return Array.from(sampleMap.entries()).map(([time, stats]) => {
    const presence = clamp01(stats.count >= 2 ? 1 : 0.6)
    const intensity = clamp01(
      0.62 * presence +
      0.38 * clamp01(stats.maxArea / 110000)
    )
    return {
      time,
      presence: Number(presence.toFixed(4)),
      intensity: Number(intensity.toFixed(4)),
      faceCount: stats.count,
      centerX: stats.centerWeight > 0
        ? Number(clamp01(stats.centerXWeightedSum / stats.centerWeight).toFixed(4))
        : undefined,
      centerY: stats.centerWeight > 0
        ? Number(clamp01(stats.centerYWeightedSum / stats.centerWeight).toFixed(4))
        : undefined
    }
  })
}

const detectEmotionModelSignals = async (filePath: string, durationSeconds: number) => {
  const modelBin = process.env.EMOTION_MODEL_BIN
  if (!modelBin) return [] as { time: number; intensity: number }[]
  const analyzeSeconds = Math.min(HOOK_ANALYZE_MAX, durationSeconds || HOOK_ANALYZE_MAX)
  return new Promise<{ time: number; intensity: number }[]>((resolve) => {
    let stdout = ''
    const proc = spawn(modelBin, [filePath, String(analyzeSeconds)], { stdio: ['ignore', 'pipe', 'pipe'] })
    proc.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })
    proc.on('error', () => resolve([]))
    proc.on('close', () => {
      try {
        const parsed = JSON.parse(stdout)
        if (!Array.isArray(parsed)) return resolve([])
        const out = parsed
          .map((entry: any) => ({
            time: Number(entry?.time),
            intensity: Number(entry?.intensity)
          }))
          .filter((entry: any) => Number.isFinite(entry.time) && Number.isFinite(entry.intensity))
          .map((entry: any) => ({
            time: entry.time,
            intensity: clamp01(entry.intensity)
          }))
        resolve(out)
      } catch {
        resolve([])
      }
    })
  })
}

let cachedTesseractBin: string | null | undefined
const resolveTesseractBinary = () => {
  if (cachedTesseractBin !== undefined) return cachedTesseractBin
  const explicit = process.env.TEXT_DENSITY_TESSERACT_BIN
  if (explicit && explicit.trim()) {
    cachedTesseractBin = explicit.trim()
    return cachedTesseractBin
  }
  const candidates = process.platform === 'win32'
    ? ['tesseract', 'C:\\Program Files\\Tesseract-OCR\\tesseract.exe']
    : ['tesseract']
  for (const candidate of candidates) {
    try {
      const result = spawnSync(candidate, ['--version'], { stdio: 'ignore' })
      if (result.status === 0) {
        cachedTesseractBin = candidate
        return cachedTesseractBin
      }
    } catch {
      // continue
    }
  }
  cachedTesseractBin = null
  return cachedTesseractBin
}

const detectTextDensityWithTesseract = async (filePath: string, durationSeconds: number) => {
  const tesseractBin = resolveTesseractBinary()
  if (!tesseractBin) return [] as { time: number; density: number; confidence?: number }[]
  const analyzeSeconds = Math.min(300, Math.max(20, durationSeconds || 120))
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'text-density-'))
  const framePattern = path.join(tmpDir, 'td-%05d.jpg')
  const frameArgs = [
    '-hide_banner',
    '-nostdin',
    '-y',
    '-i',
    filePath,
    '-t',
    String(analyzeSeconds),
    '-vf',
    'fps=1,scale=540:-1:flags=lanczos',
    '-q:v',
    '8',
    framePattern
  ]
  try {
    await runFfmpeg(frameArgs)
  } catch {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true })
    } catch {
      // ignore
    }
    return [] as { time: number; density: number; confidence?: number }[]
  }
  try {
    const frames = fs.readdirSync(tmpDir)
      .filter((name) => name.toLowerCase().endsWith('.jpg'))
      .sort()
      .slice(0, 300)
    const samples: Array<{ time: number; density: number; confidence?: number }> = []
    for (let index = 0; index < frames.length; index += 1) {
      const framePath = path.join(tmpDir, frames[index])
      let textOut = ''
      try {
        const result = spawnSync(
          tesseractBin,
          [framePath, 'stdout', '--psm', '6', '-l', 'eng'],
          { encoding: 'utf8', timeout: 1800 }
        )
        if (result.status === 0) textOut = String(result.stdout || '')
      } catch {
        textOut = ''
      }
      const cleaned = textOut.replace(/\s+/g, ' ').trim()
      const charCount = cleaned.length
      const density = clamp01(charCount / 90)
      const uppercaseRatio = cleaned.length
        ? clamp01((cleaned.match(/[A-Z]/g)?.length || 0) / cleaned.length)
        : 0
      const confidence = Number(clamp01(0.56 + uppercaseRatio * 0.22 + (charCount > 16 ? 0.12 : 0)).toFixed(4))
      samples.push({
        time: index,
        density: Number(density.toFixed(4)),
        confidence
      })
    }
    return samples
  } catch {
    return [] as { time: number; density: number; confidence?: number }[]
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true })
    } catch {
      // ignore
    }
  }
}

const detectTextDensity = async (filePath: string, durationSeconds: number) => {
  const modelBin = process.env.TEXT_DENSITY_MODEL_BIN || process.env.TEXT_DENSITY_BIN
  if (!modelBin) {
    if (String(process.env.TEXT_DENSITY_ENABLE_TESSERACT || '').toLowerCase() === '1') {
      return detectTextDensityWithTesseract(filePath, durationSeconds)
    }
    return [] as { time: number; density: number; confidence?: number }[]
  }
  const analyzeSeconds = Math.min(HOOK_ANALYZE_MAX, durationSeconds || HOOK_ANALYZE_MAX)
  return new Promise<{ time: number; density: number; confidence?: number }[]>((resolve) => {
    let stdout = ''
    const proc = spawn(modelBin, [filePath, String(analyzeSeconds)], { stdio: ['ignore', 'pipe', 'pipe'] })
    proc.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })
    proc.on('error', () => resolve([]))
    proc.on('close', () => {
      try {
        const parsed = JSON.parse(stdout)
        const list = Array.isArray(parsed)
          ? parsed
          : Array.isArray(parsed?.samples)
            ? parsed.samples
            : []
        if (!Array.isArray(list)) return resolve([])
        const output = list
          .map((entry: any) => ({
            time: Number(entry?.time ?? entry?.second ?? entry?.t),
            density: Number(entry?.density ?? entry?.textDensity ?? entry?.value),
            confidence: Number(entry?.confidence ?? entry?.score ?? entry?.textConfidence)
          }))
          .filter((entry: any) => Number.isFinite(entry.time) && Number.isFinite(entry.density))
          .map((entry: any) => ({
            time: entry.time,
            density: clamp01(entry.density),
            confidence: Number.isFinite(entry.confidence) ? clamp01(entry.confidence) : undefined
          }))
        if (!output.length && String(process.env.TEXT_DENSITY_ENABLE_TESSERACT || '').toLowerCase() === '1') {
          void detectTextDensityWithTesseract(filePath, durationSeconds).then(resolve).catch(() => resolve([]))
          return
        }
        resolve(output)
      } catch {
        if (String(process.env.TEXT_DENSITY_ENABLE_TESSERACT || '').toLowerCase() === '1') {
          void detectTextDensityWithTesseract(filePath, durationSeconds).then(resolve).catch(() => resolve([]))
          return
        }
        resolve([] as { time: number; density: number; confidence?: number }[])
      }
    })
  })
}

const isRangeCoveredBySegments = (start: number, end: number, segments: Segment[]) => {
  const sorted = segments.slice().sort((a, b) => a.start - b.start)
  let cursor = start
  for (const seg of sorted) {
    if (seg.end <= cursor) continue
    if (seg.start > cursor) return false
    if (seg.end >= end) return true
    cursor = seg.end
  }
  return false
}

const scoreWindow = (start: number, duration: number, windows: EngagementWindow[]) => {
  const end = start + duration
  const avg = averageWindowMetric(windows, start, end, (window) => window.score)
  if (!Number.isFinite(avg) || avg <= 0) return -Infinity
  return avg
}

const pickBestHook = (
  durationSeconds: number,
  segments: TimeRange[],
  windows: EngagementWindow[]
) => {
  const candidates = new Set<number>()
  segments.forEach((seg) => candidates.add(seg.start))
  windows
    .slice()
    .sort((a, b) => b.score - a.score)
    .slice(0, 12)
    .forEach((win) => candidates.add(Math.max(0, win.time - 1)))
  windows
    .slice()
    .sort((a, b) => {
      const aCombo = 0.6 * a.emotionIntensity + 0.4 * a.sceneChangeRate
      const bCombo = 0.6 * b.emotionIntensity + 0.4 * b.sceneChangeRate
      return bCombo - aCombo
    })
    .slice(0, 10)
    .forEach((win) => candidates.add(Math.max(0, win.time - 1)))
  ;[0, 1, 2].forEach((start) => candidates.add(start))
  for (let start = 0; start <= Math.max(0, durationSeconds - HOOK_MIN); start += 2) {
    candidates.add(start)
  }
  windows
    .filter((window) => window.time <= STRATEGIST_HOOK_WINDOW_SEC)
    .sort((a, b) => b.score - a.score)
    .slice(0, 6)
    .forEach((window) => candidates.add(window.time))

  const maxDuration = Math.min(HOOK_MAX, durationSeconds || HOOK_MAX)
  const minDuration = Math.min(HOOK_MIN, maxDuration)

  const evaluated: Array<{ start: number; duration: number; score: number }> = []
  const effectiveDuration = durationSeconds > 0 ? durationSeconds : HOOK_ANALYZE_MAX
  const urgencyWindow = Math.max(
    STRATEGIST_HOOK_WINDOW_SEC,
    Math.max(STRATEGIST_HOOK_WINDOW_SEC, effectiveDuration * 0.35)
  )

  for (const start of candidates) {
    for (let duration = maxDuration; duration >= minDuration; duration -= 1) {
      const end = start + duration
      if (end > durationSeconds) continue
      if (!isRangeCoveredBySegments(start, end, segments)) continue
      const baseScore = scoreWindow(start, duration, windows)
      if (!Number.isFinite(baseScore)) continue
      const avgSpeech = averageWindowMetric(windows, start, end, (window) => window.speechIntensity)
      const avgEmotion = averageWindowMetric(windows, start, end, (window) => window.emotionIntensity)
      const avgExcitement = averageWindowMetric(windows, start, end, (window) => window.vocalExcitement)
      const avgMotion = averageWindowMetric(windows, start, end, (window) => window.sceneChangeRate)
      const avgFace = averageWindowMetric(windows, start, end, (window) => window.facePresence)
      const hasSpike = averageWindowMetric(windows, start, end, (window) => window.emotionalSpike) > 0.05
      const emotionalMotionBlend = Math.min(1, 0.62 * avgEmotion + 0.38 * avgMotion)
      const urgency = clamp01(1 - start / urgencyWindow)
      const earlyBoost = 0.16 * urgency
      const latePenalty =
        start > STRATEGIST_LATE_HOOK_PENALTY_SEC
          ? Math.min(0.2, (start - STRATEGIST_LATE_HOOK_PENALTY_SEC) / 90)
          : 0
      const longHookPenalty = duration >= 14 ? Math.min(0.05, (duration - 13) * 0.01) : 0
      const mediumHookBonus = duration >= 8 && duration <= 16 ? 0.02 : 0
      const score =
        baseScore +
        0.13 * emotionalMotionBlend +
        0.11 * avgExcitement +
        0.09 * avgEmotion +
        0.06 * avgSpeech +
        0.05 * avgMotion +
        0.04 * avgFace +
        (hasSpike ? 0.08 : 0) +
        mediumHookBonus +
        earlyBoost -
        latePenalty -
        longHookPenalty
      evaluated.push({ start, duration, score })
    }
  }
  if (!evaluated.length) {
    return { start: 0, duration: minDuration, score: 0.5 }
  }
  evaluated.sort((a, b) => b.score - a.score || a.start - b.start)
  const bestAny = evaluated[0]
  const bestRelocationCandidate = evaluated.find(
    (entry) =>
      entry.start >= HOOK_RELOCATE_MIN_START &&
      entry.score >= bestAny.score - HOOK_RELOCATE_SCORE_TOLERANCE
  )
  const selected =
    bestAny.start < HOOK_RELOCATE_MIN_START && bestRelocationCandidate ? bestRelocationCandidate : bestAny
  return { start: selected.start, duration: selected.duration, score: clamp01(selected.score) }
}

const subtractRange = (segments: Segment[], range: TimeRange) => {
  const result: Segment[] = []
  for (const seg of segments) {
    if (range.end <= seg.start || range.start >= seg.end) {
      result.push(seg)
      continue
    }
    if (range.start > seg.start) result.push({ ...seg, start: seg.start, end: Math.max(seg.start, range.start) })
    if (range.end < seg.end) result.push({ ...seg, start: Math.min(seg.end, range.end), end: seg.end })
  }
  return result.filter((seg) => seg.end - seg.start > 0.25)
}

const mergeRanges = (ranges: TimeRange[]) => {
  if (!ranges.length) return []
  const sorted = ranges.slice().sort((a, b) => a.start - b.start)
  const merged: TimeRange[] = []
  let current = { ...sorted[0] }
  for (let i = 1; i < sorted.length; i += 1) {
    const next = sorted[i]
    if (next.start <= current.end) {
      current.end = Math.max(current.end, next.end)
    } else {
      merged.push(current)
      current = { ...next }
    }
  }
  merged.push(current)
  return merged
}

const subtractRanges = (segments: Segment[], ranges: TimeRange[]) => {
  let result = segments.map((seg) => ({ ...seg }))
  const ordered = mergeRanges(ranges)
  for (const range of ordered) {
    result = subtractRange(result, range)
  }
  return result
}

const buildFaceAbsenceFlags = (windows: EngagementWindow[], minDuration = 2) => {
  const hasSignal = windows.some((w) => w.facePresence > 0.2)
  if (!hasSignal) return windows.map(() => false)
  const flags = windows.map((w) => w.facePresence < 0.2)
  const output = windows.map(() => false)
  let runStart: number | null = null
  for (let i = 0; i <= flags.length; i += 1) {
    const flag = i < flags.length ? flags[i] : false
    if (flag && runStart === null) runStart = i
    if ((!flag || i === flags.length) && runStart !== null) {
      const runEnd = i
      if (runEnd - runStart >= minDuration) {
        for (let j = runStart; j < runEnd; j += 1) output[j] = true
      }
      runStart = null
    }
  }
  return output
}

const detectFillerWindows = (windows: EngagementWindow[], silences: TimeRange[], aggressiveMode = false) => {
  const isSilentAt = (time: number) => {
    const windowEnd = time + 1
    return silences.some((s) => time < s.end && windowEnd > s.start)
  }
  return windows.map((w) => {
    if (isSilentAt(w.time)) return false
    const speechCeiling = aggressiveMode ? 0.35 : 0.31
    const energyCeiling = aggressiveMode ? 0.34 : 0.3
    const lowSpeech = w.speechIntensity < speechCeiling
    const lowEnergy = w.audioEnergy < energyCeiling && w.audioEnergy > 0.03
    const lowExcitement = w.vocalExcitement < (aggressiveMode ? 0.4 : 0.36)
    const lowEmotion = w.emotionIntensity < (aggressiveMode ? 0.45 : 0.4)
    const staticVisual = w.motionScore < 0.18 && w.sceneChangeRate < 0.18
    return lowSpeech && lowEnergy && (lowExcitement || lowEmotion || staticVisual)
  })
}

const buildBoringFlags = (windows: EngagementWindow[], silences: TimeRange[], aggressiveMode = false) => {
  const faceAbsent = buildFaceAbsenceFlags(windows, aggressiveMode ? 1.5 : 2)
  const fillerFlags = detectFillerWindows(windows, silences, aggressiveMode)
  const isSilentAt = (time: number) => {
    const windowEnd = time + 1
    return silences.some((s) => time < s.end && windowEnd > s.start)
  }
  return windows.map((w, idx) => {
    const silent = isSilentAt(w.time) && w.audioEnergy < (aggressiveMode ? 0.18 : 0.16)
    const lowSpeech = w.speechIntensity < (aggressiveMode ? 0.35 : 0.31) && w.audioEnergy < (aggressiveMode ? 0.28 : 0.24)
    const lowMotion = w.motionScore < (aggressiveMode ? 0.3 : 0.26) && w.sceneChangeRate < (aggressiveMode ? 0.3 : 0.26)
    const staticVisual = w.motionScore < (aggressiveMode ? 0.18 : 0.14) && w.sceneChangeRate < (aggressiveMode ? 0.18 : 0.14)
    const lowExcitement = w.vocalExcitement < (aggressiveMode ? 0.4 : 0.36)
    const lowEmotion = w.emotionIntensity < (aggressiveMode ? 0.45 : 0.4)
    const excitementScore =
      0.46 * w.score +
      0.2 * w.speechIntensity +
      0.14 * w.vocalExcitement +
      0.1 * w.emotionIntensity +
      0.1 * w.sceneChangeRate
    const lowExcitementComposite = excitementScore < (aggressiveMode ? 0.42 : 0.38)
    const strongWindow = w.score > (aggressiveMode ? 0.7 : 0.66) || (w.speechIntensity > 0.62 && w.vocalExcitement > 0.56)
    const retentionRisk = 1 - (0.55 * w.score + 0.18 * w.speechIntensity + 0.15 * w.vocalExcitement + 0.12 * w.emotionIntensity)
    const lowRetention = retentionRisk > (aggressiveMode ? 0.58 : 0.61) && w.score < (aggressiveMode ? 0.43 : 0.4)
    const weakWindow =
      w.score < (aggressiveMode ? 0.4 : 0.36) &&
      w.speechIntensity < (aggressiveMode ? 0.45 : 0.42) &&
      w.vocalExcitement < (aggressiveMode ? 0.42 : 0.38)
    const emotionalMoment = w.emotionIntensity > (aggressiveMode ? 0.74 : 0.68) || w.vocalExcitement > (aggressiveMode ? 0.74 : 0.68) || w.emotionalSpike > 0
    if (strongWindow) return false
    if (emotionalMoment) return false
    if (silent) return true
    if (fillerFlags[idx]) return true
    if (lowRetention && (lowSpeech || lowMotion || lowEmotion || faceAbsent[idx])) return true
    if (weakWindow && (lowMotion || staticVisual || lowExcitement)) return true
    if (faceAbsent[idx] && (lowSpeech || lowMotion || lowExcitementComposite)) return true
    if (lowSpeech && (lowMotion || lowExcitement || lowEmotion)) return true
    if (lowExcitementComposite && (lowMotion || lowSpeech || staticVisual)) return true
    if (staticVisual && w.audioEnergy < (aggressiveMode ? 0.28 : 0.24)) return true
    if (aggressiveMode && lowExcitementComposite && lowEmotion) return true
    return false
  })
}

const buildBoringCuts = (flags: boolean[], aggressiveMode = false) => {
  const ranges: TimeRange[] = []
  let runStart: number | null = null
  for (let i = 0; i <= flags.length; i += 1) {
    const flag = i < flags.length ? flags[i] : false
    if (flag && runStart === null) runStart = i
    if ((!flag || i === flags.length) && runStart !== null) {
      const runEnd = i
      const runLen = runEnd - runStart
      if (runLen >= CUT_MIN) {
        if (runLen <= CUT_MAX) {
          ranges.push({ start: runStart, end: runEnd })
        } else {
          const maxRemove = runLen * (aggressiveMode ? AGGRESSIVE_MAX_CUT_RATIO : MAX_CUT_RATIO)
          let removed = 0
          const guard = aggressiveMode ? Math.max(0.2, CUT_GUARD_SEC - 0.1) : CUT_GUARD_SEC
          let cursor = runStart + guard
          const endLimit = runEnd - guard
          const gapMultiplier = aggressiveMode ? AGGRESSIVE_CUT_GAP_MULTIPLIER : 1
          let patternIdx = 0
          while (cursor + CUT_MIN <= endLimit) {
            let cutLen = CUT_LEN_PATTERN[patternIdx % CUT_LEN_PATTERN.length]
            if (aggressiveMode) cutLen += 0.35
            cutLen = Math.max(CUT_MIN, Math.min(CUT_MAX, cutLen))
            let actualLen = Math.min(cutLen, endLimit - cursor)
            if (actualLen < CUT_MIN) break
            if (removed + actualLen > maxRemove) {
              actualLen = Math.max(CUT_MIN, maxRemove - removed)
              if (actualLen < CUT_MIN) break
            }
            ranges.push({ start: cursor, end: cursor + actualLen })
            removed += actualLen
            cursor += actualLen + CUT_GAP_PATTERN[patternIdx % CUT_GAP_PATTERN.length] * gapMultiplier
            patternIdx += 1
          }
        }
      }
      runStart = null
    }
  }
  return mergeRanges(ranges)
}

const buildStrategicFallbackCuts = (windows: EngagementWindow[], durationSeconds: number, aggressiveMode = false) => {
  const minDuration = aggressiveMode ? 35 : 40
  if (!windows.length || durationSeconds < minDuration) return [] as TimeRange[]
  const edgePadding = aggressiveMode ? 7 : 8
  const cutLead = aggressiveMode ? 2.4 : 2.2
  const cutTail = aggressiveMode ? 4.2 : 3.8
  const candidates = windows
    .filter((window) => window.time >= edgePadding && window.time <= Math.max(edgePadding, durationSeconds - 6))
    .map((window) => ({
      start: Math.max(0, window.time - cutLead),
      end: Math.min(durationSeconds, window.time + cutTail),
      score:
        0.58 * window.score +
        0.16 * window.speechIntensity +
        0.12 * window.vocalExcitement +
        0.08 * window.emotionIntensity +
        0.06 * window.sceneChangeRate
    }))
    .sort((a, b) => a.score - b.score)

  const desired = clamp(
    Math.floor(durationSeconds / (aggressiveMode ? 85 : 95)) + 1,
    1,
    aggressiveMode ? 9 : 7
  )
  const selected: TimeRange[] = []
  const spacingBuffer = Math.max(
    aggressiveMode ? 5.5 : 7,
    durationSeconds / Math.max(10, desired * 2.5)
  )
  for (const candidate of candidates) {
    if (selected.length >= desired) break
    const overlaps = selected.some(
      (existing) => candidate.start < existing.end + spacingBuffer && candidate.end > existing.start - spacingBuffer
    )
    if (overlaps) continue
    selected.push({ start: candidate.start, end: candidate.end })
  }
  return mergeRanges(selected)
}

const getRangesDurationSeconds = (ranges: TimeRange[]) => {
  return ranges.reduce((sum, range) => sum + Math.max(0, range.end - range.start), 0)
}

const getMinimumRemovedSeconds = (durationSeconds: number, aggressiveMode = false) => {
  if (!Number.isFinite(durationSeconds) || durationSeconds < LONG_FORM_RESCUE_MIN_DURATION) return 0
  const ratio = aggressiveMode ? LONG_FORM_MIN_EDIT_RATIO + 0.01 : LONG_FORM_MIN_EDIT_RATIO
  const target = durationSeconds * ratio
  return clamp(target, LONG_FORM_MIN_EDIT_SECONDS, aggressiveMode ? LONG_FORM_MAX_EDIT_SECONDS + 30 : LONG_FORM_MAX_EDIT_SECONDS)
}

const buildLongFormRescueCuts = (
  windows: EngagementWindow[],
  durationSeconds: number,
  missingSeconds: number,
  aggressiveMode = false,
  existingCuts: TimeRange[] = []
) => {
  if (!windows.length || durationSeconds < LONG_FORM_RESCUE_MIN_DURATION || missingSeconds <= 0) {
    return [] as TimeRange[]
  }
  const edgePadding = aggressiveMode ? 6 : 8
  const cutLength = aggressiveMode ? 6.1 : 5.6
  const minSpacing = aggressiveMode ? 8.5 : 10.5
  const existing = mergeRanges(existingCuts)
  const candidates = windows
    .filter((window) => window.time >= edgePadding && window.time <= Math.max(edgePadding, durationSeconds - edgePadding))
    .map((window) => {
      const center = window.time + 0.5
      const start = clamp(center - cutLength / 2, edgePadding, Math.max(edgePadding, durationSeconds - edgePadding - cutLength))
      const end = Math.min(durationSeconds - edgePadding, start + cutLength)
      const engagementCost =
        0.58 * window.score +
        0.16 * window.speechIntensity +
        0.12 * window.vocalExcitement +
        0.08 * window.emotionIntensity +
        0.06 * window.sceneChangeRate
      return {
        start: Number(start.toFixed(3)),
        end: Number(end.toFixed(3)),
        score: engagementCost
      }
    })
    .filter((entry) => entry.end - entry.start >= CUT_MIN - 0.4)
    .sort((a, b) => a.score - b.score)

  const targetCuts = clamp(
    Math.ceil(missingSeconds / Math.max(CUT_MIN - 0.4, cutLength - 0.2)),
    1,
    Math.max(4, Math.floor(durationSeconds / (aggressiveMode ? 20 : 24)))
  )

  const selected: TimeRange[] = []
  for (const candidate of candidates) {
    if (selected.length >= targetCuts) break
    const overlapsExisting = existing.some(
      (range) => candidate.start < range.end + 0.35 && candidate.end > range.start - 0.35
    )
    if (overlapsExisting) continue
    const tooClose = selected.some(
      (range) => candidate.start < range.end + minSpacing && candidate.end > range.start - minSpacing
    )
    if (tooClose) continue
    selected.push({ start: candidate.start, end: candidate.end })
  }

  return mergeRanges(selected)
}

const buildConservativeFallbackSegments = (durationSeconds: number, aggressiveMode = false) => {
  const total = Number.isFinite(durationSeconds) ? Math.max(0, durationSeconds) : 0
  if (total <= 0.25) {
    return [{ start: 0, end: total, speed: 1 } as Segment]
  }
  const base: Segment[] = [{ start: 0, end: Number(total.toFixed(3)), speed: 1 }]
  if (total < LONG_FORM_RESCUE_MIN_DURATION) return base

  const cutLength = aggressiveMode ? 5.6 : 5.1
  const spacing = aggressiveMode ? 42 : 55
  const startOffset = aggressiveMode ? 12 : 14
  const edgePadding = aggressiveMode ? 6 : 8
  const cuts: TimeRange[] = []
  for (let cursor = startOffset; cursor + cutLength < total - edgePadding; cursor += spacing) {
    const start = Number(cursor.toFixed(3))
    const end = Number(Math.min(total - edgePadding, cursor + cutLength).toFixed(3))
    if (end - start > 0.3) cuts.push({ start, end })
  }
  if (!cuts.length) return base
  const fallback = subtractRanges(base, cuts)
  return fallback.length ? fallback : base
}

const computeEditedRuntimeSeconds = (segments: Segment[]) => {
  return segments.reduce((sum, seg) => {
    const speed = seg.speed && seg.speed > 0 ? seg.speed : 1
    return sum + Math.max(0, (seg.end - seg.start) / speed)
  }, 0)
}

const computeKeptTimelineSeconds = (segments: Segment[]) => {
  return segments.reduce((sum, seg) => sum + Math.max(0, seg.end - seg.start), 0)
}

const computeHookEmotionProfile = (
  windows: EngagementWindow[],
  start: number,
  end: number
) => {
  if (!windows.length || end <= start) {
    return {
      emotionalPull: null as number | null,
      tensionRamp: null as number | null
    }
  }
  const duration = Math.max(0.6, end - start)
  const earlyEnd = start + duration * 0.38
  const lateStart = start + duration * 0.62
  const early = averageWindowMetric(windows, start, earlyEnd, (window) => (
    0.52 * window.emotionIntensity +
    0.24 * window.vocalExcitement +
    0.14 * (window.curiosityTrigger ?? 0) +
    0.1 * (window.actionSpike ?? 0)
  ))
  const late = averageWindowMetric(windows, lateStart, end, (window) => (
    0.5 * window.emotionIntensity +
    0.22 * window.vocalExcitement +
    0.16 * (window.curiosityTrigger ?? 0) +
    0.12 * (window.actionSpike ?? 0)
  ))
  const full = averageWindowMetric(windows, start, end, (window) => (
    0.48 * window.emotionIntensity +
    0.22 * window.vocalExcitement +
    0.14 * (window.curiosityTrigger ?? 0) +
    0.1 * (window.actionSpike ?? 0) +
    0.06 * window.sceneChangeRate
  ))
  return {
    emotionalPull: Number(clamp01(full).toFixed(4)),
    tensionRamp: Number(clamp((late - early), -1, 1).toFixed(4))
  }
}

const buildSegmentStatsSummary = (segments: Segment[]) => {
  const normalized = segments
    .map((segment) => ({
      start: Number(segment.start),
      end: Number(segment.end),
      speed: Number(segment.speed && segment.speed > 0 ? segment.speed : 1)
    }))
    .filter((segment) => Number.isFinite(segment.start) && Number.isFinite(segment.end) && segment.end > segment.start)
    .sort((a, b) => a.start - b.start)
  if (!normalized.length) {
    return {
      segmentCount: 0,
      cutCount: 0,
      keptTimelineSeconds: 0,
      editedRuntimeSeconds: 0,
      averageSegmentSeconds: 0,
      medianSegmentSeconds: 0,
      minSegmentSeconds: 0,
      maxSegmentSeconds: 0,
      withinTargetRatio: 0,
      averageSpeed: 1,
      maxSpeed: 1,
      averageGapSeconds: 0,
      removedByGapsSeconds: 0
    }
  }
  const durations = normalized.map((segment) => Math.max(0, segment.end - segment.start))
  const sortedDurations = durations.slice().sort((a, b) => a - b)
  const medianDuration = sortedDurations[Math.floor(sortedDurations.length / 2)] || 0
  const withinTargetCount = durations.filter((duration) => duration >= CUT_MIN - 0.15 && duration <= CUT_MAX + 0.15).length
  const gaps: number[] = []
  for (let index = 1; index < normalized.length; index += 1) {
    const gap = Math.max(0, normalized[index].start - normalized[index - 1].end)
    if (gap > 0) gaps.push(gap)
  }
  const averageGap = gaps.length ? gaps.reduce((sum, gap) => sum + gap, 0) / gaps.length : 0
  const speeds = normalized.map((segment) => segment.speed)
  const avgSpeed = speeds.reduce((sum, speed) => sum + speed, 0) / Math.max(1, speeds.length)
  const keptTimelineSeconds = computeKeptTimelineSeconds(normalized as Segment[])
  const editedRuntimeSeconds = computeEditedRuntimeSeconds(normalized as Segment[])
  return {
    segmentCount: normalized.length,
    cutCount: Math.max(0, normalized.length - 1),
    keptTimelineSeconds: Number(keptTimelineSeconds.toFixed(3)),
    editedRuntimeSeconds: Number(editedRuntimeSeconds.toFixed(3)),
    averageSegmentSeconds: Number((durations.reduce((sum, duration) => sum + duration, 0) / durations.length).toFixed(3)),
    medianSegmentSeconds: Number(medianDuration.toFixed(3)),
    minSegmentSeconds: Number(Math.min(...durations).toFixed(3)),
    maxSegmentSeconds: Number(Math.max(...durations).toFixed(3)),
    withinTargetRatio: Number((withinTargetCount / Math.max(1, normalized.length)).toFixed(4)),
    averageSpeed: Number(avgSpeed.toFixed(3)),
    maxSpeed: Number(Math.max(...speeds).toFixed(3)),
    averageGapSeconds: Number(averageGap.toFixed(3)),
    removedByGapsSeconds: Number(gaps.reduce((sum, gap) => sum + gap, 0).toFixed(3))
  }
}

const buildRetentionMetadataSummary = ({
  durationSeconds,
  segments,
  windows,
  hook,
  styleProfile,
  nicheProfile,
  styleArchetypeBlend,
  behaviorStyleProfile,
  styleFeatureSnapshot,
  autoEscalationEvents,
  judge,
  strategy,
  retentionScore,
  retentionScoreBefore,
  retentionScoreAfter,
  attempts,
  patternInterruptCount,
  patternInterruptDensity,
  boredomRemovedRatio,
  qualityGateOverride,
  optimizationNotes,
  hookSelectionSource,
  contentFormat,
  targetPlatform,
  strategyProfile
}: {
  durationSeconds: number
  segments: Segment[]
  windows: EngagementWindow[]
  hook?: HookCandidate | null
  styleProfile?: ContentStyleProfile | null
  nicheProfile?: VideoNicheProfile | null
  styleArchetypeBlend?: StyleArchetypeBlend | null
  behaviorStyleProfile?: RetentionBehaviorStyleProfile | null
  styleFeatureSnapshot?: TimelineFeatureSnapshot | null
  autoEscalationEvents?: AutoEscalationEvent[] | null
  judge?: RetentionJudgeReport | null
  strategy?: RetentionRetryStrategy | null
  retentionScore?: number | null
  retentionScoreBefore?: number | null
  retentionScoreAfter?: number | null
  attempts?: RetentionAttemptRecord[]
  patternInterruptCount?: number
  patternInterruptDensity?: number
  boredomRemovedRatio?: number
  qualityGateOverride?: { applied: boolean; reason: string } | null
  optimizationNotes?: string[]
  hookSelectionSource?: 'auto' | 'user_selected' | 'fallback'
  contentFormat?: RetentionContentFormat | null
  targetPlatform?: RetentionTargetPlatform | null
  strategyProfile?: RetentionStrategyProfile | null
}) => {
  const segmentStats = buildSegmentStatsSummary(segments)
  const hookRangeEnd = hook ? hook.start + hook.duration : null
  const hookEmotion = hook && hookRangeEnd !== null
    ? computeHookEmotionProfile(windows, hook.start, hookRangeEnd)
    : { emotionalPull: null as number | null, tensionRamp: null as number | null }
  const safeDuration = Math.max(0.1, Number.isFinite(durationSeconds) ? durationSeconds : 0)
  const removedSeconds = Math.max(0, safeDuration - segmentStats.keptTimelineSeconds)
  const compressionRatio = safeDuration > 0
    ? clamp01((safeDuration - segmentStats.editedRuntimeSeconds) / safeDuration)
    : 0
  const resolvedArchetypeBlend = styleArchetypeBlend || behaviorStyleProfile?.archetypeBlend || null
  const autoEscalationCount = Array.isArray(autoEscalationEvents) ? autoEscalationEvents.length : 0
  const improvements: string[] = []
  if (styleProfile?.style) {
    improvements.push(
      `Detected ${styleProfile.style} style signals (confidence ${(styleProfile.confidence * 100).toFixed(0)}%) and adapted pacing profile.`
    )
  }
  if (nicheProfile?.niche) {
    improvements.push(
      `Detected ${nicheProfile.niche.replace('_', ' ')} niche (confidence ${(nicheProfile.confidence * 100).toFixed(0)}%) and tuned pacing accordingly.`
    )
  }
  if (behaviorStyleProfile) {
    improvements.push(
      `Applied adaptive behavior profile (${behaviorStyleProfile.styleName}) with ~${behaviorStyleProfile.avgCutInterval.toFixed(1)}s average cut interval.`
    )
  }
  if (hook) {
    const hookSourceLabel =
      hookSelectionSource === 'user_selected'
        ? 'User-selected hook was locked to the opening.'
        : hookSelectionSource === 'fallback'
          ? 'Fallback hook was used to keep the opening attention-grabbing.'
          : 'Best-performing hook candidate was moved to the opening.'
    improvements.push(
      `${hookSourceLabel} Hook window ${hook.start.toFixed(1)}s-${(hook.start + hook.duration).toFixed(1)}s.`
    )
  }
  if (removedSeconds >= 1.2) {
    improvements.push(`Removed ${removedSeconds.toFixed(1)}s of low-signal footage to tighten pacing.`)
  }
  if (segmentStats.cutCount > 0) {
    improvements.push(`Applied ${segmentStats.cutCount} cut${segmentStats.cutCount === 1 ? '' : 's'} to reduce dead time.`)
  }
  if (segmentStats.averageSpeed > 1.02) {
    improvements.push(`Applied selective speed-ups (avg ${segmentStats.averageSpeed.toFixed(2)}x).`)
  }
  if (Number(patternInterruptCount ?? 0) > 0) {
    improvements.push(`Inserted ${Number(patternInterruptCount)} pattern interrupt${Number(patternInterruptCount) === 1 ? '' : 's'} for retention resets.`)
  }
  if (autoEscalationCount > 0) {
    improvements.push(`Auto-escalation guarantee fired ${autoEscalationCount} time${autoEscalationCount === 1 ? '' : 's'} to prevent flat pacing.`)
  }
  if (strategy && strategy !== 'BASELINE') {
    improvements.push(`Used ${strategy.replace('_', ' ').toLowerCase()} retry strategy to improve retention quality.`)
  }
  if (targetPlatform && targetPlatform !== 'auto') {
    const platformLabel = targetPlatform === 'instagram_reels'
      ? 'Instagram Reels'
      : targetPlatform === 'tiktok'
        ? 'TikTok'
        : 'YouTube'
    improvements.push(`Tuned pacing and quality gate targets for ${platformLabel}.`)
  }
  if (qualityGateOverride?.applied && qualityGateOverride.reason) {
    improvements.push(`Quality gate override: ${qualityGateOverride.reason}`)
  }
  const normalizedNotes = Array.isArray(optimizationNotes)
    ? optimizationNotes
        .map((note) => String(note || '').trim())
        .filter(Boolean)
        .slice(0, 8)
    : []
  for (const note of normalizedNotes) {
    if (!improvements.includes(note)) improvements.push(note)
  }
  const resolvedAfterScore = Number.isFinite(Number(retentionScoreAfter))
    ? Number(retentionScoreAfter)
    : Number.isFinite(Number(retentionScore))
      ? Number(retentionScore)
      : Number.isFinite(Number(judge?.retention_score))
        ? Number(judge?.retention_score)
        : null
  const resolvedBeforeScore = Number.isFinite(Number(retentionScoreBefore))
    ? Number(retentionScoreBefore)
    : null
  const retentionDelta = resolvedBeforeScore !== null && resolvedAfterScore !== null
    ? Number((resolvedAfterScore - resolvedBeforeScore).toFixed(1))
    : null
  if (retentionDelta !== null && retentionDelta >= 0.5) {
    improvements.unshift(`Projected retention improved by +${retentionDelta.toFixed(1)} points vs source baseline.`)
  } else if (retentionDelta !== null && retentionDelta <= -0.5) {
    improvements.unshift(`Projected retention is ${Math.abs(retentionDelta).toFixed(1)} points below source baseline.`)
  }
  return {
    metadataVersion: 2,
    generatedAt: toIsoNow(),
    hook: hook
      ? {
          start: Number(hook.start.toFixed(3)),
          end: Number((hook.start + hook.duration).toFixed(3)),
          duration: Number(hook.duration.toFixed(3)),
          score: Number(hook.score.toFixed(4)),
          auditScore: Number(hook.auditScore.toFixed(4)),
          auditPassed: hook.auditPassed,
          synthetic: Boolean(hook.synthetic),
          reason: hook.reason || null,
          text: hook.text || null,
          emotionalPull: hookEmotion.emotionalPull,
          tensionRamp: hookEmotion.tensionRamp
        }
      : null,
    pacing: {
      targetWindowSeconds: { min: CUT_MIN, max: CUT_MAX },
      ...segmentStats
    },
    niche: nicheProfile
      ? {
          name: nicheProfile.niche,
          confidence: nicheProfile.confidence,
          rationale: nicheProfile.rationale.slice(0, 3)
        }
      : null,
    style: {
      contentStyle: styleProfile
        ? {
            name: styleProfile.style,
            confidence: Number(styleProfile.confidence.toFixed(4)),
            rationale: styleProfile.rationale.slice(0, 3)
          }
        : null,
      archetypeBlend: resolvedArchetypeBlend,
      behaviorProfile: behaviorStyleProfile
        ? {
            styleName: behaviorStyleProfile.styleName,
            avgCutInterval: Number(behaviorStyleProfile.avgCutInterval.toFixed(3)),
            patternInterruptInterval: Number(behaviorStyleProfile.patternInterruptInterval.toFixed(3)),
            zoomFrequencyPer10Seconds: Number(behaviorStyleProfile.zoomFrequencyPer10Seconds.toFixed(4)),
            captionEmphasisRatePer10Seconds: Number(behaviorStyleProfile.captionEmphasisRatePer10Seconds.toFixed(4)),
            energyEscalationCurve: behaviorStyleProfile.energyEscalationCurve,
            autoEscalationWindowSec: Number(behaviorStyleProfile.autoEscalationWindowSec.toFixed(3))
          }
        : null
    },
    timeline: {
      sourceDurationSeconds: Number(safeDuration.toFixed(3)),
      removedSeconds: Number(removedSeconds.toFixed(3)),
      compressionRatio: Number(compressionRatio.toFixed(4)),
      boredomRemovedRatio: Number(clamp01(Number(boredomRemovedRatio ?? 0)).toFixed(4)),
      autoEscalationCount
    },
    retention: {
      score: resolvedAfterScore,
      beforeScore: resolvedBeforeScore,
      afterScore: resolvedAfterScore,
      delta: retentionDelta,
      strategy: strategy ?? null,
      strategyProfile: strategyProfile ?? judge?.strategy_profile ?? null,
      contentFormat: contentFormat ?? judge?.content_format ?? null,
      targetPlatform: targetPlatform ?? judge?.target_platform ?? null,
      hookSelectionSource: hookSelectionSource ?? 'auto',
      patternInterruptCount: Number(patternInterruptCount ?? 0),
      patternInterruptDensity: Number((patternInterruptDensity ?? 0).toFixed(4)),
      styleTimelineFeatures: styleFeatureSnapshot ?? null,
      whyKeepWatching: Array.isArray(judge?.why_keep_watching) ? judge!.why_keep_watching.slice(0, 3) : [],
      genericFlags: Array.isArray(judge?.what_is_generic) ? judge!.what_is_generic.slice(0, 3) : [],
      improvements: improvements.slice(0, 12)
    },
    qualityGate: judge
      ? {
          passed: judge.passed,
          scores: {
            retention: judge.retention_score,
            hook: judge.hook_strength,
            pacing: judge.pacing_score,
            clarity: judge.clarity_score,
            emotionalPull: judge.emotional_pull
          },
          thresholds: judge.applied_thresholds,
          gateMode: judge.gate_mode,
          override: qualityGateOverride
        }
      : null,
    attempts: (attempts || []).slice(0, 6).map((attempt) => ({
      attempt: attempt.attempt,
      strategy: attempt.strategy,
      passed: attempt.judge.passed,
      retentionScore: attempt.judge.retention_score,
      hookStrength: attempt.judge.hook_strength,
      emotionalPull: attempt.judge.emotional_pull,
      pacingScore: attempt.judge.pacing_score,
      predictedRetention: attempt.predictedRetention ?? null,
      variantScore: attempt.variantScore ?? null
    }))
  }
}

const buildVerticalMetadataSummary = ({
  durationSeconds,
  clipRanges,
  windows,
  hookCandidates
}: {
  durationSeconds: number
  clipRanges: TimeRange[]
  windows: EngagementWindow[]
  hookCandidates: HookCandidate[]
}) => {
  const safeDuration = Math.max(0.1, Number.isFinite(durationSeconds) ? durationSeconds : 0)
  const clipDetails = clipRanges.map((range, index) => {
    const duration = Math.max(0, range.end - range.start)
    const engagementScore = averageWindowMetric(windows, range.start, range.end, (window) => (
      0.36 * (window.hookScore ?? window.score) +
      0.2 * window.emotionIntensity +
      0.14 * window.vocalExcitement +
      0.1 * (window.curiosityTrigger ?? 0) +
      0.1 * (window.actionSpike ?? 0) +
      0.1 * window.sceneChangeRate
    ))
    return {
      clip: index + 1,
      start: Number(range.start.toFixed(3)),
      end: Number(range.end.toFixed(3)),
      duration: Number(duration.toFixed(3)),
      engagementScore: Number(clamp01(engagementScore).toFixed(4))
    }
  })
  const coverage = clipRanges.reduce((sum, range) => sum + Math.max(0, range.end - range.start), 0)
  return {
    metadataVersion: 2,
    generatedAt: toIsoNow(),
    selectionMode: 'best_parts_ranked',
    clipCount: clipRanges.length,
    coverageRatio: Number(clamp01(coverage / safeDuration).toFixed(4)),
    clips: clipDetails,
    topHookReference: hookCandidates.length
      ? {
          start: Number(hookCandidates[0].start.toFixed(3)),
          duration: Number(hookCandidates[0].duration.toFixed(3)),
          score: Number(hookCandidates[0].score.toFixed(4))
        }
      : null
  }
}

const computeEditImpactRatio = (segments: Segment[], durationSeconds: number) => {
  const total = Number.isFinite(durationSeconds) ? Math.max(0, durationSeconds) : 0
  if (total <= 0) return 0
  const kept = clamp(computeKeptTimelineSeconds(segments), 0, total)
  const runtime = clamp(computeEditedRuntimeSeconds(segments), 0, total)
  const cutImpact = (total - kept) / total
  const paceImpact = Math.max(0, (kept - runtime) / total)
  return Math.max(0, cutImpact + paceImpact)
}

const buildGuaranteedFallbackSegments = (durationSeconds: number, options: EditOptions) => {
  const total = Number.isFinite(durationSeconds) ? Math.max(0, durationSeconds) : 0
  if (total <= 0.25) return [{ start: 0, end: total, speed: 1 } as Segment]

  const normalizedTotal = roundForFilter(total)
  const introLen = clamp(normalizedTotal * 0.06, 4, 10)
  const outroLen = clamp(normalizedTotal * 0.04, 3, 8)
  const workingEnd = Math.max(introLen + MIN_RENDER_SEGMENT_SECONDS, normalizedTotal - outroLen)
  const keepBase = options.aggressiveMode ? 6.2 : 7.4
  const cutGap = options.aggressiveMode ? 2.2 : 1.6
  const speedPattern = options.onlyCuts ? [1] : [1.18, 1.24, 1.12, 1.2]
  const segments: Segment[] = [
    { start: 0, end: roundForFilter(introLen), speed: 1 }
  ]

  let cursor = roundForFilter(introLen + cutGap * 0.6)
  let index = 0
  while (cursor < workingEnd - MIN_RENDER_SEGMENT_SECONDS) {
    const keepLen = keepBase + (index % 3 === 0 ? 0.85 : index % 3 === 1 ? -0.55 : 0.3)
    const end = Math.min(workingEnd, cursor + keepLen)
    const duration = end - cursor
    if (duration < MIN_RENDER_SEGMENT_SECONDS) break
    const speed = speedPattern[index % speedPattern.length]
    segments.push({
      start: roundForFilter(cursor),
      end: roundForFilter(end),
      speed: Number(clamp(speed, 1, 1.36).toFixed(3))
    })
    cursor = roundForFilter(end + cutGap)
    index += 1
  }

  const outroStart = Math.max(0, roundForFilter(normalizedTotal - outroLen))
  if (!segments.some((segment) => segment.end >= outroStart - MIN_RENDER_SEGMENT_SECONDS)) {
    segments.push({ start: outroStart, end: normalizedTotal, speed: 1 })
  } else {
    const tail = segments[segments.length - 1]
    tail.end = normalizedTotal
    tail.speed = options.onlyCuts ? 1 : tail.speed
  }

  return segments
}

const buildDeterministicFallbackEditPlan = (durationSeconds: number, options: EditOptions): EditPlan => {
  const total = Number.isFinite(durationSeconds) ? Math.max(0, durationSeconds) : 0
  const segments = buildGuaranteedFallbackSegments(total, options)
  const runtimeStyleResolution = resolveRuntimeStyleProfile({
    mode: options.retentionStrategyProfile,
    contentStyle: 'story',
    niche: 'story',
    contentStyleConfidence: 0.45,
    nicheConfidence: 0.45,
    explicitBlend: options.styleArchetypeBlend || null
  })
  const baseRange = [{ start: 0, end: total, speed: 1 }]
  const keepRanges = segments.map((segment) => ({ start: segment.start, end: segment.end, speed: 1 }))
  const removedSegments = total > 0 ? subtractRanges(baseRange, keepRanges) : []
  const compressedSegments = segments
    .filter((segment) => (segment.speed ?? 1) > 1.01)
    .map((segment) => ({ start: segment.start, end: segment.end }))
  const hookDuration = total > 0
    ? clamp(Math.min(HOOK_MAX, Math.max(HOOK_MIN, total * 0.12)), Math.min(HOOK_MIN, total), total)
    : 0
  const fallbackHook: HookCandidate = {
    start: 0,
    duration: roundForFilter(hookDuration),
    score: 0.45,
    auditScore: 0.5,
    auditPassed: hookDuration >= HOOK_MIN,
    text: '',
    reason: hookDuration >= HOOK_MIN
      ? 'Deterministic fallback hook used due analysis failure.'
      : 'Fallback hook is shorter than required 5-8s range.',
    synthetic: true
  }
  const fallbackNicheProfile: VideoNicheProfile = {
    niche: 'story',
    confidence: 0.5,
    rationale: ['Fallback mode used; defaulting niche to story pacing.'],
    styleAlignment: null,
    metrics: {
      avgSpeech: 0,
      avgScene: 0,
      avgEmotion: 0,
      spikeRatio: 0,
      transcriptCueCount: 0,
      durationSeconds: Number(total.toFixed(3))
    }
  }
  const fallbackDecisionTimeline = buildEditDecisionTimeline({
    styleName: runtimeStyleResolution.profile.styleName,
    hook: { start: fallbackHook.start },
    segments,
    patternInterruptCount: 0
  })
  const fallbackStyleFeatureSnapshot = extractTimelineFeatures({
    timeline: fallbackDecisionTimeline,
    durationSeconds: total
  })
  return {
    hook: fallbackHook,
    segments,
    silences: [],
    removedSegments,
    compressedSegments,
    engagementWindows: [],
    hookCandidates: [fallbackHook],
    patternInterruptCount: 0,
    patternInterruptDensity: 0,
    boredomRemovedRatio: Number(clamp01(getRangesDurationSeconds(removedSegments) / Math.max(0.1, total)).toFixed(4)),
    storyReorderMap: segments.map((segment, orderedIndex) => ({
      sourceStart: Number(segment.start.toFixed(3)),
      sourceEnd: Number(segment.end.toFixed(3)),
      orderedIndex
    })),
    hookFailureReason: fallbackHook.auditPassed ? null : fallbackHook.reason,
    nicheProfile: fallbackNicheProfile,
    styleArchetypeBlend: runtimeStyleResolution.blend,
    behaviorStyleProfile: runtimeStyleResolution.profile,
    autoEscalationEvents: [],
    editDecisionTimeline: fallbackDecisionTimeline,
    styleFeatureSnapshot: fallbackStyleFeatureSnapshot
  }
}

const buildRangesFromFlags = (flags: boolean[], minDurationSeconds = 1) => {
  const out: TimeRange[] = []
  let runStart: number | null = null
  for (let i = 0; i <= flags.length; i += 1) {
    const flag = i < flags.length ? flags[i] : false
    if (flag && runStart === null) runStart = i
    if ((!flag || i === flags.length) && runStart !== null) {
      const runEnd = i
      if (runEnd - runStart >= minDurationSeconds) out.push({ start: runStart, end: runEnd })
      runStart = null
    }
  }
  return out
}

const buildSilenceTrimCuts = (silences: TimeRange[], durationSeconds: number, aggressiveMode = false) => {
  if (!silences.length || durationSeconds <= 0) return [] as TimeRange[]
  const keepPadding = aggressiveMode ? 0.08 : 0.12
  const minTrim = aggressiveMode ? 0.22 : 0.28
  const edgePadding = aggressiveMode ? 0.14 : 0.18
  const trims: TimeRange[] = []
  for (const silence of silences) {
    const rawStart = clamp(silence.start, 0, durationSeconds)
    const rawEnd = clamp(silence.end, 0, durationSeconds)
    if (rawEnd - rawStart < SILENCE_MIN) continue
    let start = rawStart + keepPadding
    let end = rawEnd - keepPadding
    if (start < edgePadding) start = edgePadding
    if (end > durationSeconds - edgePadding) end = durationSeconds - edgePadding
    if (end - start < minTrim) continue
    trims.push({
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3))
    })
  }
  return mergeRanges(trims)
}

const buildContinuityProtectionRanges = (windows: EngagementWindow[], aggressiveMode = false) => {
  if (!windows.length) return [] as TimeRange[]
  const total = windows.length
  const activeWindows = windows.filter((window) => (
    window.audioEnergy > 0.02 ||
    window.sceneChangeRate > 0 ||
    window.speechIntensity > 0.05 ||
    window.score > 0.08
  ))
  const basis = activeWindows.length ? activeWindows : windows
  const avgScene = basis.reduce((sum, window) => sum + window.sceneChangeRate, 0) / basis.length
  const avgSpeech = basis.reduce((sum, window) => sum + window.speechIntensity, 0) / basis.length
  const sceneFloor = clamp(avgScene + (aggressiveMode ? 0.02 : 0.06), 0.24, 0.72)
  const speechFloor = clamp(avgSpeech + 0.04, aggressiveMode ? 0.34 : 0.38, 0.82)
  const emotionFloor = aggressiveMode ? 0.62 : 0.66
  const preserveFlags = windows.map((window) => {
    const sceneAnchor = window.sceneChangeRate >= sceneFloor
    const speechAnchor = window.speechIntensity >= speechFloor
    const emotionalAnchor = window.emotionIntensity >= emotionFloor || window.emotionalSpike > 0
    const strongAnchor = window.score >= (aggressiveMode ? 0.72 : 0.68)
    return sceneAnchor || speechAnchor || emotionalAnchor || strongAnchor
  })
  return buildRangesFromFlags(preserveFlags, 1).map((range) => ({
    start: Math.max(0, range.start - 0.15),
    end: Math.min(total, range.end + 0.15)
  }))
}

const applyContinuityGuardsToCuts = (
  candidateCuts: TimeRange[],
  windows: EngagementWindow[],
  aggressiveMode = false
) => {
  if (!candidateCuts.length) return [] as TimeRange[]
  const protectionRanges = buildContinuityProtectionRanges(windows, aggressiveMode)
  if (!protectionRanges.length) return mergeRanges(candidateCuts)
  const removable = candidateCuts.map((range) => ({ start: range.start, end: range.end, speed: 1 }))
  const guarded = subtractRanges(removable, protectionRanges)
  const minCut = aggressiveMode ? 0.28 : 0.34
  return mergeRanges(
    guarded
      .filter((range) => range.end - range.start >= minCut)
      .map((range) => ({
        start: Number(range.start.toFixed(3)),
        end: Number(range.end.toFixed(3))
      }))
  )
}

const inferPacingProfile = (
  windows: EngagementWindow[],
  durationSeconds: number,
  aggressiveMode: boolean
): PacingProfile => {
  const profiles: Record<PacingNiche, Omit<PacingProfile, 'niche'>> = {
    high_energy: {
      minLen: 4.2,
      maxLen: 8.4,
      earlyTarget: 5,
      middleTarget: 5.7,
      lateTarget: 5.2,
      jitter: 0.3,
      speedCap: 1.38
    },
    education: {
      minLen: 4.9,
      maxLen: 9.6,
      earlyTarget: 5.6,
      middleTarget: 6.4,
      lateTarget: 5.8,
      jitter: 0.22,
      speedCap: 1.3
    },
    talking_head: {
      minLen: 5.4,
      maxLen: 10.2,
      earlyTarget: 5.9,
      middleTarget: 6.9,
      lateTarget: 6.1,
      jitter: 0.18,
      speedCap: 1.26
    },
    story: {
      minLen: 4.8,
      maxLen: 9.8,
      earlyTarget: 5.5,
      middleTarget: 6.5,
      lateTarget: 5.9,
      jitter: 0.24,
      speedCap: 1.32
    }
  }
  if (!windows.length || durationSeconds <= 0) {
    const fallback = profiles.story
    return { niche: 'story', ...fallback }
  }

  const activeWindows = windows.filter((window) => (
    window.audioEnergy > 0.02 ||
    window.sceneChangeRate > 0 ||
    window.speechIntensity > 0.05 ||
    window.score > 0.08
  ))
  const basis = activeWindows.length ? activeWindows : windows
  const total = basis.length
  const avgScene = basis.reduce((sum, window) => sum + window.sceneChangeRate, 0) / total
  const avgSpeech = basis.reduce((sum, window) => sum + window.speechIntensity, 0) / total
  const avgEmotion = basis.reduce((sum, window) => sum + window.emotionIntensity, 0) / total
  const spikeRatio = basis.filter((window) => window.emotionalSpike > 0).length / total

  let niche: PacingNiche = 'story'
  if (avgScene > 0.42 || avgEmotion > 0.55 || spikeRatio > 0.16) {
    niche = 'high_energy'
  } else if (avgSpeech > 0.58 && avgScene < 0.24) {
    niche = 'talking_head'
  } else if (avgSpeech > 0.48 && avgScene < 0.34) {
    niche = 'education'
  }

  const base = profiles[niche]
  const shortFormFactor = durationSeconds < 55 ? 0.7 : durationSeconds < 90 ? 0.35 : 0
  const aggressiveShift = aggressiveMode ? 0.55 : 0
  const minLen = Number(clamp(base.minLen - aggressiveShift * 0.45 - shortFormFactor * 0.12, CUT_MIN, PACE_MAX).toFixed(2))
  const maxLen = Number(clamp(base.maxLen - aggressiveShift * 0.5 - shortFormFactor * 0.2, Math.min(CUT_MAX - 0.4, minLen + 0.8), CUT_MAX).toFixed(2))
  const speedCap = Number(clamp(base.speedCap + (aggressiveMode ? 0.1 : 0), 1.2, 1.5).toFixed(3))
  return {
    niche,
    minLen,
    maxLen,
    earlyTarget: Math.max(minLen, base.earlyTarget - aggressiveShift * 0.5),
    middleTarget: Math.max(minLen, base.middleTarget - aggressiveShift * 0.5),
    lateTarget: Math.max(minLen, base.lateTarget - aggressiveShift * 0.5),
    jitter: base.jitter + (aggressiveMode ? 0.04 : 0),
    speedCap
  }
}

const inferVideoNicheProfile = ({
  windows,
  transcriptCues,
  durationSeconds,
  pacingProfile,
  styleProfile
}: {
  windows: EngagementWindow[]
  transcriptCues: TranscriptCue[]
  durationSeconds: number
  pacingProfile: PacingProfile
  styleProfile?: ContentStyleProfile | null
}): VideoNicheProfile => {
  if (!windows.length || durationSeconds <= 0) {
    return {
      niche: pacingProfile?.niche || 'story',
      confidence: 0.52,
      rationale: ['Limited signal detected, defaulting to story pacing niche.'],
      styleAlignment: styleProfile?.style || null,
      metrics: {
        avgSpeech: 0,
        avgScene: 0,
        avgEmotion: 0,
        spikeRatio: 0,
        transcriptCueCount: Array.isArray(transcriptCues) ? transcriptCues.length : 0,
        durationSeconds: Number(durationSeconds || 0)
      }
    }
  }
  const activeWindows = windows.filter((window) => (
    window.audioEnergy > 0.02 ||
    window.sceneChangeRate > 0 ||
    window.speechIntensity > 0.05 ||
    window.score > 0.08
  ))
  const basis = activeWindows.length ? activeWindows : windows
  const total = Math.max(1, basis.length)
  const avgSpeech = basis.reduce((sum, window) => sum + window.speechIntensity, 0) / total
  const avgScene = basis.reduce((sum, window) => sum + window.sceneChangeRate, 0) / total
  const avgEmotion = basis.reduce((sum, window) => sum + window.emotionIntensity, 0) / total
  const spikeRatio = basis.filter((window) => window.emotionalSpike > 0 || window.emotionIntensity > 0.72).length / total
  const transcriptText = transcriptCues.map((cue) => cue.text).join(' ').toLowerCase()
  const tutorialHits = countPhraseHits(transcriptText, TUTORIAL_STYLE_KEYWORDS)
  const gamingHits = countPhraseHits(transcriptText, GAMING_STYLE_KEYWORDS)
  const reactionHits = countPhraseHits(transcriptText, REACTION_STYLE_KEYWORDS)
  const niche = pacingProfile.niche
  const rationale: string[] = []
  let confidence = 0.55
  if (niche === 'high_energy') {
    confidence += 0.2 * clamp01((avgScene - 0.26) / 0.4)
    confidence += 0.14 * clamp01((avgEmotion - 0.42) / 0.35)
    confidence += 0.11 * clamp01((spikeRatio - 0.1) / 0.25)
    if (gamingHits > 0 || reactionHits > 0) confidence += 0.05
    rationale.push('High scene-change and emotion spikes suggest high-energy content.')
  } else if (niche === 'talking_head') {
    confidence += 0.22 * clamp01((avgSpeech - 0.42) / 0.4)
    confidence += 0.1 * clamp01((0.34 - avgScene) / 0.34)
    if (styleProfile?.style === 'vlog' || styleProfile?.style === 'tutorial') confidence += 0.05
    rationale.push('Consistent speech with lower scene churn suggests talking-head pacing.')
  } else if (niche === 'education') {
    confidence += 0.2 * clamp01((avgSpeech - 0.4) / 0.4)
    confidence += 0.08 * clamp01((0.36 - avgScene) / 0.36)
    confidence += 0.1 * clamp01(tutorialHits / 3)
    rationale.push('Instructional language and steady cadence indicate education niche.')
  } else {
    confidence += 0.12 * clamp01(avgEmotion / 0.65)
    confidence += 0.08 * clamp01(avgSpeech / 0.65)
    confidence += 0.08 * clamp01(1 - Math.abs(avgScene - 0.34))
    rationale.push('Balanced narrative signals indicate story niche.')
  }
  if (!rationale.length) rationale.push('Niche inferred from pacing and engagement windows.')
  if (styleProfile) {
    const aligned =
      (niche === 'high_energy' && (styleProfile.style === 'reaction' || styleProfile.style === 'gaming')) ||
      (niche === 'talking_head' && styleProfile.style === 'vlog') ||
      (niche === 'education' && styleProfile.style === 'tutorial') ||
      (niche === 'story' && styleProfile.style === 'story')
    if (aligned) {
      confidence += 0.07
      rationale.push(`Style profile (${styleProfile.style}) aligns with detected niche.`)
    }
  }
  return {
    niche,
    confidence: Number(clamp01(confidence).toFixed(4)),
    rationale: rationale.slice(0, 4),
    styleAlignment: styleProfile?.style || null,
    metrics: {
      avgSpeech: Number(avgSpeech.toFixed(4)),
      avgScene: Number(avgScene.toFixed(4)),
      avgEmotion: Number(avgEmotion.toFixed(4)),
      spikeRatio: Number(spikeRatio.toFixed(4)),
      transcriptCueCount: transcriptCues.length,
      durationSeconds: Number(durationSeconds.toFixed(3))
    }
  }
}

const applyPacingPattern = (
  segments: Segment[],
  minLen: number,
  maxLen: number,
  windows: EngagementWindow[],
  durationSeconds: number,
  aggressiveMode: boolean,
  profile: PacingProfile
) => {
  const pickPacingSpeed = (start: number, end: number) => {
    const engagement = averageWindowMetric(windows, start, end, (window) => window.score)
    const speech = averageWindowMetric(windows, start, end, (window) => window.speechIntensity)
    const scene = averageWindowMetric(windows, start, end, (window) => window.sceneChangeRate)
    const excitement = averageWindowMetric(windows, start, end, (window) => window.vocalExcitement)
    const phase = durationSeconds > 0 ? start / durationSeconds : 0
    let speed = 1
    if (engagement < 0.28 || (scene < 0.18 && speech < 0.34)) {
      speed = aggressiveMode ? 1.38 : 1.26
    } else if (engagement < 0.4) {
      speed = aggressiveMode ? 1.26 : 1.16
    } else if (engagement < 0.52 && speech < 0.32) {
      speed = aggressiveMode ? 1.16 : 1.08
    }
    if (profile.niche === 'high_energy' && scene < 0.32) {
      speed += 0.06
    } else if (profile.niche === 'talking_head' && speech > 0.55) {
      speed = Math.max(1, speed - 0.05)
    } else if (profile.niche === 'education' && speech > 0.5) {
      speed = Math.max(1, speed - 0.04)
    }
    if (scene > 0.6) {
      speed = Math.max(1, speed - 0.06)
    }
    if (excitement > 0.72) {
      speed = Math.max(1, speed - 0.08)
    }
    // Preserve opening/closing cadence so the video doesn't feel rushed at key narrative points.
    if (phase < 0.12 || phase > 0.9) {
      speed = Math.min(speed, profile.niche === 'high_energy' ? 1.12 : 1.08)
    }
    return Number(clamp(speed, 1, profile.speedCap).toFixed(3))
  }

  const out: Segment[] = []
  let patternIdx = 0
  for (const seg of segments) {
    let cursor = seg.start
    const end = seg.end
    while (end - cursor > maxLen) {
      const previewEnd = Math.min(end, cursor + 4)
      const engagement = averageWindowMetric(windows, cursor, previewEnd, (window) => window.score)
      const speech = averageWindowMetric(windows, cursor, previewEnd, (window) => window.speechIntensity)
      const scene = averageWindowMetric(windows, cursor, previewEnd, (window) => window.sceneChangeRate)
      const excitement = averageWindowMetric(windows, cursor, previewEnd, (window) => window.vocalExcitement)
      const phase = durationSeconds > 0 ? cursor / durationSeconds : 0
      let target = phase < 0.2 ? profile.earlyTarget : phase > 0.82 ? profile.lateTarget : profile.middleTarget
      if (engagement < 0.28) target -= 1.1
      else if (engagement > 0.72) target += 1.0
      if (scene > 0.58) target -= 0.35
      if (speech > 0.62 && profile.niche !== 'high_energy') target += 0.35
      if (excitement > 0.7) target -= 0.55
      if (aggressiveMode) target -= 0.65
      const jitter = patternIdx % 2 === 0 ? -profile.jitter : profile.jitter
      const desired = Math.max(minLen, Math.min(maxLen, target + jitter))
      const nextEnd = Math.min(end, cursor + desired)
      const speed = pickPacingSpeed(cursor, nextEnd)
      out.push({ ...seg, start: cursor, end: nextEnd, speed })
      cursor = nextEnd
      patternIdx += 1
    }
    const remaining = end - cursor
    if (remaining > 0.1) {
      if (remaining < minLen && out.length) {
        out[out.length - 1].end = end
      } else {
        const speed = pickPacingSpeed(cursor, end)
        out.push({ ...seg, start: cursor, end, speed })
      }
    }
  }
  return out
}

const maintainSceneChangeFrequency = (
  segments: Segment[],
  windows: EngagementWindow[],
  aggressiveMode: boolean
) => {
  if (segments.length <= 1 || !windows.length) return segments
  const activeWindows = windows.filter((window) => (
    window.audioEnergy > 0.02 ||
    window.sceneChangeRate > 0 ||
    window.speechIntensity > 0.05 ||
    window.score > 0.08
  ))
  const basis = activeWindows.length ? activeWindows : windows
  const baselineSceneRate = basis.reduce((sum, window) => sum + window.sceneChangeRate, 0) / basis.length
  if (baselineSceneRate <= 0.05) return segments
  const weightedScene = segments.reduce((sum, seg) => {
    const speed = seg.speed && seg.speed > 0 ? seg.speed : 1
    const runtime = Math.max(0.1, (seg.end - seg.start) / speed)
    const scene = averageWindowMetric(windows, seg.start, seg.end, (window) => window.sceneChangeRate)
    return sum + runtime * scene
  }, 0)
  const totalRuntime = segments.reduce((sum, seg) => {
    const speed = seg.speed && seg.speed > 0 ? seg.speed : 1
    return sum + Math.max(0.1, (seg.end - seg.start) / speed)
  }, 0)
  const keptSceneRate = totalRuntime > 0 ? weightedScene / totalRuntime : baselineSceneRate
  const minimumTarget = baselineSceneRate * (aggressiveMode ? 0.78 : 0.82)
  if (keptSceneRate >= minimumTarget) return segments

  const out = segments.map((seg) => ({ ...seg }))
  const speedCap = aggressiveMode ? 1.5 : 1.36
  const entries = out
    .map((seg, idx) => ({
      idx,
      seg,
      scene: averageWindowMetric(windows, seg.start, seg.end, (window) => window.sceneChangeRate),
      score: averageWindowMetric(windows, seg.start, seg.end, (window) => window.score)
    }))
    .sort((a, b) => a.scene - b.scene || a.score - b.score)

  let deficit = minimumTarget - keptSceneRate
  for (const entry of entries) {
    if (deficit <= 0) break
    if (entry.scene >= baselineSceneRate) continue
    const current = entry.seg.speed && entry.seg.speed > 0 ? entry.seg.speed : 1
    const delta = clamp((baselineSceneRate - entry.scene) * 0.35, 0.04, aggressiveMode ? 0.14 : 0.1)
    const next = Number(clamp(current + delta, 1, speedCap).toFixed(3))
    if (next <= current + 0.01) continue
    out[entry.idx].speed = next
    deficit -= 0.04
  }
  return out
}

const stabilizeSpeechIntensity = (
  segments: Segment[],
  windows: EngagementWindow[],
  aggressiveMode: boolean
) => {
  if (segments.length <= 1) return segments
  const scored = segments.map((seg, idx) => {
    const current = seg.speed && seg.speed > 0 ? seg.speed : 1
    return {
      idx,
      seg,
      current,
      speech: averageWindowMetric(windows, seg.start, seg.end, (window) => window.speechIntensity),
      scene: averageWindowMetric(windows, seg.start, seg.end, (window) => window.sceneChangeRate),
      score: averageWindowMetric(windows, seg.start, seg.end, (window) => window.score)
    }
  })
  const sortedSpeech = scored.map((entry) => entry.speech).sort((a, b) => a - b)
  const medianSpeech = sortedSpeech.length
    ? sortedSpeech[Math.floor(sortedSpeech.length / 2)]
    : 0.4
  const maxStep = aggressiveMode ? 0.17 : 0.13
  const speedCap = aggressiveMode ? 1.48 : 1.34
  const out = segments.map((seg) => ({ ...seg }))
  let prevSpeed = scored[0]?.current ?? 1

  for (const entry of scored) {
    let target = entry.current
    const lowSpeech = entry.speech < Math.max(0.18, medianSpeech * 0.74)
    const weakEngagement = entry.score < (aggressiveMode ? 0.48 : 0.44)
    if (lowSpeech && weakEngagement) {
      target = Math.min(speedCap, target + (entry.scene < 0.25 ? 0.09 : 0.06))
    } else if (entry.speech > Math.max(0.62, medianSpeech * 1.25)) {
      target = Math.max(1, target - 0.05)
    }
    if (entry.idx > 0 && Math.abs(target - prevSpeed) > maxStep) {
      target = prevSpeed + Math.sign(target - prevSpeed) * maxStep
    }
    const rounded = Number(clamp(target, 1, speedCap).toFixed(3))
    out[entry.idx].speed = rounded
    prevSpeed = rounded
  }

  return out
}

const buildEditPlan = async (
  filePath: string,
  durationSeconds: number,
  options: EditOptions = DEFAULT_EDIT_OPTIONS,
  onStage?: (stage: 'cutting' | 'hooking' | 'pacing') => void | Promise<void>,
  context?: {
    transcriptCues?: TranscriptCue[]
    aggressionLevel?: RetentionAggressionLevel
    hookCalibration?: HookCalibrationProfile | null
  }
) => {
  const aggressionLevel = parseRetentionAggressionLevel(
    context?.aggressionLevel ??
    options.retentionAggressionLevel ??
    DEFAULT_EDIT_OPTIONS.retentionAggressionLevel
  )
  const aggressionPreset = RETENTION_AGGRESSION_PRESET[aggressionLevel]
  if (onStage) await onStage('cutting')
  // Run independent analysis tasks in parallel to save wall-clock time.
  const fastMode = Boolean(options.fastMode)
  const skipFacePresence = fastMode || options.smartZoom === false || ANALYSIS_DISABLE_FACE_DETECTION
  const skipTextDensity = fastMode || ANALYSIS_DISABLE_TEXT_DENSITY
  const skipEmotionModel = fastMode || ANALYSIS_DISABLE_EMOTION_MODEL
  const tasks: Array<Promise<any>> = []
  tasks.push(detectSilences(filePath, durationSeconds).catch(() => []))
  tasks.push(detectAudioEnergy(filePath, durationSeconds).catch(() => []))
  tasks.push(detectSceneChanges(filePath, durationSeconds).catch(() => []))
  // Face detection can be skipped in fast mode to cut analysis latency.
  if (!skipFacePresence) {
    tasks.push(detectFacePresence(filePath, durationSeconds).catch(() => []))
  } else {
    tasks.push(Promise.resolve([]))
  }
  tasks.push(skipTextDensity ? Promise.resolve([]) : detectTextDensity(filePath, durationSeconds).catch(() => []))
  tasks.push(skipEmotionModel ? Promise.resolve([]) : detectEmotionModelSignals(filePath, durationSeconds).catch(() => []))
  const [silences, energySamples, sceneChanges, faceSamples, textSamples, emotionSamples] = await Promise.all(tasks)
  const transcriptCues = Array.isArray(context?.transcriptCues) ? context!.transcriptCues! : []
  const windows = enrichWindowsWithCognitiveScores({
    windows: buildEngagementWindows(durationSeconds, energySamples, sceneChanges, faceSamples, textSamples, emotionSamples),
    durationSeconds,
    silences,
    transcriptCues
  })
  const styleProfile = inferContentStyleProfile({
    windows,
    transcriptCues,
    durationSeconds
  })
  const styleAdjustedAggressionLevel = getStyleAdjustedAggressionLevel(aggressionLevel, styleProfile)
  const basePacingProfile = applyStyleToPacingProfile(
    inferPacingProfile(windows, durationSeconds, options.aggressiveMode),
    styleProfile,
    options.aggressiveMode
  )
  const nicheProfile = inferVideoNicheProfile({
    windows,
    transcriptCues,
    durationSeconds,
    pacingProfile: basePacingProfile,
    styleProfile
  })
  const runtimeStyleResolution = resolveRuntimeStyleProfile({
    mode: options.retentionStrategyProfile,
    contentStyle: styleProfile.style,
    niche: nicheProfile.niche,
    contentStyleConfidence: styleProfile.confidence,
    nicheConfidence: nicheProfile.confidence,
    explicitBlend: options.styleArchetypeBlend || null
  })
  const styleArchetypeBlend = runtimeStyleResolution.blend
  const behaviorStyleProfile = runtimeStyleResolution.profile
  const pacingProfile = applyBehaviorStyleProfileToPacingProfile(basePacingProfile, behaviorStyleProfile)
  const emotionalTuning = resolveEmotionalTuningProfile({
    styleProfile,
    nicheProfile,
    aggressionLevel: styleAdjustedAggressionLevel
  })
  const silenceTrimCuts = options.removeBoring
    ? buildSilenceTrimCuts(silences, durationSeconds, options.aggressiveMode)
    : []

  const boringFlags = options.removeBoring
    ? buildBoringFlags(windows, silences, options.aggressiveMode)
    : windows.map(() => false)
  const detectedRemovedSegments = options.removeBoring
    ? buildBoringCuts(boringFlags, options.aggressiveMode)
    : []
  const fallbackRemovedSegments = options.removeBoring && !detectedRemovedSegments.length
    ? buildStrategicFallbackCuts(windows, durationSeconds, options.aggressiveMode)
    : []
  let contentRemovedCandidates = mergeRanges(
    detectedRemovedSegments.length ? detectedRemovedSegments : fallbackRemovedSegments
  )
  let candidateRemovedSegments = mergeRanges([
    ...contentRemovedCandidates,
    ...silenceTrimCuts
  ])
  let removedSegments = options.removeBoring
    ? mergeRanges([
        ...applyContinuityGuardsToCuts(contentRemovedCandidates, windows, options.aggressiveMode),
        ...silenceTrimCuts
      ])
    : []
  if (options.removeBoring) {
    const minimumRemovedSeconds = getMinimumRemovedSeconds(durationSeconds, options.aggressiveMode)
    const removedSeconds = getRangesDurationSeconds(removedSegments)
    if (minimumRemovedSeconds > 0 && removedSeconds < minimumRemovedSeconds) {
      const rescueCuts = buildLongFormRescueCuts(
        windows,
        durationSeconds,
        minimumRemovedSeconds - removedSeconds,
        options.aggressiveMode,
        removedSegments.length ? removedSegments : candidateRemovedSegments
      )
      if (rescueCuts.length) {
        contentRemovedCandidates = mergeRanges([...contentRemovedCandidates, ...rescueCuts])
        candidateRemovedSegments = mergeRanges([...contentRemovedCandidates, ...silenceTrimCuts])
        const guardedWithSilence = mergeRanges([
          ...applyContinuityGuardsToCuts(contentRemovedCandidates, windows, true),
          ...silenceTrimCuts
        ])
        const guardedRemovedSeconds = getRangesDurationSeconds(guardedWithSilence)
        if (guardedRemovedSeconds >= removedSeconds + 0.15) {
          removedSegments = guardedWithSilence
        }
      }
    }
  }
  const compressedSegments: TimeRange[] = []

  const baseSegments = [{ start: 0, end: durationSeconds, speed: 1 }]
  const keepSegments = removedSegments.length ? subtractRanges(baseSegments, removedSegments) : baseSegments

  const minLen = pacingProfile.minLen
  const maxLen = pacingProfile.maxLen
  const pacingInput = keepSegments.length ? keepSegments : [{ start: 0, end: durationSeconds, speed: 1 }]
  const pacedSegments = options.onlyCuts
    ? pacingInput
    : applyPacingPattern(pacingInput, minLen, maxLen, windows, durationSeconds, options.aggressiveMode, pacingProfile)
  const speechStabilizedSegments = options.onlyCuts
    ? pacedSegments
    : stabilizeSpeechIntensity(pacedSegments, windows, options.aggressiveMode)
  const normalizedKeep = options.onlyCuts
    ? enforceSegmentLengths(speechStabilizedSegments, minLen, maxLen, windows)
    : refineSegmentsForRetention(speechStabilizedSegments, windows, minLen, maxLen)

  if (onStage) await onStage('hooking')
  const topHookCandidates = pickTopHookCandidates({
    durationSeconds,
    segments: normalizedKeep,
    windows,
    transcriptCues,
    hookCalibration: context?.hookCalibration && context.hookCalibration.enabled
      ? context.hookCalibration
      : null,
    styleProfile,
    nicheProfile,
    aggressionLevel: styleAdjustedAggressionLevel
  })
  const hookVariants = [
    topHookCandidates.selected,
    ...topHookCandidates.topCandidates
  ].filter((candidate, index, list) => (
    list.findIndex((entry) => (
      Math.abs(entry.start - candidate.start) < 0.01 &&
      Math.abs(entry.duration - candidate.duration) < 0.01
    )) === index
  ))
  const hook = hookVariants[0] || topHookCandidates.selected
  const hookRange: TimeRange = {
    start: hook.start,
    end: Number((hook.start + hook.duration).toFixed(3))
  }

  if (onStage) await onStage('pacing')
  const shouldMoveHook = options.autoHookMove && !options.onlyCuts
  const withoutHook = shouldMoveHook ? subtractRange(normalizedKeep, hookRange) : normalizedKeep
  const finalSegments = enforceSegmentLengths(withoutHook.map((seg) => ({ ...seg })), minLen, maxLen, windows)
  const sceneBalancedSegments = options.onlyCuts
    ? finalSegments
    : maintainSceneChangeFrequency(finalSegments, windows, options.aggressiveMode)
  const speechBalancedSegments = options.onlyCuts
    ? sceneBalancedSegments
    : stabilizeSpeechIntensity(sceneBalancedSegments, windows, options.aggressiveMode)
  const boredomApplied = applyBoredomModelToSegments({
    segments: speechBalancedSegments,
    windows,
    aggressionLevel: styleAdjustedAggressionLevel,
    hookRange
  })
  const emotionalBeatAdjusted = applyEmotionalBeatCuts({
    segments: boredomApplied.segments,
    windows,
    aggressionLevel: styleAdjustedAggressionLevel,
    hookRange,
    styleProfile,
    nicheProfile
  })
  const interruptInjected = injectPatternInterrupts({
    segments: emotionalBeatAdjusted.segments,
    durationSeconds,
    aggressionLevel: styleAdjustedAggressionLevel,
    targetIntervalSeconds: behaviorStyleProfile.patternInterruptInterval
  })
  const endingSpikeSegments = enforceEndingSpike({
    segments: interruptInjected.segments,
    windows,
    durationSeconds
  })
  const beatAnchors = detectRhythmAnchors({
    windows,
    durationSeconds,
    styleProfile
  })
  const emotionalBeatAnchors = detectEmotionalBeatAnchors({
    windows,
    durationSeconds,
    styleProfile,
    nicheProfile,
    aggressionLevel: styleAdjustedAggressionLevel
  })
  const mergedBeatAnchors = mergeBeatAnchorSets({
    rhythmAnchors: beatAnchors,
    emotionalAnchors: [...emotionalBeatAnchors, ...emotionalBeatAdjusted.anchors],
    durationSeconds
  })
  const rhythmAlignedSegments = alignSegmentsToRhythm({
    segments: endingSpikeSegments,
    durationSeconds,
    anchors: mergedBeatAnchors,
    styleProfile
  })
  const lowEnergyThreshold = behaviorStyleProfile.energyEscalationCurve === 'aggressive'
    ? 0.52
    : behaviorStyleProfile.energyEscalationCurve === 'steady'
      ? 0.54
      : 0.57
  const energySamplesForTimeline = buildEnergySamplesFromWindows(windows)
  const autoEscalationResult = options.onlyCuts
    ? { segments: rhythmAlignedSegments, events: [] as AutoEscalationEvent[], count: 0 }
    : applyAutoEscalationGuarantee({
        segments: rhythmAlignedSegments,
        energySamples: energySamplesForTimeline,
        flatWindowSeconds: clamp(behaviorStyleProfile.autoEscalationWindowSec, 5.2, 9.2),
        lowEnergyThreshold,
        maxSpeed: clamp(pacingProfile.speedCap + 0.04, 1.18, 1.34)
      })
  const finalTimelineSegments = autoEscalationResult.segments
  const totalPatternInterruptCount = interruptInjected.count + autoEscalationResult.count
  const runtimeSeconds = Math.max(0.1, computeEditedRuntimeSeconds(finalTimelineSegments))
  const totalPatternInterruptDensity = Number((totalPatternInterruptCount / runtimeSeconds).toFixed(4))
  const decisionTimeline = buildEditDecisionTimeline({
    styleName: behaviorStyleProfile.styleName,
    hook: hook ? { start: hook.start } : null,
    segments: finalTimelineSegments,
    cues: transcriptCues.map((cue) => ({
      start: cue.start,
      text: cue.text,
      keywordIntensity: cue.keywordIntensity,
      curiosityTrigger: cue.curiosityTrigger
    })),
    patternInterruptCount: totalPatternInterruptCount,
    autoEscalationEvents: autoEscalationResult.events,
    includeBrollMarkers: styleArchetypeBlend.cinematic_lifestyle_archive >= 0.24
  })
  const styleFeatureSnapshot = extractTimelineFeatures({
    timeline: decisionTimeline,
    durationSeconds,
    energySamples: energySamplesForTimeline
  })

  return {
    hook,
    segments: finalTimelineSegments,
    silences,
    removedSegments: mergeRanges([...removedSegments, ...boredomApplied.removedRanges]),
    compressedSegments,
    engagementWindows: windows
      .map((window) => ({
        ...window,
        hookScore: clamp01((window.hookScore ?? window.score) * aggressionPreset.hookRelocateBias),
        boredomScore: window.boredomScore ?? 0
      })),
    hookCandidates: hookVariants,
    boredomRanges: boredomApplied.removedRanges,
    patternInterruptCount: totalPatternInterruptCount,
    patternInterruptDensity: totalPatternInterruptDensity,
    boredomRemovedRatio: Number(clamp01(getRangesDurationSeconds(boredomApplied.removedRanges) / Math.max(0.1, durationSeconds)).toFixed(4)),
    storyReorderMap: finalTimelineSegments.map((segment, orderedIndex) => ({
      sourceStart: Number(segment.start.toFixed(3)),
      sourceEnd: Number(segment.end.toFixed(3)),
      orderedIndex
    })),
    hookFailureReason: topHookCandidates.hookFailureReason,
    transcriptSignals: {
      cueCount: transcriptCues.length,
      hasTranscript: transcriptCues.length > 0
    },
    styleProfile,
    nicheProfile,
    beatAnchors: mergedBeatAnchors,
    emotionalBeatAnchors,
    emotionalBeatCutCount: emotionalBeatAdjusted.cutCount,
    emotionalLeadTrimmedSeconds: emotionalBeatAdjusted.trimmedSeconds,
    emotionalTuning,
    hookVariants,
    hookCalibration: context?.hookCalibration ?? null,
    styleArchetypeBlend,
    behaviorStyleProfile,
    autoEscalationEvents: autoEscalationResult.events,
    editDecisionTimeline: decisionTimeline,
    styleFeatureSnapshot
  }
}

const applySegmentEffects = (
  segments: Segment[],
  windows: EngagementWindow[],
  options: EditOptions,
  hookRange?: TimeRange | null
) => {
  const aggressionPreset = RETENTION_AGGRESSION_PRESET[options.retentionAggressionLevel || 'medium']
  const hardMaxZoom = Math.min(options.autoZoomMax || ZOOM_HARD_MAX, ZOOM_HARD_MAX)
  const maxZoomDelta = Math.max(0, hardMaxZoom - 1)
  const totalDuration = segments.reduce((sum, seg) => {
    const speed = seg.speed && seg.speed > 0 ? seg.speed : 1
    return sum + Math.max(0, (seg.end - seg.start) / speed)
  }, 0)
  const maxZoomDuration = totalDuration * ZOOM_MAX_DURATION_RATIO * clamp(aggressionPreset.zoomBoost, 0.85, 1.5)
  const hasFaceSignal = windows.some((w) => w.facePresence > 0.2)

  const segmentScores = segments.map((seg) => {
    const relevant = windows.filter((w) => w.time >= seg.start && w.time < seg.end)
    const avg = (key: keyof EngagementWindow) =>
      relevant.length ? relevant.reduce((sum, w) => sum + (w[key] as number), 0) / relevant.length : 0
    const facePresence = avg('facePresence')
    const emotionIntensity = avg('emotionIntensity')
    const vocalExcitement = avg('vocalExcitement')
    const speechIntensity = avg('speechIntensity')
    const motionScore = avg('motionScore')
    const emotionalSpike = avg('emotionalSpike')
    const faceFocus = relevant.reduce((acc, window) => {
      const x = Number(window.faceCenterX)
      const y = Number(window.faceCenterY)
      if (!Number.isFinite(x) || !Number.isFinite(y)) return acc
      const faceWeight = clamp(
        Number.isFinite(window.faceIntensity)
          ? Number(window.faceIntensity)
          : window.facePresence,
        0.12,
        1
      )
      if (faceWeight <= 0) return acc
      return {
        weight: acc.weight + faceWeight,
        x: acc.x + clamp01(x) * faceWeight,
        y: acc.y + clamp01(y) * faceWeight
      }
    }, {
      weight: 0,
      x: 0,
      y: 0
    })
    const faceFocusX = faceFocus.weight > 0
      ? clamp(faceFocus.x / faceFocus.weight, 0.08, 0.92)
      : 0.5
    const faceFocusY = faceFocus.weight > 0
      ? clamp(faceFocus.y / faceFocus.weight, 0.08, 0.88)
      : 0.42
    const isHook = hookRange ? seg.start < hookRange.end && seg.end > hookRange.start : false
    const emphasisScore = Math.min(
      1,
      emotionIntensity * 0.38 +
      vocalExcitement * 0.2 +
      speechIntensity * 0.18 +
      motionScore * 0.16 +
      emotionalSpike * 0.08
    )
    const speechEmphasis = speechIntensity > 0.58 ? 0.06 : 0
    const scoreBoost = isHook ? 0.12 : 0
    const score = emphasisScore + scoreBoost + speechEmphasis
    return {
      seg,
      facePresence,
      speechIntensity,
      motionScore,
      emotionIntensity,
      vocalExcitement,
      emotionalSpike,
      faceFocusX,
      faceFocusY,
      isHook,
      score
    }
  })
  const speechBaseline = segmentScores.length
    ? segmentScores.reduce((sum, entry) => sum + entry.speechIntensity, 0) / segmentScores.length
    : 0.4

  const zoomCandidates = segmentScores
    .filter((entry) => hasFaceSignal && entry.facePresence >= 0.25)
    .filter((entry) => {
      if (options.aggressiveMode) return entry.score >= 0.33 || entry.speechIntensity >= 0.58
      if (entry.isHook) return entry.score >= 0.42
      const hasSpeechOrMotion = entry.speechIntensity >= speechBaseline * 0.72 || entry.motionScore >= 0.42
      return entry.score >= 0.52 && hasSpeechOrMotion
    })
    .sort((a, b) => b.score - a.score)

  let remainingZoom = maxZoomDuration
  const zoomMap = new Map<Segment, number>()
  for (const entry of zoomCandidates) {
    if (remainingZoom <= 0) break
    const speed = entry.seg.speed && entry.seg.speed > 0 ? entry.seg.speed : 1
    const duration = Math.max(0, (entry.seg.end - entry.seg.start) / speed)
    if (duration <= 0) continue
    if (duration > remainingZoom) continue
    const speechFactor = Math.min(1, entry.speechIntensity / Math.max(0.2, speechBaseline))
    const baseZoom = (0.045 + 0.05 * entry.score + 0.015 * speechFactor + (entry.isHook ? 0.02 : 0)) * aggressionPreset.zoomBoost
    zoomMap.set(entry.seg, Math.min(maxZoomDelta, baseZoom))
    remainingZoom -= duration
  }

  return segments.map((seg) => {
    const score = segmentScores.find((entry) => entry.seg === seg)
    const hasSpike = (score?.emotionalSpike ?? 0) > 0.05
    const speechPeak = (score?.speechIntensity ?? 0) >= Math.max(0.58, speechBaseline * 1.2)
    const motionEmphasis = (score?.motionScore ?? 0) > 0.5 && (score?.emotionIntensity ?? 0) > 0.48
    const calmNarrative = (score?.emotionIntensity ?? 0) < 0.4 && (score?.speechIntensity ?? 0) < 0.3 && (score?.motionScore ?? 0) < 0.25
    const alreadyStrong = (score?.score ?? 0) >= 0.74 && (score?.speechIntensity ?? 0) >= 0.45
    const hookBoost = score?.isHook ? 0.02 : 0
    let zoom = seg.zoom ?? 0
    let brightness = seg.brightness ?? 0
    if (!alreadyStrong && hasFaceSignal && options.smartZoom && (!calmNarrative || options.aggressiveMode)) {
      const desired = zoomMap.get(seg) ?? 0
      zoom = Math.max(zoom, desired + hookBoost)
    }
    if (!alreadyStrong && options.emotionalBoost && (hasSpike || speechPeak)) {
      brightness = Math.max(brightness, speechPeak ? 0.02 : 0.03)
    }
    const jumpTrigger = (
      (score?.motionScore ?? 0) >= (options.aggressiveMode ? 0.38 : 0.56) ||
      (score?.speechIntensity ?? 0) >= Math.max(0.62, speechBaseline * 1.24) ||
      Boolean(score?.isHook && options.aggressiveMode)
    )
    const transitionStyle: SegmentTransitionStyle = options.jumpCuts && jumpTrigger ? 'jump' : 'smooth'
    const soundFxLevel = Number(clamp(
      options.soundFx
        ? (
          (score?.isHook ? 0.2 : 0) +
          (jumpTrigger ? 0.32 : 0) +
          (speechPeak ? 0.2 : 0) +
          (motionEmphasis ? 0.24 : 0) +
          (options.aggressiveMode ? 0.1 : 0)
        )
        : 0,
      0,
      0.9
    ).toFixed(3))
    const faceFocusX = Number(clamp(score?.faceFocusX ?? 0.5, 0.08, 0.92).toFixed(4))
    const faceFocusY = Number(clamp(score?.faceFocusY ?? 0.42, 0.08, 0.88).toFixed(4))
    zoom = Math.min(maxZoomDelta || 0, zoom)
    return {
      ...seg,
      zoom,
      brightness,
      faceFocusX,
      faceFocusY,
      transitionStyle,
      soundFxLevel,
      emphasize: Boolean(hasSpike || speechPeak || motionEmphasis || score?.isHook)
    }
  })
}

const applyZoomEasing = (segments: Segment[]) => {
  const eased: Segment[] = []
  for (const seg of segments) {
    const zoom = seg.zoom ?? 0
    const duration = seg.end - seg.start
    if (zoom <= 0 || duration <= ZOOM_EASE_SEC * 2) {
      eased.push(seg)
      continue
    }
    const ease = Math.min(ZOOM_EASE_SEC, duration / 4)
    if (ease <= 0) {
      eased.push(seg)
      continue
    }
    const easeZoom = zoom * 0.4
    const midStart = seg.start + ease
    const midEnd = seg.end - ease
    eased.push({ ...seg, end: midStart, zoom: easeZoom })
    if (midEnd - midStart > 0.05) {
      eased.push({ ...seg, start: midStart, end: midEnd, zoom })
    }
    eased.push({ ...seg, start: midEnd, zoom: easeZoom })
  }
  return eased
}

const normalizeSegmentForRender = (segment: Segment, durationSeconds: number): Segment | null => {
  const rawStart = Number(segment.start)
  const rawEnd = Number(segment.end)
  if (!Number.isFinite(rawStart) || !Number.isFinite(rawEnd)) return null
  const maxDuration = Number.isFinite(durationSeconds) && durationSeconds > 0
    ? durationSeconds
    : Number.MAX_SAFE_INTEGER
  const start = roundForFilter(clamp(Math.min(rawStart, rawEnd), 0, maxDuration))
  const end = roundForFilter(clamp(Math.max(rawStart, rawEnd), 0, maxDuration))
  if (end - start < MIN_RENDER_SEGMENT_SECONDS) return null
  const speed = Number.isFinite(segment.speed) && Number(segment.speed) > 0
    ? clamp(Number(segment.speed), 0.25, 4)
    : 1
  const zoom = Number.isFinite(segment.zoom)
    ? clamp(Number(segment.zoom), 0, ZOOM_HARD_MAX - 1)
    : 0
  const brightness = Number.isFinite(segment.brightness)
    ? clamp(Number(segment.brightness), -0.45, 0.45)
    : 0
  return { ...segment, start, end, speed, zoom, brightness }
}

const mergeSegmentsToLimitCount = (segments: Segment[], maxSegments: number) => {
  if (segments.length <= maxSegments) return segments
  const merged = segments.map((seg) => ({ ...seg }))
  while (merged.length > maxSegments && merged.length > 1) {
    let mergeIdx = 0
    let bestGap = Number.POSITIVE_INFINITY
    for (let i = 0; i < merged.length - 1; i += 1) {
      const gap = Math.max(0, merged[i + 1].start - merged[i].end)
      if (gap < bestGap) {
        bestGap = gap
        mergeIdx = i
      }
    }
    const left = merged[mergeIdx]
    const right = merged[mergeIdx + 1]
    merged.splice(mergeIdx, 2, {
      start: left.start,
      end: right.end,
      speed: 1,
      zoom: 0,
      brightness: 0,
      audioGain: Number(clamp(((left.audioGain ?? 1) + (right.audioGain ?? 1)) / 2, 0.8, 1.24).toFixed(3))
    })
  }
  return merged
}

const getMaxCutsPer10Seconds = ({
  renderMode,
  targetPlatform
}: {
  renderMode: RenderMode
  targetPlatform: RetentionTargetPlatform
}) => {
  const platform = parseRetentionTargetPlatform(targetPlatform)
  const profile = PLATFORM_MAX_CUTS_PER_10_SECONDS[platform] || PLATFORM_MAX_CUTS_PER_10_SECONDS.auto
  return renderMode === 'vertical' ? profile.vertical : profile.horizontal
}

const enforceCutDensityLimit = ({
  segments,
  durationSeconds,
  renderMode,
  targetPlatform
}: {
  segments: Segment[]
  durationSeconds: number
  renderMode: RenderMode
  targetPlatform: RetentionTargetPlatform
}) => {
  const safeDuration = Math.max(1, Number(durationSeconds || 0))
  const maxCutsPerTen = getMaxCutsPer10Seconds({ renderMode, targetPlatform })
  const maxCuts = Math.max(1, Math.floor((safeDuration / 10) * maxCutsPerTen))
  const maxSegments = Math.max(2, maxCuts + 1)
  return mergeSegmentsToLimitCount(segments, maxSegments)
}

const enforceLongFormComprehensionFloor = ({
  segments,
  hookRange,
  durationSeconds,
  renderMode,
  contentFormat
}: {
  segments: Segment[]
  hookRange: TimeRange
  durationSeconds: number
  renderMode: RenderMode
  contentFormat: RetentionContentFormat
}) => {
  if (renderMode === 'vertical') return segments
  if (contentFormat === 'tiktok_short') return segments
  if (durationSeconds < LONG_FORM_RUNTIME_THRESHOLD_SECONDS) return segments
  const contextStart = Number(clamp(hookRange.end, 0, Math.max(0, durationSeconds - 0.8)).toFixed(3))
  const contextWindowEnd = Number(clamp(contextStart + LONG_FORM_CONTEXT_WINDOW_SECONDS, contextStart + 0.8, durationSeconds).toFixed(3))
  if (contextWindowEnd <= contextStart) return segments
  const contextCoverage = segments.reduce((sum, segment) => {
    const overlap = Math.max(0, Math.min(segment.end, contextWindowEnd) - Math.max(segment.start, contextStart))
    return sum + overlap
  }, 0)
  if (contextCoverage >= LONG_FORM_MIN_CONTEXT_SECONDS) return segments
  const contextSeedEnd = Number(clamp(
    contextStart + Math.max(LONG_FORM_MIN_CONTEXT_SECONDS, Math.min(4.8, LONG_FORM_CONTEXT_WINDOW_SECONDS * 0.35)),
    contextStart + 0.8,
    durationSeconds
  ).toFixed(3))
  if (contextSeedEnd <= contextStart) return segments
  const seeded = [
    ...segments.map((segment) => ({ ...segment })),
    { start: contextStart, end: contextSeedEnd, speed: 1 }
  ].sort((left, right) => left.start - right.start || left.end - right.end)
  return seeded
}

const prepareSegmentsForRender = (segments: Segment[], durationSeconds: number) => {
  const normalized = segments
    .map((segment) => normalizeSegmentForRender(segment, durationSeconds))
    .filter((segment): segment is Segment => Boolean(segment))
    .sort((a, b) => a.start - b.start)
  if (!normalized.length) return normalized
  const compacted: Segment[] = [{ ...normalized[0] }]
  for (let i = 1; i < normalized.length; i += 1) {
    const next = normalized[i]
    const prev = compacted[compacted.length - 1]
    const gap = next.start - prev.end
    if (gap <= MERGE_ADJACENT_SEGMENT_GAP_SEC) {
      prev.end = roundForFilter(Math.max(prev.end, next.end))
      prev.speed = 1
      prev.zoom = 0
      prev.brightness = 0
      prev.audioGain = Number(clamp(((prev.audioGain ?? 1) + (next.audioGain ?? 1)) / 2, 0.8, 1.24).toFixed(3))
      continue
    }
    compacted.push({ ...next })
  }
  return mergeSegmentsToLimitCount(compacted, MAX_RENDER_SEGMENTS)
}

const buildAtempoChain = (speed: number) => {
  if (!Number.isFinite(speed) || speed === 1) return ''
  const chain: number[] = []
  let remaining = speed
  while (remaining > 2) {
    chain.push(2)
    remaining /= 2
  }
  while (remaining < 0.5) {
    chain.push(0.5)
    remaining /= 0.5
  }
  chain.push(remaining)
  return chain.map((value) => `atempo=${Number(value.toFixed(3))}`).join(',')
}

const buildConcatFilter = (
  segments: Segment[],
  opts: {
    withAudio: boolean
    hasAudioStream: boolean
    targetWidth: number
    targetHeight: number
    fit: HorizontalFitMode
    enableFades?: boolean
  }
) => {
  const parts: string[] = []
  const scalePad = buildFrameFitFilter(opts.fit, opts.targetWidth, opts.targetHeight)
  const durations: number[] = []
  const canGenerateNoiseFx = opts.withAudio && hasFfmpegFilter('anoisesrc')
  const nonZoomIndexes = segments
    .map((segment, idx) => ({
      idx,
      zoom: segment.zoom && segment.zoom > 0 ? segment.zoom : 0
    }))
    .filter((entry) => entry.zoom <= 0)
    .map((entry) => entry.idx)
  const nonZoomSourceLabels = new Map<number, string>()

  // Share one pre-fitted stream across non-zoom segments to keep large concat
  // graphs from spawning a scale filter per segment.
  if (nonZoomIndexes.length === 1) {
    const idx = nonZoomIndexes[0]
    const label = `vbase${idx}`
    parts.push(`[0:v]${scalePad}[${label}]`)
    nonZoomSourceLabels.set(idx, label)
  } else if (nonZoomIndexes.length > 1) {
    const labels = nonZoomIndexes.map((idx) => {
      const label = `vbase${idx}`
      nonZoomSourceLabels.set(idx, label)
      return `[${label}]`
    }).join('')
    parts.push(`[0:v]${scalePad},split=${nonZoomIndexes.length}${labels}`)
  }

  segments.forEach((seg, idx) => {
    const speed = seg.speed && seg.speed > 0 ? seg.speed : 1
    const zoom = seg.zoom && seg.zoom > 0 ? seg.zoom : 0
    const brightness = seg.brightness && seg.brightness !== 0 ? seg.brightness : 0
    const segDuration = Math.max(0.01, roundForFilter((seg.end - seg.start) / speed))
    durations.push(segDuration)
    const vTrim = `trim=start=${toFilterNumber(seg.start)}:end=${toFilterNumber(seg.end)}`
    const vSpeed = speed !== 1 ? `,setpts=(PTS-STARTPTS)/${toFilterNumber(speed)}` : ',setpts=PTS-STARTPTS'
    const zoomScale = 1 + zoom
    const zoomCropScale = zoomScale > 0 ? 1 / zoomScale : 1
    const focusX = Number.isFinite(seg.faceFocusX) ? clamp(Number(seg.faceFocusX), 0.08, 0.92) : 0.5
    const focusY = Number.isFinite(seg.faceFocusY) ? clamp(Number(seg.faceFocusY), 0.08, 0.88) : 0.42
    const cropXExpr = `'max(0,min(iw-ow,${toFilterNumber(focusX)}*iw-ow/2))'`
    const cropYExpr = `'max(0,min(ih-oh,${toFilterNumber(focusY)}*ih-oh/2))'`
    const vZoom = zoom > 0
      ? `,scale=iw*${toFilterNumber(zoomScale)}:ih*${toFilterNumber(zoomScale)},crop=iw*${toFilterNumber(zoomCropScale)}:ih*${toFilterNumber(zoomCropScale)}:${cropXExpr}:${cropYExpr}`
      : ''
    const vBright = brightness !== 0 ? `,eq=brightness=${brightness}:saturation=1.05` : ''
    const nonZoomLabel = nonZoomSourceLabels.get(idx)
    if (nonZoomLabel) {
      parts.push(`[${nonZoomLabel}]${vTrim}${vSpeed}${vBright}[v${idx}]`)
    } else {
      parts.push(`[0:v]${vTrim}${vSpeed}${vZoom}${vBright},${scalePad}[v${idx}]`)
    }

    if (opts.withAudio) {
      const aSpeed = speed !== 1 ? buildAtempoChain(speed) : ''
      const gain = Number.isFinite(seg.audioGain) ? clamp(Number(seg.audioGain), 0.8, 1.24) : 1
      const aGain = Math.abs(gain - 1) >= 0.01 ? `volume=${toFilterNumber(gain)}` : ''
      const aNormalize = 'aformat=sample_rates=48000:channel_layouts=stereo'
      const fadeLen = roundForFilter(0.04)
      const afadeIn = `afade=t=in:st=0:d=${toFilterNumber(fadeLen)}`
      const afadeOut = `afade=t=out:st=${toFilterNumber(Math.max(0, segDuration - fadeLen))}:d=${toFilterNumber(fadeLen)}`
      const soundFxLevel = Number.isFinite(seg.soundFxLevel) ? clamp(Number(seg.soundFxLevel), 0, 1) : 0
      const addSoundFx = canGenerateNoiseFx && soundFxLevel >= 0.16 && segDuration >= 0.14
      const baseAudioLabel = addSoundFx ? `ab${idx}` : `a${idx}`
      if (opts.hasAudioStream) {
        const guard = roundForFilter(0.04)
        const aTrim = `atrim=start=${toFilterNumber(Math.max(0, seg.start - guard))}:end=${toFilterNumber(seg.end + guard)}`
        const chain = [aTrim, 'asetpts=PTS-STARTPTS', aSpeed, aGain, afadeIn, afadeOut, aNormalize].filter(Boolean).join(',')
        parts.push(`[0:a]${chain}[${baseAudioLabel}]`)
      } else {
        const chain = [`anullsrc=r=48000:cl=stereo`, `atrim=duration=${toFilterNumber(segDuration)}`, 'asetpts=PTS-STARTPTS', aSpeed, aGain, afadeIn, afadeOut, aNormalize]
          .filter(Boolean)
          .join(',')
        parts.push(`${chain}[${baseAudioLabel}]`)
      }
      if (addSoundFx) {
        const noiseDuration = clamp(0.09 + 0.12 * soundFxLevel, 0.08, Math.min(0.24, Math.max(0.08, segDuration - 0.02)))
        const noiseFadeDur = Math.max(0.03, noiseDuration * 0.74)
        const noiseFadeStart = Math.max(0.01, noiseDuration - noiseFadeDur)
        const noiseVolume = clamp(0.012 + 0.026 * soundFxLevel, 0.008, 0.05)
        const noiseLabel = `nfx${idx}`
        const noiseChain = [
          `anoisesrc=r=48000:color=white:duration=${toFilterNumber(segDuration)}`,
          'aformat=sample_rates=48000:channel_layouts=stereo',
          'highpass=f=1300',
          'lowpass=f=9000',
          `volume=${toFilterNumber(noiseVolume)}`,
          `afade=t=out:st=${toFilterNumber(noiseFadeStart)}:d=${toFilterNumber(noiseFadeDur)}`
        ].join(',')
        parts.push(`${noiseChain}[${noiseLabel}]`)
        parts.push(`[${baseAudioLabel}][${noiseLabel}]amix=inputs=2:weights='1 0.95':normalize=0[a${idx}]`)
      }
    }
  })

  const enableFades = opts.enableFades !== false
  if (segments.length <= 1 || STITCH_FADE_SEC <= 0 || !enableFades) {
    if (opts.withAudio) {
      const inputs = segments.map((_, idx) => `[v${idx}][a${idx}]`).join('')
      parts.push(`${inputs}concat=n=${segments.length}:v=1:a=1[outv][outa]`)
    } else {
      const inputs = segments.map((_, idx) => `[v${idx}]`).join('')
      parts.push(`${inputs}concat=n=${segments.length}:v=1:a=0[outv]`)
    }
    return parts.join(';')
  }

  const fades: number[] = []
  let cumulative = durations[0] || 0
  let vPrev = `v0`
  for (let i = 1; i < segments.length; i += 1) {
    const prevTransition = segments[i - 1]?.transitionStyle
    const nextTransition = segments[i]?.transitionStyle
    const jumpBoundary = prevTransition === 'jump' || nextTransition === 'jump'
    const desiredFade = jumpBoundary ? JUMPCUT_FADE_SEC : STITCH_FADE_SEC
    const fade = Math.min(desiredFade, (durations[i - 1] || desiredFade) / 2, (durations[i] || desiredFade) / 2)
    const safeFade = Math.max(0.004, roundForFilter(fade))
    const offset = Math.max(0, roundForFilter(cumulative - safeFade))
    const outLabel = `vx${i}`
    parts.push(`[${vPrev}][v${i}]xfade=transition=fade:duration=${toFilterNumber(safeFade)}:offset=${toFilterNumber(offset)}[${outLabel}]`)
    fades.push(safeFade)
    vPrev = outLabel
    cumulative += (durations[i] || 0) - safeFade
  }

  if (opts.withAudio) {
    let aPrev = `a0`
    for (let i = 1; i < segments.length; i += 1) {
      const fade = fades[i - 1] ?? STITCH_FADE_SEC
      const outLabel = `ax${i}`
      parts.push(`[${aPrev}][a${i}]acrossfade=d=${toFilterNumber(fade)}:c1=tri:c2=tri[${outLabel}]`)
      aPrev = outLabel
    }
    parts.push(`[${vPrev}]format=yuv420p[outv]`)
    parts.push(`[${aPrev}]aformat=sample_rates=48000:channel_layouts=stereo[outa]`)
  } else {
    parts.push(`[${vPrev}]format=yuv420p[outv]`)
  }

  return parts.join(';')
}

const escapeFilterPath = (value: string) => {
  const escaped = value.replace(/\\/g, '\\\\').replace(/:/g, '\\:').replace(/'/g, "\\'")
  return `'${escaped}'`
}

let cachedFontFile: string | null | undefined
const getSystemFontFile = () => {
  if (cachedFontFile !== undefined) return cachedFontFile
  const candidates = process.platform === 'win32'
    ? ['C:\\\\Windows\\\\Fonts\\\\arial.ttf', 'C:\\\\Windows\\\\Fonts\\\\segoeui.ttf', 'C:\\\\Windows\\\\Fonts\\\\calibri.ttf']
    : process.platform === 'darwin'
      ? ['/System/Library/Fonts/Supplemental/Arial.ttf', '/System/Library/Fonts/Supplemental/Helvetica.ttf', '/Library/Fonts/Arial.ttf']
      : ['/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf', '/usr/share/fonts/truetype/freefont/FreeSans.ttf']
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      cachedFontFile = candidate
      return cachedFontFile
    }
  }
  cachedFontFile = null
  return cachedFontFile
}

const scoreSegment = (segment: Segment, windows: EngagementWindow[]) => {
  const relevant = windows.filter((w) => w.time >= segment.start && w.time < segment.end)
  if (!relevant.length) return 0
  return relevant.reduce((sum, w) => sum + w.score, 0) / relevant.length
}

const applyStoryStructure = (
  segments: Segment[],
  windows: EngagementWindow[],
  durationSeconds: number,
  styleProfile?: ContentStyleProfile | null
) => {
  if (segments.length <= 3) return segments
  const scored = segments.map((seg, idx) => ({ seg, idx, score: scoreSegment(seg, windows) }))
  const reordered = segments.slice()
  const style = styleProfile?.style || 'story'
  const tutorialMode = style === 'tutorial'
  const highEnergyMode = style === 'reaction' || style === 'gaming'

  // Lift a strong mid-video beat closer to the front to improve narrative momentum.
  const middleStart = Math.max(0, durationSeconds * (tutorialMode ? 0.24 : 0.2))
  const middleEnd = Math.max(middleStart + 1, durationSeconds * (tutorialMode ? 0.72 : 0.78))
  const middleCandidates = scored
    .filter((entry) => entry.seg.start >= middleStart && entry.seg.start <= middleEnd)
    .sort((a, b) => b.score - a.score)
  const middleHighlight = middleCandidates[0]
  if (middleHighlight) {
    const fromIdx = reordered.findIndex((seg) => seg === middleHighlight.seg)
    if (fromIdx > (tutorialMode ? 2 : 1)) {
      const [moved] = reordered.splice(fromIdx, 1)
      reordered.splice(tutorialMode ? 2 : 1, 0, moved)
    }
  }

  // Keep a high-energy late beat as the closer.
  const tailStart = Math.max(0, durationSeconds * 0.6)
  const tailCandidates = scored
    .filter((entry) => entry.seg.start >= tailStart)
    .sort((a, b) => b.score - a.score)
  const bestTail = tailCandidates[0]
  if (bestTail) {
    const tailIdx = reordered.findIndex((seg) => seg === bestTail.seg)
    if (tailIdx >= 0 && tailIdx !== reordered.length - 1) {
      const [moved] = reordered.splice(tailIdx, 1)
      reordered.push(moved)
    }
  }

  if (highEnergyMode) {
    const frontBoost = scored
      .filter((entry) => entry.seg.start <= Math.max(8, durationSeconds * 0.4))
      .sort((a, b) => b.score - a.score)
      .slice(0, 2)
      .map((entry) => entry.seg)
    for (const seg of frontBoost) {
      const idx = reordered.findIndex((item) => item === seg)
      if (idx > 2) {
        const [moved] = reordered.splice(idx, 1)
        reordered.splice(2, 0, moved)
      }
    }
  }

  return reordered
}

const resolveSubtitleFontName = (fontId?: SubtitleFontId) => {
  if (fontId === 'sans_bold') return 'DejaVu Sans'
  if (fontId === 'condensed') return 'DejaVu Sans Condensed'
  if (fontId === 'serif_bold') return 'DejaVu Serif'
  return 'Impact'
}

const toAssColorFromHex = (value?: string | null) => {
  const compact = String(value || '').trim().replace(/^#/, '').toUpperCase()
  const normalized = /^[0-9A-F]{6}$/.test(compact) ? compact : 'FFFFFF'
  const rr = normalized.slice(0, 2)
  const gg = normalized.slice(2, 4)
  const bb = normalized.slice(4, 6)
  return `&H00${bb}${gg}${rr}`
}

const toAssTimestamp = (seconds: number) => {
  const safe = Math.max(0, Number(seconds) || 0)
  const totalWhole = Math.floor(safe)
  const hours = Math.floor(totalWhole / 3600)
  const minutes = Math.floor((totalWhole % 3600) / 60)
  const secs = totalWhole % 60
  const centis = Math.floor((safe - totalWhole) * 100)
  return `${hours}:${String(minutes).padStart(2, '0')}:${String(secs).padStart(2, '0')}.${String(centis).padStart(2, '0')}`
}

const escapeAssDialogueText = (text: string) =>
  String(text || '')
    .replace(/\\/g, '\\\\')
    .replace(/\{/g, '\\{')
    .replace(/\}/g, '\\}')
    .replace(/\r?\n/g, '\\N')

const buildMrBeastAnimatedAss = ({
  srtPath,
  workingDir,
  style
}: {
  srtPath: string
  workingDir: string
  style?: string | null
}) => {
  const config = parseSubtitleStyleConfig(style)
  if (config.preset !== 'mrbeast_animated') return null
  const cues = parseTranscriptCues(srtPath)
  if (!cues.length) return null
  const fontName = resolveSubtitleFontName(config.fontId)
  const primaryColor = toAssColorFromHex(config.textColor)
  const secondaryColor = toAssColorFromHex(config.accentColor)
  const outlineColor = toAssColorFromHex(config.outlineColor)
  const outlineWidth = Math.max(1, Math.min(12, Math.round(Number(config.outlineWidth || 6))))
  const baseTag = config.animation === 'pop'
    ? `{\\an2\\fscx82\\fscy82\\bord${outlineWidth}\\t(0,130,\\fscx100\\fscy100)\\t(130,240,\\fscx106\\fscy106)\\t(240,320,\\fscx100\\fscy100)}`
    : `{\\an2\\bord${outlineWidth}}`

  const assLines: string[] = [
    '[Script Info]',
    'ScriptType: v4.00+',
    'PlayResX: 1080',
    'PlayResY: 1920',
    'WrapStyle: 2',
    'ScaledBorderAndShadow: yes',
    '',
    '[V4+ Styles]',
    'Format: Name,Fontname,Fontsize,PrimaryColour,SecondaryColour,OutlineColour,BackColour,Bold,Italic,Underline,StrikeOut,ScaleX,ScaleY,Spacing,Angle,BorderStyle,Outline,Shadow,Alignment,MarginL,MarginR,MarginV,Encoding',
    `Style: Beast,${fontName},58,${primaryColor},${secondaryColor},${outlineColor},&H64000000,-1,0,0,0,100,100,1,0,1,${outlineWidth},0,2,30,30,70,1`,
    '',
    '[Events]',
    'Format: Layer,Start,End,Style,Name,MarginL,MarginR,MarginV,Effect,Text'
  ]

  for (const cue of cues) {
    const start = Number.isFinite(cue.start) ? cue.start : 0
    const end = Number.isFinite(cue.end) ? cue.end : start + 0.8
    if (end <= start + 0.03) continue
    const text = escapeAssDialogueText(String(cue.text || '').toUpperCase())
    if (!text) continue
    assLines.push(
      `Dialogue: 0,${toAssTimestamp(start)},${toAssTimestamp(end)},Beast,,0,0,0,,${baseTag}${text}`
    )
  }
  if (assLines.length <= 13) return null
  const assPath = path.join(
    workingDir,
    `${path.basename(srtPath, path.extname(srtPath))}-mrbeast.ass`
  )
  fs.writeFileSync(assPath, assLines.join('\n'))
  return assPath
}

const buildSubtitleStyle = (style?: string | null) => {
  const styleConfig = parseSubtitleStyleConfig(style)
  const base = {
    FontName: 'DejaVu Sans',
    FontSize: '42',
    PrimaryColour: '&H00FFFFFF',
    OutlineColour: '&H80000000',
    BackColour: '&H00000000',
    BorderStyle: '1',
    Outline: '2',
    Shadow: '0',
    Alignment: '2'
  }
  const mrBeastStyle: Partial<typeof base> = {
    FontName: resolveSubtitleFontName(styleConfig.fontId),
    FontSize: '58',
    PrimaryColour: toAssColorFromHex(styleConfig.textColor),
    OutlineColour: toAssColorFromHex(styleConfig.outlineColor),
    Outline: String(Math.max(1, Math.min(12, Math.round(Number(styleConfig.outlineWidth || 6))))),
    Shadow: '0',
    Alignment: '2'
  }
  const styles: Record<string, Partial<typeof base>> = {
    minimal: {},
    basicclean: { FontSize: '40', Outline: '1' },
    clean: { FontSize: '40', Outline: '1' },
    bold: { FontName: 'DejaVu Sans', FontSize: '48', Outline: '3', Shadow: '1' },
    boldpop: { FontName: 'DejaVu Sans', FontSize: '48', Outline: '3', Shadow: '1', PrimaryColour: '&H0000FFFF' },
    mrbeastanimated: mrBeastStyle,
    neon: { PrimaryColour: '&H00F5FF00', OutlineColour: '&H80000000', Shadow: '2', Outline: '3' },
    neonglow: { PrimaryColour: '&H00F5FF00', OutlineColour: '&H80000000', Shadow: '2', Outline: '3' },
    cinematic: { FontName: 'DejaVu Serif', FontSize: '40', Outline: '2', Shadow: '1' },
    outlineheavy: { FontName: 'DejaVu Serif', FontSize: '40', Outline: '3', Shadow: '1' },
    highcontrast: { PrimaryColour: '&H0000FFFF', OutlineColour: '&H80000000', Outline: '3' },
    blackbox: { BorderStyle: '3', BackColour: '&H80000000', Outline: '0', Shadow: '0' },
    captionbox: { BorderStyle: '3', BackColour: '&H80000000', Outline: '0', Shadow: '0' },
    karaoke: { FontSize: '46', Outline: '3', Shadow: '1' },
    karaokehighlight: { FontSize: '46', Outline: '3', Shadow: '1' }
  }
  const key = (normalizeSubtitlePreset(style) || DEFAULT_SUBTITLE_PRESET).toLowerCase().replace(/[\s_-]/g, '')
  const selection = styles[key] || styles.minimal
  const merged = { ...base, ...selection }
  return Object.entries(merged)
    .map(([k, v]) => `${k}=${v}`)
    .join(',')
}

const splitWhisperArgs = (raw?: string | null) => {
  if (!raw) return [] as string[]
  return String(raw)
    .trim()
    .split(/\s+/)
    .filter(Boolean)
}

const resolveGeneratedSubtitlePath = (inputPath: string, workingDir: string) => {
  const baseName = path.basename(inputPath, path.extname(inputPath))
  const exact = path.join(workingDir, `${baseName}.srt`)
  if (fs.existsSync(exact)) return exact
  const candidates = fs.readdirSync(workingDir)
    .filter((name) => name.startsWith(`${baseName}.`) && name.toLowerCase().endsWith('.srt'))
    .map((name) => {
      const fullPath = path.join(workingDir, name)
      let mtimeMs = 0
      try {
        mtimeMs = fs.statSync(fullPath).mtimeMs
      } catch {
        mtimeMs = 0
      }
      return { fullPath, mtimeMs }
    })
    .sort((a, b) => b.mtimeMs - a.mtimeMs)
  return candidates.length ? candidates[0].fullPath : null
}

const runWhisperTranscribe = async ({
  command,
  args,
  inputPath,
  workingDir,
  label
}: {
  command: string
  args: string[]
  inputPath: string
  workingDir: string
  label: string
}) => {
  return new Promise<string | null>((resolve) => {
    const proc = spawn(command, args)
    proc.on('error', () => resolve(null))
    proc.on('close', (code) => {
      if (code !== 0) {
        console.warn(`subtitle generation failed via ${label} (exit ${code})`)
        return resolve(null)
      }
      resolve(resolveGeneratedSubtitlePath(inputPath, workingDir))
    })
  })
}

const generateSubtitles = async (inputPath: string, workingDir: string) => {
  const model = process.env.WHISPER_MODEL || 'base'
  const configuredArgs = splitWhisperArgs(process.env.WHISPER_ARGS)
  const baseArgs = configuredArgs.length
    ? configuredArgs
    : ['--model', model, '--output_format', 'srt', '--output_dir', workingDir, '--word_timestamps', 'True']

  const attempts: Array<{ command: string; args: string[]; label: string }> = []
  const addAttempt = (command: string | undefined, args: string[], label: string) => {
    if (!command) return
    const normalized = String(command).trim()
    if (!normalized) return
    const duplicate = attempts.some((attempt) => attempt.command === normalized && attempt.args.join('\u001f') === args.join('\u001f'))
    if (duplicate) return
    attempts.push({ command: normalized, args, label })
  }

  addAttempt(process.env.WHISPER_BIN, [inputPath, ...baseArgs], 'WHISPER_BIN')
  addAttempt('whisper', [inputPath, ...baseArgs], 'whisper')
  addAttempt('python', ['-m', 'whisper', inputPath, ...baseArgs], 'python -m whisper')
  addAttempt('python3', ['-m', 'whisper', inputPath, ...baseArgs], 'python3 -m whisper')
  addAttempt('py', ['-m', 'whisper', inputPath, ...baseArgs], 'py -m whisper')

  for (const attempt of attempts) {
    const output = await runWhisperTranscribe({
      command: attempt.command,
      args: attempt.args,
      inputPath,
      workingDir,
      label: attempt.label
    })
    if (output) return output
  }
  return null
}

const generateProxy = async (inputPath: string, outPath: string, opts?: { width?: number; height?: number }) => {
  const width = opts?.width ?? ANALYSIS_PROXY_WIDTH
  const height = opts?.height ?? ANALYSIS_PROXY_HEIGHT
  const scale = `scale='min(${width},iw)':'min(${height},ih)':force_original_aspect_ratio=decrease,pad=${width}:${height}:(ow-iw)/2:(oh-ih)/2,setsar=1`
  const args = ['-hide_banner', '-nostdin', '-y', '-i', inputPath, '-vf', scale, '-c:v', 'libx264', '-preset', 'superfast', '-crf', '28', '-threads', '0', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-c:a', 'copy', outPath]
  await runFfmpeg(args)
}

const normalizeStoredEngagementWindows = (raw: any): EngagementWindow[] => {
  if (!Array.isArray(raw)) return []
  return raw
    .map((window: any) => ({
      time: Number(window?.time),
      audioEnergy: clamp01(Number(window?.audioEnergy ?? 0)),
      speechIntensity: clamp01(Number(window?.speechIntensity ?? 0)),
      motionScore: clamp01(Number(window?.motionScore ?? 0)),
      facePresence: clamp01(Number(window?.facePresence ?? 0)),
      faceIntensity: Number.isFinite(Number(window?.faceIntensity))
        ? clamp01(Number(window.faceIntensity))
        : undefined,
      faceCenterX: Number.isFinite(Number(window?.faceCenterX))
        ? clamp01(Number(window.faceCenterX))
        : undefined,
      faceCenterY: Number.isFinite(Number(window?.faceCenterY))
        ? clamp01(Number(window.faceCenterY))
        : undefined,
      textDensity: clamp01(Number(window?.textDensity ?? 0)),
      sceneChangeRate: clamp01(Number(window?.sceneChangeRate ?? 0)),
      emotionalSpike: Number(window?.emotionalSpike ? 1 : 0),
      vocalExcitement: clamp01(Number(window?.vocalExcitement ?? 0)),
      emotionIntensity: clamp01(Number(window?.emotionIntensity ?? 0)),
      score: clamp01(Number(window?.score ?? 0)),
      hookScore: Number.isFinite(Number(window?.hookScore)) ? clamp01(Number(window.hookScore)) : undefined,
      curiosityTrigger: Number.isFinite(Number(window?.curiosityTrigger)) ? clamp01(Number(window.curiosityTrigger)) : undefined,
      actionSpike: Number.isFinite(Number(window?.actionSpike)) ? clamp01(Number(window.actionSpike)) : undefined
    }))
    .filter((window: EngagementWindow) => Number.isFinite(window.time) && window.time >= 0)
}

const normalizeStoredHookCandidates = (raw: any): HookCandidate[] => {
  if (!Array.isArray(raw)) return []
  return raw
    .map((candidate: any) => {
      const start = Number(candidate?.start)
      const duration = Number(candidate?.duration)
      if (!Number.isFinite(start) || !Number.isFinite(duration) || duration <= 0) return null
      const score = Number.isFinite(Number(candidate?.score)) ? clamp01(Number(candidate.score)) : 0
      const auditScore = Number.isFinite(Number(candidate?.auditScore))
        ? clamp01(Number(candidate.auditScore))
        : score
      return {
        start: Number(start.toFixed(3)),
        duration: Number(duration.toFixed(3)),
        score: Number(score.toFixed(4)),
        auditScore: Number(auditScore.toFixed(4)),
        auditPassed: Boolean(candidate?.auditPassed),
        text: typeof candidate?.text === 'string' ? candidate.text : '',
        reason: typeof candidate?.reason === 'string' ? candidate.reason : '',
        synthetic: Boolean(candidate?.synthetic)
      } as HookCandidate
    })
    .filter((candidate): candidate is HookCandidate => Boolean(candidate))
}

const parsePreferredHookCandidateFromPayload = (raw: any): HookCandidate | null => {
  if (!raw || typeof raw !== 'object') return null
  const normalized = normalizeStoredHookCandidates([raw])
  return normalized[0] ?? null
}

const matchPreferredHookCandidate = ({
  preferred,
  candidates
}: {
  preferred: HookCandidate | null
  candidates: HookCandidate[]
}): HookCandidate | null => {
  if (!preferred) return null
  if (
    !Number.isFinite(preferred.start) ||
    !Number.isFinite(preferred.duration) ||
    preferred.duration < HOOK_MIN - 0.01 ||
    preferred.duration > HOOK_MAX + 0.01
  ) {
    return null
  }
  if (!Array.isArray(candidates) || candidates.length === 0) return preferred
  const matched = candidates.find((candidate) => (
    Math.abs(candidate.start - preferred.start) <= HOOK_SELECTION_MATCH_START_TOLERANCE_SEC &&
    Math.abs(candidate.duration - preferred.duration) <= HOOK_SELECTION_MATCH_DURATION_TOLERANCE_SEC
  ))
  return matched || null
}

const getHookCandidatesFromAnalysis = (analysisRaw: any): HookCandidate[] => {
  const analysis = analysisRaw && typeof analysisRaw === 'object' ? analysisRaw : {}
  const pipelineSteps = analysis.pipelineSteps && typeof analysis.pipelineSteps === 'object'
    ? analysis.pipelineSteps
    : {}
  const rawCandidates = [
    ...(Array.isArray(analysis.hook_variants) ? analysis.hook_variants : []),
    ...(Array.isArray(analysis.hook_candidates) ? analysis.hook_candidates : []),
    ...(Array.isArray(analysis?.editPlan?.hookVariants) ? analysis.editPlan.hookVariants : []),
    ...(Array.isArray(analysis?.editPlan?.hookCandidates) ? analysis.editPlan.hookCandidates : []),
    ...(Array.isArray((pipelineSteps as any)?.HOOK_SCORING?.meta?.topCandidates)
      ? (pipelineSteps as any).HOOK_SCORING.meta.topCandidates
      : []),
    ...(Array.isArray((pipelineSteps as any)?.BEST_MOMENT_SCORING?.meta?.topCandidates)
      ? (pipelineSteps as any).BEST_MOMENT_SCORING.meta.topCandidates
      : []),
    ...((pipelineSteps as any)?.HOOK_SELECT_AND_AUDIT?.meta?.selectedHook
      ? [(pipelineSteps as any).HOOK_SELECT_AND_AUDIT.meta.selectedHook]
      : [])
  ]
  const normalized = normalizeStoredHookCandidates(rawCandidates)
  const seen = new Set<string>()
  const deduped: HookCandidate[] = []
  for (const candidate of normalized) {
    const key = `${candidate.start.toFixed(3)}:${candidate.duration.toFixed(3)}`
    if (seen.has(key)) continue
    seen.add(key)
    deduped.push(candidate)
  }
  return deduped
}

const waitForPreferredHookSelection = async ({
  jobId,
  candidates,
  timeoutMs = HOOK_SELECTION_WAIT_MS,
  pollMs = HOOK_SELECTION_POLL_MS
}: {
  jobId: string
  candidates: HookCandidate[]
  timeoutMs?: number
  pollMs?: number
}) => {
  if (!jobId || !Array.isArray(candidates) || candidates.length === 0) return null
  const waitMs = Math.max(0, Number(timeoutMs || 0))
  if (waitMs <= 0) return null
  const pollInterval = clamp(Number(pollMs || HOOK_SELECTION_POLL_MS), 120, 2_000)
  const deadline = Date.now() + waitMs
  while (Date.now() < deadline) {
    if (isPipelineCanceled(jobId)) throw new JobCanceledError(jobId)
    const sleepMs = Math.min(pollInterval, Math.max(40, deadline - Date.now()))
    await delay(sleepMs)
    const snapshot = await prisma.job.findUnique({
      where: { id: jobId },
      select: { status: true, analysis: true }
    })
    if (!snapshot) break
    const status = String(snapshot.status || '').toLowerCase()
    if (status === 'failed' || status === 'completed') break
    const analysis = ((snapshot.analysis as any) || {}) as Record<string, any>
    const preferred = parsePreferredHookCandidateFromPayload(analysis.preferred_hook)
    const matched = matchPreferredHookCandidate({
      preferred,
      candidates
    })
    if (matched) return matched
  }
  return null
}

const buildVerticalClipRanges = (
  durationSeconds: number,
  requestedCount: number,
  opts?: {
    windows?: EngagementWindow[]
    platformProfile?: PlatformProfile
  }
) => {
  const total = Math.max(0, durationSeconds || 0)
  if (total <= 0) return [{ start: 0, end: 0 }]
  const platformProfile = PLATFORM_EDIT_PROFILES[parsePlatformProfile(opts?.platformProfile, 'auto')] || PLATFORM_EDIT_PROFILES.auto
  let clipCount = clamp(requestedCount || 1, 1, MAX_VERTICAL_CLIPS)
  const maxFeasibleByLength = Math.max(1, Math.floor(total / Math.max(1, platformProfile.verticalMinClipSeconds)))
  clipCount = Math.min(clipCount, maxFeasibleByLength)
  const windows = Array.isArray(opts?.windows) ? opts!.windows! : []

  const fallbackRanges = (() => {
    const chunk = total / clipCount
    const ranges: TimeRange[] = []
    for (let index = 0; index < clipCount; index += 1) {
      const start = Number((index * chunk).toFixed(3))
      const end = Number((index === clipCount - 1 ? total : (index + 1) * chunk).toFixed(3))
      if (end - start > 0.2) ranges.push({ start, end })
    }
    return ranges.length ? ranges : [{ start: 0, end: total }]
  })()

  if (!windows.length) return fallbackRanges

  const targetClipDuration = Number(clamp(
    total / Math.max(1, clipCount * platformProfile.verticalClipDurationDivisor),
    Math.max(2, platformProfile.verticalMinClipSeconds),
    Math.max(
      Math.max(2, platformProfile.verticalMinClipSeconds),
      platformProfile.verticalMaxClipSeconds
    )
  ).toFixed(3))
  const step = total > 300 ? 2 : 1
  const candidates: Array<{ range: TimeRange; score: number }> = []

  for (let start = 0; start + targetClipDuration <= total; start += step) {
    const end = Number((start + targetClipDuration).toFixed(3))
    const startRounded = Number(start.toFixed(3))
    const coreScore = averageWindowMetric(windows, startRounded, end, (window) => (
      0.3 * window.score +
      0.2 * window.emotionIntensity +
      0.16 * window.vocalExcitement +
      0.13 * (window.actionSpike ?? 0) +
      0.1 * window.motionScore +
      0.07 * window.sceneChangeRate +
      0.04 * window.speechIntensity
    ))
    const spikeScore = averageWindowMetric(windows, startRounded, end, (window) => (
      Math.max(
        window.emotionIntensity,
        window.vocalExcitement,
        window.motionScore,
        window.sceneChangeRate,
        window.actionSpike ?? 0
      )
    ))
    const score = Number((coreScore * 0.78 + spikeScore * 0.22).toFixed(5))
    candidates.push({
      range: { start: startRounded, end },
      score
    })
  }

  if (!candidates.length) return fallbackRanges
  const sorted = candidates
    .slice()
    .sort((a, b) => b.score - a.score || a.range.start - b.range.start)
  const selected: TimeRange[] = []
  const minSpacing = Math.max(2, targetClipDuration * clamp(platformProfile.verticalSpacingRatio, 0.1, 0.5))
  for (const entry of sorted) {
    if (selected.length >= clipCount) break
    const overlaps = selected.some((range) => (
      entry.range.start < range.end + minSpacing &&
      entry.range.end > range.start - minSpacing
    ))
    if (overlaps) continue
    selected.push(entry.range)
  }
  if (selected.length < clipCount) {
    for (const range of fallbackRanges) {
      if (selected.length >= clipCount) break
      const overlaps = selected.some((existing) => (
        range.start < existing.end + minSpacing &&
        range.end > existing.start - minSpacing
      ))
      if (overlaps) continue
      selected.push(range)
    }
  }
  if (selected.length < clipCount) {
    // If spacing constraints are too strict for dense edits, fill remaining slots deterministically.
    for (const range of fallbackRanges) {
      if (selected.length >= clipCount) break
      const exists = selected.some((existing) => (
        Math.abs(existing.start - range.start) < 0.01 &&
        Math.abs(existing.end - range.end) < 0.01
      ))
      if (exists) continue
      selected.push(range)
    }
  }
  if (selected.length < clipCount) {
    const chunk = total / clipCount
    for (let index = 0; index < clipCount && selected.length < clipCount; index += 1) {
      const start = Number((index * chunk).toFixed(3))
      const end = Number((index === clipCount - 1 ? total : (index + 1) * chunk).toFixed(3))
      if (end - start <= 0.2) continue
      const exists = selected.some((existing) => (
        Math.abs(existing.start - start) < 0.01 &&
        Math.abs(existing.end - end) < 0.01
      ))
      if (exists) continue
      selected.push({ start, end })
    }
  }
  if (!selected.length) return fallbackRanges
  return selected
    .slice(0, clipCount)
    .sort((a, b) => a.start - b.start)
}

const buildFrameFitFilter = (fit: HorizontalFitMode, width: number, height: number) => {
  if (fit === 'cover') {
    return [
      `scale=${width}:${height}:force_original_aspect_ratio=increase`,
      `crop=${width}:${height}`,
      'setsar=1',
      'format=yuv420p'
    ].join(',')
  }
  return [
    `scale=${width}:${height}:force_original_aspect_ratio=decrease`,
    `pad=${width}:${height}:(ow-iw)/2:(oh-ih)/2`,
    'setsar=1',
    'format=yuv420p'
  ].join(',')
}

const computeVerticalTopHeightPx = (mode: VerticalModeSettings, outputHeight: number) => {
  const explicitTop = Number(mode?.topHeightPx)
  let topHeight = Number.isFinite(explicitTop) && explicitTop > 0
    ? explicitTop
    : outputHeight * DEFAULT_VERTICAL_TOP_HEIGHT_PCT
  topHeight = Math.round(topHeight)
  return Math.round(clamp(topHeight, 200, Math.max(200, outputHeight - 200)))
}

const normalizeVerticalCropToSource = ({
  crop,
  sourceWidth,
  sourceHeight
}: {
  crop: VerticalWebcamCrop | null
  sourceWidth: number
  sourceHeight: number
}) => {
  const defaultHeight = Math.round(clamp(sourceHeight * 0.4, 48, sourceHeight))
  const defaultY = Math.round(clamp(sourceHeight * 0.05, 0, Math.max(0, sourceHeight - defaultHeight)))
  const fallbackCrop = {
    x: 0,
    y: defaultY,
    w: sourceWidth,
    h: defaultHeight
  }
  let x = 0
  let y = defaultY
  let w = sourceWidth
  let h = defaultHeight

  if (crop) {
    const rawValues = [crop.x, crop.y, crop.w, crop.h]
    if (rawValues.some((value) => !Number.isFinite(value))) {
      return fallbackCrop
    }
    const isNormalized = crop.w <= 1.0001 && crop.h <= 1.0001 && crop.x >= -0.0001 && crop.y >= -0.0001
    if (isNormalized) {
      x = crop.x * sourceWidth
      y = crop.y * sourceHeight
      w = crop.w * sourceWidth
      h = crop.h * sourceHeight
    } else {
      x = crop.x
      y = crop.y
      w = crop.w
      h = crop.h
    }
  }

  x = clamp(x, 0, Math.max(0, sourceWidth - 2))
  y = clamp(y, 0, Math.max(0, sourceHeight - 2))
  w = clamp(w, 2, sourceWidth - x)
  h = clamp(h, 2, sourceHeight - y)
  if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(w) || !Number.isFinite(h) || w <= 1 || h <= 1) {
    return fallbackCrop
  }
  return {
    x: Math.round(x),
    y: Math.round(y),
    w: Math.round(w),
    h: Math.round(h)
  }
}

const buildVerticalBottomFilter = (fit: VerticalFitMode, outWidth: number, outHeight: number) => {
  if (fit === 'contain') {
    return [
      `scale=w=${outWidth}:h=${outHeight}:force_original_aspect_ratio=decrease`,
      `pad=${outWidth}:${outHeight}:(ow-iw)/2:(oh-ih)/2`,
      'setsar=1',
      'format=yuv420p'
    ].join(',')
  }
  return [
    `scale=w=${outWidth}:h=${outHeight}:force_original_aspect_ratio=increase`,
    `crop=w=${outWidth}:h=${outHeight}`,
    'setsar=1',
    'format=yuv420p'
  ].join(',')
}

const buildVerticalStackedFilterGraph = ({
  start,
  end,
  crop,
  outputWidth,
  outputHeight,
  topHeight,
  bottomFit,
  withAudio
}: {
  start: number
  end: number
  crop: VerticalWebcamCrop
  outputWidth: number
  outputHeight: number
  topHeight: number
  bottomFit: VerticalFitMode
  withAudio: boolean
}) => {
  const bottomHeight = Math.max(1, outputHeight - topHeight)
  const filters = [
    `[0:v]trim=start=${start}:end=${end},setpts=PTS-STARTPTS,split=2[vfull][vweb]`,
    [
      `[vweb]crop=w=${crop.w}:h=${crop.h}:x=${crop.x}:y=${crop.y}`,
      `scale=w=${outputWidth}:h=${topHeight}:force_original_aspect_ratio=increase`,
      `crop=w=${outputWidth}:h=${topHeight}`,
      'setsar=1',
      'format=yuv420p[top]'
    ].join(','),
    `[vfull]${buildVerticalBottomFilter(bottomFit, outputWidth, bottomHeight)}[bottom]`,
    '[top][bottom]vstack=inputs=2[outv]'
  ]
  if (withAudio) {
    filters.push('[0:a]atrim=start=' + start + ':end=' + end + ',asetpts=PTS-STARTPTS,aformat=sample_rates=48000:channel_layouts=stereo[outa]')
  }
  return filters.join(';')
}

const buildVerticalSingleFilterGraph = ({
  start,
  end,
  outputWidth,
  outputHeight,
  fit,
  withAudio
}: {
  start: number
  end: number
  outputWidth: number
  outputHeight: number
  fit: VerticalFitMode
  withAudio: boolean
}) => {
  const filters = [
    `[0:v]trim=start=${start}:end=${end},setpts=PTS-STARTPTS,${buildVerticalBottomFilter(fit, outputWidth, outputHeight)}[outv]`
  ]
  if (withAudio) {
    filters.push('[0:a]atrim=start=' + start + ':end=' + end + ',asetpts=PTS-STARTPTS,aformat=sample_rates=48000:channel_layouts=stereo[outa]')
  }
  return filters.join(';')
}

const renderVerticalClip = async ({
  inputPath,
  outputPath,
  start,
  end,
  verticalMode,
  sourceWidth,
  sourceHeight,
  withAudio,
  videoPreset,
  videoCrf,
  audioBitrate,
  audioSampleRate,
  audioFilters,
  subtitlePath,
  subtitleIsAss,
  subtitleStyle
}: {
  inputPath: string
  outputPath: string
  start: number
  end: number
  verticalMode: VerticalModeSettings
  sourceWidth: number
  sourceHeight: number
  withAudio: boolean
  videoPreset: string
  videoCrf: string
  audioBitrate: string
  audioSampleRate: string
  audioFilters: string[]
  subtitlePath?: string | null
  subtitleIsAss?: boolean
  subtitleStyle?: string | null
}) => {
  const outputWidth = Math.round(clamp(verticalMode.output.width, 240, 4320))
  const outputHeight = Math.round(clamp(verticalMode.output.height, 426, 7680))
  const layout = parseVerticalLayoutMode(verticalMode.layout, 'stacked')
  const fit = parseVerticalFitMode(verticalMode.bottomFit, 'cover')
  const baseFilterComplex = layout === 'single'
    ? buildVerticalSingleFilterGraph({
        start,
        end,
        outputWidth,
        outputHeight,
        fit,
        withAudio
      })
    : (() => {
        const topHeight = computeVerticalTopHeightPx(verticalMode, outputHeight)
        const sourceCrop = normalizeVerticalCropToSource({
          crop: verticalMode.webcamCrop,
          sourceWidth,
          sourceHeight
        })
        return buildVerticalStackedFilterGraph({
          start,
          end,
          crop: sourceCrop,
          outputWidth,
          outputHeight,
          topHeight,
            bottomFit: fit,
            withAudio
          })
      })()
  const subtitleFilter = subtitlePath
    ? (
      subtitleIsAss
        ? `subtitles=${escapeFilterPath(subtitlePath)}`
        : `subtitles=${escapeFilterPath(subtitlePath)}:force_style='${buildSubtitleStyle(subtitleStyle)}'`
    )
    : ''
  const filterWithSubtitles = subtitleFilter
    ? `${baseFilterComplex};[outv]${subtitleFilter}[vsub]`
    : baseFilterComplex
  const shouldPolishAudio = withAudio && audioFilters.length > 0
  const filterComplex = shouldPolishAudio
    ? `${filterWithSubtitles};[outa]${audioFilters.join(',')}[aout]`
    : filterWithSubtitles

  const args = [
    '-y',
    '-nostdin',
    '-hide_banner',
    '-loglevel',
    'error',
    '-filter_threads',
    String(RENDER_FILTER_THREADS),
    '-i',
    inputPath,
    '-movflags',
    '+faststart',
    '-c:v',
    'libx264',
    '-preset',
    videoPreset,
    '-crf',
    videoCrf,
    '-threads',
    '0',
    '-pix_fmt',
    'yuv420p',
    '-filter_complex',
    filterComplex,
    '-map',
    subtitleFilter ? '[vsub]' : '[outv]'
  ]
  if (withAudio) {
    args.push(
      '-map',
      shouldPolishAudio ? '[aout]' : '[outa]',
      '-c:a',
      'aac',
      '-b:a',
      audioBitrate,
      '-ar',
      audioSampleRate,
      '-ac',
      '2'
    )
  } else {
    args.push('-an')
  }
  args.push(outputPath)
  await runFfmpeg(args)
}

const applyAudioPolishToSegments = ({
  segments,
  windows,
  styleProfile,
  aggressionLevel,
  musicDuck
}: {
  segments: Segment[]
  windows: EngagementWindow[]
  styleProfile?: ContentStyleProfile | null
  aggressionLevel: RetentionAggressionLevel
  musicDuck: boolean
}) => {
  return segments.map((segment) => {
    const energy = averageWindowMetric(windows, segment.start, segment.end, (window) => window.audioEnergy)
    const speech = averageWindowMetric(windows, segment.start, segment.end, (window) => window.speechIntensity)
    const emotion = averageWindowMetric(windows, segment.start, segment.end, (window) => window.emotionIntensity)
    const vocal = averageWindowMetric(windows, segment.start, segment.end, (window) => window.vocalExcitement)
    const hasLowEnergy = energy < 0.38 && speech < 0.52
    const hasPeakEnergy = energy > 0.8 || emotion > 0.78 || vocal > 0.76
    let gain = 1
    if (segment.emphasize) gain += 0.02
    if (hasLowEnergy) gain += aggressionLevel === 'viral' ? 0.08 : aggressionLevel === 'high' ? 0.06 : 0.04
    if (hasPeakEnergy) gain -= 0.04
    if (styleProfile?.style === 'tutorial') gain += 0.03
    if ((styleProfile?.style === 'reaction' || styleProfile?.style === 'gaming') && hasPeakEnergy) gain -= 0.03
    if (musicDuck && !segment.emphasize && hasLowEnergy) gain -= 0.02
    const audioGain = Number(clamp(gain, 0.8, 1.24).toFixed(3))
    if (Math.abs(audioGain - 1) < 0.005) return { ...segment, audioGain: 1 }
    return {
      ...segment,
      audioGain
    }
  })
}

const buildAudioFilters = ({
  aggressionLevel,
  styleProfile,
  audioProfile
}: {
  aggressionLevel: RetentionAggressionLevel
  styleProfile?: ContentStyleProfile | null
  audioProfile?: AudioStreamProfile | null
}) => {
  const sourceChannels = Math.max(1, Number(audioProfile?.channels || 2))
  const sourceLayout = String(audioProfile?.channelLayout || '').toLowerCase()
  const sourceIsMono = sourceChannels <= 1 || sourceLayout.includes('mono')
  const targetLoudness = aggressionLevel === 'viral'
    ? -13.4
    : aggressionLevel === 'high'
      ? -13.8
      : aggressionLevel === 'low'
        ? -14.6
        : -14
  const compressionRatio = styleProfile?.style === 'tutorial' ? 2.8 : 3.2
  const attackMs = styleProfile?.style === 'tutorial' ? 18 : 12
  const releaseMs = styleProfile?.style === 'tutorial' ? 240 : 170
  const denoiseFloor = aggressionLevel === 'viral' ? -20 : aggressionLevel === 'high' ? -21.5 : -22.5
  const presenceBoost = styleProfile?.style === 'tutorial' ? 2.5 : 2.2
  const airBoost = styleProfile?.style === 'tutorial' ? 1.8 : 1.4
  const filters: string[] = [
    'highpass=f=72',
    'lowpass=f=17800',
    // Mild tonal shaping for clearer dialog without harshness.
    'equalizer=f=170:t=q:w=0.9:g=-1.4',
    'equalizer=f=280:t=q:w=1.0:g=-1.1',
    `equalizer=f=3300:t=q:w=1.2:g=${toFilterNumber(presenceBoost)}`,
    `equalizer=f=7200:t=q:w=1.1:g=${toFilterNumber(airBoost)}`,
    'equalizer=f=11200:t=q:w=0.8:g=0.8'
  ]
  if (hasFfmpegFilter('afftdn')) {
    filters.splice(2, 0, `afftdn=nf=${toFilterNumber(denoiseFloor)}:tn=1`)
  }
  if (hasFfmpegFilter('deesser')) {
    filters.push('deesser')
  }
  if (sourceIsMono) {
    if (hasFfmpegFilter('haas')) {
      // Haas stereoization gives mono footage a subtle spatial spread.
      filters.push('haas')
    } else if (hasFfmpegFilter('extrastereo')) {
      filters.push('extrastereo=m=1.45:c=0.0')
    }
  }
  if (hasFfmpegFilter('dynaudnorm')) {
    filters.push('dynaudnorm=f=85:g=11:p=0.88:m=9')
  }
  filters.push(
    `acompressor=threshold=-17dB:ratio=${toFilterNumber(compressionRatio)}:attack=${toFilterNumber(attackMs)}:release=${toFilterNumber(releaseMs)}:makeup=2`,
    `loudnorm=I=${toFilterNumber(targetLoudness)}:TP=-1.2:LRA=9`
  )
  if (hasFfmpegFilter('alimiter')) {
    filters.push('alimiter=limit=0.97:level=true')
  }
  return filters
}

const RETENTION_RENDER_THRESHOLD = 58

const resolveRuntimeRetentionProfile = ({
  renderMode,
  runtimeSeconds,
  requestedAggression,
  requestedStrategy,
  targetPlatform
}: {
  renderMode: RenderMode
  runtimeSeconds: number
  requestedAggression: RetentionAggressionLevel
  requestedStrategy: RetentionStrategyProfile
  targetPlatform: RetentionTargetPlatform
}) => {
  const runtime = Math.max(1, Number(runtimeSeconds || 0))
  const isVerticalShortForm = renderMode === 'vertical'
  const isLongForm = !isVerticalShortForm && runtime >= LONG_FORM_RUNTIME_THRESHOLD_SECONDS
  let strategy = parseRetentionStrategyProfile(requestedStrategy)
  let aggression = parseRetentionAggressionLevel(requestedAggression)
  const notes: string[] = []

  if (isVerticalShortForm) {
    strategy = 'viral'
    aggression = 'viral'
    if (requestedStrategy !== strategy || requestedAggression !== aggression) {
      notes.push('Vertical mode uses short-form viral pacing to maximize retention.')
    }
  } else {
    if (strategy === 'viral') {
      strategy = 'balanced'
      notes.push('Horizontal mode caps strategy at balanced to preserve narrative clarity.')
    }
    if (aggression === 'high' || aggression === 'viral') {
      aggression = 'medium'
      notes.push('Horizontal mode caps aggression at medium to avoid overcutting context.')
    }
    if (strategy === 'safe') {
      aggression = 'low'
    } else if (aggression === 'low') {
      strategy = 'safe'
    } else {
      strategy = 'balanced'
      aggression = 'medium'
    }
    if (isLongForm) {
      notes.push('Long-form runtime detected; applied long-form pacing safeguards.')
    }
  }
  const referencePreset = RETENTION_STYLE_REFERENCE_PRESETS[strategy]
  if (referencePreset?.referenceAnchors?.length) {
    notes.push(`Style anchors: ${referencePreset.referenceAnchors.join(', ')}.`)
  }

  return {
    strategy,
    aggression,
    isVerticalShortForm,
    isLongForm,
    targetPlatform,
    notes
  }
}

const inferRetentionContentFormat = ({
  runtimeSeconds,
  windows,
  renderMode,
  nicheProfile,
  targetPlatform
}: {
  runtimeSeconds: number
  windows: EngagementWindow[]
  renderMode?: RenderMode
  nicheProfile?: VideoNicheProfile | null
  targetPlatform?: RetentionTargetPlatform
}): RetentionContentFormat => {
  const runtime = Math.max(1, Number(runtimeSeconds || 0))
  const normalizedPlatform = parseRetentionTargetPlatform(targetPlatform)
  if (renderMode === 'vertical') return 'tiktok_short'
  const speechAvg = windows.length
    ? windows.reduce((sum, window) => sum + window.speechIntensity, 0) / windows.length
    : 0
  const sceneAvg = windows.length
    ? windows.reduce((sum, window) => sum + window.sceneChangeRate, 0) / windows.length
    : 0
  if (
    runtime >= 140 &&
    speechAvg >= 0.55 &&
    sceneAvg <= 0.28 &&
    (nicheProfile?.niche === 'talking_head' || nicheProfile?.niche === 'education')
  ) {
    return 'podcast_clip'
  }
  if (normalizedPlatform === 'tiktok' || normalizedPlatform === 'instagram_reels') {
    // Keep horizontal uploads context-safe by default unless creator explicitly chose vertical.
    return 'youtube_long'
  }
  return 'youtube_long'
}

const resolvePlatformAdjustedFormatWeights = ({
  contentFormat,
  targetPlatform
}: {
  contentFormat: RetentionContentFormat
  targetPlatform: RetentionTargetPlatform
}) => {
  const baseWeights = FORMAT_SCORE_WEIGHTS[contentFormat] || FORMAT_SCORE_WEIGHTS.youtube_long
  const platform = parseRetentionTargetPlatform(targetPlatform)
  const tuning = PLATFORM_SCORE_TUNING[platform] || PLATFORM_SCORE_TUNING.auto
  return {
    ...baseWeights,
    targetSegmentSeconds: Number(clamp(baseWeights.targetSegmentSeconds * tuning.segmentScale, 2.2, 7.2).toFixed(2)),
    interruptIntervalSeconds: Number(clamp(baseWeights.interruptIntervalSeconds * tuning.interruptScale, 2.8, 9.5).toFixed(2))
  }
}

const computeFeedbackQualityGateOffset = (analysisRaw: any) => {
  const analysis = analysisRaw && typeof analysisRaw === 'object' ? analysisRaw : {}
  const history = Array.isArray(analysis.retention_feedback_history)
    ? analysis.retention_feedback_history.slice(-12)
    : []
  if (!history.length) return 0
  const composites: number[] = []
  for (const item of history) {
    if (!item || typeof item !== 'object') continue
    const metrics = [
      { value: normalizePercentMetric((item as any).watchPercent ?? (item as any).watch_percent), weight: 0.28 },
      { value: normalizePercentMetric((item as any).hookHoldPercent ?? (item as any).hook_hold_percent), weight: 0.22 },
      { value: normalizePercentMetric((item as any).completionPercent ?? (item as any).completion_percent), weight: 0.22 },
      { value: normalizePercentMetric((item as any).rewatchRate ?? (item as any).rewatch_rate), weight: 0.08 },
      {
        value: (() => {
          const score = normalizeScore100((item as any).manualScore ?? (item as any).manual_score)
          return score === null ? null : Number(clamp01(score / 100).toFixed(4))
        })(),
        weight: 0.2
      }
    ].filter((entry) => entry.value !== null && Number.isFinite(Number(entry.value)))
    const totalWeight = metrics.reduce((sum, entry) => sum + entry.weight, 0)
    if (totalWeight <= 0) continue
    const composite = metrics.reduce((sum, entry) => sum + Number(entry.value) * entry.weight, 0) / totalWeight
    if (Number.isFinite(composite)) {
      composites.push(clamp01(composite))
    }
  }
  if (!composites.length) return 0
  const mean = composites.reduce((sum, value) => sum + value, 0) / composites.length
  if (mean < 0.42) return 3
  if (mean < 0.5) return 2
  if (mean < 0.58) return 1
  if (mean > 0.8) return -2
  if (mean > 0.72) return -1
  return 0
}

const computeRetentionScore = (
  segments: Segment[],
  windows: EngagementWindow[],
  hookScore: number,
  captionsEnabled: boolean,
  extras?: {
    removedRanges?: TimeRange[]
    patternInterruptCount?: number
    contentFormat?: RetentionContentFormat
    targetPlatform?: RetentionTargetPlatform
  }
) => {
  const runtimeSeconds = Math.max(1, computeEditedRuntimeSeconds(segments))
  const contentFormat = extras?.contentFormat || inferRetentionContentFormat({
    runtimeSeconds,
    windows,
    renderMode: 'horizontal',
    nicheProfile: null,
    targetPlatform: extras?.targetPlatform ?? 'auto'
  })
  const targetPlatform = parseRetentionTargetPlatform(extras?.targetPlatform)
  const formatWeights = resolvePlatformAdjustedFormatWeights({
    contentFormat,
    targetPlatform
  })
  const interruptTargetInterval = formatWeights.interruptIntervalSeconds
  const interruptTargetCount = Math.max(1, Math.ceil(runtimeSeconds / interruptTargetInterval))
  const lengths = segments.map((seg) => seg.end - seg.start).filter((len) => len > 0)
  const avgLen = lengths.length ? lengths.reduce((sum, len) => sum + len, 0) / lengths.length : 0
  const pacingScore = avgLen > 0
    ? Math.max(0, 1 - Math.abs(avgLen - formatWeights.targetSegmentSeconds) / Math.max(3.8, formatWeights.targetSegmentSeconds + 2.2))
    : 0.5
  const energies = windows.map((w) => w.audioEnergy)
  const mean = energies.length ? energies.reduce((sum, v) => sum + v, 0) / energies.length : 0
  const variance = energies.length ? energies.reduce((sum, v) => sum + (v - mean) ** 2, 0) / energies.length : 0
  const consistency = mean > 0 ? Math.max(0, 1 - Math.sqrt(variance) / (mean + 0.01)) : 0.4
  const hook = Number.isFinite(hookScore) ? Math.max(0, Math.min(1, hookScore)) : 0.5
  const emotionalSpikeDensity = windows.length
    ? windows.filter((window) => window.emotionalSpike > 0 || (window.emotionIntensity > 0.66)).length / windows.length
    : 0
  const removedSeconds = getRangesDurationSeconds(extras?.removedRanges ?? [])
  const timelineSeconds = Math.max(0.1, computeKeptTimelineSeconds(segments) + removedSeconds)
  const boredomRemovalRatio = clamp01(removedSeconds / timelineSeconds)
  const interruptDensityRaw = (extras?.patternInterruptCount ?? 0) / runtimeSeconds
  const interruptDensity = clamp01((extras?.patternInterruptCount ?? 0) / interruptTargetCount)
  const subtitleScore = captionsEnabled ? 1 : 0.6
  const audioScore = 0.82
  const score = Math.round(100 * (
    formatWeights.hook * hook +
    formatWeights.consistency * consistency +
    formatWeights.pacing * pacingScore +
    formatWeights.boredomRemoval * boredomRemovalRatio +
    formatWeights.emotionalSpikeDensity * emotionalSpikeDensity +
    formatWeights.interruptDensity * interruptDensity +
    formatWeights.subtitle * subtitleScore +
    formatWeights.audio * audioScore
  ))
  const notes: string[] = []
  if (avgLen > formatWeights.targetSegmentSeconds + 2.2) {
    notes.push(`Pacing is slower than ${contentFormat.replace('_', ' ')} target; tighten mid-sections.`)
  }
  if (!captionsEnabled) notes.push('Enable auto subtitles for stronger retention.')
  if (hook < 0.6) notes.push('Hook strength is moderate; consider re-recording the opening.')
  if (boredomRemovalRatio < 0.06) notes.push('Boredom removal ratio is low; increase retention aggression level.')
  if (interruptDensity < 0.95) notes.push('Pattern interrupts are sparse; add more emphasis beats.')
  return {
    score: Math.max(0, Math.min(100, score)),
    notes,
    details: {
      hook,
      pacingScore,
      emotionalSpikeDensity,
      boredomRemovalRatio,
      interruptDensity,
      interruptDensityRaw: Number(interruptDensityRaw.toFixed(4)),
      runtimeSeconds: Number(runtimeSeconds.toFixed(3)),
      contentFormat,
      targetPlatform,
      pacingTargetSeconds: Number(formatWeights.targetSegmentSeconds.toFixed(2))
    }
  }
}

const buildRetentionJudgeReport = ({
  retentionScore,
  hook,
  windows,
  clarityPenalty,
  captionsEnabled,
  patternInterruptCount,
  removedRanges,
  segments,
  thresholds,
  contentFormat,
  targetPlatform,
  strategyProfile
}: {
  retentionScore: ReturnType<typeof computeRetentionScore>
  hook: HookCandidate
  windows: EngagementWindow[]
  clarityPenalty: number
  captionsEnabled: boolean
  patternInterruptCount: number
  removedRanges: TimeRange[]
  segments: Segment[]
  thresholds?: QualityGateThresholds
  contentFormat?: RetentionContentFormat
  targetPlatform?: RetentionTargetPlatform
  strategyProfile?: RetentionStrategyProfile
}): RetentionJudgeReport => {
  const appliedThresholds = normalizeQualityGateThresholds(thresholds)
  const runtimeSeconds = Math.max(1, computeEditedRuntimeSeconds(segments))
  const resolvedContentFormat = contentFormat || (retentionScore?.details?.contentFormat as RetentionContentFormat) || 'youtube_long'
  const resolvedTargetPlatform = parseRetentionTargetPlatform(
    targetPlatform || (retentionScore?.details as any)?.targetPlatform || 'auto'
  )
  const formatWeights = resolvePlatformAdjustedFormatWeights({
    contentFormat: resolvedContentFormat,
    targetPlatform: resolvedTargetPlatform
  })
  const interruptIntervalTarget = Number(formatWeights?.interruptIntervalSeconds || (runtimeSeconds <= 90 ? 4 : 6))
  const interruptTargetCount = Math.max(1, Math.ceil(runtimeSeconds / interruptIntervalTarget))
  const interruptCoverage = clamp01(patternInterruptCount / interruptTargetCount)
  const interruptCoverageTarget =
    resolvedContentFormat === 'podcast_clip'
      ? 0.72
      : resolvedContentFormat === 'youtube_long'
        ? 0.86
        : 0.92
  const emotionalPull = Math.round(100 * clamp01(
    0.45 * averageWindowMetric(windows, 0, Math.max(1, windows.length), (window) => window.emotionIntensity) +
    0.22 * averageWindowMetric(windows, 0, Math.max(1, windows.length), (window) => window.vocalExcitement) +
    0.21 * retentionScore.details.emotionalSpikeDensity +
    0.12 * hook.auditScore
  ))
  const hookStrength = Math.round(100 * clamp01(
    0.65 * hook.score + 0.35 * hook.auditScore
  ))
  const pacing = Math.round(100 * clamp01(
    0.7 * retentionScore.details.pacingScore + 0.3 * interruptCoverage
  ))
  const clarity = Math.round(100 * clamp01(
    0.72 * (1 - clarityPenalty) +
    0.14 * (captionsEnabled ? 1 : 0.7) +
    0.14 * (hook.auditPassed ? 1 : 0.6)
  ))
  const retention = Math.round(retentionScore.score)

  const whyKeepWatching: string[] = []
  if (hookStrength >= 80) whyKeepWatching.push('Hook opens with a high-impact moment that promises payoff.')
  if (emotionalPull >= 70) whyKeepWatching.push('Emotional intensity rises quickly and stays above baseline.')
  if (pacing >= 70) whyKeepWatching.push('Frequent editorial interrupts keep momentum and reduce drop-off risk.')
  if (whyKeepWatching.length === 0) whyKeepWatching.push('Retention signals are mixed; stronger setup/payoff needed.')

  const whatIsGeneric: string[] = []
  if (interruptCoverage < interruptCoverageTarget) {
    whatIsGeneric.push(`Interrupt density is below ${resolvedContentFormat.replace('_', ' ')} target for this runtime.`)
  }
  if (retentionScore.details.boredomRemovalRatio < 0.07) whatIsGeneric.push('Too much low-arousal material remains.')
  if (!hook.auditPassed) whatIsGeneric.push('Hook is not fully understandable without prior context.')
  if (whatIsGeneric.length === 0) whatIsGeneric.push('Generic signals are low for this attempt.')

  return {
    retention_score: retention,
    hook_strength: hookStrength,
    pacing_score: pacing,
    clarity_score: clarity,
    emotional_pull: emotionalPull,
    content_format: resolvedContentFormat,
    target_platform: resolvedTargetPlatform,
    strategy_profile: strategyProfile || 'balanced',
    why_keep_watching: whyKeepWatching.slice(0, 3),
    what_is_generic: whatIsGeneric.slice(0, 3),
    required_fixes: {
      stronger_hook: hookStrength < appliedThresholds.hook_strength,
      raise_emotion: emotionalPull < appliedThresholds.emotional_pull,
      improve_pacing: pacing < appliedThresholds.pacing_score,
      increase_interrupts: interruptCoverage < interruptCoverageTarget
    },
    applied_thresholds: appliedThresholds,
    gate_mode:
      appliedThresholds.hook_strength === QUALITY_GATE_THRESHOLDS.hook_strength &&
      appliedThresholds.emotional_pull === QUALITY_GATE_THRESHOLDS.emotional_pull &&
      appliedThresholds.pacing_score === QUALITY_GATE_THRESHOLDS.pacing_score &&
      appliedThresholds.retention_score === QUALITY_GATE_THRESHOLDS.retention_score
        ? 'strict'
        : 'adaptive',
    passed:
      hookStrength >= appliedThresholds.hook_strength &&
      emotionalPull >= appliedThresholds.emotional_pull &&
      pacing >= appliedThresholds.pacing_score &&
      retention >= appliedThresholds.retention_score
  }
}

const executeQualityGateRetriesForTest = (judgeOutcomes: boolean[], maxRetries = MAX_QUALITY_GATE_RETRIES) => {
  const strategies: RetentionRetryStrategy[] = ['BASELINE', 'HOOK_FIRST', 'EMOTION_FIRST', 'PACING_FIRST']
  const attempts: Array<{ attempt: number; strategy: RetentionRetryStrategy; passed: boolean }> = []
  for (let attemptIndex = 0; attemptIndex < strategies.length; attemptIndex += 1) {
    const strategy = strategies[attemptIndex]
    const passed = Boolean(judgeOutcomes[attemptIndex])
    attempts.push({
      attempt: attemptIndex + 1,
      strategy,
      passed
    })
    if (passed || attemptIndex >= maxRetries) break
  }
  return attempts
}

const buildTimelineWithHookAtStartForTest = (segments: Segment[], hook: HookCandidate) => {
  const hookRange: TimeRange = { start: hook.start, end: hook.start + hook.duration }
  const withoutHook = subtractRange(segments.map((segment) => ({ ...segment })), hookRange)
  return [{ ...hookRange, speed: 1 }, ...withoutHook]
}

const boostSegmentsForRetention = (
  segments: Segment[],
  windows: EngagementWindow[],
  aggressiveMode: boolean
) => {
  if (segments.length <= 1) return segments
  const out = segments.map((seg) => ({ ...seg }))
  const scored = out
    .map((seg, idx) => {
      const score = averageWindowMetric(windows, seg.start, seg.end, (window) => window.score)
      const speech = averageWindowMetric(windows, seg.start, seg.end, (window) => window.speechIntensity)
      const runtime = Math.max(0.1, (seg.end - seg.start) / (seg.speed && seg.speed > 0 ? seg.speed : 1))
      return { idx, score, speech, runtime }
    })
    // keep intro hook mostly intact
    .filter((entry) => entry.idx > 0 && entry.runtime >= 2)
    .sort((a, b) => a.score - b.score || b.runtime - a.runtime)

  const totalRuntime = out.reduce((sum, seg) => {
    const speed = seg.speed && seg.speed > 0 ? seg.speed : 1
    return sum + Math.max(0, (seg.end - seg.start) / speed)
  }, 0)
  const maxBoostedRuntime = Math.max(3, totalRuntime * 0.18)
  let boostedRuntime = 0

  for (const entry of scored) {
    if (boostedRuntime >= maxBoostedRuntime) break
    const seg = out[entry.idx]
    const current = seg.speed && seg.speed > 0 ? seg.speed : 1
    let target = current
    if (entry.score < 0.28) {
      target = aggressiveMode ? 1.42 : 1.3
    } else if (entry.score < 0.42 && entry.speech < 0.35) {
      target = aggressiveMode ? 1.28 : 1.16
    }
    target = Number(clamp(target, 1, aggressiveMode ? 1.5 : 1.35).toFixed(3))
    if (target <= current + 0.02) continue
    seg.speed = target
    boostedRuntime += entry.runtime
  }

  return out
}

class PlanLimitError extends Error {
  status: number
  code: string
  feature: string
  requiredPlan: string
  checkoutUrl?: string | null
  constructor(message: string, feature: string, requiredPlan: string, checkoutUrl?: string | null, code?: string) {
    super(message)
    this.status = 403
    this.code = code ?? 'PLAN_LIMIT_EXCEEDED'
    this.feature = feature
    this.requiredPlan = requiredPlan
    this.checkoutUrl = checkoutUrl
  }
}

const getRequestedQuality = (value?: string | null, fallback?: string | null): ExportQuality => {
  if (value) return normalizeQuality(value)
  if (fallback) return normalizeQuality(fallback)
  return '720p'
}

const ensureUsageWithinLimits = async (
  userId: string,
  userEmail: string | undefined,
  durationMinutes: number,
  tier: PlanTier,
  plan: { maxRendersPerMonth: number | null; maxMinutesPerMonth: number | null },
  renderMode: RenderMode
) => {
  const monthKey = getMonthKey()
  const renderViolation = await getRenderLimitViolation({
    userId,
    email: userEmail,
    tier,
    plan,
    renderMode,
    allowAtLimitForFree: true
  })
  if (renderViolation) {
    throw new PlanLimitError(
      renderViolation.payload?.message || 'Monthly render limit reached. Upgrade to continue.',
      'renders',
      renderViolation.requiredPlan,
      undefined,
      renderViolation.code
    )
  }
  const usage = await getUsageForMonth(userId, monthKey)
  if (plan.maxMinutesPerMonth !== null && (usage?.minutesUsed ?? 0) + durationMinutes > plan.maxMinutesPerMonth) {
    const requiredPlan = getRequiredPlanForRenders(tier)
    throw new PlanLimitError(
      'Monthly minutes limit reached. Upgrade to continue.',
      'minutes',
      requiredPlan,
      undefined,
      'MINUTES_LIMIT_REACHED'
    )
  }
  return { usage, monthKey }
}

const getEditOptionsForUser = async (
  userId: string,
  overrides?: {
    retentionAggressionLevel?: RetentionAggressionLevel | null
    retentionStrategyProfile?: RetentionStrategyProfile | null
    onlyCuts?: boolean | null
    autoCaptions?: boolean | null
    subtitleStyle?: string | null
  }
) => {
  const settings = await prisma.userSettings.findUnique({ where: { userId } })
  const { tier, plan } = await getUserPlan(userId)
  const features = getPlanFeatures(tier)
  const subtitlesEnabled = features.subtitles.enabled
  const rawSubtitle = String(overrides?.subtitleStyle ?? settings?.subtitleStyle ?? DEFAULT_SUBTITLE_PRESET)
  const normalizedSubtitle = normalizeSubtitlePreset(rawSubtitle) ?? DEFAULT_SUBTITLE_PRESET
  const subtitleStyle =
    subtitlesEnabled && isSubtitlePresetAllowed(normalizedSubtitle, tier) ? rawSubtitle : DEFAULT_SUBTITLE_PRESET
  const autoCaptionsOverride = typeof overrides?.autoCaptions === 'boolean' ? overrides.autoCaptions : null
  const onlyCuts = typeof overrides?.onlyCuts === 'boolean'
    ? overrides.onlyCuts
    : (settings?.onlyCuts ?? DEFAULT_EDIT_OPTIONS.onlyCuts)
  const removeBoring = onlyCuts ? true : settings?.removeBoring ?? DEFAULT_EDIT_OPTIONS.removeBoring
  const requestedStrategy = parseRetentionStrategyProfile(
    overrides?.retentionStrategyProfile ??
    strategyFromAggressionLevel(
      parseRetentionAggressionLevel(
        overrides?.retentionAggressionLevel ??
        (settings?.aggressiveMode ? 'high' : DEFAULT_EDIT_OPTIONS.retentionAggressionLevel)
      )
    )
  )
  const requestedAggression = parseRetentionAggressionLevel(
    overrides?.retentionAggressionLevel ?? STRATEGY_TO_AGGRESSION[requestedStrategy]
  )
  const allowedAggression: RetentionAggressionLevel =
    features.advancedEffects ? requestedAggression : (requestedAggression === 'low' ? 'low' : 'medium')
  const allowedStrategy = strategyFromAggressionLevel(allowedAggression)
  const aggressiveMode = onlyCuts ? false : isAggressiveRetentionLevel(allowedAggression)
  const baseOptions: EditOptions = {
    autoHookMove: onlyCuts ? false : (settings?.autoHookMove ?? DEFAULT_EDIT_OPTIONS.autoHookMove),
    removeBoring,
    onlyCuts,
    smartZoom: onlyCuts ? false : (settings?.smartZoom ?? DEFAULT_EDIT_OPTIONS.smartZoom),
    jumpCuts: onlyCuts ? false : (settings?.jumpCuts ?? DEFAULT_EDIT_OPTIONS.jumpCuts),
    transitions: onlyCuts ? false : (settings?.transitions ?? DEFAULT_EDIT_OPTIONS.transitions),
    soundFx: onlyCuts ? false : (settings?.soundFx ?? DEFAULT_EDIT_OPTIONS.soundFx),
    emotionalBoost: onlyCuts ? false : (features.advancedEffects ? (settings?.emotionalBoost ?? DEFAULT_EDIT_OPTIONS.emotionalBoost) : false),
    aggressiveMode,
    autoCaptions: subtitlesEnabled
      ? (autoCaptionsOverride ?? settings?.autoCaptions ?? DEFAULT_EDIT_OPTIONS.autoCaptions)
      : false,
    musicDuck: onlyCuts ? false : (settings?.musicDuck ?? DEFAULT_EDIT_OPTIONS.musicDuck),
    subtitleStyle,
    autoZoomMax: settings?.autoZoomMax ?? plan.autoZoomMax,
    retentionAggressionLevel: allowedAggression,
    retentionStrategyProfile: allowedStrategy
  }
  const options = applyRetentionStyleReferencePreset({
    options: baseOptions,
    strategy: allowedStrategy,
    allowAdvancedEffects: Boolean(features.advancedEffects)
  })
  return {
    options,
    plan,
    tier
  }
}

const analyzeJob = async (jobId: string, options: EditOptions, requestId?: string) => {
  console.log(`[${requestId || 'noid'}] analyze start ${jobId}`)
  const job = await prisma.job.findUnique({ where: { id: jobId } })
  if (!job) throw new Error('not_found')
  if (!hasFfmpeg()) {
    await updateJob(jobId, { status: 'failed', error: 'ffmpeg_missing' })
    throw new Error('ffmpeg_missing')
  }

  const tmpIn = path.join(os.tmpdir(), `${jobId}-analysis`)
  const analysisWorkDir = fs.mkdtempSync(path.join(os.tmpdir(), `${jobId}-analysis-work-`))
  const absTmpIn = path.resolve(tmpIn)
  try {
    await downloadObjectToFile({ key: job.inputPath, destPath: tmpIn })
  } catch (e) {
    await updateJob(jobId, { status: 'failed', error: 'download_failed' })
    throw new Error('download_failed')
  }
  if (!fs.existsSync(tmpIn)) {
    await updateJob(jobId, { status: 'failed', error: 'input_file_missing_after_download' })
    throw new Error('input_file_missing_after_download')
  }
  const inStats = fs.statSync(tmpIn)
  if (!inStats.isFile() || inStats.size <= 0) {
    await updateJob(jobId, { status: 'failed', error: 'input_file_empty_after_download' })
    throw new Error('input_file_empty_after_download')
  }
  console.log(`[${requestId || 'noid'}] analyze input`, {
    inputPath: absTmpIn,
    inputBytes: inStats.size
  })
  try {
    const duration = getDurationSeconds(tmpIn)
    if (!duration || !Number.isFinite(duration) || duration <= 0) {
      await updateJob(jobId, { status: 'failed', error: 'duration_unavailable' })
      throw new Error('duration_unavailable')
    }

    await updateJob(jobId, { status: 'analyzing', progress: 15, inputDurationSeconds: Math.round(duration) })
    await ensureBucket(OUTPUT_BUCKET, false)

    // Generate a low-res proxy and analyze the proxy to save CPU/time.
    const tmpProxy = path.join(os.tmpdir(), `${jobId}-proxy.mp4`)
    if (!ANALYSIS_SKIP_PROXY) {
      try {
        await generateProxy(tmpIn, tmpProxy)
        // upload proxy for client preview
        const proxyBucketPath = `${job.userId}/${jobId}/proxy.mp4`
        try {
          const proxyBuf = fs.readFileSync(tmpProxy)
          try {
            await uploadBufferToOutput({ key: proxyBucketPath, body: proxyBuf, contentType: 'video/mp4' })
            await updateJob(jobId, { analysis: { ...(job.analysis as any || {}), proxyPath: proxyBucketPath }, progress: 20 })
          } catch (e) {
            console.warn('proxy upload failed', e)
          }
        } catch (e) {
          // non-fatal: continue analysis even if upload fails
          console.warn('proxy upload failed', e)
        }
      } catch (e) {
        console.warn('proxy generation failed, falling back to original for analysis', e)
      }
    } else {
      console.log(`[${requestId || 'noid'}] proxy generation skipped for analysis`)
    }

    const initialRenderConfig = parseRenderConfigFromAnalysis(job.analysis as any, (job as any)?.renderSettings)
    const requestedStrategyProfile = parseRetentionStrategyProfile(
      options.retentionStrategyProfile ?? getRetentionStrategyFromJob(job)
    )
    const requestedAggressionLevel = parseRetentionAggressionLevel(
      options.retentionAggressionLevel ??
      STRATEGY_TO_AGGRESSION[requestedStrategyProfile] ??
      getRetentionAggressionFromJob(job)
    )
    const requestedTargetPlatform = getRetentionTargetPlatformFromJob(job)
    const runtimeRetentionProfile = resolveRuntimeRetentionProfile({
      renderMode: initialRenderConfig.mode,
      runtimeSeconds: duration,
      requestedAggression: requestedAggressionLevel,
      requestedStrategy: requestedStrategyProfile,
      targetPlatform: requestedTargetPlatform
    })
    const strategyProfile = runtimeRetentionProfile.strategy
    const aggressionLevel = runtimeRetentionProfile.aggression
    const hookCalibration = await loadHookCalibrationProfile(job.userId)

    const transcriptCues = await runRetentionStep({
      jobId,
      step: 'TRANSCRIBE',
      maxRetries: 1,
      statusUpdate: { status: 'analyzing', progress: 18 },
      run: async () => {
        const transcriptSrt = await generateSubtitles(tmpIn, analysisWorkDir).catch(() => null)
        if (!transcriptSrt) return [] as TranscriptCue[]
        return parseTranscriptCues(transcriptSrt)
      },
      summarize: (cues) => ({ cueCount: cues.length, hasTranscript: cues.length > 0 })
    })

    let editPlan: EditPlan | null = null
    let extractedFrameCount = 0
    if (duration) {
      try {
        const analyzePath = fs.existsSync(tmpProxy) ? tmpProxy : tmpIn
        editPlan = await runRetentionStep({
          jobId,
          step: 'FRAME_ANALYSIS',
          maxRetries: 1,
          statusUpdate: { status: 'cutting', progress: 24 },
          run: async () => {
            const frameDir = path.join(analysisWorkDir, 'frames')
            const frames = await extractFramesEveryHalfSecond(analyzePath, frameDir, duration)
            extractedFrameCount = frames.length
            const plan = await buildEditPlan(
              analyzePath,
              duration,
              {
                ...options,
                retentionAggressionLevel: aggressionLevel,
                retentionStrategyProfile: strategyProfile,
                aggressiveMode: options.onlyCuts ? false : isAggressiveRetentionLevel(aggressionLevel)
              },
              async (stage) => {
                if (stage === 'cutting') {
                  await updatePipelineStepState(jobId, 'BEST_MOMENT_SCORING', {
                    status: 'running',
                    attempts: 1,
                    startedAt: toIsoNow(),
                    lastError: null
                  })
                  await updatePipelineStepState(jobId, 'BOREDOM_SCORING', {
                    status: 'running',
                    attempts: 1,
                    startedAt: toIsoNow(),
                    lastError: null
                  })
                  await updateJob(jobId, { status: 'cutting', progress: 30 })
                } else if (stage === 'hooking') {
                  await updatePipelineStepState(jobId, 'HOOK_SELECT_AND_AUDIT', {
                    status: 'running',
                    attempts: 1,
                    startedAt: toIsoNow(),
                    lastError: null
                  })
                  await updatePipelineStepState(jobId, 'HOOK_SCORING', {
                    status: 'running',
                    attempts: 1,
                    startedAt: toIsoNow(),
                    lastError: null
                  })
                  await updateJob(jobId, { status: 'hooking', progress: 36 })
                } else if (stage === 'pacing') {
                  await updatePipelineStepState(jobId, 'TIMELINE_REORDER', {
                    status: 'running',
                    attempts: 1,
                    startedAt: toIsoNow(),
                    lastError: null
                  })
                  await updatePipelineStepState(jobId, 'PACING_AND_INTERRUPTS', {
                    status: 'running',
                    attempts: 1,
                    startedAt: toIsoNow(),
                    lastError: null
                  })
                  await updatePipelineStepState(jobId, 'STORY_REORDER', {
                    status: 'running',
                    attempts: 1,
                    startedAt: toIsoNow(),
                    lastError: null
                  })
                  await updatePipelineStepState(jobId, 'PACING_ENFORCEMENT', {
                    status: 'running',
                    attempts: 1,
                    startedAt: toIsoNow(),
                    lastError: null
                  })
                  await updateJob(jobId, { status: 'pacing', progress: 44 })
                }
              },
              { transcriptCues, aggressionLevel, hookCalibration }
            )
            return plan
          },
          summarize: (plan) => ({
            frameCount: extractedFrameCount,
            windows: plan.engagementWindows.length,
            hookScore: Number(plan.hook.score.toFixed(3)),
            hookAuditScore: Number(plan.hook.auditScore.toFixed(3)),
            hookAuditPassed: Boolean(plan.hook.auditPassed),
            segmentCount: plan.segments.length
          })
        })
        await updatePipelineStepState(jobId, 'BEST_MOMENT_SCORING', {
          status: 'completed',
          completedAt: toIsoNow(),
          meta: {
            hook: editPlan.hook,
            topCandidates: editPlan.hookCandidates ?? [],
            hookCalibration
          }
        })
        await updatePipelineStepState(jobId, 'HOOK_SELECT_AND_AUDIT', {
          status: 'completed',
          completedAt: toIsoNow(),
          meta: {
            selectedHook: editPlan.hook,
            hookFailureReason: editPlan.hookFailureReason ?? null
          }
        })
        await updatePipelineStepState(jobId, 'HOOK_SCORING', {
          status: 'completed',
          completedAt: toIsoNow(),
          meta: {
            hook: editPlan.hook,
            topCandidates: editPlan.hookCandidates ?? []
          }
        })
        await updatePipelineStepState(jobId, 'BOREDOM_SCORING', {
          status: 'completed',
          completedAt: toIsoNow(),
          meta: {
            removedRanges: editPlan.boredomRanges ?? [],
            totalRemovedSeconds: Number(
              getRangesDurationSeconds(editPlan.boredomRanges ?? []).toFixed(3)
            )
          }
        })
        await updatePipelineStepState(jobId, 'STORY_REORDER', {
          status: 'completed',
          completedAt: toIsoNow(),
          meta: {
            segmentCount: editPlan.segments.length
          }
        })
        await updatePipelineStepState(jobId, 'TIMELINE_REORDER', {
          status: 'completed',
          completedAt: toIsoNow(),
          meta: {
            reorderMap: editPlan.storyReorderMap ?? []
          }
        })
        await updatePipelineStepState(jobId, 'PACING_AND_INTERRUPTS', {
          status: 'completed',
          completedAt: toIsoNow(),
          meta: {
            patternInterruptCount: editPlan.patternInterruptCount ?? 0,
            patternInterruptDensity: editPlan.patternInterruptDensity ?? 0
          }
        })
        await updatePipelineStepState(jobId, 'PACING_ENFORCEMENT', {
          status: 'completed',
          completedAt: toIsoNow(),
          meta: {
            patternInterruptCount: editPlan.patternInterruptCount ?? 0
          }
        })
      } catch (e) {
        if (e instanceof HookGateError) throw e
        console.warn(`[${requestId || 'noid'}] buildEditPlan failed during analyze, using deterministic fallback`, e)
        editPlan = buildDeterministicFallbackEditPlan(duration, options)
      }
    }

    // preserve any proxyPath that was uploaded earlier so the frontend can preview
    const freshJob = await prisma.job.findUnique({ where: { id: jobId } })
    const existingAnalysis = (freshJob?.analysis as any) || (job.analysis as any) || {}
    const existingProxyPath = existingAnalysis?.proxyPath ?? null
    const latestRenderSettings = (freshJob as any)?.renderSettings ?? (job as any)?.renderSettings
    const renderConfig = parseRenderConfigFromAnalysis(existingAnalysis, latestRenderSettings)
    const resolvedTargetPlatform = getRetentionTargetPlatformFromJob({
      analysis: existingAnalysis,
      renderSettings: latestRenderSettings
    })
    const resolvedPlatformProfile = getPlatformProfileFromJob({
      analysis: existingAnalysis,
      renderSettings: latestRenderSettings
    })
    const analyzeContentFormat = inferRetentionContentFormat({
      runtimeSeconds: duration,
      windows: editPlan?.engagementWindows ?? [],
      renderMode: renderConfig.mode,
      nicheProfile: editPlan?.nicheProfile ?? null,
      targetPlatform: resolvedTargetPlatform
    })
    const analyzeMetadataSummary = buildRetentionMetadataSummary({
      durationSeconds: duration,
      segments: editPlan?.segments ?? [],
      windows: editPlan?.engagementWindows ?? [],
      hook: editPlan?.hook ?? null,
      styleProfile: editPlan?.styleProfile ?? null,
      nicheProfile: editPlan?.nicheProfile ?? null,
      styleArchetypeBlend: editPlan?.styleArchetypeBlend ?? null,
      behaviorStyleProfile: editPlan?.behaviorStyleProfile ?? null,
      styleFeatureSnapshot: editPlan?.styleFeatureSnapshot ?? null,
      autoEscalationEvents: editPlan?.autoEscalationEvents ?? [],
      patternInterruptCount: editPlan?.patternInterruptCount ?? 0,
      patternInterruptDensity: editPlan?.patternInterruptDensity ?? 0,
      boredomRemovedRatio: editPlan?.boredomRemovedRatio ?? 0,
      contentFormat: analyzeContentFormat,
      targetPlatform: resolvedTargetPlatform,
      strategyProfile
    })
    const analysis = buildPersistedRenderAnalysis({
      existing: {
        ...existingAnalysis,
        metadata_version: 2,
        metadata_summary: analyzeMetadataSummary,
        duration: duration ?? 0,
        size: fs.existsSync(tmpIn) ? fs.statSync(tmpIn).size : 0,
        filename: path.basename(job.inputPath),
        hook_start_time: editPlan?.hook?.start ?? null,
        hook_end_time: editPlan?.hook ? editPlan.hook.start + editPlan.hook.duration : null,
        hook_score: editPlan?.hook?.score ?? null,
        hook_audit_score: editPlan?.hook?.auditScore ?? null,
        hook_text: editPlan?.hook?.text ?? null,
        hook_reason: editPlan?.hook?.reason ?? null,
        hook_synthetic: editPlan?.hook?.synthetic ?? false,
        hook_failure_reason: editPlan?.hookFailureReason ?? null,
        removed_segments: editPlan?.removedSegments ?? [],
        compressed_segments: editPlan?.compressedSegments ?? [],
        hook_candidates: editPlan?.hookCandidates ?? [],
        hook_variants: editPlan?.hookVariants ?? editPlan?.hookCandidates ?? [],
        hook_calibration: editPlan?.hookCalibration ?? hookCalibration,
        boredom_ranges: editPlan?.boredomRanges ?? [],
        boredom_removed_ratio: editPlan?.boredomRemovedRatio ?? 0,
        retentionAggressionLevel: aggressionLevel,
        retentionLevel: aggressionLevel,
        retentionStrategyProfile: strategyProfile,
        retentionStrategy: strategyProfile,
        retentionTargetPlatform: resolvedTargetPlatform,
        retention_target_platform: resolvedTargetPlatform,
        retentionPlatform: resolvedTargetPlatform,
        targetPlatform: resolvedTargetPlatform,
        platform: resolvedTargetPlatform,
        platformProfile: resolvedPlatformProfile,
        platform_profile: resolvedPlatformProfile,
        retentionContentFormat: analyzeContentFormat,
        retention_content_format: analyzeContentFormat,
        retention_runtime_profile: {
          strategy: strategyProfile,
          aggression: aggressionLevel,
          isLongForm: runtimeRetentionProfile.isLongForm,
          isVerticalShortForm: runtimeRetentionProfile.isVerticalShortForm,
          notes: runtimeRetentionProfile.notes
        },
        style_profile: editPlan?.styleProfile ?? null,
        niche_profile: editPlan?.nicheProfile ?? null,
        beat_anchors: editPlan?.beatAnchors ?? [],
        emotional_beat_anchors: editPlan?.emotionalBeatAnchors ?? [],
        emotional_beat_cut_count: editPlan?.emotionalBeatCutCount ?? 0,
        emotional_lead_trimmed_seconds: editPlan?.emotionalLeadTrimmedSeconds ?? 0,
        emotional_tuning_profile: editPlan?.emotionalTuning ?? null,
        style_archetype_blend: editPlan?.styleArchetypeBlend ?? null,
        behavior_style_profile: editPlan?.behaviorStyleProfile ?? null,
        edit_decision_timeline: editPlan?.editDecisionTimeline ?? null,
        style_timeline_features: editPlan?.styleFeatureSnapshot ?? null,
        auto_escalation_events: editPlan?.autoEscalationEvents ?? [],
        auto_escalation_count: Number(editPlan?.autoEscalationEvents?.length ?? 0),
        transcript_signals: editPlan?.transcriptSignals ?? {
          cueCount: transcriptCues.length,
          hasTranscript: transcriptCues.length > 0
        },
        extracted_frame_count: extractedFrameCount,
        pattern_interrupt_count: editPlan?.patternInterruptCount ?? 0,
        pattern_interrupt_density: editPlan?.patternInterruptDensity ?? 0,
        story_reorder_map: editPlan?.storyReorderMap ?? [],
        editPlan,
        proxyPath: existingProxyPath
      },
      renderConfig,
      retentionTargetPlatform: resolvedTargetPlatform,
      platformProfile: resolvedPlatformProfile,
      onlyCuts: options.onlyCuts
    })
    const analysisPath = `${job.userId}/${jobId}/analysis.json`
    try {
      await uploadBufferToOutput({ key: analysisPath, body: Buffer.from(JSON.stringify(analysis)), contentType: 'application/json' })
    } catch (e) {
      console.warn('analysis upload failed', e)
    }
    await updateJob(jobId, {
      status: editPlan ? 'pacing' : 'analyzing',
      progress: editPlan ? 50 : 30,
      inputDurationSeconds: duration ? Math.round(duration) : null,
      renderSettings: buildPersistedRenderSettings(renderConfig, {
        retentionAggressionLevel: aggressionLevel,
        retentionStrategyProfile: strategyProfile,
        retentionTargetPlatform: resolvedTargetPlatform,
        platformProfile: resolvedPlatformProfile,
        onlyCuts: options.onlyCuts
      }),
      analysis: analysis
    })
    console.log(`[${requestId || 'noid'}] analyze complete ${jobId}`)
    return analysis
  } finally {
    safeUnlink(tmpIn)
    try {
      fs.rmSync(analysisWorkDir, { recursive: true, force: true })
    } catch (e) {
      // ignore
    }
  }
}

const processJob = async (
  jobId: string,
  user: { id: string; email?: string },
  requestedQuality: ExportQuality | undefined,
  options: EditOptions,
  requestId?: string
) => {
  console.log(`[${requestId || 'noid'}] process start ${jobId}`)
  const job = await prisma.job.findUnique({ where: { id: jobId } })
  if (!job) throw new Error('not_found')
  const throwIfCanceled = () => {
    if (isPipelineCanceled(jobId)) throw new JobCanceledError(jobId)
  }
  throwIfCanceled()
  if (!hasFfmpeg()) {
    await updateJob(jobId, { status: 'failed', error: 'ffmpeg_missing' })
    throw new Error('ffmpeg_missing')
  }

  const settings = await prisma.userSettings.findUnique({ where: { userId: user.id } })
  const { tier, plan } = await getUserPlan(user.id)
  const features = getPlanFeatures(tier)
  const desiredQuality = requestedQuality ?? getRequestedQuality(job.requestedQuality, settings?.exportQuality)
  const finalQuality = clampQualityForTier(desiredQuality, tier)
  if (finalQuality !== desiredQuality) {
    const requiredPlan = getRequiredPlanForQuality(desiredQuality)
    throw new PlanLimitError('Upgrade to export at this resolution.', 'quality', requiredPlan)
  }
  const renderConfig = parseRenderConfigFromAnalysis(job.analysis as any, (job as any)?.renderSettings)
  const requestedStrategyProfile = parseRetentionStrategyProfile(
    options.retentionStrategyProfile ?? getRetentionStrategyFromJob(job)
  )
  const requestedAggressionLevel = parseRetentionAggressionLevel(
    options.retentionAggressionLevel ??
    STRATEGY_TO_AGGRESSION[requestedStrategyProfile] ??
    getRetentionAggressionFromJob(job)
  )
  const retentionTargetPlatform = getRetentionTargetPlatformFromJob(job)
  const platformProfileId = getPlatformProfileFromJob(job)
  const platformProfile = PLATFORM_EDIT_PROFILES[platformProfileId] || PLATFORM_EDIT_PROFILES.auto
  let strategyProfile = requestedStrategyProfile
  let aggressionLevel = requestedAggressionLevel
  const feedbackQualityGateOffset = computeFeedbackQualityGateOffset(job.analysis as any)
  const hookCalibration = await loadHookCalibrationProfile(job.userId)
  const rawSubtitleStyle = String(options.subtitleStyle ?? settings?.subtitleStyle ?? DEFAULT_SUBTITLE_PRESET)
  const normalizedRawSubtitle = normalizeSubtitlePreset(rawSubtitleStyle) ?? DEFAULT_SUBTITLE_PRESET
  const hasExplicitSubtitleOverride = normalizedRawSubtitle !== DEFAULT_SUBTITLE_PRESET
  let subtitleStyle = rawSubtitleStyle
  let subtitleStyleSource: 'user' | 'platform' | 'default' = hasExplicitSubtitleOverride ? 'user' : 'default'
  if (!hasExplicitSubtitleOverride) {
    const preferredPlatformPreset = platformProfile.defaultSubtitlePreset
    const canUsePlatformPreset = features.subtitles.enabled && isSubtitlePresetAllowed(preferredPlatformPreset, tier)
    subtitleStyle = canUsePlatformPreset ? preferredPlatformPreset : DEFAULT_SUBTITLE_PRESET
    subtitleStyleSource = canUsePlatformPreset ? 'platform' : 'default'
  }
  const normalizedSubtitle = normalizeSubtitlePreset(subtitleStyle) ?? DEFAULT_SUBTITLE_PRESET
  const baseCrf = getDefaultCrfForQuality(finalQuality)
  const adjustedCrf = Math.round(clamp(baseCrf + platformProfile.crfDelta, 16, 30))
  const ffPreset = options.fastMode
    ? 'superfast'
    : (process.env.FFMPEG_PRESET || platformProfile.videoPreset)
  const ffCrf = options.fastMode
    ? '28'
    : (process.env.FFMPEG_CRF || String(adjustedCrf))
  const ffAudioBitrate = resolveAudioBitrateArg(process.env.FFMPEG_AUDIO_BITRATE, platformProfile.audioBitrateKbps)
  const ffAudioSampleRate = String(
    resolveAudioSampleRate(process.env.FFMPEG_AUDIO_SAMPLE_RATE, platformProfile.audioSampleRate)
  )
  if (renderConfig.mode === 'horizontal') {
    if (options.autoCaptions) {
      if (!features.subtitles.enabled) {
        throw new PlanLimitError('Subtitles are temporarily disabled.', 'subtitles', 'creator')
      }
      if (!isSubtitlePresetAllowed(normalizedSubtitle, tier)) {
        const requiredPlan = getRequiredPlanForSubtitlePreset(normalizedSubtitle)
        throw new PlanLimitError('Upgrade to unlock subtitle styles.', 'subtitles', requiredPlan)
      }
    }
    const autoZoomMax = Number(options.autoZoomMax ?? features.autoZoomMax)
    if (Number.isFinite(autoZoomMax) && autoZoomMax > features.autoZoomMax) {
      const requiredPlan = getRequiredPlanForAutoZoom(autoZoomMax)
      throw new PlanLimitError('Upgrade to unlock higher auto zoom limits.', 'autoZoomMax', requiredPlan)
    }
    const wantsAdvanced = Boolean(options.emotionalBoost) || Boolean(options.aggressiveMode)
    if (wantsAdvanced && !features.advancedEffects) {
      const requiredPlan = getRequiredPlanForAdvancedEffects()
      throw new PlanLimitError('Upgrade to unlock advanced effects.', 'advancedEffects', requiredPlan)
    }
  }
  const watermarkEnabled = renderConfig.mode === 'horizontal' ? features.watermark : false

  await updateJob(jobId, {
    requestedQuality: desiredQuality,
    finalQuality,
    watermarkApplied: watermarkEnabled,
    priority: features.priorityQueue,
    priorityLevel: features.priorityQueue ? 1 : 2
  })

  await ensureBucket(INPUT_BUCKET, true)
  await ensureBucket(OUTPUT_BUCKET, false)

  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), `${jobId}-`))
  const tmpIn = path.join(workDir, 'input')
  const tmpOut = path.join(workDir, 'output.mp4')
  const absTmpIn = path.resolve(tmpIn)
  const absTmpOut = path.resolve(tmpOut)
  try {
    await downloadObjectToFile({ key: job.inputPath, destPath: tmpIn })
  } catch (e) {
    await updateJob(jobId, { status: 'failed', error: 'download_failed' })
    throw new Error('download_failed')
  }
  throwIfCanceled()
  if (!fs.existsSync(tmpIn)) {
    await updateJob(jobId, { status: 'failed', error: 'input_file_missing_after_download' })
    throw new Error('input_file_missing_after_download')
  }
  const inStats = fs.statSync(tmpIn)
  if (!inStats.isFile() || inStats.size <= 0) {
    await updateJob(jobId, { status: 'failed', error: 'input_file_empty_after_download' })
    throw new Error('input_file_empty_after_download')
  }
  console.log(`[${requestId || 'noid'}] process paths`, {
    inputPath: absTmpIn,
    outputPath: absTmpOut,
    inputBytes: inStats.size
  })
  let subtitlePath: string | null = null
  let subtitleIsAss = false
  let outputUploadFallbackUsed = false
  const failedOutputUploads: string[] = []
  try {
    const storedDuration = job.inputDurationSeconds && job.inputDurationSeconds > 0 ? job.inputDurationSeconds : null
    const durationSeconds = storedDuration ?? getDurationSeconds(tmpIn) ?? 0
    if (!durationSeconds || durationSeconds <= 0) {
      await updateJob(jobId, { status: 'failed', error: 'duration_unavailable' })
      throw new Error('duration_unavailable')
    }
    const durationMinutes = toMinutes(durationSeconds)
    await updateJob(jobId, { inputDurationSeconds: Math.round(durationSeconds) })
    const runtimeRetentionProfile = resolveRuntimeRetentionProfile({
      renderMode: renderConfig.mode,
      runtimeSeconds: durationSeconds,
      requestedAggression: requestedAggressionLevel,
      requestedStrategy: requestedStrategyProfile,
      targetPlatform: retentionTargetPlatform
    })
    strategyProfile = runtimeRetentionProfile.strategy
    aggressionLevel = runtimeRetentionProfile.aggression
    const runtimeRetentionNotes = runtimeRetentionProfile.notes

    await ensureUsageWithinLimits(user.id, user.email, durationMinutes, tier, plan, renderConfig.mode)

    if (renderConfig.mode === 'vertical') {
      const sourceStream = probeVideoStream(tmpIn)
      if (!sourceStream?.width || !sourceStream?.height) {
        throw new Error('vertical_source_dimensions_unavailable')
      }
      const latestVerticalJob = await prisma.job.findUnique({
        where: { id: jobId },
        select: { analysis: true }
      })
      const verticalAnalysis = (latestVerticalJob?.analysis as any) || (job.analysis as any) || {}
      const verticalWindows = normalizeStoredEngagementWindows(
        verticalAnalysis?.editPlan?.engagementWindows ||
        verticalAnalysis?.engagement_windows ||
        verticalAnalysis?.engagementWindows
      )
      const verticalHookCandidates: HookCandidate[] = []
      const resolvedVerticalMode = renderConfig.verticalMode
        ? {
            ...defaultVerticalModeSettings(),
            ...renderConfig.verticalMode,
            output: {
              ...defaultVerticalModeSettings().output,
              ...(renderConfig.verticalMode.output || {})
            }
          }
        : defaultVerticalModeSettings()
      const clipRanges = buildVerticalClipRanges(durationSeconds || 0, renderConfig.verticalClipCount, {
        windows: verticalWindows,
        platformProfile: platformProfileId
      })
      const renderedClipPaths: string[] = []
      const outputPaths: string[] = []
      const hasInputAudio = hasAudioStream(tmpIn)
      const verticalAudioProfile = hasInputAudio ? probeAudioStream(tmpIn) : null
      const verticalAudioFilters = hasInputAudio
        ? buildAudioFilters({
            aggressionLevel,
            styleProfile: null,
            audioProfile: verticalAudioProfile
          })
        : []
      const localOutDir = path.join(process.cwd(), 'outputs', job.userId, jobId)
      fs.mkdirSync(localOutDir, { recursive: true })
      let verticalSourceCues: TranscriptCue[] = []
      if (options.autoCaptions) {
        await updateJob(jobId, { status: 'subtitling', progress: 62, watermarkApplied: false })
        const generatedVerticalSubtitlePath = await generateSubtitles(tmpIn, workDir)
        if (generatedVerticalSubtitlePath) {
          verticalSourceCues = parseTranscriptCues(generatedVerticalSubtitlePath)
        }
      }

      await updatePipelineStepState(jobId, 'RENDER_FINAL', {
        status: 'running',
        attempts: 1,
        startedAt: toIsoNow(),
        lastError: null
      })
      await updateJob(jobId, { status: 'rendering', progress: 80, watermarkApplied: false })

      for (let idx = 0; idx < clipRanges.length; idx += 1) {
        const range = clipRanges[idx]
        const localClipPath = path.join(localOutDir, `vertical-clip-${idx + 1}.mp4`)
        let clipSubtitlePath: string | null = null
        let clipSubtitleIsAss = false
        if (options.autoCaptions && verticalSourceCues.length) {
          const clipSegment: Segment = {
            start: Number(range.start.toFixed(3)),
            end: Number(range.end.toFixed(3)),
            speed: 1
          }
          const remappedClipCues = remapTranscriptCuesToEditedTimeline(verticalSourceCues, [clipSegment])
          if (remappedClipCues.length) {
            const clipSrtPath = path.join(workDir, `vertical-clip-${idx + 1}-captions.srt`)
            const writtenClipSrt = writeTranscriptCuesToSrt(remappedClipCues, clipSrtPath)
            if (writtenClipSrt) {
              clipSubtitlePath = writtenClipSrt
              if (normalizedSubtitle === 'mrbeast_animated') {
                const clipAssPath = buildMrBeastAnimatedAss({
                  srtPath: clipSubtitlePath,
                  workingDir: workDir,
                  style: subtitleStyle
                })
                if (clipAssPath) {
                  clipSubtitlePath = clipAssPath
                  clipSubtitleIsAss = true
                }
              }
            }
          }
        }
        await renderVerticalClip({
          inputPath: tmpIn,
          outputPath: localClipPath,
          start: range.start,
          end: range.end,
          verticalMode: resolvedVerticalMode,
          sourceWidth: sourceStream.width,
          sourceHeight: sourceStream.height,
          withAudio: hasInputAudio,
          videoPreset: ffPreset,
          videoCrf: ffCrf,
          audioBitrate: ffAudioBitrate,
          audioSampleRate: ffAudioSampleRate,
          audioFilters: verticalAudioFilters,
          subtitlePath: clipSubtitlePath,
          subtitleIsAss: clipSubtitleIsAss,
          subtitleStyle
        })
        const clipStats = fs.statSync(localClipPath)
        if (!clipStats.isFile() || clipStats.size <= 0) {
          throw new Error(`vertical_clip_empty_${idx + 1}`)
        }
        renderedClipPaths.push(localClipPath)
      }

      await updateJob(jobId, { progress: 95 })

      for (let idx = 0; idx < renderedClipPaths.length; idx += 1) {
        const clipPath = renderedClipPaths[idx]
        const key = `${job.userId}/${jobId}/vertical/clip-${idx + 1}.mp4`
        try {
          await uploadFileToOutput({ key, filePath: clipPath, contentType: 'video/mp4' })
        } catch (error) {
          outputUploadFallbackUsed = true
          failedOutputUploads.push(key)
          console.warn(`[${requestId || 'noid'}] vertical output upload failed, serving local fallback`, {
            key,
            error: (error as any)?.message || error
          })
        }
        outputPaths.push(key)
      }

      if (!outputPaths.length) {
        await updateJob(jobId, { status: 'failed', error: 'output_upload_missing' })
        throw new Error('output_upload_missing')
      }

      const finalRenderConfig: RenderConfig = {
        ...renderConfig,
        verticalMode: resolvedVerticalMode
      }
      const verticalMetadataSummary = buildVerticalMetadataSummary({
        durationSeconds,
        clipRanges,
        windows: verticalWindows,
        hookCandidates: verticalHookCandidates
      })
      const verticalContentFormat = inferRetentionContentFormat({
        runtimeSeconds: durationSeconds,
        windows: verticalWindows,
        renderMode: finalRenderConfig.mode,
        nicheProfile: null,
        targetPlatform: retentionTargetPlatform
      })
      const nextAnalysis = buildPersistedRenderAnalysis({
        existing: {
          ...((job.analysis as any) || {}),
          metadata_version: 2,
          retentionAggressionLevel: aggressionLevel,
          retentionLevel: aggressionLevel,
          retentionStrategyProfile: strategyProfile,
          retentionStrategy: strategyProfile,
          retentionTargetPlatform: retentionTargetPlatform,
          retention_target_platform: retentionTargetPlatform,
          retentionPlatform: retentionTargetPlatform,
          targetPlatform: retentionTargetPlatform,
          platform: retentionTargetPlatform,
          platformProfile: platformProfileId,
          platform_profile: platformProfileId,
          retentionContentFormat: verticalContentFormat,
          retention_content_format: verticalContentFormat,
          retention_runtime_profile: {
            strategy: strategyProfile,
            aggression: aggressionLevel,
            isLongForm: runtimeRetentionProfile.isLongForm,
            isVerticalShortForm: runtimeRetentionProfile.isVerticalShortForm,
            notes: runtimeRetentionNotes
          },
          vertical_clip_ranges: clipRanges.map((range, index) => ({
            clip: index + 1,
            start: Number(range.start.toFixed(3)),
            end: Number(range.end.toFixed(3)),
            duration: Number(Math.max(0, range.end - range.start).toFixed(3))
          })),
          metadata_summary: verticalMetadataSummary,
          output_upload_fallback: outputUploadFallbackUsed
            ? {
                used: true,
                failedOutputs: failedOutputUploads,
                mode: 'local',
                updatedAt: toIsoNow()
              }
            : ((job.analysis as any)?.output_upload_fallback ?? null)
        },
        renderConfig: finalRenderConfig,
        retentionTargetPlatform: retentionTargetPlatform,
        platformProfile: platformProfileId,
        onlyCuts: options.onlyCuts,
        outputPaths
      })

      await updatePipelineStepState(jobId, 'RENDER_FINAL', {
        status: 'completed',
        completedAt: toIsoNow(),
        meta: { segmentCount: clipRanges.length, outputPaths }
      })
      await updatePipelineStepState(jobId, 'RETENTION_SCORE', {
        status: 'completed',
        completedAt: toIsoNow(),
        meta: { score: null, mode: 'vertical' }
      })

      await updateJob(jobId, {
        status: 'completed',
        progress: 100,
        outputPath: outputPaths[0],
        finalQuality,
        watermarkApplied: false,
        retentionScore: null,
        optimizationNotes: null,
        renderSettings: buildPersistedRenderSettings(finalRenderConfig, {
          retentionAggressionLevel: aggressionLevel,
          retentionStrategyProfile: strategyProfile,
          retentionTargetPlatform: retentionTargetPlatform,
          platformProfile: platformProfileId,
          onlyCuts: options.onlyCuts
        }),
        analysis: nextAnalysis
      })

      const monthKey = getMonthKey()
      await incrementUsageForMonth(user.id, monthKey, 1, durationMinutes)
      await incrementRenderUsage(user.id, monthKey, 1)
      console.log(`[${requestId || 'noid'}] process complete ${jobId}`)
      return
    }

    let processed = false
    let retentionScore: number | null = null
    let retentionScoreBeforeEdit: number | null = null
    let retentionScoreAfterEdit: number | null = null
    let optimizationNotes: string[] = []
    if (runtimeRetentionNotes.length) {
      optimizationNotes.push(...runtimeRetentionNotes)
    }
    if (subtitleStyleSource === 'platform' && options.autoCaptions) {
      optimizationNotes.push(
        `Applied ${platformProfile.label} profile caption default (${normalizedSubtitle.replace(/_/g, ' ')}).`
      )
    }
    let retentionAttempts: RetentionAttemptRecord[] = []
    let selectedJudge: RetentionJudgeReport | null = null
    let selectedHook: HookCandidate | null = null
    let selectedHookSelectionSource: 'auto' | 'user_selected' | 'fallback' = 'auto'
    let selectedPatternInterruptCount = 0
    let selectedPatternInterruptDensity = 0
    let selectedBoredomRemovalRatio = 0
    let selectedStrategy: RetentionRetryStrategy = 'BASELINE'
    let selectedStoryReorderMap: Array<{ sourceStart: number; sourceEnd: number; orderedIndex: number }> = []
    let hasTranscriptSignals = false
    let contentSignalStrength = 0.42
    let qualityGateOverride: { applied: boolean; reason: string } | null = null
    let engagementWindowsForAnalysis: EngagementWindow[] = []
    let finalSegmentsForAnalysis: Segment[] = []
    let styleProfileForAnalysis: ContentStyleProfile | null = ((job.analysis as any)?.style_profile as ContentStyleProfile) || null
    let nicheProfileForAnalysis: VideoNicheProfile | null = ((job.analysis as any)?.niche_profile as VideoNicheProfile) || null
    let styleArchetypeBlendForAnalysis: StyleArchetypeBlend | null = ((job.analysis as any)?.style_archetype_blend as StyleArchetypeBlend) || null
    let behaviorStyleProfileForAnalysis: RetentionBehaviorStyleProfile | null = ((job.analysis as any)?.behavior_style_profile as RetentionBehaviorStyleProfile) || null
    let autoEscalationEventsForAnalysis: AutoEscalationEvent[] = Array.isArray((job.analysis as any)?.auto_escalation_events)
      ? (((job.analysis as any)?.auto_escalation_events as AutoEscalationEvent[]) || [])
      : []
    let editDecisionTimelineForAnalysis: EditDecisionTimeline | null = ((job.analysis as any)?.edit_decision_timeline as EditDecisionTimeline) || null
    let styleFeatureSnapshotForAnalysis: TimelineFeatureSnapshot | null = ((job.analysis as any)?.style_timeline_features as TimelineFeatureSnapshot) || null
    let selectedContentFormat: RetentionContentFormat = inferRetentionContentFormat({
      runtimeSeconds: durationSeconds,
      windows: [],
      renderMode: renderConfig.mode,
      nicheProfile: nicheProfileForAnalysis,
      targetPlatform: retentionTargetPlatform
    })
    let beatAnchorsForAnalysis: number[] = Array.isArray((job.analysis as any)?.beat_anchors)
      ? ((job.analysis as any).beat_anchors as number[])
      : []
    let emotionalBeatAnchorsForAnalysis: number[] = Array.isArray((job.analysis as any)?.emotional_beat_anchors)
      ? ((job.analysis as any).emotional_beat_anchors as number[])
      : []
    let emotionalBeatCutCountForAnalysis = Number.isFinite(Number((job.analysis as any)?.emotional_beat_cut_count))
      ? Number((job.analysis as any).emotional_beat_cut_count)
      : 0
    let emotionalLeadTrimmedSecondsForAnalysis = Number.isFinite(Number((job.analysis as any)?.emotional_lead_trimmed_seconds))
      ? Number((job.analysis as any).emotional_lead_trimmed_seconds)
      : 0
    let emotionalTuningForAnalysis = ((job.analysis as any)?.emotional_tuning_profile as EmotionalTuningProfile) || null
    let hookVariantsForAnalysis: HookCandidate[] = getHookCandidatesFromAnalysis(job.analysis as any)
    let hookCalibrationForAnalysis: HookCalibrationProfile | null = ((job.analysis as any)?.hook_calibration as HookCalibrationProfile) || hookCalibration
    let selectedAutoEscalationEvents: AutoEscalationEvent[] = autoEscalationEventsForAnalysis
    let audioProfileForAnalysis: AudioStreamProfile | null = null
    let audioFiltersForAnalysis: string[] = []
    if (hasFfmpeg()) {
      const qualityTarget = getTargetDimensions(finalQuality)
      const sourceProbe = probeVideoStream(tmpIn)
      const target = resolveHorizontalTargetDimensions({
        horizontalMode: renderConfig.horizontalMode,
        qualityTarget,
        sourceWidth: sourceProbe?.width,
        sourceHeight: sourceProbe?.height
      })
      const horizontalFit = parseHorizontalFitMode(renderConfig.horizontalMode.fit, 'contain')

      const storedPlan = (job.analysis as any)?.editPlan as EditPlan | undefined
      let editPlan: EditPlan | null = storedPlan?.segments ? storedPlan : null
      if (!editPlan && durationSeconds) {
        try {
          editPlan = await buildEditPlan(tmpIn, durationSeconds, {
            ...options,
            retentionAggressionLevel: aggressionLevel,
            retentionStrategyProfile: strategyProfile,
            aggressiveMode: options.onlyCuts ? false : isAggressiveRetentionLevel(aggressionLevel)
          }, undefined, {
            aggressionLevel,
            hookCalibration
          })
        } catch (err) {
          console.warn(`[${requestId || 'noid'}] edit-plan generation failed during process, using deterministic fallback`, err)
          editPlan = buildDeterministicFallbackEditPlan(durationSeconds, options)
          optimizationNotes.push('AI edit plan fallback: deterministic rescue plan used.')
        }
      }
      if (editPlan?.styleProfile) styleProfileForAnalysis = editPlan.styleProfile
      if (editPlan?.nicheProfile) nicheProfileForAnalysis = editPlan.nicheProfile
      if (Number(editPlan?.emotionalBeatCutCount || 0) > 0) {
        optimizationNotes.push(
          `Emotion-aware pacing inserted ${Number(editPlan?.emotionalBeatCutCount)} additional cut${Number(editPlan?.emotionalBeatCutCount) === 1 ? '' : 's'}.`
        )
      }
      if (Number(editPlan?.emotionalLeadTrimmedSeconds || 0) > 0.2) {
        optimizationNotes.push(
          `Trimmed ${Number(editPlan?.emotionalLeadTrimmedSeconds || 0).toFixed(1)}s of low-signal lead-ins before emotional peaks.`
        )
      }
      if (editPlan?.emotionalTuning) {
        optimizationNotes.push(
          `Emotional tuning profile: threshold ${Number(editPlan.emotionalTuning.thresholdOffset).toFixed(2)}, spacing x${Number(editPlan.emotionalTuning.spacingMultiplier).toFixed(2)}, lead-trim x${Number(editPlan.emotionalTuning.leadTrimMultiplier).toFixed(2)}.`
        )
      }
      engagementWindowsForAnalysis = editPlan?.engagementWindows ?? []
      if (Array.isArray(editPlan?.beatAnchors)) beatAnchorsForAnalysis = editPlan.beatAnchors
      if (Array.isArray(editPlan?.emotionalBeatAnchors)) emotionalBeatAnchorsForAnalysis = editPlan.emotionalBeatAnchors
      if (Number.isFinite(Number(editPlan?.emotionalBeatCutCount))) emotionalBeatCutCountForAnalysis = Number(editPlan?.emotionalBeatCutCount)
      if (Number.isFinite(Number(editPlan?.emotionalLeadTrimmedSeconds))) {
        emotionalLeadTrimmedSecondsForAnalysis = Number(editPlan?.emotionalLeadTrimmedSeconds)
      }
      if (editPlan?.emotionalTuning) emotionalTuningForAnalysis = editPlan.emotionalTuning
      if (Array.isArray(editPlan?.hookVariants) && editPlan.hookVariants.length) {
        hookVariantsForAnalysis = editPlan.hookVariants
      } else if (Array.isArray(editPlan?.hookCandidates) && editPlan.hookCandidates.length) {
        hookVariantsForAnalysis = editPlan.hookCandidates
      }
      if (editPlan?.hookCalibration) hookCalibrationForAnalysis = editPlan.hookCalibration
      if (editPlan?.styleArchetypeBlend) styleArchetypeBlendForAnalysis = editPlan.styleArchetypeBlend
      if (editPlan?.behaviorStyleProfile) behaviorStyleProfileForAnalysis = editPlan.behaviorStyleProfile
      if (Array.isArray(editPlan?.autoEscalationEvents)) autoEscalationEventsForAnalysis = editPlan.autoEscalationEvents
      if (editPlan?.editDecisionTimeline) editDecisionTimelineForAnalysis = editPlan.editDecisionTimeline
      if (editPlan?.styleFeatureSnapshot) styleFeatureSnapshotForAnalysis = editPlan.styleFeatureSnapshot
      const energySamplesForEscalation = buildEnergySamplesFromWindows(editPlan?.engagementWindows ?? [])

      await updateJob(jobId, { status: 'story', progress: 55 })

      selectedContentFormat = inferRetentionContentFormat({
        runtimeSeconds: durationSeconds,
        windows: editPlan?.engagementWindows ?? engagementWindowsForAnalysis,
        renderMode: renderConfig.mode,
        nicheProfile: nicheProfileForAnalysis,
        targetPlatform: retentionTargetPlatform
      })
      const allowAggressiveStoryReorder = selectedContentFormat === 'tiktok_short'
      const baseSegments: Segment[] = editPlan
        ? editPlan.segments
        : buildGuaranteedFallbackSegments(durationSeconds || 0, options)
      const storySegments = editPlan && !options.onlyCuts
        ? (
          allowAggressiveStoryReorder
            ? applyStoryStructure(baseSegments, editPlan.engagementWindows, durationSeconds, editPlan.styleProfile)
            : baseSegments
        )
        : baseSegments
      hasTranscriptSignals = Boolean(
        editPlan?.transcriptSignals?.hasTranscript ||
        (
          (editPlan?.hook?.text || '')
            .trim()
            .split(/\s+/)
            .filter(Boolean)
            .length >= 4
        )
      )
      contentSignalStrength = computeContentSignalStrength(editPlan?.engagementWindows ?? [])
      const qualityGateThresholds = resolveQualityGateThresholds({
        aggressionLevel,
        hasTranscript: hasTranscriptSignals,
        signalStrength: contentSignalStrength,
        contentFormat: selectedContentFormat,
        targetPlatform: retentionTargetPlatform,
        feedbackOffset: feedbackQualityGateOffset
      })
      qualityGateOverride = null
      const latestJobHookSnapshot = await prisma.job.findUnique({
        where: { id: jobId },
        select: { analysis: true }
      })
      const latestAnalysisForHook = (
        (latestJobHookSnapshot?.analysis as any) ||
        (job.analysis as any) ||
        {}
      ) as Record<string, any>
      const preferredHookCandidateRaw =
        options.preferredHookCandidate ||
        parsePreferredHookCandidateFromPayload(latestAnalysisForHook.preferred_hook)
      const hookCandidatesFromStoredAnalysis = getHookCandidatesFromAnalysis(latestAnalysisForHook)
      const hookCandidates = (
        editPlan?.hookCandidates?.length
          ? editPlan.hookCandidates
          : hookCandidatesFromStoredAnalysis.length
            ? hookCandidatesFromStoredAnalysis
            : (editPlan?.hook ? [editPlan.hook] : [])
      ).filter((candidate): candidate is HookCandidate => Boolean(candidate))
      const hookDecision = selectRenderableHookCandidate({
        candidates: hookCandidates,
        aggressionLevel,
        hasTranscript: hasTranscriptSignals,
        signalStrength: contentSignalStrength
      })
      let resolvedHookDecision: HookSelectionDecision | null = hookDecision
      if (!resolvedHookDecision) {
        const fallbackHook = buildFallbackHookCandidateFromStorySegments({
          segments: storySegments,
          windows: editPlan?.engagementWindows ?? [],
          silences: editPlan?.silences ?? [],
          durationSeconds
        }) || {
          start: Number((storySegments[0]?.start ?? 0).toFixed(3)),
          duration: Number(
            clamp(
              storySegments[0] ? (storySegments[0].end - storySegments[0].start) : 6,
              HOOK_MIN,
              HOOK_MAX
            ).toFixed(3)
          ),
          score: 0.46,
          auditScore: 0.44,
          auditPassed: false,
          text: '',
          reason: 'Fallback hook generated from earliest stable segment due weak candidate pool.',
          synthetic: true
        }
        resolvedHookDecision = {
          candidate: fallbackHook,
          confidence: getHookCandidateConfidence(fallbackHook),
          threshold: resolveHookScoreThreshold({
            aggressionLevel,
            hasTranscript: hasTranscriptSignals,
            signalStrength: contentSignalStrength
          }),
          usedFallback: true,
          reason: fallbackHook.reason
        }
        optimizationNotes.push('Hook fallback applied: no strong candidate passed; selected strongest low-silence highlight.')
      }
      if (!resolvedHookDecision) {
        throw new HookGateError('Hook candidate unavailable for render after fallback resolution')
      }
      let preferredHookCandidate = matchPreferredHookCandidate({
        preferred: preferredHookCandidateRaw,
        candidates: hookCandidates
      })
      if (preferredHookCandidateRaw && !preferredHookCandidate) {
        optimizationNotes.push('Preferred hook selection was unavailable for this render; using highest-scoring hook.')
      }
      if (!preferredHookCandidate && hookCandidates.length > 1 && HOOK_SELECTION_WAIT_MS > 0) {
        const selectionWindowEndsAt = new Date(Date.now() + HOOK_SELECTION_WAIT_MS).toISOString()
        await updatePipelineStepState(jobId, 'HOOK_SELECT_AND_AUDIT', {
          status: 'running',
          attempts: 1,
          startedAt: toIsoNow(),
          lastError: null,
          meta: {
            waitingForUserSelection: true,
            selectionWindowMs: HOOK_SELECTION_WAIT_MS,
            selectionWindowEndsAt,
            hookCandidates: hookCandidates.slice(0, HOOK_SELECTION_MAX_CANDIDATES),
            hasTranscriptSignals,
            contentSignalStrength: Number(contentSignalStrength.toFixed(4))
          }
        })
        await updateJob(jobId, { status: 'hooking', progress: 56 })
        const waitedPreferredHook = await waitForPreferredHookSelection({
          jobId,
          candidates: hookCandidates,
          timeoutMs: HOOK_SELECTION_WAIT_MS,
          pollMs: HOOK_SELECTION_POLL_MS
        })
        if (waitedPreferredHook) {
          preferredHookCandidate = waitedPreferredHook
          optimizationNotes.push(
            `User-selected hook applied during hook stage (${waitedPreferredHook.start.toFixed(1)}s-${(waitedPreferredHook.start + waitedPreferredHook.duration).toFixed(1)}s).`
          )
        }
        await updateJob(jobId, { status: 'story', progress: 55 })
      }
      const initialHook = preferredHookCandidate || resolvedHookDecision.candidate
      selectedHookSelectionSource = preferredHookCandidate
        ? 'user_selected'
        : resolvedHookDecision.usedFallback
          ? 'fallback'
          : 'auto'
      if (preferredHookCandidate) {
        optimizationNotes.push(
          `User-selected hook pinned to opening (${preferredHookCandidate.start.toFixed(1)}s-${(preferredHookCandidate.start + preferredHookCandidate.duration).toFixed(1)}s).`
        )
      }
      const orderedHookCandidates = [
        initialHook,
        ...hookCandidates.filter((candidate) => (
          Math.abs(candidate.start - initialHook.start) > 0.01 ||
          Math.abs(candidate.duration - initialHook.duration) > 0.01
        ))
      ]
      await updatePipelineStepState(jobId, 'HOOK_SELECT_AND_AUDIT', {
        status: 'completed',
        completedAt: toIsoNow(),
        meta: {
          selectedHook: initialHook,
          confidence: Number(resolvedHookDecision.confidence.toFixed(4)),
          threshold: resolvedHookDecision.threshold,
          usedFallback: resolvedHookDecision.usedFallback,
          reason: resolvedHookDecision.reason,
          hookSelectionSource: selectedHookSelectionSource,
          hasTranscriptSignals,
          contentSignalStrength: Number(contentSignalStrength.toFixed(4))
        }
      })
      if (resolvedHookDecision.usedFallback && resolvedHookDecision.reason) {
        optimizationNotes.push(resolvedHookDecision.reason)
      }

      const reorderForEmotion = (segments: Segment[]) => {
        if (!editPlan || segments.length <= 3) return segments
        const scored = segments
          .map((segment) => ({
            segment,
            score: averageWindowMetric(
              editPlan.engagementWindows,
              segment.start,
              segment.end,
              (window) => (
                0.45 * window.emotionIntensity +
                0.25 * window.vocalExcitement +
                0.2 * (window.hookScore ?? window.score) +
                0.1 * (window.curiosityTrigger ?? 0)
              )
            )
          }))
          .sort((a, b) => b.score - a.score)
        const strongest = scored[0]?.segment
        const secondStrongest = scored[1]?.segment
        if (!strongest) return segments
        const middle = segments.filter((segment) => segment !== strongest && segment !== secondStrongest)
        const tensionLead = secondStrongest ? [secondStrongest] : []
        return [...tensionLead, ...middle, strongest]
      }
      const allowAggressiveEmotionReorder = selectedContentFormat === 'tiktok_short'

      const applyPacingRetry = (segments: Segment[]) => {
        if (!editPlan) return segments
        const stricter = enforceSegmentLengths(
          segments.map((segment) => ({ ...segment })),
          CUT_MIN,
          CUT_MAX,
          editPlan.engagementWindows
        )
        return stricter.map((segment) => {
          const score = averageWindowMetric(editPlan.engagementWindows, segment.start, segment.end, (window) => window.score)
          const speech = averageWindowMetric(editPlan.engagementWindows, segment.start, segment.end, (window) => window.speechIntensity)
          const baseSpeed = segment.speed && segment.speed > 0 ? segment.speed : 1
          if (score < 0.45 && speech < 0.52) {
            return { ...segment, speed: Number(clamp(baseSpeed + 0.1, 1, 1.15).toFixed(3)) }
          }
          return segment
        })
      }

      const buildAttemptSegments = (strategy: RetentionRetryStrategy, hookCandidate: HookCandidate) => {
        const baseHookRange: TimeRange = {
          start: hookCandidate.start,
          end: Number((hookCandidate.start + hookCandidate.duration).toFixed(3))
        }
        const hookRange = editPlan
          ? tightenHookRangeForRetention({
              range: baseHookRange,
              windows: editPlan.engagementWindows,
              silences: editPlan.silences || [],
              durationSeconds
            })
          : baseHookRange
        const effectiveHookCandidate: HookCandidate = {
          ...hookCandidate,
          start: hookRange.start,
          duration: Number(Math.max(0.2, hookRange.end - hookRange.start).toFixed(3))
        }
        const hookSegment: Segment = { ...hookRange, speed: 1, emphasize: true }
        let story = storySegments.map((segment) => ({ ...segment }))
        if (options.autoHookMove && !options.onlyCuts) {
          story = subtractRange(story, hookRange)
        }
        if (strategy === 'HOOK_FIRST') {
          // Trim early exposition after the hook so payoff is approached faster.
          story = story.map((segment, index) => {
            if (index > 0) return segment
            const start = segment.start
            const end = Math.min(segment.end, start + 4.8)
            const speed = Number(clamp((segment.speed ?? 1) + 0.08, 1, 1.16).toFixed(3))
            return { ...segment, start, end, speed }
          })
        } else if (strategy === 'EMOTION_FIRST') {
          story = allowAggressiveEmotionReorder ? reorderForEmotion(story) : applyPacingRetry(story)
        } else if (strategy === 'PACING_FIRST') {
          story = applyPacingRetry(story)
        } else if (strategy === 'RESCUE_MODE') {
          story = applyPacingRetry(story)
          if (editPlan) {
            const scored = story
              .map((segment, index) => ({
                segment,
                index,
                score: averageWindowMetric(editPlan.engagementWindows, segment.start, segment.end, (window) => window.score),
                speech: averageWindowMetric(editPlan.engagementWindows, segment.start, segment.end, (window) => window.speechIntensity),
                runtime: Math.max(0, segment.end - segment.start)
              }))
            const maxRemovableSeconds = Math.max(6, durationSeconds * 0.14)
            let removedSeconds = 0
            const removeIndexes = new Set<number>()
            for (const entry of scored.sort((a, b) => a.score - b.score || b.runtime - a.runtime)) {
              if (removedSeconds >= maxRemovableSeconds) break
              if (entry.index === 0) continue
              if (entry.runtime < 1.7) continue
              if (entry.score > 0.5 || entry.speech > 0.58) continue
              removeIndexes.add(entry.index)
              removedSeconds += entry.runtime
            }
            const filteredStory = story.filter((_, index) => !removeIndexes.has(index))
            story = filteredStory.length ? filteredStory : story
            story = enforceSegmentLengths(story, CUT_MIN, CUT_MAX, editPlan.engagementWindows).map((segment) => {
              const score = averageWindowMetric(editPlan.engagementWindows, segment.start, segment.end, (window) => window.score)
              const speech = averageWindowMetric(editPlan.engagementWindows, segment.start, segment.end, (window) => window.speechIntensity)
              const baseSpeed = segment.speed && segment.speed > 0 ? segment.speed : 1
              if (score < 0.52 && speech < 0.6) {
                return { ...segment, speed: Number(clamp(baseSpeed + 0.12, 1, 1.22).toFixed(3)) }
              }
              return segment
            })
          }
        }
        let ordered = options.autoHookMove && !options.onlyCuts
          ? [hookSegment, ...story]
          : story
        ordered = ordered.filter((segment) => segment.end - segment.start > 0.25)
        ordered = enforceLongFormComprehensionFloor({
          segments: ordered,
          hookRange,
          durationSeconds,
          renderMode: renderConfig.mode,
          contentFormat: selectedContentFormat
        })
        const effected = editPlan && !options.onlyCuts
          ? applySegmentEffects(
              ordered,
              editPlan.engagementWindows,
              {
                ...options,
                retentionAggressionLevel: aggressionLevel,
                retentionStrategyProfile: strategyProfile,
                aggressiveMode: (options.onlyCuts ? false : isAggressiveRetentionLevel(aggressionLevel)) || strategy !== 'BASELINE'
              },
              hookRange
            )
          : ordered
        const interruptAggression: RetentionAggressionLevel =
          strategy === 'HOOK_FIRST'
            ? 'viral'
            : strategy === 'RESCUE_MODE'
              ? 'viral'
            : strategy === 'PACING_FIRST'
              ? (aggressionLevel === 'low' ? 'medium' : 'high')
              : aggressionLevel
        const styleAdjustedInterruptAggression = getStyleAdjustedAggressionLevel(
          interruptAggression,
          editPlan?.styleProfile
        )
        const interruptInjected = injectPatternInterrupts({
          segments: effected,
          durationSeconds,
          aggressionLevel: styleAdjustedInterruptAggression,
          targetIntervalSeconds: behaviorStyleProfileForAnalysis?.patternInterruptInterval
        })
        const withZoom = editPlan && !options.onlyCuts
          ? applyZoomEasing(interruptInjected.segments)
          : interruptInjected.segments
        const withCutDensityLimit = enforceCutDensityLimit({
          segments: withZoom,
          durationSeconds,
          renderMode: renderConfig.mode,
          targetPlatform: retentionTargetPlatform
        })
        const escalationLowEnergyThreshold = behaviorStyleProfileForAnalysis?.energyEscalationCurve === 'aggressive'
          ? 0.52
          : behaviorStyleProfileForAnalysis?.energyEscalationCurve === 'steady'
            ? 0.54
            : 0.57
        const autoEscalationResult = options.onlyCuts
          ? { segments: withCutDensityLimit, events: [] as AutoEscalationEvent[], count: 0 }
          : applyAutoEscalationGuarantee({
              segments: withCutDensityLimit,
              energySamples: energySamplesForEscalation,
              flatWindowSeconds: clamp(
                Number(behaviorStyleProfileForAnalysis?.autoEscalationWindowSec ?? 6),
                5.2,
                9.2
              ),
              lowEnergyThreshold: escalationLowEnergyThreshold,
              maxSpeed: clamp(
                behaviorStyleProfileForAnalysis?.energyEscalationCurve === 'aggressive' ? 1.32 : 1.26,
                1.18,
                1.34
              )
            })
        const totalPatternInterruptCount = interruptInjected.count + autoEscalationResult.count
        const runtimeSeconds = Math.max(0.1, computeEditedRuntimeSeconds(autoEscalationResult.segments))
        return {
          hook: effectiveHookCandidate,
          hookRange,
          segments: autoEscalationResult.segments,
          autoEscalationEvents: autoEscalationResult.events,
          patternInterruptCount: totalPatternInterruptCount,
          patternInterruptDensity: Number((totalPatternInterruptCount / runtimeSeconds).toFixed(4))
        }
      }

      let finalSegments: Segment[] = []
      const attemptStrategies = RETENTION_VARIANT_STRATEGIES.slice(0, Math.max(1, MAX_QUALITY_GATE_RETRIES + 1))
      const attemptEvaluations: Array<{
        strategy: RetentionRetryStrategy
        hookCandidate: HookCandidate
        segments: Segment[]
        judge: RetentionJudgeReport
        retention: ReturnType<typeof computeRetentionScore>
        predictedRetention: number
        variantScore: number
        patternInterruptCount: number
        patternInterruptDensity: number
        autoEscalationEvents: AutoEscalationEvent[]
      }> = []
      for (let attemptIndex = 0; attemptIndex < attemptStrategies.length; attemptIndex += 1) {
        const strategy = attemptStrategies[attemptIndex]
        const hookCandidate = (
          preferredHookCandidate
            ? preferredHookCandidate
            : strategy === 'HOOK_FIRST'
              ? (orderedHookCandidates[1] || orderedHookCandidates[0] || initialHook)
              : strategy === 'EMOTION_FIRST'
                ? (orderedHookCandidates[2] || orderedHookCandidates[0] || initialHook)
                : strategy === 'PACING_FIRST'
                  ? (orderedHookCandidates[3] || orderedHookCandidates[0] || initialHook)
                  : initialHook
        )
        const attempt = buildAttemptSegments(strategy, hookCandidate)
        const effectiveHookCandidate = attempt.hook
        const retention = computeRetentionScore(
          attempt.segments,
          editPlan?.engagementWindows ?? [],
          effectiveHookCandidate.score,
          options.autoCaptions,
          {
            removedRanges: editPlan?.removedSegments ?? [],
            patternInterruptCount: attempt.patternInterruptCount,
            contentFormat: selectedContentFormat,
            targetPlatform: retentionTargetPlatform
          }
        )
        const clarityPenalty = hookCandidate.auditPassed
          ? 0.08
          : hasTranscriptSignals
            ? 0.3
            : 0.2
        const judge = buildRetentionJudgeReport({
          retentionScore: retention,
          hook: effectiveHookCandidate,
          windows: editPlan?.engagementWindows ?? [],
          clarityPenalty,
          captionsEnabled: options.autoCaptions,
          patternInterruptCount: attempt.patternInterruptCount,
          removedRanges: editPlan?.removedSegments ?? [],
          segments: attempt.segments,
          thresholds: qualityGateThresholds,
          contentFormat: selectedContentFormat,
          targetPlatform: retentionTargetPlatform,
          strategyProfile
        })
        const predictedRetention = predictVariantRetention({
          strategy,
          judge,
          hook: effectiveHookCandidate,
          hookCalibration: hookCalibrationForAnalysis,
          styleProfile: styleProfileForAnalysis
        })
        const variantScore = Number((
          0.8 * predictedRetention +
          0.2 * judge.retention_score +
          (judge.passed ? 3.5 : 0)
        ).toFixed(2))
        retentionAttempts.push({
          attempt: attemptIndex + 1,
          strategy,
          judge,
          hook: effectiveHookCandidate,
          patternInterruptCount: attempt.patternInterruptCount,
          patternInterruptDensity: attempt.patternInterruptDensity,
          boredomRemovalRatio: retention.details.boredomRemovalRatio,
          predictedRetention,
          variantScore
        })
        attemptEvaluations.push({
          strategy,
          hookCandidate: effectiveHookCandidate,
          segments: attempt.segments,
          judge,
          retention,
          predictedRetention,
          variantScore,
          patternInterruptCount: attempt.patternInterruptCount,
          patternInterruptDensity: attempt.patternInterruptDensity,
          autoEscalationEvents: attempt.autoEscalationEvents
        })
      }
      if (attemptEvaluations.length) {
        const passedAttempts = attemptEvaluations.filter((attempt) => attempt.judge.passed)
        const candidatePool = passedAttempts.length ? passedAttempts : attemptEvaluations
        const winner = candidatePool
          .slice()
          .sort((a, b) => (
            b.variantScore - a.variantScore ||
            b.predictedRetention - a.predictedRetention ||
            b.judge.retention_score - a.judge.retention_score
          ))[0]
        if (winner) {
          finalSegments = winner.segments
          selectedHook = winner.hookCandidate
          selectedJudge = winner.judge
          retentionScore = winner.judge.retention_score
          selectedPatternInterruptCount = winner.patternInterruptCount
          selectedPatternInterruptDensity = winner.patternInterruptDensity
          selectedBoredomRemovalRatio = winner.retention.details.boredomRemovalRatio
          selectedAutoEscalationEvents = winner.autoEscalationEvents
          selectedStrategy = winner.strategy
          selectedStoryReorderMap = finalSegments.map((segment, orderedIndex) => ({
            sourceStart: Number(segment.start.toFixed(3)),
            sourceEnd: Number(segment.end.toFixed(3)),
            orderedIndex
          }))
          optimizationNotes = [
            ...optimizationNotes,
            ...winner.retention.notes,
            ...winner.judge.why_keep_watching.map((line) => `Why keep watching: ${line}`)
          ]
        }
      }

      if (!selectedJudge) {
        const reason = 'Retention judge unavailable after retries'
        await updatePipelineStepState(jobId, 'STORY_QUALITY_GATE', {
          status: 'failed',
          completedAt: toIsoNow(),
          lastError: reason,
          meta: {
            attempts: retentionAttempts,
            thresholds: qualityGateThresholds,
            hasTranscriptSignals,
            contentSignalStrength: Number(contentSignalStrength.toFixed(4)),
            contentFormat: selectedContentFormat,
            targetPlatform: retentionTargetPlatform,
            strategyProfile
          }
        })
        await updatePipelineStepState(jobId, 'RETENTION_SCORE', {
          status: 'failed',
          completedAt: toIsoNow(),
          lastError: reason,
          meta: {
            attempts: retentionAttempts,
            thresholds: qualityGateThresholds,
            hasTranscriptSignals,
            contentSignalStrength: Number(contentSignalStrength.toFixed(4)),
            contentFormat: selectedContentFormat,
            targetPlatform: retentionTargetPlatform,
            strategyProfile
          }
        })
        await updateJob(jobId, { status: 'failed', error: `FAILED_QUALITY_GATE: ${reason}` })
        throw new QualityGateError(reason, {
          attempts: retentionAttempts,
          thresholds: qualityGateThresholds,
          hasTranscriptSignals,
          contentSignalStrength: Number(contentSignalStrength.toFixed(4)),
          contentFormat: selectedContentFormat,
          targetPlatform: retentionTargetPlatform,
          strategyProfile
        })
      }
      if (!selectedJudge.passed) {
        let overrideReason = maybeAllowQualityGateOverride({
          judge: selectedJudge,
          thresholds: qualityGateThresholds,
          hasTranscript: hasTranscriptSignals,
          signalStrength: contentSignalStrength
        })
        if (!overrideReason) {
          const rescueHookCandidate = preferredHookCandidate || orderedHookCandidates.find((candidate) => candidate.auditPassed) || initialHook
          const rescueAttempt = buildAttemptSegments('RESCUE_MODE', rescueHookCandidate)
          const rescueRetention = computeRetentionScore(
            rescueAttempt.segments,
            editPlan?.engagementWindows ?? [],
            rescueHookCandidate.score,
            options.autoCaptions,
            {
              removedRanges: editPlan?.removedSegments ?? [],
              patternInterruptCount: rescueAttempt.patternInterruptCount,
              contentFormat: selectedContentFormat,
              targetPlatform: retentionTargetPlatform
            }
          )
          const rescueThresholds = normalizeQualityGateThresholds({
            hook_strength: clamp(
              qualityGateThresholds.hook_strength - 10,
              QUALITY_GATE_THRESHOLD_FLOORS.hook_strength,
              qualityGateThresholds.hook_strength
            ),
            emotional_pull: clamp(
              qualityGateThresholds.emotional_pull - 10,
              QUALITY_GATE_THRESHOLD_FLOORS.emotional_pull,
              qualityGateThresholds.emotional_pull
            ),
            pacing_score: clamp(
              qualityGateThresholds.pacing_score - 8,
              QUALITY_GATE_THRESHOLD_FLOORS.pacing_score,
              qualityGateThresholds.pacing_score
            ),
            retention_score: clamp(
              qualityGateThresholds.retention_score - 10,
              QUALITY_GATE_THRESHOLD_FLOORS.retention_score,
              qualityGateThresholds.retention_score
            )
          })
          const rescueClarityPenalty = rescueHookCandidate.auditPassed
            ? 0.1
            : hasTranscriptSignals
              ? 0.26
              : 0.18
          const rescueJudge = buildRetentionJudgeReport({
            retentionScore: rescueRetention,
            hook: rescueHookCandidate,
            windows: editPlan?.engagementWindows ?? [],
            clarityPenalty: rescueClarityPenalty,
            captionsEnabled: options.autoCaptions,
            patternInterruptCount: rescueAttempt.patternInterruptCount,
            removedRanges: editPlan?.removedSegments ?? [],
            segments: rescueAttempt.segments,
            thresholds: rescueThresholds,
            contentFormat: selectedContentFormat,
            targetPlatform: retentionTargetPlatform,
            strategyProfile
          })
          retentionAttempts.push({
            attempt: retentionAttempts.length + 1,
            strategy: 'RESCUE_MODE',
            judge: rescueJudge,
            hook: rescueHookCandidate,
            patternInterruptCount: rescueAttempt.patternInterruptCount,
            patternInterruptDensity: rescueAttempt.patternInterruptDensity,
            boredomRemovalRatio: rescueRetention.details.boredomRemovalRatio
          })
          finalSegments = rescueAttempt.segments
          selectedHook = rescueHookCandidate
          selectedJudge = rescueJudge
          retentionScore = rescueJudge.retention_score
          selectedPatternInterruptCount = rescueAttempt.patternInterruptCount
          selectedPatternInterruptDensity = rescueAttempt.patternInterruptDensity
          selectedBoredomRemovalRatio = rescueRetention.details.boredomRemovalRatio
          selectedAutoEscalationEvents = rescueAttempt.autoEscalationEvents
          selectedStrategy = 'RESCUE_MODE'
          selectedStoryReorderMap = finalSegments.map((segment, orderedIndex) => ({
            sourceStart: Number(segment.start.toFixed(3)),
            sourceEnd: Number(segment.end.toFixed(3)),
            orderedIndex
          }))
          optimizationNotes.push(
            'Applied rescue edit pass for low-signal footage (harder cuts, stronger pacing, extra interrupts).'
          )
          if (rescueJudge.passed) {
            overrideReason = 'Rescue edit pass raised quality enough to render.'
          } else if (shouldForceRescueRender(rescueJudge)) {
            overrideReason = 'Rescue render forced at adaptive floor to avoid hard-failing low-signal uploads.'
          }
        }
        if (overrideReason) {
          qualityGateOverride = { applied: true, reason: overrideReason }
          optimizationNotes.push(overrideReason)
        } else {
          const forcedReason = 'Forced render fallback: quality gate did not pass, but rescue edit was produced to avoid upload failure.'
          qualityGateOverride = { applied: true, reason: forcedReason }
          optimizationNotes.push(forcedReason)
        }
      }

      const baselineWindows = editPlan?.engagementWindows ?? engagementWindowsForAnalysis
      if (baselineWindows.length && durationSeconds > 0) {
        const openingHookScore = averageWindowMetric(
          baselineWindows,
          0,
          Math.min(durationSeconds, HOOK_MAX),
          (window) => window.hookScore ?? window.score
        )
        const baselineHookScore = clamp01(
          Number.isFinite(openingHookScore) && openingHookScore > 0
            ? openingHookScore
            : Number(selectedHook?.score ?? initialHook?.score ?? 0.5)
        )
        const sourceBaselineRetention = computeRetentionScore(
          [{ start: 0, end: durationSeconds, speed: 1 }],
          baselineWindows,
          baselineHookScore,
          options.autoCaptions,
          {
            removedRanges: [],
            patternInterruptCount: 0,
            contentFormat: selectedContentFormat,
            targetPlatform: retentionTargetPlatform
          }
        )
        retentionScoreBeforeEdit = sourceBaselineRetention.score
      }
      retentionScoreAfterEdit = Number.isFinite(Number(retentionScore))
        ? Number(retentionScore)
        : Number.isFinite(Number(selectedJudge?.retention_score))
          ? Number(selectedJudge?.retention_score)
          : null
      if (
        Number.isFinite(Number(retentionScoreBeforeEdit)) &&
        Number.isFinite(Number(retentionScoreAfterEdit)) &&
        Number(retentionScoreAfterEdit) < Number(retentionScoreBeforeEdit)
      ) {
        optimizationNotes.push(
          `Retention model warning: edit scored ${Number(retentionScoreAfterEdit)} vs source baseline ${Number(retentionScoreBeforeEdit)}.`
        )
      }

      await updatePipelineStepState(jobId, 'STORY_QUALITY_GATE', {
        status: 'completed',
        completedAt: toIsoNow(),
        meta: {
          attemptCount: retentionAttempts.length,
          selectedStrategy,
          selectedJudge,
          attempts: retentionAttempts,
          thresholds: qualityGateThresholds,
          hasTranscriptSignals,
          contentSignalStrength: Number(contentSignalStrength.toFixed(4)),
          contentFormat: selectedContentFormat,
          targetPlatform: retentionTargetPlatform,
          strategyProfile,
          qualityGateOverride
        }
      })
      await updatePipelineStepState(jobId, 'RETENTION_SCORE', {
        status: 'completed',
        completedAt: toIsoNow(),
        meta: {
          score: retentionScore,
          judge: selectedJudge,
          attempts: retentionAttempts,
          thresholds: qualityGateThresholds,
          hasTranscriptSignals,
          contentSignalStrength: Number(contentSignalStrength.toFixed(4)),
          contentFormat: selectedContentFormat,
          targetPlatform: retentionTargetPlatform,
          strategyProfile,
          qualityGateOverride
        }
      })

      // Enforce zoom duration cap: never exceed ZOOM_MAX_DURATION_RATIO of total duration.
      try {
        const totalDuration = durationSeconds || 0
        const zoomSegments = finalSegments.filter((s) => (s.zoom ?? 0) > 0)
        const zoomDuration = zoomSegments.reduce((sum, s) => sum + Math.max(0, s.end - s.start), 0)
        const maxZoomAllowed = Math.max(0, ZOOM_MAX_DURATION_RATIO * totalDuration)
        if (zoomDuration > maxZoomAllowed && zoomSegments.length) {
          // Sort by emphasize/score (segments with emphasize should keep zoom first), otherwise by length
          const prioritized = finalSegments
            .map((s) => ({ seg: s, score: (s as any).emphasize ? 2 : 0, len: s.end - s.start }))
            .sort((a, b) => b.score - a.score || b.len - a.len)
          let running = 0
          for (const entry of prioritized) {
            const s = entry.seg
            const segLen = Math.max(0, s.end - s.start)
            if ((s.zoom ?? 0) > 0) {
              if (running + segLen <= maxZoomAllowed) {
                running += segLen
                continue
              }
              // remove zoom from lower priority segments until under cap
              s.zoom = 0
            }
          }
        }
      } catch (e) {
        // non-fatal
        console.warn('zoom-cap enforcement failed', e)
      }

      if (options.autoCaptions) {
        await updateJob(jobId, { status: 'subtitling', progress: 62 })
        subtitlePath = await generateSubtitles(tmpIn, workDir)
        if (!subtitlePath) {
          optimizationNotes.push('Auto subtitles skipped: no caption engine available.')
        }
      }

      await updateJob(jobId, { status: 'audio', progress: 68 })

      const hasAudio = hasAudioStream(tmpIn)
      audioProfileForAnalysis = hasAudio ? probeAudioStream(tmpIn) : null
      const withAudio = true
      if (withAudio && finalSegments.length) {
        finalSegments = applyAudioPolishToSegments({
          segments: finalSegments,
          windows: editPlan?.engagementWindows ?? [],
          styleProfile: styleProfileForAnalysis,
          aggressionLevel,
          musicDuck: options.musicDuck
        })
      }
      const audioFilters = withAudio
        ? buildAudioFilters({
            aggressionLevel,
            styleProfile: styleProfileForAnalysis,
            audioProfile: audioProfileForAnalysis
          })
        : []
      audioFiltersForAnalysis = audioFilters.slice()
      await updateJob(jobId, { status: 'retention', progress: 72 })

      const plannedSegmentCount = finalSegments.length
      finalSegments = prepareSegmentsForRender(finalSegments, durationSeconds)
      if (finalSegments.length !== plannedSegmentCount) {
        optimizationNotes.push(
          `Render stabilization adjusted segments (${plannedSegmentCount} -> ${finalSegments.length}) for long-form reliability.`
        )
      }
      const impactBeforeRescue = computeEditImpactRatio(finalSegments, durationSeconds)
      const minImpact = durationSeconds >= LONG_FORM_RESCUE_MIN_DURATION
        ? MIN_EDIT_IMPACT_RATIO_LONG
        : MIN_EDIT_IMPACT_RATIO_SHORT
      if (impactBeforeRescue < minImpact) {
        const rescuedSegments = prepareSegmentsForRender(
          buildGuaranteedFallbackSegments(durationSeconds, options),
          durationSeconds
        )
        if (rescuedSegments.length) {
          finalSegments = rescuedSegments
          const impactAfterRescue = computeEditImpactRatio(finalSegments, durationSeconds)
          optimizationNotes.push(
            `Edit impact rescue applied (${(impactBeforeRescue * 100).toFixed(1)}% -> ${(impactAfterRescue * 100).toFixed(1)}%).`
          )
        }
      }
      if (!finalSegments.length) {
        await updateJob(jobId, { status: 'failed', error: 'no_renderable_segments' })
        throw new Error('no_renderable_segments')
      }
      finalSegmentsForAnalysis = finalSegments.map((segment) => ({ ...segment }))
      if (selectedHook && finalSegmentsForAnalysis.length) {
        const styleName = behaviorStyleProfileForAnalysis?.styleName || `${strategyProfile}_adaptive_v1`
        const resolvedBlendForDecision = styleArchetypeBlendForAnalysis || behaviorStyleProfileForAnalysis?.archetypeBlend || null
        editDecisionTimelineForAnalysis = buildEditDecisionTimeline({
          styleName,
          hook: { start: selectedHook.start },
          segments: finalSegmentsForAnalysis,
          patternInterruptCount: selectedPatternInterruptCount,
          autoEscalationEvents: selectedAutoEscalationEvents,
          includeBrollMarkers: Boolean(
            resolvedBlendForDecision &&
            Number(resolvedBlendForDecision.cinematic_lifestyle_archive) >= 0.24
          )
        })
        styleFeatureSnapshotForAnalysis = extractTimelineFeatures({
          timeline: editDecisionTimelineForAnalysis,
          durationSeconds,
          energySamples: energySamplesForEscalation
        })
      }
      autoEscalationEventsForAnalysis = selectedAutoEscalationEvents
      if (behaviorStyleProfileForAnalysis && !styleArchetypeBlendForAnalysis) {
        styleArchetypeBlendForAnalysis = behaviorStyleProfileForAnalysis.archetypeBlend
      }
      if (options.autoCaptions && subtitlePath) {
        const sourceCues = parseTranscriptCues(subtitlePath)
        const remappedCues = remapTranscriptCuesToEditedTimeline(sourceCues, finalSegments)
        if (!remappedCues.length) {
          subtitlePath = null
          subtitleIsAss = false
          optimizationNotes.push('Auto subtitles skipped: no transcript overlap after edits.')
        } else {
          const remappedSubtitlePath = path.join(
            workDir,
            `${path.basename(subtitlePath, path.extname(subtitlePath))}-edited.srt`
          )
          const writtenSubtitlePath = writeTranscriptCuesToSrt(remappedCues, remappedSubtitlePath)
          if (writtenSubtitlePath) {
            subtitlePath = writtenSubtitlePath
          }
          if (normalizedSubtitle === 'mrbeast_animated') {
            const assPath = buildMrBeastAnimatedAss({
              srtPath: subtitlePath,
              workingDir: workDir,
              style: subtitleStyle
            })
            if (assPath) {
              subtitlePath = assPath
              subtitleIsAss = true
              optimizationNotes.push('Applied high-energy animated caption style.')
            } else {
              subtitleIsAss = false
              optimizationNotes.push('Animated caption fallback applied: using static caption styling.')
            }
          } else {
            subtitleIsAss = false
          }
        }
      }

      await updatePipelineStepState(jobId, 'RENDER_FINAL', {
        status: 'running',
        attempts: 1,
        startedAt: toIsoNow(),
        lastError: null
      })
      await updateJob(jobId, { status: 'rendering', progress: 80 })

      const hasSegments = finalSegments.length >= 1
      const argsBase = [
        '-y',
        '-nostdin',
        '-hide_banner',
        '-loglevel',
        'error',
        '-filter_threads',
        String(RENDER_FILTER_THREADS),
        '-i',
        tmpIn,
        '-movflags',
        '+faststart',
        '-c:v',
        'libx264',
        '-preset',
        ffPreset,
        '-crf',
        ffCrf,
        '-threads',
        '0',
        '-pix_fmt',
        'yuv420p'
      ]
      if (withAudio) {
        argsBase.push('-c:a', 'aac', '-b:a', ffAudioBitrate, '-ar', ffAudioSampleRate, '-ac', '2')
      }

      const watermarkFont = getSystemFontFile()
      const watermarkFontArg = watermarkFont ? `:fontfile=${escapeFilterPath(watermarkFont)}` : ''

      // Prefer an image watermark if available (uses favicon from frontend/public),
      // otherwise fall back to a subtle text watermark. The image will be overlaid
      // at bottom-right with a small inset.
      const defaultWatermarkImage = path.join(process.cwd(), 'frontend', 'public', 'favicon-32x32.png')
      const watermarkImagePath = process.env.WATERMARK_IMAGE_PATH || defaultWatermarkImage
      const watermarkImageExists = watermarkEnabled && fs.existsSync(watermarkImagePath)
      const watermarkFilter = watermarkImageExists
        ? `[outv][1:v]overlay=x=main_w-overlay_w-12:y=main_h-overlay_h-12:format=auto`
        : watermarkEnabled
        ? `drawtext=text='AutoEditor'${watermarkFontArg}:x=w-tw-12:y=h-th-12:fontsize=18:fontcolor=white@0.45:box=1:boxcolor=black@0.25:boxborderw=6`
        : ''
      const subtitleFilter = subtitlePath
        ? (
          subtitleIsAss
            ? `subtitles=${escapeFilterPath(subtitlePath)}`
            : `subtitles=${escapeFilterPath(subtitlePath)}:force_style='${buildSubtitleStyle(subtitleStyle)}'`
        )
        : ''

      const probe = sourceProbe
      if (probe && finalSegments.length) {
        finalSegments.forEach((seg, idx) => {
          console.log(
            `[${requestId || 'noid'}] segment ${idx} ${seg.start}-${seg.end} width=${probe.width} height=${probe.height} sar=${probe.sampleAspectRatio} fps=${probe.frameRate}`
          )
        })
      } else if (!probe) {
        console.warn(`[${requestId || 'noid'}] segment preflight ffprobe unavailable`)
      }

      const logFfmpegFailure = (label: string, args: string[], err: any) => {
        const stderr = typeof err?.stderr === 'string' ? err.stderr : ''
        console.error(`[${requestId || 'noid'}] ffmpeg ${label} failed`, {
          cmd: formatFfmpegCommand(args),
          stderr
        })
      }

      const summarizeFfmpegError = (err: any) => {
        const stderr = typeof err?.stderr === 'string' ? err.stderr.trim() : ''
        const message = err?.message ? String(err.message) : 'ffmpeg_failed'
        if (!stderr) return message
        const trimmed = stderr.split(/\r?\n/).filter(Boolean).slice(-2).join(' | ')
        const combined = `${message}: ${trimmed}`
        return combined.length > 200 ? combined.slice(0, 200) : combined
      }

      const runFfmpegWithFilter = async (
        argsPrefix: string[],
        filter: string,
        mapArgs: string[],
        outputPath: string,
        label: string
      ) => {
        const args = [...argsPrefix]
        let filterScriptPath: string | null = null
        if (filter.length > FILTER_COMPLEX_SCRIPT_THRESHOLD) {
          filterScriptPath = path.join(workDir, `filter-${label}-${Date.now()}-${crypto.randomUUID().slice(0, 8)}.txt`)
          fs.writeFileSync(filterScriptPath, filter)
          args.push('-filter_complex_script', filterScriptPath)
        } else {
          args.push('-filter_complex', filter)
        }
        args.push(...mapArgs, outputPath)
        try {
          await runFfmpeg(args)
        } catch (err) {
          logFfmpegFailure(label, args, err)
          throw err
        } finally {
          safeUnlink(filterScriptPath)
        }
      }

      const runSegmentFileFallback = async (segments: Segment[]) => {
        const segmentFiles: string[] = []
        const concatListPath = path.join(workDir, `segment-list-${Date.now()}-${crypto.randomUUID().slice(0, 8)}.txt`)
        try {
          for (let idx = 0; idx < segments.length; idx += 1) {
            const seg = segments[idx]
            const speed = seg.speed && seg.speed > 0 ? seg.speed : 1
            const vTrim = `trim=start=${toFilterNumber(seg.start)}:end=${toFilterNumber(seg.end)}`
            const vSpeed = speed !== 1 ? `setpts=(PTS-STARTPTS)/${toFilterNumber(speed)}` : 'setpts=PTS-STARTPTS'
            const vChain = `[0:v]${vTrim},${vSpeed},${buildFrameFitFilter(horizontalFit, target.width, target.height)}[vout]`
            const filterParts = [vChain]
            if (withAudio) {
              const segDuration = Math.max(0.01, roundForFilter((seg.end - seg.start) / speed))
              const aSpeed = speed !== 1 ? buildAtempoChain(speed) : ''
              const gain = Number.isFinite(seg.audioGain) ? clamp(Number(seg.audioGain), 0.8, 1.24) : 1
              const aGain = Math.abs(gain - 1) >= 0.01 ? `volume=${toFilterNumber(gain)}` : ''
              if (hasAudio) {
                const aChain = [
                  `[0:a]atrim=start=${toFilterNumber(Math.max(0, seg.start - 0.02))}:end=${toFilterNumber(seg.end + 0.02)}`,
                  'asetpts=PTS-STARTPTS',
                  aSpeed,
                  aGain,
                  'aformat=sample_rates=48000:channel_layouts=stereo[aout]'
                ].filter(Boolean).join(',')
                filterParts.push(aChain)
              } else {
                const aChain = [
                  'anullsrc=r=48000:cl=stereo',
                  `atrim=duration=${toFilterNumber(segDuration)}`,
                  'asetpts=PTS-STARTPTS',
                  aSpeed,
                  aGain,
                  'aformat=sample_rates=48000:channel_layouts=stereo[aout]'
                ].filter(Boolean).join(',')
                filterParts.push(aChain)
              }
            }
            const segmentFilter = filterParts.join(';')
            const segmentPath = path.join(workDir, `segment-${String(idx).padStart(4, '0')}.mp4`)
            const mapArgs = ['-map', '[vout]']
            if (withAudio) mapArgs.push('-map', '[aout]')
            await runFfmpegWithFilter(argsBase, segmentFilter, mapArgs, segmentPath, `segment-${idx}`)
            segmentFiles.push(segmentPath)
          }

          if (!segmentFiles.length) {
            throw new Error('segment_file_fallback_empty')
          }

          const concatLines = segmentFiles
            .map((filePath) => `file '${filePath.replace(/'/g, "'\\''")}'`)
            .join('\n')
          fs.writeFileSync(concatListPath, concatLines)

          const concatCopyArgs = [
            '-y',
            '-nostdin',
            '-hide_banner',
            '-loglevel',
            'error',
            '-f',
            'concat',
            '-safe',
            '0',
            '-i',
            concatListPath,
            '-c',
            'copy',
            '-movflags',
            '+faststart',
            tmpOut
          ]
          try {
            await runFfmpeg(concatCopyArgs)
          } catch (concatCopyErr) {
            logFfmpegFailure('segment-concat-copy', concatCopyArgs, concatCopyErr)
            const concatEncodeArgs = [
              '-y',
              '-nostdin',
              '-hide_banner',
              '-loglevel',
              'error',
              '-f',
              'concat',
              '-safe',
              '0',
              '-i',
              concatListPath,
              '-movflags',
              '+faststart',
              '-c:v',
              'libx264',
              '-preset',
              ffPreset,
              '-crf',
              ffCrf,
              '-threads',
              '0',
              '-pix_fmt',
              'yuv420p'
            ]
            if (withAudio) {
              concatEncodeArgs.push('-c:a', 'aac', '-b:a', ffAudioBitrate, '-ar', ffAudioSampleRate, '-ac', '2')
            }
            concatEncodeArgs.push(tmpOut)
            await runFfmpeg(concatEncodeArgs)
          }

          const shouldPostProcessSubtitle = Boolean(subtitleFilter && subtitlePath)
          const shouldPostProcessAudio = withAudio && audioFilters.length > 0
          if (shouldPostProcessSubtitle || shouldPostProcessAudio) {
            const postProcessArgs = [
              '-y',
              '-nostdin',
              '-hide_banner',
              '-loglevel',
              'error',
              '-i',
              tmpOut,
              '-movflags',
              '+faststart'
            ]
            if (shouldPostProcessSubtitle && subtitlePath) {
              postProcessArgs.push(
                '-c:v',
                'libx264',
                '-preset',
                ffPreset,
                '-crf',
                ffCrf,
                '-threads',
                '0',
                '-pix_fmt',
                'yuv420p',
                '-vf',
                subtitleIsAss
                  ? `subtitles=${escapeFilterPath(subtitlePath)}`
                  : `subtitles=${escapeFilterPath(subtitlePath)}:force_style='${buildSubtitleStyle(subtitleStyle)}'`
              )
            } else {
              postProcessArgs.push('-c:v', 'copy')
            }
            if (withAudio) {
              if (shouldPostProcessAudio) {
                postProcessArgs.push('-af', audioFilters.join(','))
              }
              postProcessArgs.push('-c:a', 'aac', '-b:a', ffAudioBitrate, '-ar', ffAudioSampleRate, '-ac', '2')
            } else {
              postProcessArgs.push('-an')
            }
            const postProcessPath = path.join(
              workDir,
              `segment-fallback-post-${Date.now()}-${crypto.randomUUID().slice(0, 8)}.mp4`
            )
            postProcessArgs.push(postProcessPath)
            try {
              await runFfmpeg(postProcessArgs)
              safeUnlink(tmpOut)
              fs.renameSync(postProcessPath, tmpOut)
            } catch (postProcessErr) {
              logFfmpegFailure('segment-postprocess', postProcessArgs, postProcessErr)
              safeUnlink(postProcessPath)
              const reason = summarizeFfmpegError(postProcessErr)
              if (shouldPostProcessSubtitle) {
                optimizationNotes.push(`Render fallback: subtitles could not be burned (${reason}).`)
              }
              if (shouldPostProcessAudio) {
                optimizationNotes.push(`Render fallback: audio polish could not be applied (${reason}).`)
              }
            }
          }
        } finally {
          safeUnlink(concatListPath)
          for (const filePath of segmentFiles) safeUnlink(filePath)
        }
      }

      try {
        if (hasSegments) {
          const fullVideoChain = [subtitleFilter, watermarkFilter].filter(Boolean).join(',')
          // If using an image watermark we must add the watermark file as a second input
          // so ffmpeg can reference it as input index 1 in the overlay filter.
          const argsWithWatermark = [...argsBase]
          if (watermarkImageExists) argsWithWatermark.push('-i', watermarkImagePath)
          const candidateVideoChains = [fullVideoChain]
          if (subtitleFilter && watermarkFilter) candidateVideoChains.push(subtitleFilter)
          if (!subtitleFilter && watermarkFilter) candidateVideoChains.push(watermarkFilter)
          if (!subtitleFilter || !options.autoCaptions) candidateVideoChains.push('')
          const videoChains = candidateVideoChains.filter((value, idx, arr) => arr.indexOf(value) === idx)
          if (!videoChains.length) videoChains.push('')
          const describeVideoChainFallback = (videoChain: string) => {
            if (!videoChain) return 'without subtitles/watermark'
            if (videoChain === subtitleFilter) return 'without watermark'
            if (videoChain === watermarkFilter) return 'without subtitles'
            return 'with reduced overlays'
          }

          const runWithChain = async (videoChain: string, enableFades: boolean) => {
            const concatFilter = buildConcatFilter(finalSegments, {
              withAudio,
              hasAudioStream: hasAudio,
              targetWidth: target.width,
              targetHeight: target.height,
              fit: horizontalFit,
              enableFades
            })
            const filterParts: string[] = [concatFilter]
            if (videoChain) {
              filterParts.push(`[outv]${videoChain}[vout]`)
            }
            if (withAudio && audioFilters.length > 0) {
              filterParts.push(`[outa]${audioFilters.join(',')}[aout]`)
            }
            const filter = filterParts.join(';')
            const videoMap = videoChain ? '[vout]' : '[outv]'
            const audioMap = withAudio ? (audioFilters.length > 0 ? '[aout]' : '[outa]') : null
            const mapArgs = ['-map', videoMap]
            if (audioMap) mapArgs.push('-map', audioMap)
            await runFfmpegWithFilter(argsWithWatermark, filter, mapArgs, tmpOut, 'concat')
          }

          let lastErr: any = null
          let ran = false
          for (const chain of videoChains) {
            const fadeVariants = options.transitions ? [true, false] : [false]
            for (const enableFades of fadeVariants) {
              try {
                await runWithChain(chain, enableFades)
                ran = true
                if (chain !== fullVideoChain) {
                  const reason = lastErr ? summarizeFfmpegError(lastErr) : 'ffmpeg_failed'
                  optimizationNotes.push(`Render fallback: ${describeVideoChainFallback(chain)} (${reason}).`)
                }
                if (options.transitions && !enableFades) {
                  const reason = lastErr ? summarizeFfmpegError(lastErr) : 'stitch_filter_failed'
                  optimizationNotes.push(`Render fallback: stitch transitions disabled (${reason}).`)
                }
                break
              } catch (err) {
                lastErr = err
              }
            }
            if (ran) break
          }
          if (!ran) {
            const reason = summarizeFfmpegError(lastErr)
            try {
              await runSegmentFileFallback(finalSegments)
              ran = true
              optimizationNotes.push(`Render fallback: segment-file stitch used (${reason}).`)
            } catch (segmentFallbackErr) {
              lastErr = segmentFallbackErr
            }
            if (!ran) {
              const finalReason = summarizeFfmpegError(lastErr)
              throw new Error(`edited_render_failed:${finalReason}`)
            }
          }
        } else {
          const fallbackArgs = [
            ...argsBase,
            '-vf',
            buildFrameFitFilter(horizontalFit, target.width, target.height),
            tmpOut
          ]
          try {
            await runFfmpeg(fallbackArgs)
          } catch (err) {
            logFfmpegFailure('single', fallbackArgs, err)
            throw err
          }
        }
        processed = true
      } catch (err) {
        processed = false
        const reason = summarizeFfmpegError(err)
        if (hasSegments && finalSegments.length) {
          const emergencySegments = prepareSegmentsForRender(
            finalSegments.map((segment) => ({ ...segment, speed: 1, zoom: 0, brightness: 0 })),
            durationSeconds
          )
          if (emergencySegments.length) {
            const emergencyFilter = buildConcatFilter(emergencySegments, {
              withAudio,
              hasAudioStream: hasAudio,
              targetWidth: target.width,
              targetHeight: target.height,
              fit: horizontalFit,
              enableFades: false
            })
            const emergencyMapArgs = ['-map', '[outv]']
            if (withAudio) emergencyMapArgs.push('-map', '[outa]')
            try {
              await runFfmpegWithFilter(argsBase, emergencyFilter, emergencyMapArgs, tmpOut, 'emergency-edited')
              processed = true
              optimizationNotes.push(`Emergency edited fallback render used (${reason}).`)
            } catch {
              throw err
            }
          }
        }
        if (!processed) throw err
      }
    }

    if (!processed) {
      throw new Error('render_failed')
    }

    if (!fs.existsSync(tmpOut)) {
      await updateJob(jobId, { status: 'failed', error: 'output_file_missing_after_render' })
      throw new Error('output_file_missing_after_render')
    }
    const tmpOutStats = fs.statSync(tmpOut)
    if (!tmpOutStats.isFile() || tmpOutStats.size <= 0) {
      await updateJob(jobId, { status: 'failed', error: 'output_file_empty_after_render' })
      throw new Error('output_file_empty_after_render')
    }

    throwIfCanceled()
    await updateJob(jobId, { progress: 95 })

    const outputPaths: string[] = []
    const localOutDir = path.join(process.cwd(), 'outputs', job.userId, jobId)
    fs.mkdirSync(localOutDir, { recursive: true })
    const outPath = `${job.userId}/${jobId}/output.mp4`
    const localOutPath = path.join(localOutDir, 'output.mp4')
    fs.copyFileSync(tmpOut, localOutPath)
    console.log(`[${requestId || 'noid'}] local output saved ${path.resolve(localOutPath)} (${tmpOutStats.size} bytes)`)
    try {
      await uploadFileToOutput({ key: outPath, filePath: tmpOut, contentType: 'video/mp4' })
    } catch (error) {
      outputUploadFallbackUsed = true
      failedOutputUploads.push(outPath)
      console.warn(`[${requestId || 'noid'}] output upload failed, serving local fallback`, {
        key: outPath,
        error: (error as any)?.message || error
      })
    }
    outputPaths.push(outPath)
    throwIfCanceled()

    if (!outputPaths.length) {
      await updateJob(jobId, { status: 'failed', error: 'output_upload_missing' })
      throw new Error('output_upload_missing')
    }

    const retentionMetadataSummary = buildRetentionMetadataSummary({
      durationSeconds,
      segments: finalSegmentsForAnalysis.length
        ? finalSegmentsForAnalysis
        : selectedStoryReorderMap.map((entry) => ({
            start: entry.sourceStart,
            end: entry.sourceEnd,
            speed: 1
          })),
      windows: engagementWindowsForAnalysis,
      hook: selectedHook,
      styleProfile: styleProfileForAnalysis,
      nicheProfile: nicheProfileForAnalysis,
      styleArchetypeBlend: styleArchetypeBlendForAnalysis,
      behaviorStyleProfile: behaviorStyleProfileForAnalysis,
      styleFeatureSnapshot: styleFeatureSnapshotForAnalysis,
      autoEscalationEvents: autoEscalationEventsForAnalysis,
      judge: selectedJudge,
      strategy: selectedStrategy,
      retentionScore,
      retentionScoreBefore: retentionScoreBeforeEdit,
      retentionScoreAfter: retentionScoreAfterEdit,
      attempts: retentionAttempts,
      patternInterruptCount: selectedPatternInterruptCount,
      patternInterruptDensity: selectedPatternInterruptDensity,
      boredomRemovedRatio: selectedBoredomRemovalRatio,
      qualityGateOverride,
      optimizationNotes,
      hookSelectionSource: selectedHookSelectionSource,
      contentFormat: selectedContentFormat,
      targetPlatform: retentionTargetPlatform,
      strategyProfile
    })

    const nextAnalysis = buildPersistedRenderAnalysis({
      existing: {
        ...((job.analysis as any) || {}),
        metadata_version: 2,
        metadata_summary: retentionMetadataSummary,
        hook_start_time: selectedHook?.start ?? (job.analysis as any)?.hook_start_time ?? null,
        hook_end_time: selectedHook ? selectedHook.start + selectedHook.duration : (job.analysis as any)?.hook_end_time ?? null,
        hook_score: selectedHook?.score ?? (job.analysis as any)?.hook_score ?? null,
        hook_audit_score: selectedHook?.auditScore ?? (job.analysis as any)?.hook_audit_score ?? null,
        hook_text: selectedHook?.text ?? (job.analysis as any)?.hook_text ?? null,
        hook_reason: selectedHook?.reason ?? (job.analysis as any)?.hook_reason ?? null,
        hook_synthetic: selectedHook?.synthetic ?? (job.analysis as any)?.hook_synthetic ?? false,
        hook_selection_source: selectedHookSelectionSource,
        selected_strategy: selectedStrategy,
        retention_attempts: retentionAttempts,
        retention_judge: selectedJudge,
        quality_gate_thresholds: selectedJudge?.applied_thresholds ?? null,
        quality_gate_override: qualityGateOverride,
        retention_score_before: retentionScoreBeforeEdit,
        retention_score_after: retentionScoreAfterEdit,
        pattern_interrupt_count: selectedPatternInterruptCount || (job.analysis as any)?.pattern_interrupt_count || 0,
        pattern_interrupt_density: selectedPatternInterruptDensity || (job.analysis as any)?.pattern_interrupt_density || 0,
        boredom_removed_ratio: selectedBoredomRemovalRatio || (job.analysis as any)?.boredom_removed_ratio || 0,
        story_reorder_map: selectedStoryReorderMap,
        style_profile: styleProfileForAnalysis,
        niche_profile: nicheProfileForAnalysis,
        style_archetype_blend: styleArchetypeBlendForAnalysis,
        behavior_style_profile: behaviorStyleProfileForAnalysis,
        edit_decision_timeline: editDecisionTimelineForAnalysis,
        style_timeline_features: styleFeatureSnapshotForAnalysis,
        auto_escalation_events: autoEscalationEventsForAnalysis,
        auto_escalation_count: Number(autoEscalationEventsForAnalysis.length || 0),
        beat_anchors: beatAnchorsForAnalysis,
        emotional_beat_anchors: emotionalBeatAnchorsForAnalysis,
        emotional_beat_cut_count: emotionalBeatCutCountForAnalysis,
        emotional_lead_trimmed_seconds: emotionalLeadTrimmedSecondsForAnalysis,
        emotional_tuning_profile: emotionalTuningForAnalysis,
        hook_variants: hookVariantsForAnalysis,
        hook_calibration: hookCalibrationForAnalysis,
        audio_profile: audioProfileForAnalysis
          ? {
              channels: audioProfileForAnalysis.channels,
              channelLayout: audioProfileForAnalysis.channelLayout,
              sampleRate: audioProfileForAnalysis.sampleRate,
              bitRate: audioProfileForAnalysis.bitRate
            }
          : null,
        audio_polish_chain: audioFiltersForAnalysis,
        output_upload_fallback: outputUploadFallbackUsed
          ? {
              used: true,
              failedOutputs: failedOutputUploads,
              mode: 'local',
              updatedAt: toIsoNow()
            }
          : ((job.analysis as any)?.output_upload_fallback ?? null),
        content_signal_strength: Number(contentSignalStrength.toFixed(4)),
        has_transcript_signals: hasTranscriptSignals,
        retentionAggressionLevel: aggressionLevel,
        retentionLevel: aggressionLevel,
        retentionStrategyProfile: strategyProfile,
        retentionStrategy: strategyProfile,
        retentionTargetPlatform: retentionTargetPlatform,
        retention_target_platform: retentionTargetPlatform,
        retentionPlatform: retentionTargetPlatform,
        targetPlatform: retentionTargetPlatform,
        platform: retentionTargetPlatform,
        platformProfile: platformProfileId,
        platform_profile: platformProfileId,
        retentionContentFormat: selectedContentFormat,
        retention_content_format: selectedContentFormat,
        retention_runtime_profile: {
          strategy: strategyProfile,
          aggression: aggressionLevel,
          isLongForm: runtimeRetentionProfile.isLongForm,
          isVerticalShortForm: runtimeRetentionProfile.isVerticalShortForm,
          notes: runtimeRetentionNotes
        }
      },
      renderConfig,
      retentionTargetPlatform: retentionTargetPlatform,
      platformProfile: platformProfileId,
      onlyCuts: options.onlyCuts,
      outputPaths
    })

    throwIfCanceled()
    await updatePipelineStepState(jobId, 'RENDER_FINAL', {
      status: 'completed',
      completedAt: toIsoNow(),
      meta: { outputPaths }
    })
    await updateJob(jobId, {
      status: 'completed',
      progress: 100,
      outputPath: outputPaths[0],
      finalQuality,
      watermarkApplied: watermarkEnabled,
      retentionScore,
      optimizationNotes: optimizationNotes.length ? optimizationNotes : null,
      renderSettings: buildPersistedRenderSettings(renderConfig, {
        retentionAggressionLevel: aggressionLevel,
        retentionStrategyProfile: strategyProfile,
        retentionTargetPlatform: retentionTargetPlatform,
        platformProfile: platformProfileId,
        onlyCuts: options.onlyCuts
      }),
      analysis: nextAnalysis
    })

    const monthKey = getMonthKey()
    await incrementUsageForMonth(user.id, monthKey, 1, durationMinutes)
    await incrementRenderUsage(user.id, monthKey, 1)
    console.log(`[${requestId || 'noid'}] process complete ${jobId}`)
  } finally {
    safeUnlink(tmpIn)
    safeUnlink(tmpOut)
    safeUnlink(subtitlePath)
  }
}

const runPipeline = async (jobId: string, user: { id: string; email?: string }, requestedQuality?: ExportQuality, requestId?: string) => {
  await pipelineJobContext.run({ jobId }, async () => {
    try {
      const existing = await prisma.job.findUnique({ where: { id: jobId } })
      if (!existing) return
      const status = String(existing.status || '').toLowerCase()
      if (status === 'completed' || status === 'failed') return
      if (isPipelineCanceled(jobId)) throw new JobCanceledError(jobId)
      const progress = Number(existing.progress ?? 0)
      if ((status === 'queued' || status === 'uploading') && (!Number.isFinite(progress) || progress < 1)) {
        console.log(`[${requestId || 'noid'}] skip pipeline ${jobId} (upload not completed yet)`)
        return
      }
      if (!hasFfmpeg()) {
        await updateJob(jobId, { status: 'failed', error: 'ffmpeg_missing' })
        throw new Error('ffmpeg_missing')
      }
      const { options } = await getEditOptionsForUser(user.id, {
        retentionAggressionLevel: getRetentionAggressionFromJob(existing),
        retentionStrategyProfile: getRetentionStrategyFromJob(existing),
        onlyCuts: getOnlyCutsFromJob(existing),
        autoCaptions: getAutoCaptionsFromPayload((existing.analysis as any) || {}),
        subtitleStyle: getSubtitleStyleFromPayload((existing.analysis as any) || {})
      })
      const styleBlendOverride = parseStyleArchetypeBlendFromPayload((existing.analysis as any) || {})
      if (styleBlendOverride) options.styleArchetypeBlend = styleBlendOverride
      if (isPipelineCanceled(jobId)) throw new JobCanceledError(jobId)
      await analyzeJob(jobId, options, requestId)
      if (isPipelineCanceled(jobId)) throw new JobCanceledError(jobId)
      await processJob(jobId, user, requestedQuality, options, requestId)
    } catch (err: any) {
      if (err instanceof PlanLimitError) {
        await updateJob(jobId, { status: 'failed', error: err.code })
        try {
          await updatePipelineStepState(jobId, 'RENDER_FINAL', {
            status: 'failed',
            completedAt: toIsoNow(),
            lastError: truncateErrorText(err.code) || 'plan_limit'
          })
        } catch (e) {
          // ignore
        }
        return
      }
      if (err instanceof HookGateError) {
        try {
          await updatePipelineStepState(jobId, 'HOOK_SELECT_AND_AUDIT', {
            status: 'failed',
            completedAt: toIsoNow(),
            lastError: truncateErrorText(err.reason) || 'FAILED_HOOK',
            meta: err.details ?? null
          })
        } catch (e) {
          // ignore
        }
        await updateJob(jobId, { status: 'failed', error: `FAILED_HOOK: ${err.reason}` })
        return
      }
      if (err instanceof QualityGateError) {
        try {
          await updatePipelineStepState(jobId, 'STORY_QUALITY_GATE', {
            status: 'failed',
            completedAt: toIsoNow(),
            lastError: truncateErrorText(err.reason) || 'FAILED_QUALITY_GATE',
            meta: err.details ?? null
          })
          await updatePipelineStepState(jobId, 'RETENTION_SCORE', {
            status: 'failed',
            completedAt: toIsoNow(),
            lastError: truncateErrorText(err.reason) || 'FAILED_QUALITY_GATE',
            meta: err.details ?? null
          })
        } catch (e) {
          // ignore
        }
        await updateJob(jobId, { status: 'failed', error: `FAILED_QUALITY_GATE: ${err.reason}` })
        return
      }
      if (err instanceof JobCanceledError || isPipelineCanceled(jobId)) {
        try {
          await updateJob(jobId, { status: 'failed', error: 'queue_canceled_by_user' })
          await updatePipelineStepState(jobId, 'RENDER_FINAL', {
            status: 'failed',
            completedAt: toIsoNow(),
            lastError: 'queue_canceled_by_user'
          })
        } catch (e) {
          // ignore cancellation update races
        }
        return
      }
      console.error(`[${requestId || 'noid'}] pipeline error`, err)
      try {
        await updatePipelineStepState(jobId, 'RENDER_FINAL', {
          status: 'failed',
          completedAt: toIsoNow(),
          lastError: truncateErrorText(err?.message || err) || 'pipeline_error'
        })
      } catch (e) {
        // ignore
      }
      await updateJob(jobId, { status: 'failed', error: formatFfmpegFailure(err) })
    } finally {
      killJobFfmpegProcesses(jobId)
      clearPipelineCanceled(jobId)
    }
  })
}

type QueueItem = { jobId: string; user: { id: string; email?: string }; requestedQuality?: ExportQuality; requestId?: string; priorityLevel: number }
const pipelineQueue: QueueItem[] = []
const queuedPipelineJobIds = new Set<string>()
const runningPipelineJobIds = new Set<string>()
let activePipelines = 0
const MAX_PIPELINES = (() => {
  const envVal = Number(process.env.JOB_CONCURRENCY || 0)
  if (envVal && Number.isFinite(envVal) && envVal > 0) return envVal
  const cpus = os.cpus() ? os.cpus().length : 1
  if (cpus <= 1) return 1
  if (cpus === 2) return 2
  if (cpus >= 4) return Math.max(2, Math.min(4, cpus - 1))
  return cpus
})()
const QUEUE_RECOVERY_INTERVAL_MS = (() => {
  const envVal = Number(process.env.JOB_QUEUE_RECOVERY_INTERVAL_MS || 0)
  if (Number.isFinite(envVal) && envVal >= 5000) return envVal
  return 30_000
})()
const STALE_PIPELINE_MS = (() => {
  const envVal = Number(process.env.STALE_PIPELINE_MS || 0)
  if (Number.isFinite(envVal) && envVal >= 60_000) return envVal
  return 90 * 60_000
})()
const STARTABLE_QUEUE_STATUSES = new Set(['queued', 'uploading'])
const STALE_RECOVERABLE_STATUSES = new Set([
  'analyzing',
  'hooking',
  'cutting',
  'pacing',
  'story',
  'subtitling',
  'audio',
  'retention',
  'rendering'
])
const CANCELABLE_PIPELINE_STATUSES = new Set([
  'queued',
  'uploading',
  'analyzing',
  'hooking',
  'cutting',
  'pacing',
  'story',
  'subtitling',
  'audio',
  'retention',
  'rendering'
])
let queueRecoveryRunning = false
let queueRecoveryLoopStarted = false
const DEFAULT_QUEUE_SLOT_SECONDS = (() => {
  const envVal = Number(process.env.JOB_QUEUE_SLOT_SECONDS || 0)
  if (Number.isFinite(envVal) && envVal >= 20) return Math.round(envVal)
  return 210
})()
const RECENT_DURATION_SAMPLE_LIMIT = (() => {
  const envVal = Number(process.env.JOB_QUEUE_DURATION_SAMPLE_LIMIT || 0)
  if (Number.isFinite(envVal) && envVal >= 5) return Math.min(120, Math.round(envVal))
  return 25
})()
const recentPipelineDurationsSeconds: number[] = []

type QueueEtaInfo = {
  queuePosition: number
  queueEtaSeconds: number
  queueDepth: number
  queueSlotSeconds: number
}

const recordPipelineDurationSeconds = (seconds: number) => {
  if (!Number.isFinite(seconds) || seconds <= 0) return
  const bounded = Math.round(clamp(seconds, 15, 10_800))
  recentPipelineDurationsSeconds.push(bounded)
  if (recentPipelineDurationsSeconds.length > RECENT_DURATION_SAMPLE_LIMIT) {
    recentPipelineDurationsSeconds.splice(0, recentPipelineDurationsSeconds.length - RECENT_DURATION_SAMPLE_LIMIT)
  }
}

const getQueueSlotEstimateSeconds = () => {
  if (!recentPipelineDurationsSeconds.length) return DEFAULT_QUEUE_SLOT_SECONDS
  const total = recentPipelineDurationsSeconds.reduce((sum, value) => sum + value, 0)
  const average = total / recentPipelineDurationsSeconds.length
  return Math.round(clamp(average, 20, 10_800))
}

const buildQueueEtaSnapshot = () => {
  const queueDepth = runningPipelineJobIds.size + pipelineQueue.length
  const queueSlotSeconds = getQueueSlotEstimateSeconds()
  const availableNow = Math.max(0, MAX_PIPELINES - activePipelines)
  const byJobId = new Map<string, QueueEtaInfo>()

  for (const runningJobId of runningPipelineJobIds) {
    byJobId.set(runningJobId, {
      queuePosition: 0,
      queueEtaSeconds: 0,
      queueDepth,
      queueSlotSeconds
    })
  }

  for (let index = 0; index < pipelineQueue.length; index += 1) {
    const item = pipelineQueue[index]
    const waitSlots = index < availableNow ? 0 : index - availableNow + 1
    const waitWaves = waitSlots > 0 ? Math.ceil(waitSlots / Math.max(1, MAX_PIPELINES)) : 0
    byJobId.set(item.jobId, {
      queuePosition: index + 1,
      queueEtaSeconds: waitWaves * queueSlotSeconds,
      queueDepth,
      queueSlotSeconds
    })
  }

  return { byJobId, queueDepth, queueSlotSeconds }
}

const removeJobFromQueue = (jobId: string) => {
  if (!jobId) return false
  let removed = false
  for (let index = pipelineQueue.length - 1; index >= 0; index -= 1) {
    if (pipelineQueue[index]?.jobId !== jobId) continue
    pipelineQueue.splice(index, 1)
    removed = true
  }
  queuedPipelineJobIds.delete(jobId)
  return removed
}

const toTimeMs = (value: unknown) => {
  if (!value) return 0
  if (value instanceof Date) return value.getTime()
  const parsed = new Date(String(value)).getTime()
  return Number.isFinite(parsed) ? parsed : 0
}

const processQueue = () => {
  while (activePipelines < MAX_PIPELINES && pipelineQueue.length > 0) {
    const next = pipelineQueue.shift()
    if (!next) return
    queuedPipelineJobIds.delete(next.jobId)
    if (runningPipelineJobIds.has(next.jobId)) {
      continue
    }
    runningPipelineJobIds.add(next.jobId)
    activePipelines += 1
    const startedAtMs = Date.now()
    void runPipeline(next.jobId, next.user, next.requestedQuality, next.requestId)
      .finally(() => {
        const elapsedSeconds = Math.max(1, Math.round((Date.now() - startedAtMs) / 1000))
        recordPipelineDurationSeconds(elapsedSeconds)
        runningPipelineJobIds.delete(next.jobId)
        activePipelines = Math.max(0, activePipelines - 1)
        processQueue()
      })
  }
}

export const enqueuePipeline = (item: QueueItem) => {
  if (!item?.jobId || !item?.user?.id) return
  if (queuedPipelineJobIds.has(item.jobId) || runningPipelineJobIds.has(item.jobId)) return
  const index = pipelineQueue.findIndex((queued) => queued.priorityLevel > item.priorityLevel)
  if (index === -1) {
    pipelineQueue.push(item)
  } else {
    pipelineQueue.splice(index, 0, item)
  }
  queuedPipelineJobIds.add(item.jobId)
  processQueue()
}

const recoverQueuedJobs = async () => {
  if (queueRecoveryRunning) return
  queueRecoveryRunning = true
  try {
    const candidates = await prisma.job.findMany({
      where: {
        status: {
          in: [...STARTABLE_QUEUE_STATUSES, ...STALE_RECOVERABLE_STATUSES] as any
        }
      },
      orderBy: { createdAt: 'asc' },
      take: 200
    })
    const jobs = Array.isArray(candidates) ? candidates : []
    const nowMs = Date.now()
    for (const job of jobs) {
      const jobId = String(job?.id || '')
      const userId = String(job?.userId || '')
      if (!jobId || !userId) continue

      const status = String(job?.status || '').toLowerCase()
      const progress = Number(job?.progress ?? 0)
      const inputPath = typeof job?.inputPath === 'string' ? job.inputPath.trim() : ''
      const uploadReady = Number.isFinite(progress) && progress >= 1 && inputPath.length > 0
      const startable = STARTABLE_QUEUE_STATUSES.has(status)
      const staleRecoverable =
        STALE_RECOVERABLE_STATUSES.has(status) &&
        nowMs - toTimeMs(job?.updatedAt) >= STALE_PIPELINE_MS

      if (!startable && !staleRecoverable) continue
      if (startable && !uploadReady) continue

      if (staleRecoverable) {
        const boundedProgress = Math.max(1, Math.min(90, Number(job?.progress || 1)))
        try {
          await updateJob(jobId, { status: 'queued', progress: boundedProgress, error: null })
          console.warn(`[queue] recovered stale job ${jobId} from ${status}`)
        } catch (err) {
          console.error('[queue] stale recovery update failed', { jobId, status, err })
          continue
        }
      }

      const requestedQuality = typeof job?.requestedQuality === 'string'
        ? normalizeQuality(job.requestedQuality)
        : undefined
      enqueuePipeline({
        jobId,
        user: { id: userId },
        requestedQuality,
        priorityLevel: Number(job?.priorityLevel ?? 2) || 2
      })
    }
  } catch (err) {
    console.error('[queue] recovery sweep failed', err)
  } finally {
    queueRecoveryRunning = false
  }
}

const startQueueRecoveryLoop = () => {
  if (queueRecoveryLoopStarted) return
  queueRecoveryLoopStarted = true
  setTimeout(() => void recoverQueuedJobs(), 2000)
  const timer = setInterval(() => void recoverQueuedJobs(), QUEUE_RECOVERY_INTERVAL_MS)
  if (typeof (timer as any).unref === 'function') {
    ;(timer as any).unref()
  }
}

startQueueRecoveryLoop()

const handleCreateJob = async (req: any, res: any) => {
  try {
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized', message: 'Login required' })
    const { filename, inputPath: providedPath, requestedQuality, contentType } = req.body
    if (!filename && !providedPath) return res.status(400).json({ error: 'filename required' })
    const id = crypto.randomUUID()
    const safeName = filename ? path.basename(filename) : path.basename(providedPath)
    const inputPath = providedPath || `${userId}/${id}/${safeName}`
    const renderConfig = parseRenderConfigFromRequest(req.body)
    const onlyCutsOverride = getOnlyCutsFromPayload(req.body)
    const retentionTuning = buildRetentionTuningFromPayload({
      payload: req.body,
      fallbackAggression: DEFAULT_EDIT_OPTIONS.retentionAggressionLevel,
      fallbackStrategy: DEFAULT_EDIT_OPTIONS.retentionStrategyProfile
    })
    const retentionPlatformTuning = buildRetentionPlatformFromPayload({
      payload: req.body,
      fallbackPlatform: 'auto'
    })
    const retentionAggressionLevel = retentionTuning.aggression
    const retentionStrategyProfile = retentionTuning.strategy
    const retentionTargetPlatform = retentionPlatformTuning.targetPlatform
    const platformProfile = getPlatformProfileFromPayload(
      req.body,
      parsePlatformProfile(retentionTargetPlatform, 'auto')
    )
    const styleBlendOverride = parseStyleArchetypeBlendFromPayload(req.body)
    const autoCaptionsOverride = getAutoCaptionsFromPayload(req.body)
    const subtitleStyleOverride = getSubtitleStyleFromPayload(req.body)

    // Ensure Supabase admin client envs are present for signed upload URLs
    const missingEnvs: string[] = []
    if (!process.env.SUPABASE_URL) missingEnvs.push('SUPABASE_URL')
    if (!process.env.SUPABASE_SERVICE_ROLE_KEY) missingEnvs.push('SUPABASE_SERVICE_ROLE_KEY')
    if (missingEnvs.length > 0) {
      console.error('jobs.create misconfigured, missing envs', missingEnvs)
      return res.status(500).json({ error: 'misconfigured', message: 'Missing env vars for storage', missing: missingEnvs })
    }

    await getOrCreateUser(userId, req.user?.email)
    const { plan, tier } = await getUserPlan(userId)
    const renderLimitViolation = await getRenderLimitViolation({
      userId,
      email: req.user?.email,
      tier,
      plan,
      renderMode: renderConfig.mode
    })
    if (renderLimitViolation) {
      return res.status(403).json(renderLimitViolation.payload)
    }
    const subtitleRequest = req.body?.subtitles
    if (subtitleRequest?.enabled) {
      const features = getPlanFeatures(tier)
      if (!features.subtitles.enabled) {
        return res.status(403).json({
          error: 'PLAN_LIMIT_EXCEEDED',
          feature: 'subtitles',
          requiredPlan: 'creator',
          message: 'Subtitles are temporarily disabled.'
        })
      }
      const allowedPresets = features.subtitles.allowedPresets
      const selectedPreset = normalizeSubtitlePreset(subtitleRequest?.preset)
      if (subtitleRequest?.preset && !selectedPreset) {
        return res.status(400).json({ error: 'invalid_subtitle_preset' })
      }
      if (subtitleRequest?.preset && allowedPresets !== 'ALL') {
        if (!selectedPreset || !allowedPresets.includes(selectedPreset)) {
          const requiredPlan = getRequiredPlanForSubtitlePreset(selectedPreset)
          return res.status(403).json({
            error: 'PLAN_LIMIT_EXCEEDED',
            feature: 'subtitles',
            requiredPlan
          })
        }
      }
    }
    await ensureBucket(INPUT_BUCKET, true)

    const settings = await prisma.userSettings.findUnique({ where: { userId } })
    const desiredQuality = getRequestedQuality(requestedQuality, settings?.exportQuality)

    const job = await prisma.job.create({
      data: {
        id,
        userId,
        status: 'queued',
        inputPath,
        progress: 0,
        requestedQuality: desiredQuality,
        priorityLevel: plan.priority ? 1 : 2,
        renderSettings: buildPersistedRenderSettings(renderConfig, {
          retentionAggressionLevel,
          retentionStrategyProfile,
          retentionTargetPlatform,
          platformProfile,
          onlyCuts: onlyCutsOverride
        }),
        analysis: buildPersistedRenderAnalysis({
          existing: {
            retentionAggressionLevel,
            retentionLevel: retentionAggressionLevel,
            retentionStrategyProfile,
            retentionStrategy: retentionStrategyProfile,
            retentionTargetPlatform,
            retention_target_platform: retentionTargetPlatform,
            retentionPlatform: retentionTargetPlatform,
            targetPlatform: retentionTargetPlatform,
            platform: retentionTargetPlatform,
            platformProfile,
            platform_profile: platformProfile,
            style_archetype_blend_override: styleBlendOverride,
            ...(autoCaptionsOverride === null ? {} : { autoCaptions: autoCaptionsOverride }),
            ...(subtitleStyleOverride ? { subtitleStyle: subtitleStyleOverride } : {}),
            pipelineSteps: normalizePipelineStepMap({}),
            ...(onlyCutsOverride === null ? {} : { onlyCuts: onlyCutsOverride, onlyHookAndCut: onlyCutsOverride })
          },
          renderConfig,
          retentionTargetPlatform,
          platformProfile,
          onlyCuts: onlyCutsOverride,
          outputPaths: null
        })
      }
    })

    if (!r2.isConfigured) {
      return res.json({ job, uploadUrl: null, inputPath, bucket: INPUT_BUCKET })
    }

    try {
      // Generate an R2 presigned PUT URL for direct upload
      const uploadContentType = typeof contentType === 'string' && contentType.trim().length
        ? contentType.trim()
        : 'video/mp4'
      const uploadUrl = await r2.generateUploadUrl(inputPath, uploadContentType)
      return res.json({ job, uploadUrl, inputPath, bucket: r2.bucket })
    } catch (e) {
      console.warn('generateUploadUrl failed, returning job only', e)
      return res.json({ job, uploadUrl: null, inputPath, bucket: r2.bucket })
    }
  } catch (err: any) {
    console.error('create job error', err?.stack || err)
    const message = err?.message || String(err) || 'Unknown error'
    res.status(500).json({ error: 'server_error', message, path: '/api/jobs/create' })
  }
}

// Create job and return upload URL or null
router.post('/', handleCreateJob)
router.post('/create', handleCreateJob)

// Generate a presigned upload URL for direct-to-R2 PUT upload
router.post('/:id/upload-url', async (req: any, res) => {
  try {
    if (!r2.isConfigured) {
      return res.status(503).json({ error: 'R2_NOT_CONFIGURED', missing: r2.missingEnvVars || [] })
    }
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const jobId = req.params.id
    const { contentType } = req.body
    if (!jobId) return res.status(400).json({ error: 'missing_job_id' })
    const job = await prisma.job.findUnique({ where: { id: jobId } })
    if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })
    const timestamp = Date.now()
    const key = `uploads/${userId}/${jobId}/${timestamp}.mp4`
    const uploadUrl = await r2.generateUploadUrl(key, contentType || 'video/mp4')
    return res.json({ uploadUrl, key })
  } catch (err) {
    console.error('upload-url error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

type CancelJobByIdArgs = {
  jobId: string
  requesterUserId?: string | null
  reason?: string
}

type CancelJobByIdResult = {
  id: string
  status: 'failed'
  running: boolean
  killedCount: number
  ownerUserId: string
}

const buildCancelError = (statusCode: number, code: string, message: string) => {
  const err: any = new Error(message)
  err.statusCode = statusCode
  err.code = code
  return err
}

export const cancelJobById = async ({
  jobId,
  requesterUserId,
  reason
}: CancelJobByIdArgs): Promise<CancelJobByIdResult> => {
  const id = String(jobId || '').trim()
  if (!id) throw buildCancelError(400, 'invalid_job_id', 'Missing job ID.')

  const job = await prisma.job.findUnique({ where: { id } })
  if (!job) throw buildCancelError(404, 'not_found', 'Job not found.')
  if (requesterUserId && job.userId !== requesterUserId) {
    throw buildCancelError(404, 'not_found', 'Job not found.')
  }

  const status = String(job.status || '').toLowerCase()
  if (status === 'completed' || status === 'failed') {
    throw buildCancelError(409, 'cannot_cancel', 'Job is already finished.')
  }
  if (!CANCELABLE_PIPELINE_STATUSES.has(status)) {
    throw buildCancelError(409, 'cannot_cancel', 'Job cannot be canceled in its current state.')
  }

  markPipelineCanceled(id)
  removeJobFromQueue(id)
  processQueue()
  const killedCount = killJobFfmpegProcesses(id)
  const isRunning = runningPipelineJobIds.has(id)
  const progress = Math.max(0, Math.min(100, Number(job.progress || 0)))
  await updateJob(id, {
    status: 'failed',
    progress,
    error: reason || 'queue_canceled_by_user'
  })
  if (!isRunning) clearPipelineCanceled(id)
  return {
    id,
    status: 'failed',
    running: isRunning,
    killedCount,
    ownerUserId: String(job.userId || '')
  }
}

const handleCancelJob = async (req: any, res: any) => {
  try {
    const canceled = await cancelJobById({
      jobId: req.params.id,
      requesterUserId: req.user?.id || null,
      reason: 'queue_canceled_by_user'
    })
    return res.json({
      ok: true,
      id: canceled.id,
      status: canceled.status,
      running: canceled.running,
      killedCount: canceled.killedCount
    })
  } catch (err) {
    const status = Number((err as any)?.statusCode || 500)
    const code = String((err as any)?.code || 'server_error')
    const message = String((err as any)?.message || 'server_error')
    if (status >= 500) return res.status(500).json({ error: 'server_error' })
    return res.status(status).json({ error: code, message })
  }
}

router.post('/:id/cancel', handleCancelJob)
router.post('/:id/cancel-queue', handleCancelJob)

// List jobs
router.get('/', async (req: any, res) => {
  try {
    const userId = req.user.id
    const jobs = await prisma.job.findMany({ where: { userId }, orderBy: { createdAt: 'desc' } })
    const queueSnapshot = buildQueueEtaSnapshot()
    const payload = jobs.map((job) => {
      const normalizedStatus = job.status === 'completed' ? 'ready' : job.status
      const queueEta =
        normalizedStatus === 'queued' || normalizedStatus === 'uploading'
          ? queueSnapshot.byJobId.get(job.id)
          : null
      return {
        id: job.id,
        status: normalizedStatus,
        createdAt: job.createdAt,
        requestedQuality: job.requestedQuality,
        watermark: job.watermarkApplied,
        inputPath: job.inputPath,
        progress: job.progress,
        queuePosition: queueEta?.queuePosition ?? null,
        queueEtaSeconds: queueEta?.queueEtaSeconds ?? null,
        renderMode: parseRenderConfigFromAnalysis(job.analysis as any, (job as any)?.renderSettings).mode
      }
    })
    res.json({ jobs: payload })
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

// Get job
router.get('/:id', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    const mapStatus = (s: string) => {
      if (!s) return 'FAILED'
      if (s === 'queued') return 'QUEUED'
      if (s === 'rendering') return 'RENDERING'
      if (s === 'completed') return 'READY'
      if (s === 'failed') return 'FAILED'
      // any intermediate states are processing
      return 'PROCESSING'
    }
    const normalizedStatus = job.status === 'completed' ? 'ready' : job.status
    const queueSnapshot = buildQueueEtaSnapshot()
    const queueEta =
      normalizedStatus === 'queued' || normalizedStatus === 'uploading'
        ? queueSnapshot.byJobId.get(job.id)
        : null

    const jobPayload: any = {
      ...job,
      // Keep the detailed pipeline status for the editor UI.
      status: normalizedStatus,
      // Legacy coarse-grained status for older clients.
      legacyStatus: mapStatus(job.status),
      watermark: job.watermarkApplied,
      queuePosition: queueEta?.queuePosition ?? null,
      queueEtaSeconds: queueEta?.queueEtaSeconds ?? null,
      renderMode: parseRenderConfigFromAnalysis(job.analysis as any, (job as any)?.renderSettings).mode,
      steps: [
        { key: 'queued', label: 'Queued' },
        { key: 'uploading', label: 'Uploading' },
        { key: 'analyzing', label: 'Analyzing' },
        { key: 'hooking', label: 'Hook' },
        { key: 'cutting', label: 'Cuts' },
        { key: 'pacing', label: 'Pacing' },
        { key: 'story', label: 'Story' },
        { key: 'audio', label: 'Audio' },
        { key: 'retention', label: 'Retention' },
        { key: 'subtitling', label: 'Subtitles' },
        { key: 'rendering', label: 'Rendering' },
        { key: 'ready', label: 'Ready' }
      ]
    }
    const outputPaths = getOutputPathsForJob(job)
    if (job.status === 'completed' && outputPaths.length > 0) {
      const resolvedUrls: string[] = []
      const resolvedSources: string[] = []
      for (let idx = 0; idx < outputPaths.length; idx += 1) {
        const outputPath = outputPaths[idx]
        try {
          const resolved = await resolveOutputUrlWithLocalFallback({
            req,
            job,
            outputPath,
            clipIndex: idx
          })
          resolvedUrls.push(resolved.url)
          resolvedSources.push(resolved.source)
        } catch (error) {
          console.warn(`[${req.requestId || 'noid'}] failed to resolve output URL`, {
            outputPath,
            error: (error as any)?.message || error
          })
        }
      }
      if (resolvedUrls.length > 0) {
        jobPayload.outputUrls = resolvedUrls
        jobPayload.outputUrl = resolvedUrls[0]
        jobPayload.outputUrlSources = resolvedSources
      }
    }
    if (outputPaths.length > 0) {
      try {
        jobPayload.fileName = path.basename(outputPaths[0])
        jobPayload.outputFiles = outputPaths.map((outputPath) => ({
          key: outputPath,
          fileName: path.basename(outputPath)
        }))
      } catch (e) {
        // ignore
      }
    }
    res.json({ job: jobPayload })
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

// Return a short-lived signed URL for downloads (only when ready)
router.post('/:id/download-url', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    if (job.status !== 'completed') return res.status(403).json({ error: 'not_ready' })
    const outputPaths = getOutputPathsForJob(job)
    if (!outputPaths.length) return res.status(404).json({ error: 'not_found' })
    const requestedClip = Number.parseInt(String(req.body?.clip ?? req.query?.clip ?? '1'), 10)
    const clipIndex = Number.isFinite(requestedClip) ? clamp(requestedClip - 1, 0, outputPaths.length - 1) : 0
    const selectedOutputPath = outputPaths[clipIndex]
    try {
      const resolved = await resolveOutputUrlWithLocalFallback({
        req,
        job,
        outputPath: selectedOutputPath,
        clipIndex
      })

      // schedule auto-delete 1 minute after user requests download
      if (resolved.source === 'remote') {
        try {
          const keyToDelete = outputPaths.length === 1 ? selectedOutputPath : null
          if (keyToDelete) {
            setTimeout(async () => {
              try {
                await deleteOutputObject(keyToDelete)
                try {
                  await updateJob(id, { outputPath: null })
                } catch (e) {
                  // ignore DB update failures
                }
                console.log(`[${req.requestId}] auto-deleted R2 object ${keyToDelete} for job ${id}`)
              } catch (err) {
                console.error('auto-delete failed', err)
              }
            }, 60_000)
          }
        } catch (e) {
          // scheduling failure shouldn't block download
        }
      }

      return res.json({ url: resolved.url, source: resolved.source })
    } catch (err) {
      return res.status(500).json({ error: 'signed_url_failed' })
    }
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

// Return signed URL for proxy preview if available
router.post('/:id/proxy-url', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    const analysis = job.analysis as any
    const proxyPath = analysis?.proxyPath
    if (!proxyPath) return res.status(404).json({ error: 'proxy_not_available' })
    await ensureBucket(OUTPUT_BUCKET, false)
    const expires = 60 * 10
    const url = await getSignedOutputUrl({ key: proxyPath, expiresIn: expires })
    return res.json({ url })
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

// Return signed URL for source input preview
router.post('/:id/input-url', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    if (!job.inputPath) return res.status(404).json({ error: 'input_not_available' })
    await ensureBucket(INPUT_BUCKET, true)
    const expires = 60 * 10
    const url = await getSignedInputUrl({ key: job.inputPath, expiresIn: expires })
    return res.json({ url })
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

const handleCompleteUpload = async (req: any, res: any) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    const status = String(job.status || '').toLowerCase()
    if (!STARTABLE_QUEUE_STATUSES.has(status)) {
      return res.status(409).json({ error: 'job_not_startable' })
    }
    const inputPath = req.body?.key || req.body?.inputPath || job.inputPath
    const requestedQuality = req.body?.requestedQuality ? normalizeQuality(req.body.requestedQuality) : job.requestedQuality
    const onlyCutsOverride = getOnlyCutsFromPayload(req.body)
    const resolvedOnlyCuts = onlyCutsOverride ?? getOnlyCutsFromJob(job)
    const tuning = buildRetentionTuningFromPayload({
      payload: req.body,
      fallbackAggression: getRetentionAggressionFromJob(job),
      fallbackStrategy: getRetentionStrategyFromJob(job)
    })
    const platformTuning = buildRetentionPlatformFromPayload({
      payload: req.body,
      fallbackPlatform: getRetentionTargetPlatformFromJob(job)
    })
    const requestedAggressionLevel = tuning.aggression
    const requestedStrategyProfile = tuning.strategy
    const requestedTargetPlatform = platformTuning.targetPlatform
    const requestedPlatformProfile = hasPlatformProfileOverride(req.body)
      ? getPlatformProfileFromPayload(req.body, getPlatformProfileFromJob(job))
      : parsePlatformProfile(getPlatformProfileFromJob(job), parsePlatformProfile(requestedTargetPlatform, 'auto'))
    const styleBlendOverride =
      parseStyleArchetypeBlendFromPayload(req.body) ??
      parseStyleArchetypeBlendFromPayload((job.analysis as any) || {})
    const autoCaptionsOverride = (
      getAutoCaptionsFromPayload(req.body) ??
      getAutoCaptionsFromPayload((job.analysis as any) || {})
    )
    const subtitleStyleOverride = (
      getSubtitleStyleFromPayload(req.body) ??
      getSubtitleStyleFromPayload((job.analysis as any) || {})
    )
    const nextRenderSettings = {
      ...((job as any)?.renderSettings || {}),
      retentionAggressionLevel: requestedAggressionLevel,
      retentionLevel: requestedAggressionLevel,
      retentionStrategyProfile: requestedStrategyProfile,
      retentionStrategy: requestedStrategyProfile,
      retentionTargetPlatform: requestedTargetPlatform,
      retention_target_platform: requestedTargetPlatform,
      targetPlatform: requestedTargetPlatform,
      platformProfile: requestedPlatformProfile,
      platform_profile: requestedPlatformProfile,
      ...(resolvedOnlyCuts === null ? {} : { onlyCuts: resolvedOnlyCuts, onlyHookAndCut: resolvedOnlyCuts })
    }
    const nextAnalysis = {
      ...((job.analysis as any) || {}),
      retentionAggressionLevel: requestedAggressionLevel,
      retentionLevel: requestedAggressionLevel,
      retentionStrategyProfile: requestedStrategyProfile,
      retentionStrategy: requestedStrategyProfile,
      retentionTargetPlatform: requestedTargetPlatform,
      retention_target_platform: requestedTargetPlatform,
      retentionPlatform: requestedTargetPlatform,
      targetPlatform: requestedTargetPlatform,
      platform: requestedTargetPlatform,
      platformProfile: requestedPlatformProfile,
      platform_profile: requestedPlatformProfile,
      style_archetype_blend_override: styleBlendOverride,
      ...(autoCaptionsOverride === null ? {} : { autoCaptions: autoCaptionsOverride }),
      ...(subtitleStyleOverride ? { subtitleStyle: subtitleStyleOverride } : {}),
      ...(resolvedOnlyCuts === null ? {} : { onlyCuts: resolvedOnlyCuts, onlyHookAndCut: resolvedOnlyCuts })
    }

    const { plan, tier } = await getUserPlan(req.user.id)
    const renderMode = parseRenderConfigFromAnalysis(job.analysis as any, (job as any)?.renderSettings).mode
    const renderLimitViolation = await getRenderLimitViolation({
      userId: req.user.id,
      email: req.user?.email,
      tier,
      plan,
      renderMode,
      allowAtLimitForFree: true
    })
    if (renderLimitViolation) {
      return res.status(403).json(renderLimitViolation.payload)
    }

    await updateJob(id, {
      inputPath,
      status: 'analyzing',
      progress: 10,
      requestedQuality: requestedQuality || job.requestedQuality,
      renderSettings: nextRenderSettings,
      analysis: nextAnalysis
    })

    res.json({ ok: true })
    enqueuePipeline({
      jobId: id,
      user: { id: req.user.id, email: req.user?.email },
      requestedQuality: requestedQuality as ExportQuality | undefined,
      requestId: req.requestId,
      priorityLevel: job.priorityLevel ?? 2
    })
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
}

// complete upload and start pipeline
router.post('/:id/complete-upload', handleCompleteUpload)
// mark uploaded (alias)
router.post('/:id/set-uploaded', handleCompleteUpload)

router.post('/:id/analyze', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    const tuning = buildRetentionTuningFromPayload({
      payload: req.body,
      fallbackAggression: getRetentionAggressionFromJob(job),
      fallbackStrategy: getRetentionStrategyFromJob(job)
    })
    const platformTuning = buildRetentionPlatformFromPayload({
      payload: req.body,
      fallbackPlatform: getRetentionTargetPlatformFromJob(job)
    })
    const requestedTargetPlatform = platformTuning.targetPlatform
    const requestedPlatformProfile = hasPlatformProfileOverride(req.body)
      ? getPlatformProfileFromPayload(req.body, getPlatformProfileFromJob(job))
      : parsePlatformProfile(getPlatformProfileFromJob(job), parsePlatformProfile(requestedTargetPlatform, 'auto'))
    const styleBlendOverride =
      parseStyleArchetypeBlendFromPayload(req.body) ??
      parseStyleArchetypeBlendFromPayload((job.analysis as any) || {})
    const autoCaptionsOverride = (
      getAutoCaptionsFromPayload(req.body) ??
      getAutoCaptionsFromPayload((job.analysis as any) || {})
    )
    const subtitleStyleOverride = (
      getSubtitleStyleFromPayload(req.body) ??
      getSubtitleStyleFromPayload((job.analysis as any) || {})
    )
    const requestedOnlyCuts = getOnlyCutsFromPayload(req.body) ?? getOnlyCutsFromJob(job)
    const nextRenderSettings = {
      ...((job as any)?.renderSettings || {}),
      retentionAggressionLevel: tuning.aggression,
      retentionLevel: tuning.aggression,
      retentionStrategyProfile: tuning.strategy,
      retentionStrategy: tuning.strategy,
      retentionTargetPlatform: requestedTargetPlatform,
      retention_target_platform: requestedTargetPlatform,
      targetPlatform: requestedTargetPlatform,
      platformProfile: requestedPlatformProfile,
      platform_profile: requestedPlatformProfile,
      ...(requestedOnlyCuts === null ? {} : { onlyCuts: requestedOnlyCuts, onlyHookAndCut: requestedOnlyCuts })
    }
    const nextAnalysis = {
      ...((job.analysis as any) || {}),
      retentionAggressionLevel: tuning.aggression,
      retentionLevel: tuning.aggression,
      retentionStrategyProfile: tuning.strategy,
      retentionStrategy: tuning.strategy,
      retentionTargetPlatform: requestedTargetPlatform,
      retention_target_platform: requestedTargetPlatform,
      retentionPlatform: requestedTargetPlatform,
      targetPlatform: requestedTargetPlatform,
      platform: requestedTargetPlatform,
      platformProfile: requestedPlatformProfile,
      platform_profile: requestedPlatformProfile,
      style_archetype_blend_override: styleBlendOverride,
      ...(autoCaptionsOverride === null ? {} : { autoCaptions: autoCaptionsOverride }),
      ...(subtitleStyleOverride ? { subtitleStyle: subtitleStyleOverride } : {}),
      ...(requestedOnlyCuts === null ? {} : { onlyCuts: requestedOnlyCuts, onlyHookAndCut: requestedOnlyCuts })
    }
    await updateJob(id, {
      renderSettings: nextRenderSettings,
      analysis: nextAnalysis
    })
    const { options } = await getEditOptionsForUser(req.user.id, {
      retentionAggressionLevel: tuning.aggression,
      retentionStrategyProfile: tuning.strategy,
      onlyCuts: requestedOnlyCuts,
      autoCaptions: autoCaptionsOverride,
      subtitleStyle: subtitleStyleOverride
    })
    options.retentionAggressionLevel = tuning.aggression
    options.retentionStrategyProfile = tuning.strategy
    options.aggressiveMode = isAggressiveRetentionLevel(tuning.aggression)
    options.styleArchetypeBlend = styleBlendOverride
    if (req.body?.fastMode) {
      options.fastMode = true
    }
    const analysis = await analyzeJob(id, options, req.requestId)
    res.json({ ok: true, analysis })
  } catch (err) {
    if (err instanceof HookGateError) {
      return res.status(422).json({
        error: 'FAILED_HOOK',
        message: err.reason,
        details: err.details ?? null
      })
    }
    console.error('analyze error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

router.post('/:id/process', async (req: any, res) => {
  const id = req.params.id
  try {
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })

    const user = await getOrCreateUser(req.user.id, req.user?.email)
    const requestedQuality = req.body?.requestedQuality ? normalizeQuality(req.body.requestedQuality) : job.requestedQuality
    const tuning = buildRetentionTuningFromPayload({
      payload: req.body,
      fallbackAggression: getRetentionAggressionFromJob(job),
      fallbackStrategy: getRetentionStrategyFromJob(job)
    })
    const platformTuning = buildRetentionPlatformFromPayload({
      payload: req.body,
      fallbackPlatform: getRetentionTargetPlatformFromJob(job)
    })
    const requestedTargetPlatform = platformTuning.targetPlatform
    const requestedPlatformProfile = hasPlatformProfileOverride(req.body)
      ? getPlatformProfileFromPayload(req.body, getPlatformProfileFromJob(job))
      : parsePlatformProfile(getPlatformProfileFromJob(job), parsePlatformProfile(requestedTargetPlatform, 'auto'))
    const styleBlendOverride =
      parseStyleArchetypeBlendFromPayload(req.body) ??
      parseStyleArchetypeBlendFromPayload((job.analysis as any) || {})
    const autoCaptionsOverride = (
      getAutoCaptionsFromPayload(req.body) ??
      getAutoCaptionsFromPayload((job.analysis as any) || {})
    )
    const subtitleStyleOverride = (
      getSubtitleStyleFromPayload(req.body) ??
      getSubtitleStyleFromPayload((job.analysis as any) || {})
    )
    const requestedOnlyCuts = getOnlyCutsFromPayload(req.body) ?? getOnlyCutsFromJob(job)
    const nextRenderSettings = {
      ...((job as any)?.renderSettings || {}),
      retentionAggressionLevel: tuning.aggression,
      retentionLevel: tuning.aggression,
      retentionStrategyProfile: tuning.strategy,
      retentionStrategy: tuning.strategy,
      retentionTargetPlatform: requestedTargetPlatform,
      retention_target_platform: requestedTargetPlatform,
      targetPlatform: requestedTargetPlatform,
      platformProfile: requestedPlatformProfile,
      platform_profile: requestedPlatformProfile,
      ...(requestedOnlyCuts === null ? {} : { onlyCuts: requestedOnlyCuts, onlyHookAndCut: requestedOnlyCuts })
    }
    const nextAnalysis = {
      ...((job.analysis as any) || {}),
      retentionAggressionLevel: tuning.aggression,
      retentionLevel: tuning.aggression,
      retentionStrategyProfile: tuning.strategy,
      retentionStrategy: tuning.strategy,
      retentionTargetPlatform: requestedTargetPlatform,
      retention_target_platform: requestedTargetPlatform,
      retentionPlatform: requestedTargetPlatform,
      targetPlatform: requestedTargetPlatform,
      platform: requestedTargetPlatform,
      platformProfile: requestedPlatformProfile,
      platform_profile: requestedPlatformProfile,
      style_archetype_blend_override: styleBlendOverride,
      ...(autoCaptionsOverride === null ? {} : { autoCaptions: autoCaptionsOverride }),
      ...(subtitleStyleOverride ? { subtitleStyle: subtitleStyleOverride } : {}),
      ...(requestedOnlyCuts === null ? {} : { onlyCuts: requestedOnlyCuts, onlyHookAndCut: requestedOnlyCuts })
    }
    await updateJob(id, {
      renderSettings: nextRenderSettings,
      analysis: nextAnalysis
    })
    const { options } = await getEditOptionsForUser(req.user.id, {
      retentionAggressionLevel: tuning.aggression,
      retentionStrategyProfile: tuning.strategy,
      onlyCuts: requestedOnlyCuts,
      autoCaptions: autoCaptionsOverride,
      subtitleStyle: subtitleStyleOverride
    })
    const hasPreferredHookPayload =
      req.body?.preferredHook !== undefined ||
      req.body?.selectedHook !== undefined
    const preferredHookPayload = parsePreferredHookCandidateFromPayload(
      req.body?.preferredHook ?? req.body?.selectedHook
    )
    const availableHookCandidates = getHookCandidatesFromAnalysis(job.analysis as any)
    const preferredHookCandidate = matchPreferredHookCandidate({
      preferred: preferredHookPayload,
      candidates: availableHookCandidates
    })
    if (hasPreferredHookPayload && !preferredHookCandidate) {
      return res.status(400).json({ error: 'invalid_preferred_hook' })
    }
    if (preferredHookCandidate) {
      options.preferredHookCandidate = preferredHookCandidate
    }
    options.retentionAggressionLevel = tuning.aggression
    options.retentionStrategyProfile = tuning.strategy
    options.aggressiveMode = isAggressiveRetentionLevel(tuning.aggression)
    options.styleArchetypeBlend = styleBlendOverride
    // Allow client to request a fast-mode re-render (overrides user settings for this run)
    if (req.body?.fastMode) {
      options.fastMode = true
    }
    await processJob(id, { id: user.id, email: user.email }, requestedQuality as ExportQuality | undefined, options, req.requestId)
    res.json({ ok: true })
  } catch (err: any) {
    if (err instanceof PlanLimitError) {
      return res.status(err.status).json({
        error: err.code,
        message: err.message,
        feature: err.feature,
        requiredPlan: err.requiredPlan,
        checkoutUrl: err.checkoutUrl ?? null
      })
    }
    if (err instanceof HookGateError) {
      return res.status(422).json({
        error: 'FAILED_HOOK',
        message: err.reason,
        details: err.details ?? null
      })
    }
    if (err instanceof QualityGateError) {
      return res.status(422).json({
        error: 'FAILED_QUALITY_GATE',
        message: err.reason,
        details: err.details ?? null
      })
    }
    console.error('process error', err)
    try {
      await updateJob(req.params.id, { status: 'failed', error: formatFfmpegFailure(err) })
    } catch (e) {
      // ignore
    }
    res.status(500).json({ error: 'server_error' })
  }
})

router.post('/:id/preferred-hook', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })

    const status = String(job.status || '').toLowerCase()
    const mutableHookStatuses = new Set([
      'queued',
      'uploading',
      'analyzing',
      'hooking',
      'cutting',
      'pacing',
      'story'
    ])
    if (!mutableHookStatuses.has(status)) {
      return res.status(409).json({
        error: 'hook_stage_complete',
        message: 'Hook selection can only be changed before subtitle/render stages.'
      })
    }

    const preferredHookPayload = parsePreferredHookCandidateFromPayload(
      req.body?.preferredHook ?? req.body?.selectedHook
    )
    if (!preferredHookPayload) {
      return res.status(400).json({ error: 'invalid_preferred_hook' })
    }

    const analysis = ((job.analysis as any) || {}) as Record<string, any>
    const availableHookCandidates = getHookCandidatesFromAnalysis(analysis)
    if (!availableHookCandidates.length) {
      return res.status(409).json({
        error: 'hook_candidates_not_ready',
        message: 'Hook options are still being generated.'
      })
    }

    const preferredHookCandidate = matchPreferredHookCandidate({
      preferred: preferredHookPayload,
      candidates: availableHookCandidates
    })
    if (!preferredHookCandidate) {
      return res.status(400).json({ error: 'invalid_preferred_hook' })
    }

    const nowIso = toIsoNow()
    const existingSteps = normalizePipelineStepMap(analysis.pipelineSteps)
    const hookStepMeta =
      existingSteps.HOOK_SELECT_AND_AUDIT?.meta &&
      typeof existingSteps.HOOK_SELECT_AND_AUDIT.meta === 'object'
        ? existingSteps.HOOK_SELECT_AND_AUDIT.meta
        : {}
    existingSteps.HOOK_SELECT_AND_AUDIT = {
      ...existingSteps.HOOK_SELECT_AND_AUDIT,
      meta: {
        ...hookStepMeta,
        selectedHook: preferredHookCandidate,
        hookSelectionSource: 'user_selected',
        preferredHookUpdatedAt: nowIso
      }
    }

    const nextAnalysis = {
      ...analysis,
      preferred_hook: preferredHookCandidate,
      preferred_hook_updated_at: nowIso,
      hook_start_time: preferredHookCandidate.start,
      hook_end_time: Number((preferredHookCandidate.start + preferredHookCandidate.duration).toFixed(3)),
      hook_text: preferredHookCandidate.text || analysis.hook_text || null,
      hook_reason: preferredHookCandidate.reason || analysis.hook_reason || null,
      hook_selection_source: 'user_selected',
      pipelineSteps: existingSteps,
      pipelineUpdatedAt: nowIso
    }

    await updateJob(id, { analysis: nextAnalysis }, { expectedUpdatedAt: job.updatedAt })
    return res.json({
      ok: true,
      preferredHook: {
        start: preferredHookCandidate.start,
        duration: preferredHookCandidate.duration
      },
      appliedAt: nowIso
    })
  } catch (err: any) {
    if (String(err?.code || '').toLowerCase() === 'job_update_conflict' || String(err?.message || '').includes('job_update_conflict')) {
      return res.status(409).json({
        error: 'hook_update_conflict',
        message: 'Hook selection changed in another request. Refresh and retry.'
      })
    }
    console.error('preferred-hook update error', err)
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/:id/reprocess', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    const status = String(job.status || '').toLowerCase()
    if (status !== 'completed' && status !== 'failed') {
      return res.status(409).json({ error: 'job_not_ready_for_reprocess' })
    }

    const user = await getOrCreateUser(req.user.id, req.user?.email)
    const requestedQuality = req.body?.requestedQuality ? normalizeQuality(req.body.requestedQuality) : job.requestedQuality
    const tuning = buildRetentionTuningFromPayload({
      payload: req.body,
      fallbackAggression: getRetentionAggressionFromJob(job),
      fallbackStrategy: getRetentionStrategyFromJob(job)
    })
    const platformTuning = buildRetentionPlatformFromPayload({
      payload: req.body,
      fallbackPlatform: getRetentionTargetPlatformFromJob(job)
    })
    const requestedAggressionLevel = tuning.aggression
    const requestedStrategyProfile = tuning.strategy
    const requestedTargetPlatform = platformTuning.targetPlatform
    const requestedPlatformProfile = hasPlatformProfileOverride(req.body)
      ? getPlatformProfileFromPayload(req.body, getPlatformProfileFromJob(job))
      : parsePlatformProfile(getPlatformProfileFromJob(job), parsePlatformProfile(requestedTargetPlatform, 'auto'))
    const styleBlendOverride =
      parseStyleArchetypeBlendFromPayload(req.body) ??
      parseStyleArchetypeBlendFromPayload((job.analysis as any) || {})
    const autoCaptionsOverride = (
      getAutoCaptionsFromPayload(req.body) ??
      getAutoCaptionsFromPayload((job.analysis as any) || {})
    )
    const subtitleStyleOverride = (
      getSubtitleStyleFromPayload(req.body) ??
      getSubtitleStyleFromPayload((job.analysis as any) || {})
    )

    const hasPreferredHookPayload =
      req.body?.preferredHook !== undefined ||
      req.body?.selectedHook !== undefined
    const preferredHookPayload = parsePreferredHookCandidateFromPayload(
      req.body?.preferredHook ?? req.body?.selectedHook
    )
    const availableHookCandidates = getHookCandidatesFromAnalysis(job.analysis as any)
    const preferredHookCandidate = matchPreferredHookCandidate({
      preferred: preferredHookPayload,
      candidates: availableHookCandidates
    })
    if (hasPreferredHookPayload && !preferredHookCandidate) {
      return res.status(400).json({ error: 'invalid_preferred_hook' })
    }

    const nextRenderSettings = {
      ...((job as any)?.renderSettings || {}),
      retentionAggressionLevel: requestedAggressionLevel,
      retentionLevel: requestedAggressionLevel,
      retentionStrategyProfile: requestedStrategyProfile,
      retentionStrategy: requestedStrategyProfile,
      retentionTargetPlatform: requestedTargetPlatform,
      retention_target_platform: requestedTargetPlatform,
      targetPlatform: requestedTargetPlatform,
      platformProfile: requestedPlatformProfile,
      platform_profile: requestedPlatformProfile,
      ...(autoCaptionsOverride === null ? {} : { autoCaptions: autoCaptionsOverride }),
      ...(subtitleStyleOverride ? { subtitleStyle: subtitleStyleOverride } : {})
    }
    const nextAnalysis = {
      ...((job.analysis as any) || {}),
      retentionAggressionLevel: requestedAggressionLevel,
      retentionLevel: requestedAggressionLevel,
      retentionStrategyProfile: requestedStrategyProfile,
      retentionStrategy: requestedStrategyProfile,
      retentionTargetPlatform: requestedTargetPlatform,
      retention_target_platform: requestedTargetPlatform,
      retentionPlatform: requestedTargetPlatform,
      targetPlatform: requestedTargetPlatform,
      platform: requestedTargetPlatform,
      platformProfile: requestedPlatformProfile,
      platform_profile: requestedPlatformProfile,
      style_archetype_blend_override: styleBlendOverride,
      ...(autoCaptionsOverride === null ? {} : { autoCaptions: autoCaptionsOverride }),
      ...(subtitleStyleOverride ? { subtitleStyle: subtitleStyleOverride } : {}),
      preferred_hook: preferredHookCandidate ?? null,
      preferred_hook_updated_at: preferredHookCandidate ? toIsoNow() : null
    }

    const priorityLevel = Number(job.priorityLevel ?? 2) || 2
    await updateJob(id, {
      status: 'queued',
      progress: 1,
      error: null,
      requestedQuality: requestedQuality || job.requestedQuality,
      outputPath: null,
      renderSettings: nextRenderSettings,
      analysis: nextAnalysis,
      priorityLevel
    })

    enqueuePipeline({
      jobId: id,
      user: { id: user.id, email: user.email },
      requestedQuality: requestedQuality as ExportQuality | undefined,
      requestId: req.requestId,
      priorityLevel
    })
    return res.json({
      ok: true,
      queued: true,
      preferredHook: preferredHookCandidate
        ? {
            start: preferredHookCandidate.start,
            duration: preferredHookCandidate.duration
          }
        : null
    })
  } catch (err) {
    console.error('reprocess error', err)
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/:id/retention-feedback', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    if (job.status !== 'completed') return res.status(403).json({ error: 'not_ready' })

    const feedback = parseRetentionFeedbackPayload(req.body || {})
    if (!feedback) {
      return res.status(400).json({ error: 'invalid_feedback_payload' })
    }

    await persistRetentionFeedbackForJob({ job, feedback })

    const hookCalibration = await loadHookCalibrationProfile(job.userId)
    return res.json({
      ok: true,
      feedback,
      hookCalibration
    })
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

router.post('/:id/platform-feedback', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    if (job.status !== 'completed') return res.status(403).json({ error: 'not_ready' })

    const feedback = parseRetentionFeedbackPayload({
      ...(req.body || {}),
      sourceType: 'platform',
      source: req.body?.source || 'platform_analytics'
    })
    if (!feedback) {
      return res.status(400).json({ error: 'invalid_feedback_payload' })
    }

    await persistRetentionFeedbackForJob({ job, feedback })
    const hookCalibration = await loadHookCalibrationProfile(job.userId)
    return res.json({
      ok: true,
      feedback,
      hookCalibration
    })
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

router.post('/:id/creator-feedback', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    if (job.status !== 'completed') return res.status(403).json({ error: 'not_ready' })

    const { tier } = await getUserPlan(req.user.id)
    if (!isPaidTier(tier)) {
      return res.status(403).json({
        error: 'PAID_ONLY_FEATURE',
        message: 'Creator correction feedback is available on paid plans.',
        requiredPlan: 'starter'
      })
    }

    const creatorFeedback = parseCreatorFeedbackPayload(req.body || {})
    if (!creatorFeedback) {
      return res.status(400).json({ error: 'invalid_creator_feedback_payload' })
    }
    const feedback = buildRetentionFeedbackFromCreatorPayload(creatorFeedback)
    const existingAnalysis = (job.analysis as any) || {}
    const creatorHistoryRaw = Array.isArray(existingAnalysis?.creator_feedback_history)
      ? existingAnalysis.creator_feedback_history
      : []
    const creatorHistory = [
      ...creatorHistoryRaw.slice(-29),
      creatorFeedback
    ]
    await persistRetentionFeedbackForJob({
      job,
      feedback,
      analysisPatch: {
        creator_feedback: creatorFeedback,
        creator_feedback_history: creatorHistory,
        creator_feedback_updated_at: toIsoNow()
      }
    })

    const hookCalibration = await loadHookCalibrationProfile(job.userId)
    return res.json({
      ok: true,
      creatorFeedback,
      feedback,
      hookCalibration
    })
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

router.get('/:id/output-url', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    const outputPaths = getOutputPathsForJob(job)
    if (!outputPaths.length) return res.status(404).json({ error: 'not_found' })
    const requestedClip = Number.parseInt(String(req.query?.clip ?? '1'), 10)
    const clipIndex = Number.isFinite(requestedClip) ? clamp(requestedClip - 1, 0, outputPaths.length - 1) : 0
    const selectedOutputPath = outputPaths[clipIndex]
    try {
      const resolved = await resolveOutputUrlWithLocalFallback({
        req,
        job,
        outputPath: selectedOutputPath,
        clipIndex
      })

      // schedule auto-delete 1 minute after user requests download
      if (resolved.source === 'remote') {
        try {
          const keyToDelete = outputPaths.length === 1 ? selectedOutputPath : null
          if (keyToDelete) {
            setTimeout(async () => {
              try {
                await deleteOutputObject(keyToDelete)
                try {
                  await updateJob(id, { outputPath: null })
                } catch (e) {
                  // ignore DB update failures
                }
                console.log(`[${req.requestId}] auto-deleted R2 object ${keyToDelete} for job ${id}`)
              } catch (err) {
                console.error('auto-delete failed', err)
              }
            }, 60_000)
          }
        } catch (e) {
          // scheduling failure shouldn't block download
        }
      }

      res.json({ url: resolved.url, source: resolved.source })
    } catch (err) {
      res.status(500).json({ error: 'signed_url_failed' })
    }
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

// Stream local output file when remote output storage/signing is unavailable.
router.get('/:id/local-output', async (req: any, res) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    if (job.status !== 'completed') return res.status(403).json({ error: 'not_ready' })
    const outputPaths = getOutputPathsForJob(job)
    if (!outputPaths.length) return res.status(404).json({ error: 'not_found' })
    const requestedClip = Number.parseInt(String(req.query?.clip ?? '1'), 10)
    const clipIndex = Number.isFinite(requestedClip) ? clamp(requestedClip - 1, 0, outputPaths.length - 1) : 0
    const localOutput = getLocalOutputFileInfo(job, clipIndex)
    if (!localOutput) return res.status(404).json({ error: 'local_output_not_found' })

    const fileSize = localOutput.size
    const rangeHeader = String(req.headers?.range || '').trim()
    const rangeMatch = /^bytes=(\d*)-(\d*)$/i.exec(rangeHeader)
    const fileName = path.basename(outputPaths[clipIndex] || localOutput.filePath)
    const wantsDownload = String(req.query?.download || '').toLowerCase() === '1' || String(req.query?.download || '').toLowerCase() === 'true'
    res.setHeader('Content-Type', 'video/mp4')
    res.setHeader('Accept-Ranges', 'bytes')
    if (wantsDownload) {
      res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`)
    }

    if (rangeMatch) {
      const rawStart = rangeMatch[1] ? Number.parseInt(rangeMatch[1], 10) : 0
      const rawEnd = rangeMatch[2] ? Number.parseInt(rangeMatch[2], 10) : fileSize - 1
      const start = clamp(Number.isFinite(rawStart) ? rawStart : 0, 0, Math.max(0, fileSize - 1))
      const end = clamp(Number.isFinite(rawEnd) ? rawEnd : fileSize - 1, start, Math.max(start, fileSize - 1))
      if (start >= fileSize || end >= fileSize || start > end) {
        res.setHeader('Content-Range', `bytes */${fileSize}`)
        return res.status(416).end()
      }
      const chunkSize = end - start + 1
      res.status(206)
      res.setHeader('Content-Range', `bytes ${start}-${end}/${fileSize}`)
      res.setHeader('Content-Length', chunkSize)
      const stream = fs.createReadStream(localOutput.filePath, { start, end })
      stream.on('error', () => res.status(500).end())
      stream.pipe(res)
      return
    }

    res.setHeader('Content-Length', fileSize)
    const stream = fs.createReadStream(localOutput.filePath)
    stream.on('error', () => res.status(500).end())
    stream.pipe(res)
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

const buildEditPlanForTest = async ({
  filePath,
  aggressionLevel,
  strategyProfile
}: {
  filePath: string
  aggressionLevel?: RetentionAggressionLevel | string
  strategyProfile?: RetentionStrategyProfile | string
}) => {
  const absolutePath = path.resolve(String(filePath || ''))
  if (!absolutePath || !fs.existsSync(absolutePath)) {
    throw new Error(`test_input_missing:${absolutePath}`)
  }
  const durationSeconds = getDurationSeconds(absolutePath)
  if (!durationSeconds || !Number.isFinite(durationSeconds) || durationSeconds <= 0) {
    throw new Error(`test_input_duration_unavailable:${absolutePath}`)
  }
  const normalizedAggression = parseRetentionAggressionLevel(aggressionLevel)
  const normalizedStrategy = parseRetentionStrategyProfile(
    strategyProfile || strategyFromAggressionLevel(normalizedAggression)
  )
  const options: EditOptions = {
    ...DEFAULT_EDIT_OPTIONS,
    retentionAggressionLevel: normalizedAggression,
    retentionStrategyProfile: normalizedStrategy,
    aggressiveMode: isAggressiveRetentionLevel(normalizedAggression)
  }
  return buildEditPlan(absolutePath, durationSeconds, options, undefined, {
    aggressionLevel: normalizedAggression
  })
}

export const __retentionTestUtils = {
  pickTopHookCandidates,
  computeRetentionScore,
  buildRetentionJudgeReport,
  resolveQualityGateThresholds,
  computeContentSignalStrength,
  parseRetentionFeedbackPayload,
  parseCreatorFeedbackPayload,
  buildRetentionFeedbackFromCreatorPayload,
  computeHookCalibrationProfileFromHistory,
  normalizeHookCalibrationWeights,
  inferContentStyleProfile,
  getStyleAdjustedAggressionLevel,
  applyStyleToPacingProfile,
  detectRhythmAnchors,
  detectEmotionalBeatAnchors,
  applyEmotionalBeatCuts,
  alignSegmentsToRhythm,
  selectRenderableHookCandidate,
  shouldForceRescueRender,
  executeQualityGateRetriesForTest,
  predictVariantRetention,
  buildTimelineWithHookAtStartForTest,
  buildPersistedRenderAnalysis,
  buildEditPlanForTest
}

export default router
