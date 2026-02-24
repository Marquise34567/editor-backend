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
import { PLAN_CONFIG, getMonthKey, type PlanTier } from '../shared/planConfig'
import { broadcastJobUpdate } from '../realtime'
import { FFMPEG_PATH, FFPROBE_PATH, formatCommand } from '../lib/ffmpeg'
import { isDevAccount } from '../lib/devAccounts'
import {
  getPlanFeatures,
  getRequiredPlanForAdvancedEffects,
  getRequiredPlanForAutoZoom,
  getRequiredPlanForQuality,
  getRequiredPlanForSubtitlePreset,
  getRequiredPlanForRenders,
  isSubtitlePresetAllowed
} from '../lib/planFeatures'
import { DEFAULT_SUBTITLE_PRESET, normalizeSubtitlePreset } from '../shared/subtitlePresets'

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

export const updateJob = async (jobId: string, data: any) => {
  const updated = await prisma.job.update({ where: { id: jobId }, data })
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
type Segment = { start: number; end: number; speed?: number; zoom?: number; brightness?: number; emphasize?: boolean }
type WebcamCrop = { x: number; y: number; width: number; height: number }
type HorizontalFitMode = 'cover' | 'contain'
type HorizontalModeOutput = 'quality' | 'source' | { width: number; height: number }
type HorizontalModeSettings = {
  output: HorizontalModeOutput
  fit: HorizontalFitMode
}
type VerticalFitMode = 'cover' | 'contain'
type VerticalWebcamCrop = { x: number; y: number; w: number; h: number }
type VerticalModeSettings = {
  enabled: boolean
  output: { width: number; height: number }
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
  textDensity: number
  sceneChangeRate: number
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
}
type HookSelectionDecision = {
  candidate: HookCandidate
  confidence: number
  threshold: number
  usedFallback: boolean
  reason: string | null
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
  beatAnchors?: number[]
  hookVariants?: HookCandidate[]
}
type EditOptions = {
  autoHookMove: boolean
  removeBoring: boolean
  onlyCuts: boolean
  smartZoom: boolean
  emotionalBoost: boolean
  aggressiveMode: boolean
  autoCaptions: boolean
  musicDuck: boolean
  subtitleStyle?: string | null
  autoZoomMax: number
  retentionAggressionLevel: RetentionAggressionLevel
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

const HOOK_MIN = 5
const HOOK_MAX = 8
const HOOK_RELOCATE_MIN_START = 6
const HOOK_RELOCATE_SCORE_TOLERANCE = 0.06
const CUT_MIN = 2
const CUT_MAX = 5
const PACE_MIN = 5
const PACE_MAX = 10
const CUT_GUARD_SEC = 0.35
const CUT_LEN_PATTERN = [2.8, 3.8, 3.2, 4.2]
const CUT_GAP_PATTERN = [0.9, 1.3, 1.0, 0.7]
const MAX_CUT_RATIO = 0.68
const AGGRESSIVE_MAX_CUT_RATIO = 0.74
const AGGRESSIVE_CUT_GAP_MULTIPLIER = 0.78
const ZOOM_HARD_MAX = 1.15
const ZOOM_MAX_DURATION_RATIO = 0.1
const ZOOM_EASE_SEC = 0.2
const STITCH_FADE_SEC = 0.08
const MIN_RENDER_SEGMENT_SECONDS = 0.08
const MERGE_ADJACENT_SEGMENT_GAP_SEC = 0.06
const FILTER_TIME_DECIMALS = 3
const MAX_RENDER_SEGMENTS = (() => {
  const envValue = Number(process.env.MAX_RENDER_SEGMENTS || 180)
  return Number.isFinite(envValue) && envValue > 0 ? Math.round(envValue) : 180
})()
const FILTER_COMPLEX_SCRIPT_THRESHOLD = (() => {
  const envValue = Number(process.env.FILTER_COMPLEX_SCRIPT_THRESHOLD || 16_000)
  return Number.isFinite(envValue) && envValue > 2_000 ? Math.round(envValue) : 16_000
})()
const SILENCE_DB = -30
const SILENCE_MIN = 0.8
const SILENCE_KEEP_PADDING_SEC = 0.2
const HOOK_ANALYZE_MAX = 1800
const SCENE_THRESHOLD = 0.45
const STRATEGIST_HOOK_WINDOW_SEC = 35
const STRATEGIST_LATE_HOOK_PENALTY_SEC = 55
const MAX_VERTICAL_CLIPS = 3
const MIN_VERTICAL_CLIP_SECONDS = 8
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
const HOOK_SELECTION_MAX_CANDIDATES = 5
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
  emotionalBoost: true,
  aggressiveMode: false,
  autoCaptions: false,
  musicDuck: true,
  subtitleStyle: DEFAULT_SUBTITLE_PRESET,
  autoZoomMax: 1.1,
  retentionAggressionLevel: 'medium'
}

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value))
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

const parseRetentionAggressionLevel = (value?: any): RetentionAggressionLevel => {
  const raw = String(value || '').trim().toLowerCase()
  if (raw === 'low') return 'low'
  if (raw === 'high') return 'high'
  if (raw === 'viral' || raw === 'max') return 'viral'
  return 'medium'
}

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

const parseVerticalClipCount = (value?: any) => {
  const parsed = Number.parseInt(String(value ?? ''), 10)
  if (!Number.isFinite(parsed)) return 1
  return clamp(parsed, 1, MAX_VERTICAL_CLIPS)
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

const getRetentionAggressionFromPayload = (payload?: any) => {
  if (!payload || typeof payload !== 'object') return DEFAULT_EDIT_OPTIONS.retentionAggressionLevel
  return parseRetentionAggressionLevel(
    (payload as any).retentionLevel ??
    (payload as any).retentionAggressionLevel ??
    (payload as any).aggressionLevel
  )
}

const getRetentionAggressionFromJob = (job?: any) => {
  const analysis = job?.analysis as any
  const settings = (job as any)?.renderSettings as any
  return parseRetentionAggressionLevel(
    settings?.retentionLevel ??
    settings?.retentionAggressionLevel ??
    analysis?.retentionLevel ??
    analysis?.retentionAggressionLevel ??
    DEFAULT_EDIT_OPTIONS.retentionAggressionLevel
  )
}

const buildPersistedRenderSettings = (
  renderConfig: RenderConfig,
  opts?: { retentionAggressionLevel?: RetentionAggressionLevel | null }
) => {
  const retentionLevel = parseRetentionAggressionLevel(opts?.retentionAggressionLevel || DEFAULT_EDIT_OPTIONS.retentionAggressionLevel)
  return {
    renderMode: renderConfig.mode,
    horizontalMode: renderConfig.horizontalMode,
    verticalClipCount: renderConfig.mode === 'vertical' ? renderConfig.verticalClipCount : 1,
    verticalMode: renderConfig.mode === 'vertical' ? renderConfig.verticalMode : null,
    retentionAggressionLevel: retentionLevel,
    retentionLevel
  }
}

const buildPersistedRenderAnalysis = ({
  existing,
  renderConfig,
  outputPaths
}: {
  existing?: any
  renderConfig: RenderConfig
  outputPaths?: string[] | null
}) => {
  const verticalCrop = renderConfig.verticalMode?.webcamCrop
    ? {
        x: renderConfig.verticalMode.webcamCrop.x,
        y: renderConfig.verticalMode.webcamCrop.y,
        width: renderConfig.verticalMode.webcamCrop.w,
        height: renderConfig.verticalMode.webcamCrop.h
      }
    : null
  return {
    ...(existing || {}),
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
  const args = [
    '-hide_banner',
    '-nostdin',
    '-y',
    '-i',
    filePath,
    '-t',
    String(analyzeSeconds),
    '-vf',
    'fps=2,scale=360:-1:flags=lanczos',
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
  const adjustedMin = clamp(profile.minLen + tempoShift, 3.2, 9)
  const adjustedMax = clamp(profile.maxLen + tempoShift * 1.25, adjustedMin + 0.8, 11.5)
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
    0.42 * window.audioEnergy +
    0.28 * (window.audioVariance ?? 0) +
    0.18 * window.vocalExcitement +
    0.12 * window.sceneChangeRate
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
  signalStrength
}: {
  aggressionLevel: RetentionAggressionLevel
  hasTranscript: boolean
  signalStrength: number
}): QualityGateThresholds => {
  const levelOffset = LEVEL_QUALITY_THRESHOLD_OFFSET[aggressionLevel]
  const transcriptOffset = hasTranscript ? 0 : -8
  const lowSignalPenalty = signalStrength < 0.42 ? -7 : signalStrength < 0.52 ? -4 : 0
  const highSignalBoost = signalStrength > 0.74 ? 2 : 0
  const baseOffset = levelOffset + transcriptOffset + lowSignalPenalty + highSignalBoost
  return normalizeQualityGateThresholds({
    hook_strength: clamp(
      QUALITY_GATE_THRESHOLDS.hook_strength + baseOffset,
      QUALITY_GATE_THRESHOLD_FLOORS.hook_strength,
      96
    ),
    emotional_pull: clamp(
      QUALITY_GATE_THRESHOLDS.emotional_pull + baseOffset,
      QUALITY_GATE_THRESHOLD_FLOORS.emotional_pull,
      94
    ),
    pacing_score: clamp(
      QUALITY_GATE_THRESHOLDS.pacing_score + Math.round(baseOffset * 0.6),
      QUALITY_GATE_THRESHOLD_FLOORS.pacing_score,
      94
    ),
    retention_score: clamp(
      QUALITY_GATE_THRESHOLDS.retention_score + Math.round(baseOffset * 0.85),
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
  faceSamples: { time: number; presence: number }[] = [],
  textSamples: { time: number; density: number }[] = [],
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
  for (const sample of faceSamples) {
    if (sample.time < 0 || sample.time >= totalSeconds) continue
    const idx = Math.floor(sample.time)
    const value = Number.isFinite(sample.presence) ? Math.max(0, Math.min(1, sample.presence)) : 0
    faceBySecond[idx] = Math.max(faceBySecond[idx], value)
  }

  const textBySecond = new Array(totalSeconds).fill(0)
  for (const sample of textSamples) {
    if (sample.time < 0 || sample.time >= totalSeconds) continue
    const idx = Math.floor(sample.time)
    const value = Number.isFinite(sample.density) ? Math.max(0, Math.min(1, sample.density)) : 0
    textBySecond[idx] = Math.max(textBySecond[idx], value)
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
    const motionScore = sceneChangeRate
    const facePresence = faceBySecond[i] || 0
    const textDensity = textBySecond[i] || 0
    const emotionalSpike = audioEnergy > meanEnergy + std * 1.5 ? 1 : 0
    const vocalExcitement = Math.min(1, Math.max(0, (audioEnergy - meanEnergy) / (std + 0.05)))
    const modelEmotion = emotionBySecond[i] || 0
    const emotionIntensity = Math.min(
      1,
      0.45 * speechIntensity + 0.2 * vocalExcitement + 0.15 * emotionalSpike + 0.2 * modelEmotion
    )
    const baseScore =
      0.2 * audioEnergy +
      0.2 * speechIntensity +
      0.15 * motionScore +
      0.15 * facePresence +
      0.15 * emotionIntensity +
      0.08 * textDensity +
      0.07 * vocalExcitement
    const hookPotential =
      0.32 * vocalExcitement +
      0.24 * emotionIntensity +
      0.2 * sceneChangeRate +
      0.14 * speechIntensity +
      0.1 * textDensity
    const introBias = i < 20 ? 0.06 : i < 40 ? 0.03 : 0
    const score = clamp01(baseScore * 0.82 + hookPotential * 0.18 + introBias)
    windows.push({
      time: i,
      audioEnergy,
      speechIntensity,
      motionScore,
      facePresence,
      textDensity,
      sceneChangeRate,
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
      0.16 * audioVariance +
      0.14 * window.speechIntensity +
      0.13 * window.emotionIntensity +
      0.12 * window.motionScore +
      0.11 * window.textDensity +
      0.12 * transcript.keywordIntensity +
      0.12 * transcript.curiosityTrigger +
      0.1 * window.facePresence
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
    averageWindowMetric(windows, start, end, (window) => window.emotionIntensity) * 0.5 +
    averageWindowMetric(windows, start, end, (window) => window.vocalExcitement) * 0.25 +
    averageWindowMetric(windows, start, end, (window) => window.speechIntensity) * 0.15 +
    averageWindowMetric(windows, start, end, (window) => window.motionScore) * 0.1
  )
  const nonVerbalClarity = clamp01(
    0.44 * emotionalSignal +
    0.22 * averageWindowMetric(windows, start, end, (window) => window.motionScore) +
    0.2 * averageWindowMetric(windows, start, end, (window) => window.speechIntensity) +
    0.14 * averageWindowMetric(windows, start, end, (window) => window.vocalExcitement)
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
  windows
}: {
  candidate: HookCandidate
  windows: EngagementWindow[]
}) => {
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
  const emotionalSpike = averageWindowMetric(windows, start, end, (window) => (
    0.7 * window.emotionIntensity + 0.3 * window.motionScore
  ))
  const faceoffScore = clamp01(
    0.4 * candidate.score +
    0.24 * candidate.auditScore +
    0.16 * energy +
    0.1 * curiosity +
    0.1 * emotionalSpike
  )
  return Number(faceoffScore.toFixed(4))
}

const pickTopHookCandidates = ({
  durationSeconds,
  segments,
  windows,
  transcriptCues
}: {
  durationSeconds: number
  segments: TimeRange[]
  windows: EngagementWindow[]
  transcriptCues: TranscriptCue[]
}) => {
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
      const durationSecondsActual = aligned.end - aligned.start
      const durationAlignment = clamp01(1 - (Math.abs(durationSecondsActual - 8) / 3))
      const contextPenalty = evaluateHookContextDependency(aligned.start, aligned.end, transcriptCues)
      const audit = runHookAudit({
        start: aligned.start,
        end: aligned.end,
        transcriptCues,
        windows
      })
      const totalScore = clamp01(
        0.28 * baseHookScore +
        0.2 * speechImpact +
        0.16 * transcriptImpact +
        0.14 * visualImpact +
        0.11 * emotionImpact +
        0.07 * durationAlignment +
        0.14 * audit.auditScore -
        0.2 * contextPenalty
      )
      evaluated.push({
        start: aligned.start,
        duration: Number((aligned.end - aligned.start).toFixed(3)),
        score: Number(totalScore.toFixed(4)),
        auditScore: audit.auditScore,
        auditPassed: audit.passed,
        text: extractHookText(aligned.start, aligned.end, transcriptCues),
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
        scoreHookFaceoffCandidate({ candidate: b, windows }) - scoreHookFaceoffCandidate({ candidate: a, windows }) ||
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
      faceoffScore: scoreHookFaceoffCandidate({ candidate, windows })
    }))
    .sort((a, b) => (
      b.faceoffScore - a.faceoffScore ||
      b.candidate.score - a.candidate.score ||
      a.candidate.start - b.candidate.start
    ))
  let selected = (faceoffRanked.find((entry) => entry.candidate.auditPassed) || faceoffRanked[0]).candidate
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
  aggressionLevel
}: {
  segments: Segment[]
  durationSeconds: number
  aggressionLevel: RetentionAggressionLevel
}) => {
  const preset = RETENTION_AGGRESSION_PRESET[aggressionLevel]
  if (!segments.length || durationSeconds <= 0) return { segments, count: 0, density: 0 }
  const out = segments.map((segment) => ({ ...segment }))
  const runtimeSeconds = Math.max(0.1, computeEditedRuntimeSeconds(out))
  const requiredInterval = runtimeSeconds <= 90 ? 4 : 6
  const minimumInterruptCount = Math.max(1, Math.ceil(runtimeSeconds / requiredInterval))
  let cursor = preset.patternIntervalMin
  let count = 0
  while (cursor < durationSeconds) {
    const segment = out.find((item) => item.start <= cursor && item.end >= cursor)
    if (segment) {
      segment.zoom = Math.max(segment.zoom ?? 0, Number((0.03 * preset.zoomBoost).toFixed(3)))
      segment.brightness = Math.max(segment.brightness ?? 0, 0.02)
      ;(segment as any).emphasize = true
      count += 1
    }
    const interval = count % 2 === 0 ? preset.patternIntervalMax : preset.patternIntervalMin
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
  if (!hasFfmpeg()) return [] as { time: number; presence: number }[]
  if (!hasFaceDetectFilter()) return [] as { time: number; presence: number }[]
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
  const sampleMap = new Map<number, number>()
  for (const line of lines) {
    if (!line.includes('lavfi.facedetect')) continue
    const timeMatch = line.match(/pts_time:([0-9.]+)/)
    if (!timeMatch) continue
    const time = Number.parseFloat(timeMatch[1])
    if (!Number.isFinite(time)) continue
    const bucket = Math.floor(time)
    sampleMap.set(bucket, 1)
  }
  return Array.from(sampleMap.entries()).map(([time, presence]) => ({ time, presence }))
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

const detectTextDensity = async (_filePath: string, _durationSeconds: number) => {
  return [] as { time: number; density: number }[]
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
  const cutHalfLength = aggressiveMode ? 2.8 : 2.5
  const candidates = windows
    .filter((window) => window.time >= edgePadding && window.time <= Math.max(edgePadding, durationSeconds - 6))
    .map((window) => ({
      start: Math.max(0, window.time - 1),
      end: Math.min(durationSeconds, window.time + cutHalfLength),
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
  const cutLength = aggressiveMode ? 3.5 : 3.1
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
    .filter((entry) => entry.end - entry.start >= (aggressiveMode ? 1.9 : 2.2))
    .sort((a, b) => a.score - b.score)

  const targetCuts = clamp(
    Math.ceil(missingSeconds / Math.max(1.8, cutLength - 0.2)),
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

  const cutLength = aggressiveMode ? 2.9 : 2.4
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
    hookFailureReason: fallbackHook.auditPassed ? null : fallbackHook.reason
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
  const keepPadding = aggressiveMode ? 0.14 : SILENCE_KEEP_PADDING_SEC
  const minTrim = aggressiveMode ? 0.32 : 0.45
  const edgePadding = aggressiveMode ? 0.4 : 0.55
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
  const minLen = Number(clamp(base.minLen - aggressiveShift * 0.6 - shortFormFactor * 0.2, 3.8, PACE_MAX).toFixed(2))
  const maxLen = Number(clamp(base.maxLen - aggressiveShift * 0.8 - shortFormFactor * 0.5, minLen + 1, 11).toFixed(2))
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
  const tasks: Array<Promise<any>> = []
  tasks.push(detectSilences(filePath, durationSeconds).catch(() => []))
  tasks.push(detectAudioEnergy(filePath, durationSeconds).catch(() => []))
  tasks.push(detectSceneChanges(filePath, durationSeconds).catch(() => []))
  // Face detection is optional if smartZoom is disabled (saves time)
  if (options.smartZoom !== false) {
    tasks.push(detectFacePresence(filePath, durationSeconds).catch(() => []))
  } else {
    tasks.push(Promise.resolve([]))
  }
  tasks.push(detectTextDensity(filePath, durationSeconds).catch(() => []))
  tasks.push(detectEmotionModelSignals(filePath, durationSeconds).catch(() => []))
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
  const pacingProfile = applyStyleToPacingProfile(
    inferPacingProfile(windows, durationSeconds, options.aggressiveMode),
    styleProfile,
    options.aggressiveMode
  )
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
  let candidateRemovedSegments = mergeRanges([
    ...(detectedRemovedSegments.length ? detectedRemovedSegments : fallbackRemovedSegments),
    ...silenceTrimCuts
  ])
  let removedSegments = options.removeBoring
    ? applyContinuityGuardsToCuts(candidateRemovedSegments, windows, options.aggressiveMode)
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
        candidateRemovedSegments = mergeRanges([...candidateRemovedSegments, ...rescueCuts])
        const guarded = applyContinuityGuardsToCuts(candidateRemovedSegments, windows, true)
        const guardedRemovedSeconds = getRangesDurationSeconds(guarded)
        removedSegments = guardedRemovedSeconds >= removedSeconds + 0.5
          ? guarded
          : candidateRemovedSegments
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
    transcriptCues
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
  const interruptInjected = injectPatternInterrupts({
    segments: boredomApplied.segments,
    durationSeconds,
    aggressionLevel: styleAdjustedAggressionLevel
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
  const rhythmAlignedSegments = alignSegmentsToRhythm({
    segments: endingSpikeSegments,
    durationSeconds,
    anchors: beatAnchors,
    styleProfile
  })

  return {
    hook,
    segments: rhythmAlignedSegments,
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
    patternInterruptCount: interruptInjected.count,
    patternInterruptDensity: interruptInjected.density,
    boredomRemovedRatio: Number(clamp01(getRangesDurationSeconds(boredomApplied.removedRanges) / Math.max(0.1, durationSeconds)).toFixed(4)),
    storyReorderMap: rhythmAlignedSegments.map((segment, orderedIndex) => ({
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
    beatAnchors,
    hookVariants
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
    zoom = Math.min(maxZoomDelta || 0, zoom)
    return { ...seg, zoom, brightness, emphasize: Boolean(hasSpike || speechPeak || motionEmphasis || score?.isHook) }
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
      brightness: 0
    })
  }
  return merged
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
    const vZoom = zoom > 0 ? `,scale=iw*${1 + zoom}:ih*${1 + zoom},crop=iw:ih` : ''
    const vBright = brightness !== 0 ? `,eq=brightness=${brightness}:saturation=1.05` : ''
    const nonZoomLabel = nonZoomSourceLabels.get(idx)
    if (nonZoomLabel) {
      parts.push(`[${nonZoomLabel}]${vTrim}${vSpeed}${vBright}[v${idx}]`)
    } else {
      parts.push(`[0:v]${vTrim}${vSpeed}${vZoom}${vBright},${scalePad}[v${idx}]`)
    }

    if (opts.withAudio) {
      const aSpeed = speed !== 1 ? buildAtempoChain(speed) : ''
      const aNormalize = 'aformat=sample_rates=48000:channel_layouts=stereo'
      const fadeLen = roundForFilter(0.04)
      const afadeIn = `afade=t=in:st=0:d=${toFilterNumber(fadeLen)}`
      const afadeOut = `afade=t=out:st=${toFilterNumber(Math.max(0, segDuration - fadeLen))}:d=${toFilterNumber(fadeLen)}`
      if (opts.hasAudioStream) {
        const guard = roundForFilter(0.04)
        const aTrim = `atrim=start=${toFilterNumber(Math.max(0, seg.start - guard))}:end=${toFilterNumber(seg.end + guard)}`
        const chain = [aTrim, 'asetpts=PTS-STARTPTS', aSpeed, afadeIn, afadeOut, aNormalize].filter(Boolean).join(',')
        parts.push(`[0:a]${chain}[a${idx}]`)
      } else {
        const chain = [`anullsrc=r=48000:cl=stereo`, `atrim=duration=${toFilterNumber(segDuration)}`, 'asetpts=PTS-STARTPTS', aSpeed, afadeIn, afadeOut, aNormalize]
          .filter(Boolean)
          .join(',')
        parts.push(`${chain}[a${idx}]`)
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
    const fade = Math.min(STITCH_FADE_SEC, (durations[i - 1] || STITCH_FADE_SEC) / 2, (durations[i] || STITCH_FADE_SEC) / 2)
    const offset = Math.max(0, roundForFilter(cumulative - fade))
    const outLabel = `vx${i}`
    parts.push(`[${vPrev}][v${i}]xfade=transition=fade:duration=${toFilterNumber(fade)}:offset=${toFilterNumber(offset)}[${outLabel}]`)
    fades.push(fade)
    vPrev = outLabel
    cumulative += (durations[i] || 0) - fade
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

const buildSubtitleStyle = (style?: string | null) => {
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
  const styles: Record<string, Partial<typeof base>> = {
    minimal: {},
    basicclean: { FontSize: '40', Outline: '1' },
    clean: { FontSize: '40', Outline: '1' },
    bold: { FontName: 'DejaVu Sans', FontSize: '48', Outline: '3', Shadow: '1' },
    boldpop: { FontName: 'DejaVu Sans', FontSize: '48', Outline: '3', Shadow: '1', PrimaryColour: '&H0000FFFF' },
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
  const key = (style || DEFAULT_SUBTITLE_PRESET).toLowerCase().replace(/[\s_-]/g, '')
  const selection = styles[key] || styles.minimal
  const merged = { ...base, ...selection }
  return Object.entries(merged)
    .map(([k, v]) => `${k}=${v}`)
    .join(',')
}

const generateSubtitles = async (inputPath: string, workingDir: string) => {
  const whisperBin = process.env.WHISPER_BIN
  if (!whisperBin) return null
  const model = process.env.WHISPER_MODEL || 'base'
  const extraArgs = process.env.WHISPER_ARGS ? process.env.WHISPER_ARGS.split(' ') : []
  const args = extraArgs.length
    ? extraArgs
    : ['--model', model, '--output_format', 'srt', '--output_dir', workingDir, '--word_timestamps', 'True']
  return new Promise<string | null>((resolve) => {
    const proc = spawn(whisperBin, [inputPath, ...args])
    proc.on('error', () => resolve(null))
    proc.on('close', (code) => {
      if (code !== 0) return resolve(null)
      const baseName = path.basename(inputPath, path.extname(inputPath))
      const output = path.join(workingDir, `${baseName}.srt`)
      if (!fs.existsSync(output)) return resolve(null)
      resolve(output)
    })
  })
}

const generateProxy = async (inputPath: string, outPath: string, opts?: { width?: number; height?: number }) => {
  const width = opts?.width ?? 960
  const height = opts?.height ?? 540
  const scale = `scale='min(${width},iw)':'min(${height},ih)':force_original_aspect_ratio=decrease,pad=${width}:${height}:(ow-iw)/2:(oh-ih)/2,setsar=1`
  const args = ['-hide_banner', '-nostdin', '-y', '-i', inputPath, '-vf', scale, '-c:v', 'libx264', '-preset', 'superfast', '-crf', '28', '-threads', '0', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-c:a', 'copy', outPath]
  await runFfmpeg(args)
}

const buildVerticalClipRanges = (durationSeconds: number, requestedCount: number) => {
  const total = Math.max(0, durationSeconds || 0)
  if (total <= 0) return [{ start: 0, end: 0 }]
  let clipCount = clamp(requestedCount || 1, 1, MAX_VERTICAL_CLIPS)
  const maxFeasibleByLength = Math.max(1, Math.floor(total / MIN_VERTICAL_CLIP_SECONDS))
  clipCount = Math.min(clipCount, maxFeasibleByLength)
  const chunk = total / clipCount
  const ranges: TimeRange[] = []
  for (let index = 0; index < clipCount; index += 1) {
    const start = Number((index * chunk).toFixed(3))
    const end = Number((index === clipCount - 1 ? total : (index + 1) * chunk).toFixed(3))
    if (end - start > 0.2) ranges.push({ start, end })
  }
  return ranges.length ? ranges : [{ start: 0, end: total }]
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
  let x = 0
  let y = defaultY
  let w = sourceWidth
  let h = defaultHeight

  if (crop) {
    const rawValues = [crop.x, crop.y, crop.w, crop.h]
    if (rawValues.some((value) => !Number.isFinite(value))) {
      throw new Error('invalid_webcam_crop_values')
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
    throw new Error('invalid_webcam_crop_values')
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

const renderVerticalClip = async ({
  inputPath,
  outputPath,
  start,
  end,
  verticalMode,
  sourceWidth,
  sourceHeight,
  withAudio
}: {
  inputPath: string
  outputPath: string
  start: number
  end: number
  verticalMode: VerticalModeSettings
  sourceWidth: number
  sourceHeight: number
  withAudio: boolean
}) => {
  const outputWidth = Math.round(clamp(verticalMode.output.width, 240, 4320))
  const outputHeight = Math.round(clamp(verticalMode.output.height, 426, 7680))
  const topHeight = computeVerticalTopHeightPx(verticalMode, outputHeight)
  const sourceCrop = normalizeVerticalCropToSource({
    crop: verticalMode.webcamCrop,
    sourceWidth,
    sourceHeight
  })
  const filterComplex = buildVerticalStackedFilterGraph({
    start,
    end,
    crop: sourceCrop,
    outputWidth,
    outputHeight,
    topHeight,
    bottomFit: parseVerticalFitMode(verticalMode.bottomFit, 'cover'),
    withAudio
  })

  const args = [
    '-y',
    '-nostdin',
    '-hide_banner',
    '-loglevel',
    'error',
    '-i',
    inputPath,
    '-movflags',
    '+faststart',
    '-c:v',
    'libx264',
    '-preset',
    process.env.FFMPEG_PRESET || 'medium',
    '-crf',
    process.env.FFMPEG_CRF || '20',
    '-threads',
    '0',
    '-pix_fmt',
    'yuv420p',
    '-filter_complex',
    filterComplex,
    '-map',
    '[outv]'
  ]
  if (withAudio) {
    args.push('-map', '[outa]', '-c:a', 'aac')
  } else {
    args.push('-an')
  }
  args.push(outputPath)
  await runFfmpeg(args)
}

const buildAudioFilters = () => {
  return [
    'highpass=f=80',
    'lowpass=f=16000',
    'afftdn',
    'acompressor=threshold=-15dB:ratio=3:attack=20:release=250',
    'loudnorm=I=-14:TP=-1.5:LRA=11'
  ]
}

const RETENTION_RENDER_THRESHOLD = 58

const computeRetentionScore = (
  segments: Segment[],
  windows: EngagementWindow[],
  hookScore: number,
  captionsEnabled: boolean,
  extras?: {
    removedRanges?: TimeRange[]
    patternInterruptCount?: number
  }
) => {
  const runtimeSeconds = Math.max(1, computeEditedRuntimeSeconds(segments))
  const interruptTargetInterval = runtimeSeconds <= 90 ? 4 : 6
  const interruptTargetCount = Math.max(1, Math.ceil(runtimeSeconds / interruptTargetInterval))
  const lengths = segments.map((seg) => seg.end - seg.start).filter((len) => len > 0)
  const avgLen = lengths.length ? lengths.reduce((sum, len) => sum + len, 0) / lengths.length : 0
  const pacingScore = avgLen > 0 ? Math.max(0, 1 - Math.abs(avgLen - 3.8) / 5.8) : 0.5
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
    0.24 * hook +
    0.16 * consistency +
    0.18 * pacingScore +
    0.14 * boredomRemovalRatio +
    0.12 * emotionalSpikeDensity +
    0.1 * interruptDensity +
    0.06 * subtitleScore +
    0.1 * audioScore
  ))
  const notes: string[] = []
  if (avgLen > 6) notes.push('Pacing is slower than short-form optimal; consider aggressive mode.')
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
      runtimeSeconds: Number(runtimeSeconds.toFixed(3))
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
  thresholds
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
}): RetentionJudgeReport => {
  const appliedThresholds = normalizeQualityGateThresholds(thresholds)
  const runtimeSeconds = Math.max(1, computeEditedRuntimeSeconds(segments))
  const interruptIntervalTarget = runtimeSeconds <= 90 ? 4 : 6
  const interruptTargetCount = Math.max(1, Math.ceil(runtimeSeconds / interruptIntervalTarget))
  const interruptCoverage = clamp01(patternInterruptCount / interruptTargetCount)
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
  if (interruptCoverage < 0.95) whatIsGeneric.push('Interrupt density is below retention target for this runtime.')
  if (retentionScore.details.boredomRemovalRatio < 0.07) whatIsGeneric.push('Too much low-arousal material remains.')
  if (!hook.auditPassed) whatIsGeneric.push('Hook is not fully understandable without prior context.')
  if (whatIsGeneric.length === 0) whatIsGeneric.push('Generic signals are low for this attempt.')

  return {
    retention_score: retention,
    hook_strength: hookStrength,
    pacing_score: pacing,
    clarity_score: clarity,
    emotional_pull: emotionalPull,
    why_keep_watching: whyKeepWatching.slice(0, 3),
    what_is_generic: whatIsGeneric.slice(0, 3),
    required_fixes: {
      stronger_hook: hookStrength < appliedThresholds.hook_strength,
      raise_emotion: emotionalPull < appliedThresholds.emotional_pull,
      improve_pacing: pacing < appliedThresholds.pacing_score,
      increase_interrupts: interruptCoverage < 0.95
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
  overrides?: { retentionAggressionLevel?: RetentionAggressionLevel | null }
) => {
  const settings = await prisma.userSettings.findUnique({ where: { userId } })
  const { tier, plan } = await getUserPlan(userId)
  const features = getPlanFeatures(tier)
  const subtitlesEnabled = features.subtitles.enabled
  const rawSubtitle = settings?.subtitleStyle ?? DEFAULT_SUBTITLE_PRESET
  const normalizedSubtitle = normalizeSubtitlePreset(rawSubtitle) ?? DEFAULT_SUBTITLE_PRESET
  const subtitleStyle =
    subtitlesEnabled && isSubtitlePresetAllowed(normalizedSubtitle, tier) ? rawSubtitle : DEFAULT_SUBTITLE_PRESET
  const onlyCuts = settings?.onlyCuts ?? DEFAULT_EDIT_OPTIONS.onlyCuts
  const removeBoring = onlyCuts ? true : settings?.removeBoring ?? DEFAULT_EDIT_OPTIONS.removeBoring
  const requestedAggression = parseRetentionAggressionLevel(
    overrides?.retentionAggressionLevel ??
    (settings?.aggressiveMode ? 'high' : DEFAULT_EDIT_OPTIONS.retentionAggressionLevel)
  )
  const allowedAggression: RetentionAggressionLevel =
    features.advancedEffects ? requestedAggression : (requestedAggression === 'low' ? 'low' : 'medium')
  const aggressiveMode = onlyCuts ? false : isAggressiveRetentionLevel(allowedAggression)
  return {
    options: {
      autoHookMove: onlyCuts ? false : (settings?.autoHookMove ?? DEFAULT_EDIT_OPTIONS.autoHookMove),
      removeBoring,
      onlyCuts,
      smartZoom: onlyCuts ? false : (settings?.smartZoom ?? DEFAULT_EDIT_OPTIONS.smartZoom),
      emotionalBoost: onlyCuts ? false : (features.advancedEffects ? (settings?.emotionalBoost ?? DEFAULT_EDIT_OPTIONS.emotionalBoost) : false),
      aggressiveMode,
      autoCaptions: onlyCuts ? false : (subtitlesEnabled ? (settings?.autoCaptions ?? DEFAULT_EDIT_OPTIONS.autoCaptions) : false),
      musicDuck: onlyCuts ? false : (settings?.musicDuck ?? DEFAULT_EDIT_OPTIONS.musicDuck),
      subtitleStyle,
      autoZoomMax: settings?.autoZoomMax ?? plan.autoZoomMax,
      retentionAggressionLevel: allowedAggression
    } as EditOptions,
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

    const aggressionLevel = parseRetentionAggressionLevel(
      (job.analysis as any)?.retentionLevel ??
      (job.analysis as any)?.retentionAggressionLevel ??
      (job as any)?.renderSettings?.retentionLevel ??
      (job as any)?.renderSettings?.retentionAggressionLevel ??
      options.retentionAggressionLevel
    )

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
              { ...options, retentionAggressionLevel: aggressionLevel },
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
              { transcriptCues, aggressionLevel }
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
            topCandidates: editPlan.hookCandidates ?? []
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
    const renderConfig = parseRenderConfigFromAnalysis(existingAnalysis, (freshJob as any)?.renderSettings ?? (job as any)?.renderSettings)
    const analysis = buildPersistedRenderAnalysis({
      existing: {
        ...existingAnalysis,
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
        boredom_ranges: editPlan?.boredomRanges ?? [],
        boredom_removed_ratio: editPlan?.boredomRemovedRatio ?? 0,
        retentionAggressionLevel: aggressionLevel,
        retentionLevel: aggressionLevel,
        style_profile: editPlan?.styleProfile ?? null,
        beat_anchors: editPlan?.beatAnchors ?? [],
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
      renderConfig
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
      renderSettings: buildPersistedRenderSettings(renderConfig, { retentionAggressionLevel: aggressionLevel }),
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
  const aggressionLevel = parseRetentionAggressionLevel(
    getRetentionAggressionFromJob(job) || options.retentionAggressionLevel
  )
  const rawSubtitleStyle = options.subtitleStyle ?? settings?.subtitleStyle ?? DEFAULT_SUBTITLE_PRESET
  const normalizedSubtitle = normalizeSubtitlePreset(rawSubtitleStyle) ?? DEFAULT_SUBTITLE_PRESET
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
  const subtitleStyle = rawSubtitleStyle
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

    await ensureUsageWithinLimits(user.id, user.email, durationMinutes, tier, plan, renderConfig.mode)

    if (renderConfig.mode === 'vertical') {
      const sourceStream = probeVideoStream(tmpIn)
      if (!sourceStream?.width || !sourceStream?.height) {
        throw new Error('vertical_source_dimensions_unavailable')
      }
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
      const clipRanges = buildVerticalClipRanges(durationSeconds || 0, renderConfig.verticalClipCount)
      const renderedClipPaths: string[] = []
      const outputPaths: string[] = []
      const hasInputAudio = hasAudioStream(tmpIn)
      const localOutDir = path.join(process.cwd(), 'outputs', job.userId, jobId)
      fs.mkdirSync(localOutDir, { recursive: true })

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
        await renderVerticalClip({
          inputPath: tmpIn,
          outputPath: localClipPath,
          start: range.start,
          end: range.end,
          verticalMode: resolvedVerticalMode,
          sourceWidth: sourceStream.width,
          sourceHeight: sourceStream.height,
          withAudio: hasInputAudio
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
      const nextAnalysis = buildPersistedRenderAnalysis({
        existing: {
          ...((job.analysis as any) || {}),
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
        renderSettings: buildPersistedRenderSettings(finalRenderConfig, { retentionAggressionLevel: aggressionLevel }),
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
    let optimizationNotes: string[] = []
    let retentionAttempts: RetentionAttemptRecord[] = []
    let selectedJudge: RetentionJudgeReport | null = null
    let selectedHook: HookCandidate | null = null
    let selectedPatternInterruptCount = 0
    let selectedPatternInterruptDensity = 0
    let selectedBoredomRemovalRatio = 0
    let selectedStrategy: RetentionRetryStrategy = 'BASELINE'
    let selectedStoryReorderMap: Array<{ sourceStart: number; sourceEnd: number; orderedIndex: number }> = []
    let hasTranscriptSignals = false
    let contentSignalStrength = 0.42
    let qualityGateOverride: { applied: boolean; reason: string } | null = null
    let styleProfileForAnalysis: ContentStyleProfile | null = ((job.analysis as any)?.style_profile as ContentStyleProfile) || null
    let beatAnchorsForAnalysis: number[] = Array.isArray((job.analysis as any)?.beat_anchors)
      ? ((job.analysis as any).beat_anchors as number[])
      : []
    let hookVariantsForAnalysis: HookCandidate[] = Array.isArray((job.analysis as any)?.hook_variants)
      ? ((job.analysis as any).hook_variants as HookCandidate[])
      : []
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
          editPlan = await buildEditPlan(tmpIn, durationSeconds, options, undefined, { aggressionLevel })
        } catch (err) {
          console.warn(`[${requestId || 'noid'}] edit-plan generation failed during process, using deterministic fallback`, err)
          editPlan = buildDeterministicFallbackEditPlan(durationSeconds, options)
          optimizationNotes.push('AI edit plan fallback: deterministic rescue plan used.')
        }
      }
      if (editPlan?.styleProfile) styleProfileForAnalysis = editPlan.styleProfile
      if (Array.isArray(editPlan?.beatAnchors)) beatAnchorsForAnalysis = editPlan.beatAnchors
      if (Array.isArray(editPlan?.hookVariants) && editPlan.hookVariants.length) {
        hookVariantsForAnalysis = editPlan.hookVariants
      } else if (Array.isArray(editPlan?.hookCandidates) && editPlan.hookCandidates.length) {
        hookVariantsForAnalysis = editPlan.hookCandidates
      }

      await updateJob(jobId, { status: 'story', progress: 55 })

      const baseSegments: Segment[] = editPlan
        ? editPlan.segments
        : buildGuaranteedFallbackSegments(durationSeconds || 0, options)
      const storySegments = editPlan && !options.onlyCuts
        ? applyStoryStructure(baseSegments, editPlan.engagementWindows, durationSeconds, editPlan.styleProfile)
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
        signalStrength: contentSignalStrength
      })
      qualityGateOverride = null
      const hookCandidates = (
        editPlan?.hookCandidates?.length
          ? editPlan.hookCandidates
          : (editPlan?.hook ? [editPlan.hook] : [])
      ).filter(Boolean)
      const hookDecision = selectRenderableHookCandidate({
        candidates: hookCandidates,
        aggressionLevel,
        hasTranscript: hasTranscriptSignals,
        signalStrength: contentSignalStrength
      })
      let resolvedHookDecision: HookSelectionDecision | null = hookDecision
      if (!resolvedHookDecision) {
        const fallbackStart = Number((storySegments[0]?.start ?? 0).toFixed(3))
        const fallbackDuration = Number(
          clamp(
            storySegments[0] ? (storySegments[0].end - storySegments[0].start) : 6,
            HOOK_MIN,
            HOOK_MAX
          ).toFixed(3)
        )
        const fallbackHook: HookCandidate = {
          start: fallbackStart,
          duration: fallbackDuration,
          score: 0.48,
          auditScore: 0.46,
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
        optimizationNotes.push('Hook fallback applied: no strong candidate passed; used earliest stable highlight.')
      }
      if (!resolvedHookDecision) {
        throw new HookGateError('Hook candidate unavailable for render after fallback resolution')
      }
      const initialHook = resolvedHookDecision.candidate
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

      const applyPacingRetry = (segments: Segment[]) => {
        if (!editPlan) return segments
        const stricter = enforceSegmentLengths(
          segments.map((segment) => ({ ...segment })),
          2.5,
          4,
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
        const hookRange: TimeRange = {
          start: hookCandidate.start,
          end: Number((hookCandidate.start + hookCandidate.duration).toFixed(3))
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
          story = reorderForEmotion(story)
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
            story = enforceSegmentLengths(story, 2.2, 3.2, editPlan.engagementWindows).map((segment) => {
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
        const effected = editPlan && !options.onlyCuts
          ? applySegmentEffects(
              ordered,
              editPlan.engagementWindows,
              { ...options, aggressiveMode: options.aggressiveMode || strategy !== 'BASELINE' },
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
          aggressionLevel: styleAdjustedInterruptAggression
        })
        const withZoom = editPlan && !options.onlyCuts
          ? applyZoomEasing(interruptInjected.segments)
          : interruptInjected.segments
        return {
          hook: hookCandidate,
          hookRange,
          segments: withZoom,
          patternInterruptCount: interruptInjected.count,
          patternInterruptDensity: interruptInjected.density
        }
      }

      let finalSegments: Segment[] = []
      const attemptStrategies: RetentionRetryStrategy[] = ['BASELINE', 'HOOK_FIRST', 'EMOTION_FIRST', 'PACING_FIRST']
      for (let attemptIndex = 0; attemptIndex < attemptStrategies.length; attemptIndex += 1) {
        const strategy = attemptStrategies[attemptIndex]
        const hookCandidate =
          strategy === 'HOOK_FIRST'
            ? (orderedHookCandidates[1] || orderedHookCandidates[0] || initialHook)
            : strategy === 'EMOTION_FIRST'
              ? (orderedHookCandidates[2] || orderedHookCandidates[0] || initialHook)
              : initialHook
        const attempt = buildAttemptSegments(strategy, hookCandidate)
        const retention = computeRetentionScore(
          attempt.segments,
          editPlan?.engagementWindows ?? [],
          hookCandidate.score,
          options.autoCaptions,
          {
            removedRanges: editPlan?.removedSegments ?? [],
            patternInterruptCount: attempt.patternInterruptCount
          }
        )
        const clarityPenalty = hookCandidate.auditPassed
          ? 0.08
          : hasTranscriptSignals
            ? 0.3
            : 0.2
        const judge = buildRetentionJudgeReport({
          retentionScore: retention,
          hook: hookCandidate,
          windows: editPlan?.engagementWindows ?? [],
          clarityPenalty,
          captionsEnabled: options.autoCaptions,
          patternInterruptCount: attempt.patternInterruptCount,
          removedRanges: editPlan?.removedSegments ?? [],
          segments: attempt.segments,
          thresholds: qualityGateThresholds
        })
        retentionAttempts.push({
          attempt: attemptIndex + 1,
          strategy,
          judge,
          hook: hookCandidate,
          patternInterruptCount: attempt.patternInterruptCount,
          patternInterruptDensity: attempt.patternInterruptDensity,
          boredomRemovalRatio: retention.details.boredomRemovalRatio
        })
        if (judge.passed || attemptIndex >= MAX_QUALITY_GATE_RETRIES) {
          finalSegments = attempt.segments
          selectedHook = hookCandidate
          selectedJudge = judge
          retentionScore = judge.retention_score
          selectedPatternInterruptCount = attempt.patternInterruptCount
          selectedPatternInterruptDensity = attempt.patternInterruptDensity
          selectedBoredomRemovalRatio = retention.details.boredomRemovalRatio
          selectedStrategy = strategy
          selectedStoryReorderMap = finalSegments.map((segment, orderedIndex) => ({
            sourceStart: Number(segment.start.toFixed(3)),
            sourceEnd: Number(segment.end.toFixed(3)),
            orderedIndex
          }))
          optimizationNotes = [
            ...optimizationNotes,
            ...retention.notes,
            ...judge.why_keep_watching.map((line) => `Why keep watching: ${line}`)
          ]
          break
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
            contentSignalStrength: Number(contentSignalStrength.toFixed(4))
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
            contentSignalStrength: Number(contentSignalStrength.toFixed(4))
          }
        })
        await updateJob(jobId, { status: 'failed', error: `FAILED_QUALITY_GATE: ${reason}` })
        throw new QualityGateError(reason, {
          attempts: retentionAttempts,
          thresholds: qualityGateThresholds,
          hasTranscriptSignals,
          contentSignalStrength: Number(contentSignalStrength.toFixed(4))
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
          const rescueHookCandidate = orderedHookCandidates.find((candidate) => candidate.auditPassed) || initialHook
          const rescueAttempt = buildAttemptSegments('RESCUE_MODE', rescueHookCandidate)
          const rescueRetention = computeRetentionScore(
            rescueAttempt.segments,
            editPlan?.engagementWindows ?? [],
            rescueHookCandidate.score,
            options.autoCaptions,
            {
              removedRanges: editPlan?.removedSegments ?? [],
              patternInterruptCount: rescueAttempt.patternInterruptCount
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
            thresholds: rescueThresholds
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
        if (!subtitlePath) optimizationNotes.push('Auto subtitles skipped: no caption engine available.')
      }

      await updateJob(jobId, { status: 'audio', progress: 68 })

      const hasAudio = hasAudioStream(tmpIn)
      const withAudio = true
      const audioFilters = withAudio ? buildAudioFilters() : []
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

      await updatePipelineStepState(jobId, 'RENDER_FINAL', {
        status: 'running',
        attempts: 1,
        startedAt: toIsoNow(),
        lastError: null
      })
      await updateJob(jobId, { status: 'rendering', progress: 80 })

      const hasSegments = finalSegments.length >= 1
      const ffPreset = (options as any)?.fastMode
        ? 'superfast'
        : (process.env.FFMPEG_PRESET || 'medium')
      const defaultCrf = finalQuality === '4k' ? '18' : finalQuality === '1080p' ? '20' : '22'
      const ffCrf = (options as any)?.fastMode
        ? '28'
        : (process.env.FFMPEG_CRF || defaultCrf)
      const argsBase = [
        '-y',
        '-nostdin',
        '-hide_banner',
        '-loglevel',
        'error',
        '-filter_threads',
        '1',
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
      if (withAudio) argsBase.push('-c:a', 'aac')

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
      const subtitleFilter = subtitlePath ? `subtitles=${escapeFilterPath(subtitlePath)}:force_style='${buildSubtitleStyle(subtitleStyle)}'` : ''

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
              if (hasAudio) {
                const aChain = [
                  `[0:a]atrim=start=${toFilterNumber(Math.max(0, seg.start - 0.02))}:end=${toFilterNumber(seg.end + 0.02)}`,
                  'asetpts=PTS-STARTPTS',
                  aSpeed,
                  'aformat=sample_rates=48000:channel_layouts=stereo[aout]'
                ].filter(Boolean).join(',')
                filterParts.push(aChain)
              } else {
                const aChain = [
                  'anullsrc=r=48000:cl=stereo',
                  `atrim=duration=${toFilterNumber(segDuration)}`,
                  'asetpts=PTS-STARTPTS',
                  aSpeed,
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
            if (withAudio) concatEncodeArgs.push('-c:a', 'aac')
            concatEncodeArgs.push(tmpOut)
            await runFfmpeg(concatEncodeArgs)
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
          const videoChains = [fullVideoChain, ''].filter((value, idx, arr) => arr.indexOf(value) === idx)

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
            for (const enableFades of [true, false]) {
              try {
                await runWithChain(chain, enableFades)
                ran = true
                if (chain !== fullVideoChain) {
                  const reason = lastErr ? summarizeFfmpegError(lastErr) : 'ffmpeg_failed'
                  optimizationNotes.push(`Render fallback: without subtitles/watermark (${reason}).`)
                }
                if (!enableFades) {
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

    const nextAnalysis = buildPersistedRenderAnalysis({
      existing: {
        ...((job.analysis as any) || {}),
        hook_start_time: selectedHook?.start ?? (job.analysis as any)?.hook_start_time ?? null,
        hook_end_time: selectedHook ? selectedHook.start + selectedHook.duration : (job.analysis as any)?.hook_end_time ?? null,
        hook_score: selectedHook?.score ?? (job.analysis as any)?.hook_score ?? null,
        hook_audit_score: selectedHook?.auditScore ?? (job.analysis as any)?.hook_audit_score ?? null,
        hook_text: selectedHook?.text ?? (job.analysis as any)?.hook_text ?? null,
        hook_reason: selectedHook?.reason ?? (job.analysis as any)?.hook_reason ?? null,
        hook_synthetic: selectedHook?.synthetic ?? (job.analysis as any)?.hook_synthetic ?? false,
        selected_strategy: selectedStrategy,
        retention_attempts: retentionAttempts,
        retention_judge: selectedJudge,
        quality_gate_thresholds: selectedJudge?.applied_thresholds ?? null,
        quality_gate_override: qualityGateOverride,
        pattern_interrupt_count: selectedPatternInterruptCount || (job.analysis as any)?.pattern_interrupt_count || 0,
        pattern_interrupt_density: selectedPatternInterruptDensity || (job.analysis as any)?.pattern_interrupt_density || 0,
        boredom_removed_ratio: selectedBoredomRemovalRatio || (job.analysis as any)?.boredom_removed_ratio || 0,
        story_reorder_map: selectedStoryReorderMap,
        style_profile: styleProfileForAnalysis,
        beat_anchors: beatAnchorsForAnalysis,
        hook_variants: hookVariantsForAnalysis,
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
        retentionLevel: aggressionLevel
      },
      renderConfig,
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
      renderSettings: buildPersistedRenderSettings(renderConfig, { retentionAggressionLevel: aggressionLevel }),
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
        retentionAggressionLevel: getRetentionAggressionFromJob(existing)
      })
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
    const { filename, inputPath: providedPath, requestedQuality } = req.body
    if (!filename && !providedPath) return res.status(400).json({ error: 'filename required' })
    const id = crypto.randomUUID()
    const safeName = filename ? path.basename(filename) : path.basename(providedPath)
    const inputPath = providedPath || `${userId}/${id}/${safeName}`
    const renderConfig = parseRenderConfigFromRequest(req.body)
    const retentionAggressionLevel = getRetentionAggressionFromPayload(req.body)

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
        renderSettings: buildPersistedRenderSettings(renderConfig, { retentionAggressionLevel }),
        analysis: buildPersistedRenderAnalysis({
          existing: {
            retentionAggressionLevel,
            retentionLevel: retentionAggressionLevel,
            pipelineSteps: normalizePipelineStepMap({})
          },
          renderConfig,
          outputPaths: null
        })
      }
    })

    if (!r2.isConfigured) {
      return res.json({ job, uploadUrl: null, inputPath, bucket: INPUT_BUCKET })
    }

    try {
      // Generate an R2 presigned PUT URL for direct upload
      const uploadUrl = await r2.generateUploadUrl(inputPath, 'video/mp4')
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

const handleCancelJob = async (req: any, res: any) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })

    const status = String(job.status || '').toLowerCase()
    if (status === 'completed' || status === 'failed') {
      return res.status(409).json({ error: 'cannot_cancel', message: 'Job is already finished.' })
    }
    if (!CANCELABLE_PIPELINE_STATUSES.has(status)) {
      return res.status(409).json({ error: 'cannot_cancel', message: 'Job cannot be canceled in its current state.' })
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
      error: 'queue_canceled_by_user'
    })
    if (!isRunning) clearPipelineCanceled(id)
    return res.json({ ok: true, id, status: 'failed', running: isRunning, killedCount })
  } catch (err) {
    return res.status(500).json({ error: 'server_error' })
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
    const hasAggressionOverride =
      req.body?.retentionLevel !== undefined ||
      req.body?.retentionAggressionLevel !== undefined ||
      req.body?.aggressionLevel !== undefined
    const requestedAggressionLevel = hasAggressionOverride
      ? getRetentionAggressionFromPayload(req.body)
      : getRetentionAggressionFromJob(job)
    const nextRenderSettings = {
      ...((job as any)?.renderSettings || {}),
      retentionAggressionLevel: requestedAggressionLevel,
      retentionLevel: requestedAggressionLevel
    }
    const nextAnalysis = {
      ...((job.analysis as any) || {}),
      retentionAggressionLevel: requestedAggressionLevel,
      retentionLevel: requestedAggressionLevel
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
    const { options } = await getEditOptionsForUser(req.user.id, {
      retentionAggressionLevel: getRetentionAggressionFromJob(job)
    })
    if (req.body?.retentionLevel || req.body?.retentionAggressionLevel || req.body?.aggressionLevel) {
      const overrideLevel = parseRetentionAggressionLevel(req.body?.retentionLevel || req.body?.retentionAggressionLevel || req.body?.aggressionLevel)
      options.retentionAggressionLevel = overrideLevel
      options.aggressiveMode = isAggressiveRetentionLevel(overrideLevel)
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
    const { options } = await getEditOptionsForUser(req.user.id, {
      retentionAggressionLevel: getRetentionAggressionFromJob(job)
    })
    if (req.body?.retentionLevel || req.body?.retentionAggressionLevel || req.body?.aggressionLevel) {
      const overrideLevel = parseRetentionAggressionLevel(req.body?.retentionLevel || req.body?.retentionAggressionLevel || req.body?.aggressionLevel)
      options.retentionAggressionLevel = overrideLevel
      options.aggressiveMode = isAggressiveRetentionLevel(overrideLevel)
    }
    // Allow client to request a fast-mode re-render (overrides user settings for this run)
    if (req.body?.fastMode) {
      ;(options as any).fastMode = true
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

export const __retentionTestUtils = {
  pickTopHookCandidates,
  computeRetentionScore,
  buildRetentionJudgeReport,
  resolveQualityGateThresholds,
  computeContentSignalStrength,
  inferContentStyleProfile,
  getStyleAdjustedAggressionLevel,
  applyStyleToPacingProfile,
  detectRhythmAnchors,
  alignSegmentsToRhythm,
  selectRenderableHookCandidate,
  shouldForceRescueRender,
  executeQualityGateRetriesForTest,
  buildTimelineWithHookAtStartForTest,
  buildPersistedRenderAnalysis
}

export default router
