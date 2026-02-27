import express from 'express'
import fs from 'fs'
import path from 'path'
import os from 'os'
import crypto from 'crypto'
import { spawn } from 'child_process'
import multer from 'multer'
import fetch from 'node-fetch'
import { FFMPEG_PATH, FFPROBE_PATH, formatCommand } from '../lib/ffmpeg'
import { prisma } from '../db/prisma'

const router = express.Router()

type RenderMode = 'horizontal' | 'vertical'
type JobStatus = 'queued' | 'processing' | 'completed' | 'failed'
type SuggestedSubMode = 'highlight_mode' | 'story_mode' | 'standard_mode'
type FormatPreset = 'youtube' | 'tiktok' | 'instagram_reels' | 'youtube_shorts' | 'custom'
type VibeChip =
  | 'energetic'
  | 'chill'
  | 'luxury'
  | 'funny'
  | 'motivational'
  | 'aesthetic'
  | 'dark'
  | 'cinematic'
type StylePreset = 'clean' | 'bold' | 'vintage' | 'glitch' | 'neon' | 'minimal' | 'meme'
type PacingPreset = 'aggressive' | 'balanced' | 'chill' | 'cinematic'
type CaptionMode = 'ai' | 'manual'
type CaptionStylePreset =
  | 'impact'
  | 'subtle'
  | 'pop'
  | 'meme'
  | 'scroll'
  | 'neon_glow'
  | 'vintage_typewriter'
type CaptionEffect = 'clean_fade' | 'kinetic_pop' | 'underline_sweep' | 'none'
type AudioOption = 'auto_sync_tracks' | 'mute' | 'voiceover_ai' | 'sfx_library'
type ZoomEffect = 'punch_zoom' | 'slow_push_in' | 'ken_burns' | 'beat_zoom'

type QuickControls = {
  autoEdit: boolean
  highlightReel: boolean
  speedRamp: boolean
  musicSync: boolean
}

type AdaptiveEditorProfile = {
  formatPreset: FormatPreset
  vibeChip: VibeChip
  stylePreset: StylePreset
  pacingPreset: PacingPreset
  pacingValue: number
  autoDetectBestMoments: boolean
  captionMode: CaptionMode
  captionStyle: CaptionStylePreset
  captionFont: string
  captionEffect: CaptionEffect
  audioOption: AudioOption
  quickControls: QuickControls
  suggestedSubMode: SuggestedSubMode
  confidence: number
  rationale: string[]
}

type UploadedVideoRecord = {
  id: string
  userId: string
  fileName: string
  storedPath: string
  createdAt: string
  metadata: VideoMetadata
  autoDetection: AutoDetectionPayload
}

type VideoMetadata = {
  width: number
  height: number
  duration: number
  fps: number
  aspectRatio: number
}

type FrameScanSummary = {
  sampledFrames: number
  sampleStride: number
  portraitSignal: number
  landscapeSignal: number
  centeredFaceVerticalSignal: number
  horizontalMotionSignal: number
  highMotionShortClipSignal: number
  motionPeaks: number[]
}

type AutoDetectionPayload = {
  metadataMode: RenderMode
  frameScanMode: RenderMode
  finalMode: RenderMode
  ambiguous: boolean
  confidence: number
  reason: string
  suggestedSubMode: SuggestedSubMode
  suggestedSubModes: SuggestedSubMode[]
  bannerMessage: string
  frameScan: FrameScanSummary
  editorProfile: AdaptiveEditorProfile
}

type RetentionPointType = 'best' | 'worst' | 'skip_zone' | 'hook' | 'emotional_peak'

type RetentionPoint = {
  id: string
  timestamp: number
  watchedPct: number
  type: RetentionPointType
  label: string
  description: string
}

type RetentionHeatCell = {
  timestamp: number
  intensity: number
}

type ThumbnailOption = {
  id: string
  url: string
  label: string
}

type RenderJobRecord = {
  id: string
  userId: string
  status: JobStatus
  createdAt: string
  updatedAt: string
  mode: RenderMode
  progress: number
  fileName: string
  videoId: string
  outputVideoUrl: string
  outputVideoPath: string
  clipUrls: string[]
  ffmpegCommands: string[]
  thumbnails: ThumbnailOption[]
  retention: {
    points: RetentionPoint[]
    heatmap: RetentionHeatCell[]
    summary: string
  }
  errorMessage: string | null
}

type SegmentInput = { start: number; end: number }

type RenderRequestPayload = {
  videoId: string
  mode: RenderMode
  manualSegments?: SegmentInput[]
  quickControls?: Partial<QuickControls>
  formatPreset?: string
  vibeChip?: string
  stylePreset?: string
  pacing?: string
  autoDetectBestMoments?: boolean
  captionMode?: string
  captionStyle?: string
  captionFont?: string
  captionEffect?: string
  zoomEffect?: string
  audioOption?: string
  suggestedSubMode?: SuggestedSubMode
}

type ProcessResult = {
  code: number | null
  stdout: string
  stderr: string
}

type FrameScanScriptResult = {
  sampledFrames?: number
  sampleStride?: number
  portraitSignal?: number
  landscapeSignal?: number
  centeredFaceVerticalSignal?: number
  horizontalMotionSignal?: number
  highMotionShortClipSignal?: number
  motionPeaks?: number[]
}

type TranscriptSummary = {
  segmentCount: number
  excerpt: string
  language: string | null
}

type ClaudeRetentionOutput = {
  hookStrength: number
  emotionLift: number
  pacingRisk: number
  summary: string
} | null

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const DEFAULT_CAPTION_FONT = 'Inter'
const CAPTION_FONT_OPTIONS = [
  'Inter',
  'Geist',
  'SF Pro Display',
  'Manrope',
  'Space Grotesk',
  'Poppins',
  'Bebas Neue',
  'DM Sans',
  'Sora',
  'Outfit'
]

const normalizeToken = (value: unknown) => String(value || '').trim().toLowerCase().replace(/\s+/g, '_')

const parseFormatPreset = (value: unknown, fallback: FormatPreset): FormatPreset => {
  const raw = normalizeToken(value)
  if (raw === 'youtube') return 'youtube'
  if (raw === 'tiktok') return 'tiktok'
  if (raw === 'instagram_reels' || raw === 'instagramreels' || raw === 'reels') return 'instagram_reels'
  if (raw === 'youtube_shorts' || raw === 'youtubeshorts' || raw === 'shorts') return 'youtube_shorts'
  if (raw === 'custom') return 'custom'
  return fallback
}

const parseVibeChip = (value: unknown, fallback: VibeChip): VibeChip => {
  const raw = normalizeToken(value)
  if (raw === 'energetic') return 'energetic'
  if (raw === 'chill') return 'chill'
  if (raw === 'luxury') return 'luxury'
  if (raw === 'funny') return 'funny'
  if (raw === 'motivational') return 'motivational'
  if (raw === 'aesthetic') return 'aesthetic'
  if (raw === 'dark') return 'dark'
  if (raw === 'cinematic') return 'cinematic'
  return fallback
}

const parseStylePreset = (value: unknown, fallback: StylePreset): StylePreset => {
  const raw = normalizeToken(value)
  if (raw === 'clean') return 'clean'
  if (raw === 'bold') return 'bold'
  if (raw === 'vintage') return 'vintage'
  if (raw === 'glitch') return 'glitch'
  if (raw === 'neon') return 'neon'
  if (raw === 'minimal') return 'minimal'
  if (raw === 'meme') return 'meme'
  return fallback
}

const parsePacingPreset = (value: unknown, fallback: PacingPreset): PacingPreset => {
  const raw = normalizeToken(value)
  if (raw === 'aggressive' || raw === 'fast' || raw === 'high') return 'aggressive'
  if (raw === 'balanced' || raw === 'normal' || raw === 'medium') return 'balanced'
  if (raw === 'chill' || raw === 'slow') return 'chill'
  if (raw === 'cinematic') return 'cinematic'
  return fallback
}

const parseCaptionMode = (value: unknown, fallback: CaptionMode): CaptionMode => {
  const raw = normalizeToken(value)
  if (raw === 'ai' || raw === 'auto') return 'ai'
  if (raw === 'manual' || raw === 'off') return 'manual'
  return fallback
}

const parseCaptionStyle = (value: unknown, fallback: CaptionStylePreset): CaptionStylePreset => {
  const raw = normalizeToken(value)
  if (raw === 'impact') return 'impact'
  if (raw === 'subtle') return 'subtle'
  if (raw === 'pop') return 'pop'
  if (raw === 'meme') return 'meme'
  if (raw === 'scroll') return 'scroll'
  if (raw === 'neon_glow' || raw === 'neonglow') return 'neon_glow'
  if (raw === 'vintage_typewriter' || raw === 'typewriter') return 'vintage_typewriter'
  return fallback
}

const parseCaptionEffect = (value: unknown, fallback: CaptionEffect): CaptionEffect => {
  const raw = normalizeToken(value)
  if (raw === 'clean_fade' || raw === 'fade') return 'clean_fade'
  if (raw === 'kinetic_pop' || raw === 'kinetic') return 'kinetic_pop'
  if (raw === 'underline_sweep' || raw === 'underline') return 'underline_sweep'
  if (raw === 'none' || raw === 'off') return 'none'
  return fallback
}

const parseAudioOption = (value: unknown, fallback: AudioOption): AudioOption => {
  const raw = normalizeToken(value)
  if (raw === 'auto_sync_tracks' || raw === 'auto_sync' || raw === 'auto') return 'auto_sync_tracks'
  if (raw === 'mute' || raw === 'silent') return 'mute'
  if (raw === 'voiceover_ai' || raw === 'voiceover') return 'voiceover_ai'
  if (raw === 'sfx_library' || raw === 'sfx') return 'sfx_library'
  return fallback
}

const parseZoomEffect = (value: unknown, fallback: ZoomEffect): ZoomEffect => {
  const raw = normalizeToken(value)
  if (raw === 'punch_zoom' || raw === 'punch') return 'punch_zoom'
  if (raw === 'slow_push_in' || raw === 'slow_push') return 'slow_push_in'
  if (raw === 'ken_burns' || raw === 'kenburns') return 'ken_burns'
  if (raw === 'beat_zoom' || raw === 'beat') return 'beat_zoom'
  return fallback
}

const parseSuggestedSubMode = (value: unknown, fallback: SuggestedSubMode): SuggestedSubMode => {
  const raw = normalizeToken(value)
  if (raw === 'highlight_mode' || raw === 'highlight') return 'highlight_mode'
  if (raw === 'story_mode' || raw === 'story') return 'story_mode'
  if (raw === 'standard_mode' || raw === 'standard') return 'standard_mode'
  return fallback
}

const sanitizeCaptionFont = (value: unknown, fallback = DEFAULT_CAPTION_FONT) => {
  const raw = String(value || '').trim()
  if (!raw) return fallback
  const matched = CAPTION_FONT_OPTIONS.find((font) => font.toLowerCase() === raw.toLowerCase())
  if (matched) return matched
  const cleaned = raw.replace(/[,:;'"\\]/g, '').slice(0, 48).trim()
  return cleaned || fallback
}

const parseQuickControls = (value: unknown, fallback: QuickControls): QuickControls => {
  if (!value || typeof value !== 'object') return { ...fallback }
  const candidate = value as any
  return {
    autoEdit: typeof candidate.autoEdit === 'boolean' ? candidate.autoEdit : fallback.autoEdit,
    highlightReel: typeof candidate.highlightReel === 'boolean' ? candidate.highlightReel : fallback.highlightReel,
    speedRamp: typeof candidate.speedRamp === 'boolean' ? candidate.speedRamp : fallback.speedRamp,
    musicSync: typeof candidate.musicSync === 'boolean' ? candidate.musicSync : fallback.musicSync
  }
}

const pacingPresetFromValue = (value: number): PacingPreset => {
  if (value >= 72) return 'aggressive'
  if (value <= 38) return 'chill'
  return 'balanced'
}

const pacingValueFromPreset = (preset: PacingPreset, fallbackByMode: RenderMode): number => {
  if (preset === 'aggressive') return fallbackByMode === 'vertical' ? 80 : 72
  if (preset === 'chill') return fallbackByMode === 'vertical' ? 52 : 34
  if (preset === 'cinematic') return fallbackByMode === 'vertical' ? 58 : 36
  return fallbackByMode === 'vertical' ? 68 : 56
}

const stableUnit = (input: string) => {
  let hash = 2166136261
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return ((hash >>> 0) % 10000) / 10000
}

const countKeywordHits = (value: string, keywords: string[]) => {
  const normalized = ` ${String(value || '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ')} `
  let hits = 0
  for (const keyword of keywords) {
    const token = ` ${keyword.toLowerCase()} `
    if (normalized.includes(token)) hits += 1
  }
  return hits
}

const pickBySeed = <T>(items: T[], seed: number, fallback: T): T => {
  if (!items.length) return fallback
  const index = Math.max(0, Math.min(items.length - 1, Math.floor(seed * items.length)))
  return items[index] ?? fallback
}

const processCwd = process.cwd()
const OUTPUT_ROOT = path.join(processCwd, 'outputs')
const VIBECUT_ROOT = path.join(OUTPUT_ROOT, 'vibecut')
const VIBECUT_UPLOAD_DIR = path.join(VIBECUT_ROOT, 'uploads')
const VIBECUT_RENDER_DIR = path.join(VIBECUT_ROOT, 'renders')

const ensureDir = (target: string) => {
  if (!fs.existsSync(target)) fs.mkdirSync(target, { recursive: true })
}

ensureDir(OUTPUT_ROOT)
ensureDir(VIBECUT_ROOT)
ensureDir(VIBECUT_UPLOAD_DIR)
ensureDir(VIBECUT_RENDER_DIR)

const uploadedVideos = new Map<string, UploadedVideoRecord>()
const jobs = new Map<string, RenderJobRecord>()

let prismaVibeCutEnabled = true
const disablePrismaVibeCut = (reason: string, error?: unknown) => {
  prismaVibeCutEnabled = false
  console.warn(`vibecut prisma disabled: ${reason}`, error || '')
}

const canUsePrismaVibeCut = () => {
  if (!prismaVibeCutEnabled) return false
  const client: any = prisma as any
  return Boolean(client?.vibeCutUpload && client?.vibeCutJob)
}

const toUploadedVideoRecord = (row: any): UploadedVideoRecord => {
  return {
    id: String(row?.id || ''),
    userId: String(row?.userId || row?.user_id || ''),
    fileName: String(row?.fileName || row?.file_name || ''),
    storedPath: String(row?.storedPath || row?.stored_path || ''),
    createdAt: new Date(row?.createdAt || row?.created_at || new Date()).toISOString(),
    metadata: (row?.metadata || {}) as VideoMetadata,
    autoDetection: (row?.autoDetection || row?.auto_detection || {}) as AutoDetectionPayload
  }
}

const toRenderJobRecord = (row: any): RenderJobRecord => {
  const retention = (row?.retention || {}) as RenderJobRecord['retention']
  return {
    id: String(row?.id || ''),
    userId: String(row?.userId || row?.user_id || ''),
    status: String(row?.status || 'queued') as JobStatus,
    createdAt: new Date(row?.createdAt || row?.created_at || new Date()).toISOString(),
    updatedAt: new Date(row?.updatedAt || row?.updated_at || new Date()).toISOString(),
    mode: String(row?.mode || 'horizontal') === 'vertical' ? 'vertical' : 'horizontal',
    progress: Number(row?.progress || 0),
    fileName: String(row?.fileName || row?.file_name || ''),
    videoId: String(row?.videoId || row?.video_id || row?.uploadId || row?.upload_id || ''),
    outputVideoUrl: String(row?.outputVideoUrl || row?.output_video_url || ''),
    outputVideoPath: String(row?.outputVideoPath || row?.output_video_path || ''),
    clipUrls: Array.isArray(row?.clipUrls) ? row.clipUrls : Array.isArray(row?.clip_urls) ? row.clip_urls : [],
    ffmpegCommands: Array.isArray(row?.ffmpegCommands) ? row.ffmpegCommands : Array.isArray(row?.ffmpeg_commands) ? row.ffmpeg_commands : [],
    thumbnails: Array.isArray(row?.thumbnails) ? row.thumbnails : [],
    retention: {
      points: Array.isArray(retention?.points) ? retention.points : [],
      heatmap: Array.isArray(retention?.heatmap) ? retention.heatmap : [],
      summary: String(retention?.summary || '')
    },
    errorMessage: row?.errorMessage ? String(row.errorMessage) : row?.error_message ? String(row.error_message) : null
  }
}

const persistUploadRecord = async (record: UploadedVideoRecord) => {
  if (!canUsePrismaVibeCut()) return
  try {
    await (prisma as any).vibeCutUpload.upsert({
      where: { id: record.id },
      update: {
        userId: record.userId,
        fileName: record.fileName,
        storedPath: record.storedPath,
        metadata: record.metadata,
        autoDetection: record.autoDetection
      },
      create: {
        id: record.id,
        userId: record.userId,
        fileName: record.fileName,
        storedPath: record.storedPath,
        metadata: record.metadata,
        autoDetection: record.autoDetection
      }
    })
  } catch (error) {
    disablePrismaVibeCut('upload_upsert_failed', error)
  }
}

const findUploadRecord = async (videoId: string, userId: string): Promise<UploadedVideoRecord | null> => {
  const inMemory = uploadedVideos.get(videoId) || null
  if (inMemory && inMemory.userId === userId) return inMemory
  if (!canUsePrismaVibeCut()) return inMemory && inMemory.userId === userId ? inMemory : null
  try {
    const row = await (prisma as any).vibeCutUpload.findUnique({ where: { id: videoId } })
    if (!row) return null
    const record = toUploadedVideoRecord(row)
    uploadedVideos.set(record.id, record)
    return record.userId === userId ? record : null
  } catch (error) {
    disablePrismaVibeCut('upload_find_failed', error)
    return inMemory && inMemory.userId === userId ? inMemory : null
  }
}

const persistJobRecord = async (record: RenderJobRecord) => {
  if (!canUsePrismaVibeCut()) return
  try {
    await (prisma as any).vibeCutJob.upsert({
      where: { id: record.id },
      update: {
        userId: record.userId,
        uploadId: record.videoId,
        status: record.status,
        mode: record.mode,
        progress: record.progress,
        fileName: record.fileName,
        outputVideoUrl: record.outputVideoUrl || null,
        outputVideoPath: record.outputVideoPath || null,
        clipUrls: record.clipUrls,
        ffmpegCommands: record.ffmpegCommands,
        thumbnails: record.thumbnails,
        retention: record.retention,
        errorMessage: record.errorMessage
      },
      create: {
        id: record.id,
        userId: record.userId,
        uploadId: record.videoId,
        status: record.status,
        mode: record.mode,
        progress: record.progress,
        fileName: record.fileName,
        outputVideoUrl: record.outputVideoUrl || null,
        outputVideoPath: record.outputVideoPath || null,
        clipUrls: record.clipUrls,
        ffmpegCommands: record.ffmpegCommands,
        thumbnails: record.thumbnails,
        retention: record.retention,
        errorMessage: record.errorMessage
      }
    })
  } catch (error) {
    disablePrismaVibeCut('job_upsert_failed', error)
  }
}

const listRecentJobsByUser = async (userId: string): Promise<RenderJobRecord[]> => {
  const fromMemory = Array.from(jobs.values()).filter((job) => job.userId === userId)
  if (!canUsePrismaVibeCut()) {
    return fromMemory
      .sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt))
      .slice(0, 12)
  }
  try {
    const rows = await (prisma as any).vibeCutJob.findMany({
      where: { userId },
      orderBy: { createdAt: 'desc' },
      take: 12
    })
    const mapped = (rows || []).map((row: any) => toRenderJobRecord(row))
    mapped.forEach((job) => jobs.set(job.id, job))
    return mapped
  } catch (error) {
    disablePrismaVibeCut('job_list_failed', error)
    return fromMemory
      .sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt))
      .slice(0, 12)
  }
}

const findJobByIdAndUser = async (jobId: string, userId: string): Promise<RenderJobRecord | null> => {
  const inMemory = jobs.get(jobId) || null
  if (inMemory && inMemory.userId === userId) return inMemory
  if (!canUsePrismaVibeCut()) return inMemory && inMemory.userId === userId ? inMemory : null
  try {
    const row = await (prisma as any).vibeCutJob.findUnique({ where: { id: jobId } })
    if (!row) return null
    const job = toRenderJobRecord(row)
    jobs.set(job.id, job)
    return job.userId === userId ? job : null
  } catch (error) {
    disablePrismaVibeCut('job_find_failed', error)
    return inMemory && inMemory.userId === userId ? inMemory : null
  }
}

const resolveScriptPath = (relativeToBackend: string) => {
  const candidates = [
    path.join(processCwd, relativeToBackend),
    path.join(processCwd, 'backend', relativeToBackend),
    path.join(processCwd, '..', relativeToBackend)
  ]
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate
  }
  return candidates[0]
}

const PYTHON_BIN = String(process.env.PYTHON_BIN || process.env.PYTHON_PATH || 'python').trim() || 'python'

const VIBECUT_FRAME_SCANNER_SCRIPT = resolveScriptPath(path.join('scripts', 'vibecut_frame_scanner.py'))
const VIBECUT_MOVIEPY_PIPELINE_SCRIPT = resolveScriptPath(path.join('scripts', 'vibecut_moviepy_pipeline.py'))
const FASTER_WHISPER_SCRIPT = resolveScriptPath(path.join('scripts', 'faster_whisper_transcribe.py'))

const runProcess = async (cmd: string, args: string[], cwd?: string): Promise<ProcessResult> => {
  return new Promise((resolve) => {
    const child = spawn(cmd, args, { cwd, stdio: ['ignore', 'pipe', 'pipe'] })
    let stdout = ''
    let stderr = ''
    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString()
    })
    child.on('error', (error) => {
      stderr += `${error?.message || error}`
      resolve({ code: 1, stdout, stderr })
    })
    child.on('close', (code) => {
      resolve({ code, stdout, stderr })
    })
  })
}

const parseRational = (value: unknown): number => {
  const raw = String(value || '').trim()
  if (!raw) return 0
  if (raw.includes('/')) {
    const [numRaw, denRaw] = raw.split('/')
    const num = Number(numRaw)
    const den = Number(denRaw)
    if (Number.isFinite(num) && Number.isFinite(den) && den !== 0) return num / den
    return 0
  }
  const numeric = Number(raw)
  return Number.isFinite(numeric) ? numeric : 0
}

const tryParseJson = (value: string) => {
  try {
    return JSON.parse(value)
  } catch {
    const start = value.indexOf('{')
    const end = value.lastIndexOf('}')
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(value.slice(start, end + 1))
      } catch {
        return null
      }
    }
    return null
  }
}

const toOutputUrl = (absolutePath: string) => {
  const relative = path.relative(OUTPUT_ROOT, absolutePath).replace(/\\/g, '/')
  return `/outputs/${relative}`
}

const ffprobeMetadata = async (videoPath: string): Promise<VideoMetadata> => {
  const args = [
    '-v', 'error',
    '-select_streams', 'v:0',
    '-show_entries', 'stream=width,height,avg_frame_rate,r_frame_rate,duration:format=duration',
    '-of', 'json',
    videoPath
  ]
  const result = await runProcess(FFPROBE_PATH, args)
  if (result.code !== 0) {
    throw new Error(`ffprobe failed: ${result.stderr || result.stdout}`)
  }

  const parsed = tryParseJson(result.stdout)
  const stream = Array.isArray(parsed?.streams) ? parsed.streams[0] : null
  const width = Number(stream?.width || 0)
  const height = Number(stream?.height || 0)
  const duration = Number(stream?.duration || parsed?.format?.duration || 0)
  const fpsFromAvg = parseRational(stream?.avg_frame_rate)
  const fpsFromRaw = parseRational(stream?.r_frame_rate)
  const fps = clamp(fpsFromAvg || fpsFromRaw || 30, 1, 240)
  const aspectRatio = width > 0 && height > 0 ? width / height : 1

  if (!width || !height || !duration) {
    throw new Error('ffprobe did not return usable metadata')
  }

  return {
    width,
    height,
    duration,
    fps,
    aspectRatio
  }
}

const probeHasAudioStream = async (videoPath: string): Promise<boolean> => {
  const args = [
    '-v', 'error',
    '-select_streams', 'a:0',
    '-show_entries', 'stream=index',
    '-of', 'default=nokey=1:noprint_wrappers=1',
    videoPath
  ]
  const result = await runProcess(FFPROBE_PATH, args)
  if (result.code !== 0) return false
  return String(result.stdout || '').trim().length > 0
}

const runFrameScan = async (inputPath: string): Promise<FrameScanSummary> => {
  const fallback: FrameScanSummary = {
    sampledFrames: 0,
    sampleStride: 0,
    portraitSignal: 0.5,
    landscapeSignal: 0.5,
    centeredFaceVerticalSignal: 0,
    horizontalMotionSignal: 0,
    highMotionShortClipSignal: 0,
    motionPeaks: []
  }

  if (!fs.existsSync(VIBECUT_FRAME_SCANNER_SCRIPT)) {
    return fallback
  }

  const result = await runProcess(PYTHON_BIN, [
    VIBECUT_FRAME_SCANNER_SCRIPT,
    '--input',
    inputPath,
    '--sample-ratio',
    '0.1'
  ])

  if (result.code !== 0) {
    console.warn('vibecut frame scanner failed', result.stderr || result.stdout)
    return fallback
  }

  const parsed = tryParseJson(result.stdout) as FrameScanScriptResult | null
  if (!parsed || typeof parsed !== 'object') return fallback

  return {
    sampledFrames: Number(parsed.sampledFrames || 0),
    sampleStride: Number(parsed.sampleStride || 0),
    portraitSignal: clamp(Number(parsed.portraitSignal || 0), 0, 1),
    landscapeSignal: clamp(Number(parsed.landscapeSignal || 0), 0, 1),
    centeredFaceVerticalSignal: clamp(Number(parsed.centeredFaceVerticalSignal || 0), 0, 1),
    horizontalMotionSignal: clamp(Number(parsed.horizontalMotionSignal || 0), 0, 1),
    highMotionShortClipSignal: clamp(Number(parsed.highMotionShortClipSignal || 0), 0, 1),
    motionPeaks: Array.isArray(parsed.motionPeaks)
      ? parsed.motionPeaks.map((value) => Number(value)).filter((value) => Number.isFinite(value))
      : []
  }
}

const buildAdaptiveEditorProfile = ({
  metadata,
  frameScan,
  finalMode,
  suggestedSubMode,
  transcript,
  fileName
}: {
  metadata: VideoMetadata
  frameScan: FrameScanSummary
  finalMode: RenderMode
  suggestedSubMode: SuggestedSubMode
  transcript?: TranscriptSummary | null
  fileName?: string | null
}): AdaptiveEditorProfile => {
  const transcriptText = String(transcript?.excerpt || '')
  const language = String(transcript?.language || '').toLowerCase()
  const segmentCount = Number(transcript?.segmentCount || 0)
  const transcriptDensity = clamp(segmentCount / Math.max(1, metadata.duration * 0.33), 0, 1)
  const fpsSignal = clamp((metadata.fps - 24) / 36, 0, 1)
  const motionSignal = clamp(frameScan.highMotionShortClipSignal * 0.64 + frameScan.horizontalMotionSignal * 0.36, 0, 1)
  const faceSignal = clamp(frameScan.centeredFaceVerticalSignal, 0, 1)

  const energyScore = clamp(motionSignal * 0.62 + fpsSignal * 0.18 + transcriptDensity * 0.2, 0, 1)
  const narrativeScore = clamp(faceSignal * 0.46 + (1 - motionSignal) * 0.26 + transcriptDensity * 0.28, 0, 1)

  const keywordText = `${fileName || ''} ${transcriptText}`.toLowerCase()
  const funnyHits = countKeywordHits(keywordText, ['funny', 'comedy', 'joke', 'lol', 'meme', 'prank', 'reaction'])
  const motivationalHits = countKeywordHits(keywordText, ['motivation', 'discipline', 'mindset', 'grind', 'hustle', 'success'])
  const luxuryHits = countKeywordHits(keywordText, ['luxury', 'wealth', 'rich', 'fashion', 'travel', 'lifestyle'])
  const darkHits = countKeywordHits(keywordText, ['dark', 'horror', 'crime', 'sad', 'fear', 'mystery'])
  const cinematicHits = countKeywordHits(keywordText, ['cinematic', 'story', 'film', 'documentary', 'journey'])

  const styleSeed = stableUnit([
    metadata.width,
    metadata.height,
    metadata.duration.toFixed(2),
    metadata.fps.toFixed(2),
    frameScan.highMotionShortClipSignal.toFixed(4),
    frameScan.centeredFaceVerticalSignal.toFixed(4),
    frameScan.motionPeaks.slice(0, 4).join(','),
    segmentCount,
    language,
    keywordText.slice(0, 220)
  ].join('|'))

  let vibeChip: VibeChip
  if (funnyHits > 0) {
    vibeChip = 'funny'
  } else if (motivationalHits > 0) {
    vibeChip = 'motivational'
  } else if (darkHits > 0) {
    vibeChip = 'dark'
  } else if (luxuryHits > 0) {
    vibeChip = styleSeed > 0.5 ? 'luxury' : 'aesthetic'
  } else if (energyScore > 0.76) {
    vibeChip = 'energetic'
  } else if (cinematicHits > 0 || (narrativeScore > 0.7 && energyScore < 0.56)) {
    vibeChip = 'cinematic'
  } else if (narrativeScore > 0.7 && energyScore < 0.52) {
    vibeChip = 'chill'
  } else {
    vibeChip = pickBySeed<VibeChip>(['aesthetic', 'luxury', 'cinematic'], styleSeed, 'aesthetic')
  }

  const styleMap: Record<VibeChip, StylePreset[]> = {
    energetic: ['bold', 'glitch', 'meme'],
    chill: ['minimal', 'clean', 'vintage'],
    luxury: ['clean', 'minimal', 'vintage'],
    funny: ['meme', 'bold', 'clean'],
    motivational: ['bold', 'clean', 'neon'],
    aesthetic: ['minimal', 'clean', 'vintage'],
    dark: ['glitch', 'neon', 'bold'],
    cinematic: ['vintage', 'clean', 'minimal']
  }
  const stylePreset = pickBySeed<StylePreset>(styleMap[vibeChip], styleSeed * 0.91 + 0.04, styleMap[vibeChip][0])

  let pacingValue = Math.round(
    (finalMode === 'vertical' ? 60 : 47) + energyScore * 33 - narrativeScore * 9 + (vibeChip === 'energetic' ? 5 : 0)
  )
  if (vibeChip === 'cinematic' || vibeChip === 'luxury') pacingValue -= 7
  if (vibeChip === 'funny') pacingValue += 6
  pacingValue = Math.round(clamp(pacingValue, 20, 92))

  let pacingPreset = pacingPresetFromValue(pacingValue)
  if (narrativeScore > 0.74 && energyScore < 0.5) pacingPreset = 'cinematic'

  const captionStyle: CaptionStylePreset =
    vibeChip === 'funny'
      ? 'meme'
      : vibeChip === 'dark'
        ? 'neon_glow'
        : pacingPreset === 'cinematic'
          ? 'subtle'
          : energyScore > 0.72
            ? 'impact'
            : energyScore < 0.36
              ? 'scroll'
              : 'pop'

  const captionEffect: CaptionEffect =
    energyScore > 0.72 ? 'kinetic_pop' : pacingPreset === 'cinematic' ? 'clean_fade' : 'underline_sweep'

  const captionFont =
    vibeChip === 'funny'
      ? 'Bebas Neue'
      : vibeChip === 'cinematic'
        ? 'Manrope'
        : vibeChip === 'dark'
          ? 'Space Grotesk'
          : vibeChip === 'aesthetic'
            ? 'Sora'
            : DEFAULT_CAPTION_FONT

  const quickControls: QuickControls = {
    autoEdit: true,
    highlightReel: finalMode === 'vertical' ? true : energyScore > 0.56,
    speedRamp: energyScore > 0.68,
    musicSync: energyScore > 0.45 || vibeChip === 'funny'
  }

  const autoDetectBestMoments = finalMode === 'vertical' ? true : energyScore > 0.42 || quickControls.highlightReel

  const audioOption: AudioOption =
    segmentCount >= 6 && narrativeScore > 0.68
      ? 'voiceover_ai'
      : energyScore > 0.78
        ? 'sfx_library'
        : 'auto_sync_tracks'

  const formatPreset: FormatPreset =
    finalMode === 'vertical'
      ? energyScore > 0.78
        ? 'tiktok'
        : narrativeScore > 0.7
          ? 'instagram_reels'
          : 'youtube_shorts'
      : metadata.aspectRatio > 2.05
        ? 'custom'
        : 'youtube'

  const profileConfidence = clamp(0.56 + energyScore * 0.16 + transcriptDensity * 0.12 + (segmentCount > 0 ? 0.06 : 0), 0.5, 0.96)

  const rationale = [
    `energy=${energyScore.toFixed(2)}, narrative=${narrativeScore.toFixed(2)}, transcript_density=${transcriptDensity.toFixed(2)}`,
    `keywords funny=${funnyHits}, motivational=${motivationalHits}, luxury=${luxuryHits}, dark=${darkHits}, cinematic=${cinematicHits}`,
    `motion=${frameScan.highMotionShortClipSignal.toFixed(2)}, face_center=${frameScan.centeredFaceVerticalSignal.toFixed(2)}, fps=${metadata.fps.toFixed(1)}`
  ]

  return {
    formatPreset,
    vibeChip,
    stylePreset,
    pacingPreset,
    pacingValue,
    autoDetectBestMoments,
    captionMode: segmentCount > 0 ? 'ai' : 'manual',
    captionStyle,
    captionFont,
    captionEffect,
    audioOption,
    quickControls,
    suggestedSubMode,
    confidence: Number(profileConfidence.toFixed(3)),
    rationale
  }
}

const detectMode = (
  metadata: VideoMetadata,
  frameScan: FrameScanSummary,
  opts?: { transcript?: TranscriptSummary | null; fileName?: string | null }
): AutoDetectionPayload => {
  const ratio = metadata.aspectRatio
  const metadataMode: RenderMode = ratio > 1.08 ? 'horizontal' : ratio < 0.92 ? 'vertical' : 'vertical'
  const ambiguous = ratio >= 0.92 && ratio <= 1.08

  const frameMode: RenderMode = frameScan.portraitSignal >= frameScan.landscapeSignal ? 'vertical' : 'horizontal'
  const frameConfidence = Math.abs(frameScan.portraitSignal - frameScan.landscapeSignal)

  const weightedVertical = (metadataMode === 'vertical' ? 0.58 : 0.42) + frameScan.portraitSignal * 0.38
  const weightedHorizontal = (metadataMode === 'horizontal' ? 0.58 : 0.42) + frameScan.landscapeSignal * 0.38
  const finalMode: RenderMode = ambiguous ? 'vertical' : weightedVertical >= weightedHorizontal ? 'vertical' : 'horizontal'

  const confidence = clamp(
    ambiguous ? 0.58 + frameConfidence * 0.25 : 0.66 + frameConfidence * 0.28,
    0.5,
    0.98
  )

  const suggestedSubModes: SuggestedSubMode[] =
    finalMode === 'vertical'
      ? [
          frameScan.centeredFaceVerticalSignal > 0.5 ? 'story_mode' : 'highlight_mode',
          frameScan.highMotionShortClipSignal > 0.32 ? 'highlight_mode' : 'standard_mode'
        ]
      : ['standard_mode']

  const uniqueSubModes = Array.from(new Set(suggestedSubModes))
  const suggestedSubMode = uniqueSubModes[0] || (finalMode === 'vertical' ? 'highlight_mode' : 'standard_mode')

  const reasonParts = [
    `ffprobe ratio ${ratio.toFixed(3)} favored ${metadataMode}`,
    `OpenCV frame scan favored ${frameMode}`,
    ambiguous ? 'square-like ratio defaulted to vertical social mode' : 'metadata + frame scan agreed on orientation weighting'
  ]

  const bannerMessage = finalMode === 'vertical'
    ? 'Auto-detected: Vertical Mode (TikTok-ready). Switch?'
    : 'Auto-detected: Horizontal Mode (YouTube-ready). Switch?'

  const editorProfile = buildAdaptiveEditorProfile({
    metadata,
    frameScan,
    finalMode,
    suggestedSubMode,
    transcript: opts?.transcript || null,
    fileName: opts?.fileName || null
  })

  return {
    metadataMode,
    frameScanMode: frameMode,
    finalMode,
    ambiguous,
    confidence,
    reason: reasonParts.join('. '),
    suggestedSubMode,
    suggestedSubModes: uniqueSubModes,
    bannerMessage,
    frameScan,
    editorProfile
  }
}

const upload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => {
      ensureDir(VIBECUT_UPLOAD_DIR)
      cb(null, VIBECUT_UPLOAD_DIR)
    },
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname || '').toLowerCase() || '.mp4'
      cb(null, `${Date.now()}_${crypto.randomUUID().slice(0, 8)}${ext}`)
    }
  }),
  limits: {
    fileSize: 8 * 1024 * 1024 * 1024
  },
  fileFilter: (_req, file, cb) => {
    const normalized = String(file.mimetype || '').toLowerCase()
    const allowed = ['video/mp4', 'video/quicktime', 'video/x-matroska', 'video/mpeg']
    if (allowed.includes(normalized)) return cb(null, true)
    if (String(file.originalname || '').toLowerCase().match(/\.(mp4|mov|mkv|mpeg)$/)) return cb(null, true)
    cb(new Error('Only mp4/mov/mkv/mpeg video files are supported'))
  }
})

const createEmptyRetentionPayload = () => ({
  points: [] as RetentionPoint[],
  heatmap: [] as RetentionHeatCell[],
  summary: 'AI retention model pending.'
})

const buildJobSummary = (job: RenderJobRecord) => ({
  id: job.id,
  status: job.status,
  mode: job.mode,
  createdAt: job.createdAt,
  progress: job.progress,
  fileName: job.fileName
})

router.post('/upload/analyze', upload.single('video'), async (req: any, res) => {
  try {
    const userId = String(req?.user?.id || '').trim()
    if (!userId) {
      return res.status(401).json({ error: 'unauthorized', message: 'Sign in required.' })
    }

    const uploaded = req?.file as Express.Multer.File | undefined
    if (!uploaded || !uploaded.path) {
      return res.status(400).json({ error: 'invalid_upload', message: 'Upload a video file in field "video".' })
    }

    const videoId = crypto.randomUUID()
    const metadata = await ffprobeMetadata(uploaded.path)
    const frameScan = await runFrameScan(uploaded.path)
    const analysisDir = path.join(VIBECUT_UPLOAD_DIR, `${videoId}_analysis`)
    ensureDir(analysisDir)
    const transcript = await readTranscriptSummary(uploaded.path, analysisDir)
    const originalFileName = uploaded.originalname || path.basename(uploaded.path)
    const autoDetection = detectMode(metadata, frameScan, {
      transcript,
      fileName: originalFileName
    })

    const record: UploadedVideoRecord = {
      id: videoId,
      userId,
      fileName: originalFileName,
      storedPath: uploaded.path,
      createdAt: new Date().toISOString(),
      metadata,
      autoDetection
    }
    uploadedVideos.set(videoId, record)
    void persistUploadRecord(record)

    return res.json({
      videoId,
      videoUrl: toOutputUrl(uploaded.path),
      fileName: record.fileName,
      metadata,
      autoDetection
    })
  } catch (error: any) {
    console.error('vibecut upload/analyze failed', error)
    return res.status(500).json({ error: 'upload_analyze_failed', message: error?.message || 'Upload analysis failed.' })
  }
})

const parseSegments = (value: unknown, duration: number) => {
  if (!Array.isArray(value)) return [] as Array<{ start: number; end: number }>
  return value
    .map((segment) => {
      const start = clamp(Number((segment as any)?.start || 0), 0, duration)
      const end = clamp(Number((segment as any)?.end || 0), 0, duration)
      return { start, end }
    })
    .filter((segment) => Number.isFinite(segment.start) && Number.isFinite(segment.end) && segment.end - segment.start >= 0.3)
    .sort((a, b) => a.start - b.start)
}

const updateJobState = (jobId: string, patch: Partial<RenderJobRecord>) => {
  const current = jobs.get(jobId)
  if (!current) return
  const next: RenderJobRecord = {
    ...current,
    ...patch,
    updatedAt: new Date().toISOString()
  }
  jobs.set(jobId, next)
  void persistJobRecord(next)
}

const runFfmpegCommand = async (args: string[], ffmpegCommands: string[]) => {
  ffmpegCommands.push(formatCommand(FFMPEG_PATH, args))
  const result = await runProcess(FFMPEG_PATH, args)
  if (result.code !== 0) {
    throw new Error(result.stderr || result.stdout || 'ffmpeg command failed')
  }
}

const readTranscriptSummary = async (inputPath: string, jobDir: string): Promise<TranscriptSummary> => {
  const fallback: TranscriptSummary = {
    segmentCount: 0,
    excerpt: '',
    language: null
  }

  if (!fs.existsSync(FASTER_WHISPER_SCRIPT)) {
    return fallback
  }

  const baseName = 'vibecut_transcript'
  const args = [
    FASTER_WHISPER_SCRIPT,
    '--input',
    inputPath,
    '--output-dir',
    jobDir,
    '--base-name',
    baseName,
    '--model',
    String(process.env.VIBECUT_WHISPER_MODEL || 'small'),
    '--vad-filter'
  ]

  const result = await runProcess(PYTHON_BIN, args)
  if (result.code !== 0) {
    console.warn('vibecut whisper failed', result.stderr || result.stdout)
    return fallback
  }

  const transcriptPath = path.join(jobDir, `${baseName}.transcript.json`)
  if (!fs.existsSync(transcriptPath)) return fallback

  try {
    const parsed = JSON.parse(fs.readFileSync(transcriptPath, 'utf8')) as any
    const segments = Array.isArray(parsed?.segments) ? parsed.segments : []
    const excerpt = segments
      .slice(0, 6)
      .map((segment) => String(segment?.text || '').trim())
      .filter(Boolean)
      .join(' ')
      .slice(0, 600)

    return {
      segmentCount: segments.length,
      excerpt,
      language: typeof parsed?.language === 'string' ? parsed.language : null
    }
  } catch (error) {
    console.warn('vibecut transcript parse failed', error)
    return fallback
  }
}

const parseClaudeJson = (value: string): ClaudeRetentionOutput => {
  const parsed = tryParseJson(value)
  if (!parsed || typeof parsed !== 'object') return null
  const hookStrength = clamp(Number((parsed as any).hookStrength ?? (parsed as any).hook_strength ?? 0.5), 0, 1)
  const emotionLift = clamp(Number((parsed as any).emotionLift ?? (parsed as any).emotion_lift ?? 0.5), 0, 1)
  const pacingRisk = clamp(Number((parsed as any).pacingRisk ?? (parsed as any).pacing_risk ?? 0.5), 0, 1)
  const summary = String((parsed as any).summary || '').trim().slice(0, 240)
  return {
    hookStrength,
    emotionLift,
    pacingRisk,
    summary: summary || 'Claude model predicted moderate retention behavior.'
  }
}

const runClaudeRetentionModel = async ({
  metadata,
  frameScan,
  transcript,
  mode
}: {
  metadata: VideoMetadata
  frameScan: FrameScanSummary
  transcript: TranscriptSummary
  mode: RenderMode
}): Promise<ClaudeRetentionOutput> => {
  const apiKey = String(process.env.ANTHROPIC_API_KEY || '').trim()
  if (!apiKey) return null

  const prompt = `You are a retention prediction assistant for short-form editors.
Return ONLY compact JSON with keys: hookStrength (0-1), emotionLift (0-1), pacingRisk (0-1), summary.
Context:
- mode: ${mode}
- duration_seconds: ${metadata.duration.toFixed(2)}
- portrait_signal: ${frameScan.portraitSignal.toFixed(3)}
- landscape_signal: ${frameScan.landscapeSignal.toFixed(3)}
- centered_face_vertical: ${frameScan.centeredFaceVerticalSignal.toFixed(3)}
- high_motion_short_clip_signal: ${frameScan.highMotionShortClipSignal.toFixed(3)}
- transcript_segment_count: ${transcript.segmentCount}
- transcript_excerpt: ${transcript.excerpt || '[none]'}
`

  try {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: process.env.CLAUDE_MODEL || 'claude-3-5-sonnet-20241022',
        max_tokens: 300,
        messages: [{ role: 'user', content: prompt }]
      })
    })

    if (!response.ok) {
      const errorText = await response.text().catch(() => '')
      console.warn('claude request failed', response.status, errorText)
      return null
    }

    const payload = await response.json().catch(() => null) as any
    const textParts = Array.isArray(payload?.content)
      ? payload.content
          .map((part: any) => (typeof part?.text === 'string' ? part.text : ''))
          .join('\n')
      : ''

    return parseClaudeJson(textParts)
  } catch (error) {
    console.warn('claude request exception', error)
    return null
  }
}

const buildRetentionSignals = ({
  duration,
  frameScan,
  claude,
  mode
}: {
  duration: number
  frameScan: FrameScanSummary
  claude: ClaudeRetentionOutput
  mode: RenderMode
}) => {
  const pointCount = 14
  const points: RetentionPoint[] = []
  const heatmap: RetentionHeatCell[] = []

  const hookBoost = claude?.hookStrength ?? (mode === 'vertical' ? 0.78 : 0.55)
  const emotionLift = claude?.emotionLift ?? 0.5
  const pacingRisk = claude?.pacingRisk ?? 0.42

  for (let index = 0; index < pointCount; index += 1) {
    const timestamp = (duration * index) / Math.max(1, pointCount - 1)
    const baselineFalloff = 92 - index * (mode === 'vertical' ? 4.6 : 3.2)
    const motionPulse = Math.sin(index * 0.75 + frameScan.highMotionShortClipSignal * 1.8) * 9
    const emotionPulse = Math.cos(index * 0.58 + emotionLift * 2.2) * 5.5
    let watchedPct = clamp(baselineFalloff + motionPulse + emotionPulse, 12, 98)

    if (timestamp <= 3) watchedPct = clamp(watchedPct + hookBoost * 15, 20, 99)
    if (index > pointCount * 0.65) watchedPct = clamp(watchedPct - pacingRisk * 8, 8, 95)

    heatmap.push({
      timestamp: Number(timestamp.toFixed(2)),
      intensity: clamp((watchedPct - 10) / 90, 0.08, 1)
    })

    points.push({
      id: `${index}`,
      timestamp: Number(timestamp.toFixed(2)),
      watchedPct: Number(watchedPct.toFixed(1)),
      type: 'hook',
      label: 'Hook Moment',
      description: 'Hook moment detected.'
    })
  }

  const sortedByScore = points.slice().sort((a, b) => b.watchedPct - a.watchedPct)
  const bestIds = new Set(sortedByScore.slice(0, 2).map((item) => item.id))
  const worstIds = new Set(sortedByScore.slice(-2).map((item) => item.id))
  const peakIds = new Set(
    points
      .filter((point, index) => {
        if (index === 0 || index === points.length - 1) return false
        return point.watchedPct > points[index - 1].watchedPct && point.watchedPct > points[index + 1].watchedPct + 1.5
      })
      .slice(0, 2)
      .map((point) => point.id)
  )

  for (const point of points) {
    if (bestIds.has(point.id)) {
      point.type = 'best'
      point.label = 'Best Part: Peak Engagement'
      point.description = 'Viewers hooked here.'
      continue
    }
    if (worstIds.has(point.id)) {
      point.type = 'worst'
      point.label = 'Worst Part: Biggest Drop'
      point.description = 'Optimize pacing here.'
      continue
    }
    if (point.timestamp <= 3.1) {
      point.type = 'hook'
      point.label = 'Hook Moment'
      point.description = 'Strong opening beat in first 3 seconds.'
      continue
    }
    if (point.watchedPct < 34 || (point.watchedPct < 44 && frameScan.horizontalMotionSignal > 0.46)) {
      point.type = 'skip_zone'
      point.label = 'Skip Zone'
      point.description = 'People skipped here. Add a stronger hook or cut faster.'
      continue
    }
    if (peakIds.has(point.id)) {
      point.type = 'emotional_peak'
      point.label = 'Emotional Peak'
      point.description = 'Sentiment lift likely improved watch depth.'
      continue
    }
    point.type = 'hook'
    point.label = 'Hook Moment'
    point.description = 'Retention is stable around this beat.'
  }

  const summary = claude?.summary || 'Retention curve generated from Whisper peaks, OpenCV motion, and heuristic sentiment prediction.'

  return { points, heatmap, summary }
}

type SegmentStrategy = {
  targetCount: number
  minSeconds: number
  maxSeconds: number
  includeIntroHook: boolean
  spacingSeconds: number
}

type CreativePipelineConfig = {
  stylePreset: StylePreset
  vibeChip: VibeChip
  audioOption: AudioOption
  zoomEffect: ZoomEffect
  withAudio: boolean
}

const resolveSegmentStrategy = ({
  mode,
  pacingPreset,
  highlightReel
}: {
  mode: RenderMode
  pacingPreset: PacingPreset
  highlightReel: boolean
}): SegmentStrategy => {
  if (mode === 'vertical') {
    if (pacingPreset === 'aggressive') {
      return { targetCount: highlightReel ? 4 : 3, minSeconds: 10, maxSeconds: 22, includeIntroHook: true, spacingSeconds: 5.5 }
    }
    if (pacingPreset === 'cinematic') {
      return { targetCount: 2, minSeconds: 22, maxSeconds: 42, includeIntroHook: false, spacingSeconds: 9 }
    }
    if (pacingPreset === 'chill') {
      return { targetCount: 2, minSeconds: 18, maxSeconds: 36, includeIntroHook: true, spacingSeconds: 8 }
    }
    return { targetCount: highlightReel ? 3 : 2, minSeconds: 14, maxSeconds: 30, includeIntroHook: true, spacingSeconds: 6.5 }
  }

  if (pacingPreset === 'aggressive') {
    return { targetCount: 6, minSeconds: 8, maxSeconds: 18, includeIntroHook: true, spacingSeconds: 4 }
  }
  if (pacingPreset === 'cinematic') {
    return { targetCount: 2, minSeconds: 26, maxSeconds: 58, includeIntroHook: false, spacingSeconds: 12 }
  }
  if (pacingPreset === 'chill') {
    return { targetCount: 3, minSeconds: 18, maxSeconds: 42, includeIntroHook: false, spacingSeconds: 9 }
  }
  return { targetCount: 4, minSeconds: 12, maxSeconds: 32, includeIntroHook: true, spacingSeconds: 7 }
}

const buildAdaptiveHighlightSegments = ({
  duration,
  points,
  strategy
}: {
  duration: number
  points: RetentionPoint[]
  strategy: SegmentStrategy
}) => {
  const sortedByScore = points.slice().sort((a, b) => b.watchedPct - a.watchedPct)
  const selected: Array<{ start: number; end: number }> = []
  const safeDuration = Math.max(0.4, duration)

  const pushSegment = (start: number, end: number) => {
    const safeStart = clamp(start, 0, Math.max(0, safeDuration - 0.4))
    const safeEnd = clamp(end, safeStart + 0.4, safeDuration)
    if (safeEnd - safeStart < 0.4) return
    if (selected.some((segment) => Math.abs(segment.start - safeStart) < strategy.spacingSeconds)) return
    selected.push({ start: Number(safeStart.toFixed(2)), end: Number(safeEnd.toFixed(2)) })
  }

  if (strategy.includeIntroHook) {
    const introEnd = clamp(strategy.minSeconds + (strategy.maxSeconds - strategy.minSeconds) * 0.35, strategy.minSeconds, strategy.maxSeconds)
    pushSegment(0, Math.min(safeDuration, introEnd))
  }

  for (const point of sortedByScore) {
    if (selected.length >= strategy.targetCount) break
    const targetDuration = clamp(
      (strategy.minSeconds + strategy.maxSeconds) / 2 + (point.watchedPct - 50) * 0.22,
      strategy.minSeconds,
      strategy.maxSeconds
    )
    const start = Math.max(0, point.timestamp - targetDuration * 0.36)
    const end = Math.min(safeDuration, start + targetDuration)
    pushSegment(start, end)
  }

  if (selected.length < strategy.targetCount) {
    const fallbackLength = clamp((strategy.minSeconds + strategy.maxSeconds) / 2, 3, Math.max(3, safeDuration))
    while (selected.length < strategy.targetCount) {
      const offsetRatio = selected.length / Math.max(1, strategy.targetCount)
      const start = clamp(offsetRatio * Math.max(0, safeDuration - fallbackLength), 0, Math.max(0, safeDuration - 0.4))
      const end = clamp(start + fallbackLength, start + 0.4, safeDuration)
      pushSegment(start, end)
      if (start >= safeDuration - 1) break
    }
  }

  return selected
    .slice(0, strategy.targetCount)
    .sort((a, b) => a.start - b.start)
}

const buildStyleVideoFilters = (stylePreset: StylePreset, vibeChip: VibeChip): string[] => {
  const styleFilters: Record<StylePreset, string[]> = {
    clean: ['eq=contrast=1.05:saturation=1.08'],
    bold: ['eq=contrast=1.13:saturation=1.2', 'unsharp=5:5:0.42:3:3:0.0'],
    vintage: ['eq=contrast=1.02:saturation=0.84:brightness=0.03'],
    glitch: ['eq=contrast=1.1:saturation=1.14', 'unsharp=3:3:0.36:3:3:0.0'],
    neon: ['eq=contrast=1.09:saturation=1.28'],
    minimal: ['eq=contrast=1.01:saturation=0.94'],
    meme: ['eq=contrast=1.14:saturation=1.24']
  }
  const vibeFilters: Record<VibeChip, string[]> = {
    energetic: ['eq=contrast=1.06:saturation=1.12'],
    chill: ['eq=contrast=0.99:saturation=0.94'],
    luxury: ['eq=contrast=1.03:saturation=1.0:brightness=0.01'],
    funny: ['eq=contrast=1.08:saturation=1.14'],
    motivational: ['eq=contrast=1.09:saturation=1.1'],
    aesthetic: ['eq=contrast=1.01:saturation=1.02:brightness=0.015'],
    dark: ['eq=contrast=1.12:saturation=0.88:brightness=-0.01'],
    cinematic: ['eq=contrast=1.04:saturation=0.95']
  }
  return [...(styleFilters[stylePreset] || []), ...(vibeFilters[vibeChip] || [])]
}

const buildAudioFilterChain = (audioOption: AudioOption): string[] => {
  if (audioOption === 'mute') return []
  if (audioOption === 'voiceover_ai') {
    return [
      'highpass=f=90',
      'lowpass=f=7800',
      'afftdn=nf=-24',
      'acompressor=threshold=-18dB:ratio=2.4:attack=18:release=140',
      'loudnorm=I=-16:TP=-1.5:LRA=9'
    ]
  }
  if (audioOption === 'sfx_library') {
    return [
      'acompressor=threshold=-20dB:ratio=2.8:attack=8:release=90',
      'treble=g=2',
      'loudnorm=I=-15:TP=-1.5:LRA=10'
    ]
  }
  return ['acompressor=threshold=-20dB:ratio=2.0:attack=15:release=130', 'loudnorm=I=-16:TP=-1.5:LRA=11']
}

const resolveVerticalZoomFilter = (zoomEffect: ZoomEffect) => {
  const zoomExpr =
    zoomEffect === 'beat_zoom'
      ? "min(1.58,zoom+0.0062)"
      : zoomEffect === 'slow_push_in'
        ? "min(1.24,zoom+0.0012)"
        : zoomEffect === 'ken_burns'
          ? "min(1.3,zoom+0.0017)"
          : "min(1.45,zoom+0.0039)"
  return `scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,zoompan=z='${zoomExpr}':d=1:s=1080x1920:fps=30`
}

const shouldOutputAudio = (config: CreativePipelineConfig) => config.withAudio && config.audioOption !== 'mute'

const runHorizontalPipeline = async ({
  inputPath,
  outputPath,
  segments,
  ffmpegCommands,
  config
}: {
  inputPath: string
  outputPath: string
  segments: Array<{ start: number; end: number }>
  ffmpegCommands: string[]
  config: CreativePipelineConfig
}) => {
  const audioEnabled = shouldOutputAudio(config)
  const tempTrimmed = path.join(path.dirname(outputPath), 'horizontal_trimmed.mp4')
  let sourcePath = inputPath

  if (segments.length > 0) {
    const filterParts: string[] = []
    const concatInputs: string[] = []

    segments.forEach((segment, index) => {
      filterParts.push(`[0:v]trim=start=${segment.start}:end=${segment.end},setpts=PTS-STARTPTS[v${index}]`)
      if (audioEnabled) {
        filterParts.push(`[0:a]atrim=start=${segment.start}:end=${segment.end},asetpts=PTS-STARTPTS[a${index}]`)
        concatInputs.push(`[v${index}][a${index}]`)
      } else {
        concatInputs.push(`[v${index}]`)
      }
    })

    if (audioEnabled) {
      filterParts.push(`${concatInputs.join('')}concat=n=${segments.length}:v=1:a=1[vout][aout]`)
    } else {
      filterParts.push(`${concatInputs.join('')}concat=n=${segments.length}:v=1:a=0[vout]`)
    }

    const trimArgs = ['-y', '-i', inputPath, '-filter_complex', filterParts.join(';'), '-map', '[vout]']
    if (audioEnabled) trimArgs.push('-map', '[aout]')
    trimArgs.push('-c:v', 'libx264', '-preset', 'medium', '-crf', '20')
    if (audioEnabled) {
      trimArgs.push('-c:a', 'aac', '-b:a', '160k')
    } else {
      trimArgs.push('-an')
    }
    trimArgs.push('-movflags', '+faststart', tempTrimmed)
    await runFfmpegCommand(trimArgs, ffmpegCommands)

    sourcePath = tempTrimmed
  }

  const videoFilters = [
    'scale=1920:1080:force_original_aspect_ratio=decrease',
    'pad=1920:1080:(ow-iw)/2:(oh-ih)/2',
    ...buildStyleVideoFilters(config.stylePreset, config.vibeChip)
  ]
  const outputArgs = ['-y', '-i', sourcePath, '-vf', videoFilters.join(','), '-c:v', 'libx264', '-preset', 'medium', '-crf', '19']
  if (audioEnabled) {
    const audioFilters = buildAudioFilterChain(config.audioOption)
    if (audioFilters.length > 0) {
      outputArgs.push('-af', audioFilters.join(','))
    }
    outputArgs.push('-c:a', 'aac', '-b:a', '192k')
  } else {
    outputArgs.push('-an')
  }
  outputArgs.push('-movflags', '+faststart', outputPath)
  await runFfmpegCommand(outputArgs, ffmpegCommands)
}

const runVerticalMoviepyPipeline = async ({
  inputPath,
  outputDir,
  segments
}: {
  inputPath: string
  outputDir: string
  segments: Array<{ start: number; end: number }>
}) => {
  if (!fs.existsSync(VIBECUT_MOVIEPY_PIPELINE_SCRIPT)) {
    return { ok: false, clipPaths: [] as string[] }
  }

  const result = await runProcess(PYTHON_BIN, [
    VIBECUT_MOVIEPY_PIPELINE_SCRIPT,
    '--input',
    inputPath,
    '--output-dir',
    outputDir,
    '--mode',
    'vertical',
    '--segments-json',
    JSON.stringify(segments)
  ])

  if (result.code !== 0) {
    console.warn('moviepy pipeline failed', result.stderr || result.stdout)
    return { ok: false, clipPaths: [] as string[] }
  }

  const parsed = tryParseJson(result.stdout) as any
  const clips = Array.isArray(parsed?.clipPaths)
    ? parsed.clipPaths
        .map((clipPath: any) => String(clipPath || ''))
        .filter((clipPath: string) => clipPath.length > 0)
    : []

  return {
    ok: clips.length > 0,
    clipPaths: clips
  }
}

const runVerticalPipeline = async ({
  inputPath,
  outputPath,
  segments,
  ffmpegCommands,
  workDir,
  config
}: {
  inputPath: string
  outputPath: string
  segments: Array<{ start: number; end: number }>
  ffmpegCommands: string[]
  workDir: string
  config: CreativePipelineConfig
}) => {
  const audioEnabled = shouldOutputAudio(config)
  const clipDir = path.join(workDir, 'clips')
  ensureDir(clipDir)

  let clipPaths: string[] = []
  const moviepyResult = await runVerticalMoviepyPipeline({
    inputPath,
    outputDir: clipDir,
    segments
  })

  if (moviepyResult.ok) {
    clipPaths = moviepyResult.clipPaths
  }

  if (clipPaths.length === 0) {
    const verticalFilter = resolveVerticalZoomFilter(config.zoomEffect)

    for (let index = 0; index < segments.length; index += 1) {
      const segment = segments[index]
      const clipPath = path.join(clipDir, `clip_${String(index + 1).padStart(2, '0')}.mp4`)
      const clipArgs = ['-y', '-ss', String(segment.start), '-to', String(segment.end), '-i', inputPath, '-vf', verticalFilter, '-c:v', 'libx264', '-preset', 'medium', '-crf', '20']
      if (audioEnabled) {
        clipArgs.push('-c:a', 'aac', '-b:a', '128k')
      } else {
        clipArgs.push('-an')
      }
      clipArgs.push('-movflags', '+faststart', clipPath)
      await runFfmpegCommand(clipArgs, ffmpegCommands)
      clipPaths.push(clipPath)
    }
  }

  if (clipPaths.length === 0) {
    throw new Error('vertical pipeline did not produce clips')
  }

  const concatListPath = path.join(workDir, 'concat_list.txt')
  fs.writeFileSync(concatListPath, clipPaths.map((clipPath) => `file '${clipPath.replace(/'/g, "''")}'`).join('\n'))

  const finalVideoFilters = buildStyleVideoFilters(config.stylePreset, config.vibeChip)
  const concatArgs = [
    '-y',
    '-f',
    'concat',
    '-safe',
    '0',
    '-i',
    concatListPath,
    '-vf',
    (finalVideoFilters.length ? finalVideoFilters : ['format=yuv420p']).join(','),
    '-c:v',
    'libx264',
    '-preset',
    'medium',
    '-crf',
    '20'
  ]
  if (audioEnabled) {
    const audioFilters = buildAudioFilterChain(config.audioOption)
    if (audioFilters.length > 0) {
      concatArgs.push('-af', audioFilters.join(','))
    }
    concatArgs.push('-c:a', 'aac', '-b:a', '128k')
  } else {
    concatArgs.push('-an')
  }
  concatArgs.push('-movflags', '+faststart', outputPath)
  await runFfmpegCommand(concatArgs, ffmpegCommands)

  return clipPaths
}

const generateThumbnailSet = async ({
  sourceVideo,
  outputDir,
  duration,
  ffmpegCommands
}: {
  sourceVideo: string
  outputDir: string
  duration: number
  ffmpegCommands: string[]
}): Promise<ThumbnailOption[]> => {
  const thumbnails: ThumbnailOption[] = []
  const count = 6
  const safeDuration = Math.max(1, duration)

  for (let index = 0; index < count; index += 1) {
    const at = clamp((safeDuration * (index + 1)) / (count + 1), 0.2, safeDuration - 0.2)
    const thumbnailPath = path.join(outputDir, `thumb_${index + 1}.jpg`)
    await runFfmpegCommand([
      '-y',
      '-ss',
      String(at),
      '-i',
      sourceVideo,
      '-frames:v',
      '1',
      '-q:v',
      '2',
      thumbnailPath
    ], ffmpegCommands)

    thumbnails.push({
      id: `thumb_${index + 1}`,
      url: toOutputUrl(thumbnailPath),
      label: `Retention Option ${index + 1}`
    })
  }

  return thumbnails
}

const coerceEditorProfile = ({
  candidate,
  fallback,
  mode
}: {
  candidate: Partial<AdaptiveEditorProfile> | null | undefined
  fallback: AdaptiveEditorProfile
  mode: RenderMode
}): AdaptiveEditorProfile => {
  const safe = candidate || {}
  const pacingPreset = parsePacingPreset((safe as any).pacingPreset, fallback.pacingPreset)
  const pacingValueRaw = Number((safe as any).pacingValue)
  const pacingValue = Number.isFinite(pacingValueRaw)
    ? Math.round(clamp(pacingValueRaw, 0, 100))
    : pacingValueFromPreset(pacingPreset, mode)
  return {
    formatPreset: parseFormatPreset((safe as any).formatPreset, fallback.formatPreset),
    vibeChip: parseVibeChip((safe as any).vibeChip, fallback.vibeChip),
    stylePreset: parseStylePreset((safe as any).stylePreset, fallback.stylePreset),
    pacingPreset,
    pacingValue,
    autoDetectBestMoments: typeof (safe as any).autoDetectBestMoments === 'boolean' ? Boolean((safe as any).autoDetectBestMoments) : fallback.autoDetectBestMoments,
    captionMode: parseCaptionMode((safe as any).captionMode, fallback.captionMode),
    captionStyle: parseCaptionStyle((safe as any).captionStyle, fallback.captionStyle),
    captionFont: sanitizeCaptionFont((safe as any).captionFont, fallback.captionFont),
    captionEffect: parseCaptionEffect((safe as any).captionEffect, fallback.captionEffect),
    audioOption: parseAudioOption((safe as any).audioOption, fallback.audioOption),
    quickControls: parseQuickControls((safe as any).quickControls, fallback.quickControls),
    suggestedSubMode: parseSuggestedSubMode((safe as any).suggestedSubMode, fallback.suggestedSubMode),
    confidence: Number.isFinite(Number((safe as any).confidence)) ? clamp(Number((safe as any).confidence), 0, 1) : fallback.confidence,
    rationale: Array.isArray((safe as any).rationale)
      ? (safe as any).rationale.map((value: any) => String(value || '').trim()).filter(Boolean).slice(0, 4)
      : fallback.rationale
  }
}

const resolveRenderProfile = ({
  mode,
  payload,
  baseProfile
}: {
  mode: RenderMode
  payload: RenderRequestPayload
  baseProfile: AdaptiveEditorProfile
}): AdaptiveEditorProfile => {
  const pacingPreset = parsePacingPreset(payload.pacing, baseProfile.pacingPreset)
  const pacingValue = payload.pacing ? pacingValueFromPreset(pacingPreset, mode) : baseProfile.pacingValue
  const quickControls = parseQuickControls(payload.quickControls, baseProfile.quickControls)

  return {
    ...baseProfile,
    formatPreset: parseFormatPreset(payload.formatPreset, baseProfile.formatPreset),
    vibeChip: parseVibeChip(payload.vibeChip, baseProfile.vibeChip),
    stylePreset: parseStylePreset(payload.stylePreset, baseProfile.stylePreset),
    pacingPreset,
    pacingValue,
    autoDetectBestMoments:
      typeof payload.autoDetectBestMoments === 'boolean' ? payload.autoDetectBestMoments : baseProfile.autoDetectBestMoments,
    captionMode: parseCaptionMode(payload.captionMode, baseProfile.captionMode),
    captionStyle: parseCaptionStyle(payload.captionStyle, baseProfile.captionStyle),
    captionFont: sanitizeCaptionFont(payload.captionFont, baseProfile.captionFont),
    captionEffect: parseCaptionEffect(payload.captionEffect, baseProfile.captionEffect),
    audioOption: parseAudioOption(payload.audioOption, baseProfile.audioOption),
    quickControls,
    suggestedSubMode: parseSuggestedSubMode(payload.suggestedSubMode, baseProfile.suggestedSubMode)
  }
}

const processRenderJob = async (jobId: string, userId: string, payload: RenderRequestPayload) => {
  const job = jobs.get(jobId)
  if (!job) return

  const source = await findUploadRecord(payload.videoId, userId)
  if (!source) {
    updateJobState(jobId, {
      status: 'failed',
      progress: 0,
      errorMessage: 'Uploaded source video was not found.'
    })
    return
  }

  const mode: RenderMode = payload.mode === 'vertical' ? 'vertical' : 'horizontal'
  const jobDir = path.join(VIBECUT_RENDER_DIR, jobId)
  ensureDir(jobDir)

  const outputFileName = `${mode}_final.mp4`
  const outputVideoPath = path.join(jobDir, outputFileName)
  const ffmpegCommands: string[] = []

  try {
    updateJobState(jobId, { status: 'processing', progress: 8, mode, outputVideoPath, outputVideoUrl: toOutputUrl(outputVideoPath) })

    const transcript = await readTranscriptSummary(source.storedPath, jobDir)
    updateJobState(jobId, { progress: 22 })

    const frameScan = await runFrameScan(source.storedPath)
    const hasAudio = await probeHasAudioStream(source.storedPath)
    const fallbackSubMode: SuggestedSubMode = mode === 'vertical' ? 'highlight_mode' : 'standard_mode'
    const inferredProfile = buildAdaptiveEditorProfile({
      metadata: source.metadata,
      frameScan,
      finalMode: mode,
      suggestedSubMode: parseSuggestedSubMode(source.autoDetection?.suggestedSubMode, fallbackSubMode),
      transcript,
      fileName: source.fileName
    })
    const baseProfile = coerceEditorProfile({
      candidate: source.autoDetection?.editorProfile || null,
      fallback: inferredProfile,
      mode
    })
    const resolvedProfile = resolveRenderProfile({
      mode,
      payload,
      baseProfile
    })
    const zoomEffect = parseZoomEffect(
      payload.zoomEffect,
      resolvedProfile.quickControls.speedRamp ? 'beat_zoom' : mode === 'vertical' ? 'punch_zoom' : 'slow_push_in'
    )
    const creativeConfig: CreativePipelineConfig = {
      stylePreset: resolvedProfile.stylePreset,
      vibeChip: resolvedProfile.vibeChip,
      audioOption: resolvedProfile.audioOption,
      zoomEffect,
      withAudio: hasAudio
    }

    const claude = await runClaudeRetentionModel({
      metadata: source.metadata,
      frameScan,
      transcript,
      mode
    })

    const retention = buildRetentionSignals({
      duration: source.metadata.duration,
      frameScan,
      claude,
      mode
    })

    updateJobState(jobId, { progress: 38, retention })

    const manualSegments = parseSegments(payload.manualSegments, source.metadata.duration)
    const segmentStrategy = resolveSegmentStrategy({
      mode,
      pacingPreset: resolvedProfile.pacingPreset,
      highlightReel: resolvedProfile.quickControls.highlightReel
    })
    const autoSegments = buildAdaptiveHighlightSegments({
      duration: source.metadata.duration,
      points: retention.points,
      strategy: segmentStrategy
    })
    let segments =
      manualSegments.length > 0
        ? manualSegments
        : resolvedProfile.autoDetectBestMoments || mode === 'vertical'
          ? autoSegments
          : []
    if (mode === 'vertical' && segments.length === 0) {
      segments = [{ start: 0, end: source.metadata.duration }]
    }

    let clipPathsAbs: string[] = []

    if (mode === 'vertical') {
      clipPathsAbs = await runVerticalPipeline({
        inputPath: source.storedPath,
        outputPath: outputVideoPath,
        segments,
        ffmpegCommands,
        workDir: jobDir,
        config: creativeConfig
      })
    } else {
      await runHorizontalPipeline({
        inputPath: source.storedPath,
        outputPath: outputVideoPath,
        segments,
        ffmpegCommands,
        config: creativeConfig
      })
    }

    updateJobState(jobId, {
      progress: 78,
      ffmpegCommands
    })

    const thumbnails = await generateThumbnailSet({
      sourceVideo: outputVideoPath,
      outputDir: jobDir,
      duration: source.metadata.duration,
      ffmpegCommands
    })

    const clipUrls = clipPathsAbs.map((clipPath) => toOutputUrl(clipPath))

    updateJobState(jobId, {
      status: 'completed',
      progress: 100,
      outputVideoPath,
      outputVideoUrl: toOutputUrl(outputVideoPath),
      clipUrls,
      thumbnails,
      ffmpegCommands,
      retention: {
        ...retention,
        summary: [
          `Adaptive profile: ${resolvedProfile.vibeChip} vibe, ${resolvedProfile.stylePreset} style, ${resolvedProfile.pacingPreset} pacing.`,
          `Cuts: ${segments.length} segment${segments.length === 1 ? '' : 's'} ${manualSegments.length > 0 ? '(manual override)' : '(auto-selected)'}.`,
          `Captions: ${resolvedProfile.captionMode}/${resolvedProfile.captionStyle}. Audio: ${resolvedProfile.audioOption}${hasAudio ? '' : ' (source has no audio stream)'}.`,
          retention.summary
        ].join(' ')
      },
      errorMessage: null
    })
  } catch (error: any) {
    console.error('vibecut render pipeline failed', error)
    updateJobState(jobId, {
      status: 'failed',
      progress: 0,
      ffmpegCommands,
      errorMessage: error?.message || 'Render pipeline failed.'
    })
  }
}

router.post('/render', async (req: any, res) => {
  try {
    const userId = String(req?.user?.id || '').trim()
    if (!userId) {
      return res.status(401).json({ error: 'unauthorized', message: 'Sign in required.' })
    }

    const payload = (req.body || {}) as RenderRequestPayload
    const videoId = String(payload.videoId || '').trim()
    if (!videoId) {
      return res.status(400).json({ error: 'invalid_video_id', message: 'videoId is required.' })
    }
    const source = await findUploadRecord(videoId, userId)
    if (!source) {
      return res.status(404).json({ error: 'video_not_found', message: 'Upload video and analyze it first.' })
    }

    const mode: RenderMode = payload.mode === 'vertical' ? 'vertical' : 'horizontal'
    const jobId = crypto.randomUUID()
    const now = new Date().toISOString()
    const emptyRetention = createEmptyRetentionPayload()

    const record: RenderJobRecord = {
      id: jobId,
      userId,
      status: 'queued',
      createdAt: now,
      updatedAt: now,
      mode,
      progress: 4,
      fileName: source.fileName,
      videoId,
      outputVideoUrl: '',
      outputVideoPath: '',
      clipUrls: [],
      ffmpegCommands: [],
      thumbnails: [],
      retention: emptyRetention,
      errorMessage: null
    }

    jobs.set(jobId, record)
    void persistJobRecord(record)

    void processRenderJob(jobId, userId, payload)

    return res.json({
      jobId,
      status: 'queued',
      progress: 4
    })
  } catch (error: any) {
    console.error('vibecut render create failed', error)
    return res.status(500).json({ error: 'render_start_failed', message: error?.message || 'Could not start render.' })
  }
})

const serializeJob = (job: RenderJobRecord) => ({
  jobId: job.id,
  status: job.status,
  progress: job.progress,
  mode: job.mode,
  outputVideoUrl: job.outputVideoUrl,
  clipUrls: job.clipUrls,
  ffmpegCommands: job.ffmpegCommands,
  thumbnails: job.thumbnails,
  retention: job.retention,
  errorMessage: job.errorMessage
})

router.get('/jobs', async (req: any, res) => {
  const userId = String(req?.user?.id || '').trim()
  if (!userId) {
    return res.status(401).json({ error: 'unauthorized', message: 'Sign in required.' })
  }
  const jobsForUser = await listRecentJobsByUser(userId)
  return res.json({ jobs: jobsForUser.map(buildJobSummary) })
})

router.get('/', async (req: any, res) => {
  const userId = String(req?.user?.id || '').trim()
  if (!userId) {
    return res.status(401).json({ error: 'unauthorized', message: 'Sign in required.' })
  }
  const jobsForUser = await listRecentJobsByUser(userId)
  return res.json({ jobs: jobsForUser.map(buildJobSummary) })
})

router.get('/jobs/:id', async (req: any, res) => {
  const userId = String(req?.user?.id || '').trim()
  if (!userId) {
    return res.status(401).json({ error: 'unauthorized', message: 'Sign in required.' })
  }
  const id = String(req.params?.id || '').trim()
  if (!id) return res.status(400).json({ error: 'invalid_id', message: 'Missing job id.' })

  const job = await findJobByIdAndUser(id, userId)
  if (!job) return res.status(404).json({ error: 'not_found', message: 'Job not found.' })

  return res.json(serializeJob(job))
})

router.get('/:id', async (req: any, res) => {
  const userId = String(req?.user?.id || '').trim()
  if (!userId) {
    return res.status(401).json({ error: 'unauthorized', message: 'Sign in required.' })
  }
  const id = String(req.params?.id || '').trim()
  if (!id) return res.status(400).json({ error: 'invalid_id', message: 'Missing job id.' })

  const job = await findJobByIdAndUser(id, userId)
  if (!job) return res.status(404).json({ error: 'not_found', message: 'Job not found.' })

  return res.json(serializeJob(job))
})

export default router
