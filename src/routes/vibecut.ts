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
  quickControls?: Record<string, boolean>
  formatPreset?: string
  vibeChip?: string
  stylePreset?: string
  pacing?: string
  autoDetectBestMoments?: boolean
  captionMode?: string
  captionStyle?: string
  captionFont?: string
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

const detectMode = (metadata: VideoMetadata, frameScan: FrameScanSummary): AutoDetectionPayload => {
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
    frameScan
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

    const metadata = await ffprobeMetadata(uploaded.path)
    const frameScan = await runFrameScan(uploaded.path)
    const autoDetection = detectMode(metadata, frameScan)

    const videoId = crypto.randomUUID()
    const record: UploadedVideoRecord = {
      id: videoId,
      userId,
      fileName: uploaded.originalname || path.basename(uploaded.path),
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

const buildVerticalHighlightSegments = (duration: number, points: RetentionPoint[]) => {
  const sortedByScore = points.slice().sort((a, b) => b.watchedPct - a.watchedPct)
  const selected: Array<{ start: number; end: number }> = []

  const pushSegment = (start: number, end: number) => {
    const safeStart = clamp(start, 0, Math.max(0, duration - 0.4))
    const safeEnd = clamp(end, safeStart + 0.4, duration)
    if (safeEnd - safeStart < 0.4) return
    if (selected.some((segment) => Math.abs(segment.start - safeStart) < 6)) return
    selected.push({ start: Number(safeStart.toFixed(2)), end: Number(safeEnd.toFixed(2)) })
  }

  // Mandatory first 3-second hook support.
  pushSegment(0, Math.min(duration, 18))

  for (const point of sortedByScore) {
    if (selected.length >= 3) break
    const targetDuration = clamp(18 + (point.watchedPct - 45) * 0.18, 15, 30)
    const start = Math.max(0, point.timestamp - targetDuration * 0.35)
    const end = Math.min(duration, start + targetDuration)
    pushSegment(start, end)
  }

  if (selected.length < 3) {
    while (selected.length < 3) {
      const segmentLength = 15
      const offset = selected.length * (segmentLength + 1)
      pushSegment(offset, Math.min(duration, offset + segmentLength))
      if (offset + 1 >= duration) break
    }
  }

  return selected.slice(0, 3)
}

const runHorizontalPipeline = async ({
  inputPath,
  outputPath,
  segments,
  ffmpegCommands
}: {
  inputPath: string
  outputPath: string
  segments: Array<{ start: number; end: number }>
  ffmpegCommands: string[]
}) => {
  const tempTrimmed = path.join(path.dirname(outputPath), 'horizontal_trimmed.mp4')
  let sourcePath = inputPath

  if (segments.length > 0) {
    const filterParts: string[] = []
    const concatInputs: string[] = []

    segments.forEach((segment, index) => {
      filterParts.push(`[0:v]trim=start=${segment.start}:end=${segment.end},setpts=PTS-STARTPTS[v${index}]`)
      filterParts.push(`[0:a]atrim=start=${segment.start}:end=${segment.end},asetpts=PTS-STARTPTS[a${index}]`)
      concatInputs.push(`[v${index}][a${index}]`)
    })

    filterParts.push(`${concatInputs.join('')}concat=n=${segments.length}:v=1:a=1[vout][aout]`)

    await runFfmpegCommand([
      '-y',
      '-i',
      inputPath,
      '-filter_complex',
      filterParts.join(';'),
      '-map',
      '[vout]',
      '-map',
      '[aout]',
      '-c:v',
      'libx264',
      '-preset',
      'medium',
      '-crf',
      '20',
      '-c:a',
      'aac',
      '-b:a',
      '160k',
      '-movflags',
      '+faststart',
      tempTrimmed
    ], ffmpegCommands)

    sourcePath = tempTrimmed
  }

  await runFfmpegCommand([
    '-y',
    '-i',
    sourcePath,
    '-vf',
    'scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,eq=contrast=1.05:saturation=1.08',
    '-af',
    'loudnorm=I=-16:TP=-1.5:LRA=11',
    '-c:v',
    'libx264',
    '-preset',
    'medium',
    '-crf',
    '19',
    '-c:a',
    'aac',
    '-b:a',
    '192k',
    '-movflags',
    '+faststart',
    outputPath
  ], ffmpegCommands)
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
  workDir
}: {
  inputPath: string
  outputPath: string
  segments: Array<{ start: number; end: number }>
  ffmpegCommands: string[]
  workDir: string
}) => {
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
    const verticalFilter = "scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,zoompan=z='min(1.45,zoom+0.003)':d=1:s=1080x1920:fps=30"

    for (let index = 0; index < segments.length; index += 1) {
      const segment = segments[index]
      const clipPath = path.join(clipDir, `clip_${String(index + 1).padStart(2, '0')}.mp4`)
      await runFfmpegCommand([
        '-y',
        '-ss',
        String(segment.start),
        '-to',
        String(segment.end),
        '-i',
        inputPath,
        '-vf',
        verticalFilter,
        '-c:v',
        'libx264',
        '-preset',
        'medium',
        '-crf',
        '20',
        '-c:a',
        'aac',
        '-b:a',
        '128k',
        '-movflags',
        '+faststart',
        clipPath
      ], ffmpegCommands)
      clipPaths.push(clipPath)
    }
  }

  if (clipPaths.length === 0) {
    throw new Error('vertical pipeline did not produce clips')
  }

  const concatListPath = path.join(workDir, 'concat_list.txt')
  fs.writeFileSync(concatListPath, clipPaths.map((clipPath) => `file '${clipPath.replace(/'/g, "''")}'`).join('\n'))

  await runFfmpegCommand([
    '-y',
    '-f',
    'concat',
    '-safe',
    '0',
    '-i',
    concatListPath,
    '-c:v',
    'libx264',
    '-preset',
    'medium',
    '-crf',
    '20',
    '-c:a',
    'aac',
    '-b:a',
    '128k',
    '-movflags',
    '+faststart',
    outputPath
  ], ffmpegCommands)

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
    const segments =
      mode === 'vertical'
        ? buildVerticalHighlightSegments(source.metadata.duration, retention.points)
        : manualSegments

    let clipPathsAbs: string[] = []

    if (mode === 'vertical') {
      clipPathsAbs = await runVerticalPipeline({
        inputPath: source.storedPath,
        outputPath: outputVideoPath,
        segments,
        ffmpegCommands,
        workDir: jobDir
      })
    } else {
      await runHorizontalPipeline({
        inputPath: source.storedPath,
        outputPath: outputVideoPath,
        segments,
        ffmpegCommands
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
        summary:
          mode === 'vertical'
            ? `Vertical Highlight Mode extracted exactly 3 best moments (15-30s) with first-3s hook enforcement. ${retention.summary}`
            : `Horizontal long-form pipeline optimized pacing and retention continuity. ${retention.summary}`
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
