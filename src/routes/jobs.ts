import express from 'express'
import fs from 'fs'
import os from 'os'
import path from 'path'
import crypto from 'crypto'
import { spawn, spawnSync } from 'child_process'
import { prisma } from '../db/prisma'
import { supabaseAdmin } from '../supabaseClient'
import { clampQualityForTier, normalizeQuality, type ExportQuality } from '../lib/gating'
import { getOrCreateUser } from '../services/users'
import { getUserPlan } from '../services/plans'
import { getUsageForMonth, incrementUsageForMonth } from '../services/usage'
import { getRenderUsageForMonth, incrementRenderUsage } from '../services/renderUsage'
import { getMonthKey, type PlanTier } from '../shared/planConfig'
import { broadcastJobUpdate } from '../realtime'
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
const FFMPEG_BIN = process.env.FFMPEG_BIN || process.env.FFMPEG_PATH || 'ffmpeg'
const FFPROBE_BIN = process.env.FFPROBE_BIN || process.env.FFPROBE_PATH || 'ffprobe'
const FFMPEG_LOG_LIMIT = 8000

const bucketChecks: Record<string, Promise<void> | null> = {}

const ensureBucket = async (name: string, isPublic: boolean) => {
  if (bucketChecks[name]) return bucketChecks[name]
  bucketChecks[name] = (async () => {
    const existing = await supabaseAdmin.storage.getBucket(name)
    if (existing.data) return
    if (existing.error) {
      const created = await supabaseAdmin.storage.createBucket(name, { public: isPublic })
      if (created.error) throw created.error
    }
  })()
  return bucketChecks[name]
}

const hasFfmpeg = () => {
  try {
    const result = spawnSync(FFMPEG_BIN, ['-version'], { stdio: 'ignore' })
    return result.status === 0
  } catch (e) {
    return false
  }
}

const hasFfprobe = () => {
  try {
    const result = spawnSync(FFPROBE_BIN, ['-version'], { stdio: 'ignore' })
    return result.status === 0
  } catch (e) {
    return false
  }
}

const runFfmpeg = (args: string[]) => {
  return new Promise<void>((resolve, reject) => {
    const proc = spawn(FFMPEG_BIN, args)
    let stderr = ''
    proc.stderr.on('data', (data) => {
      if (stderr.length >= FFMPEG_LOG_LIMIT) return
      stderr += data.toString()
    })
    proc.on('error', (err) => reject(err))
    proc.on('close', (code) => {
      if (code === 0) return resolve()
      const err: any = new Error(`ffmpeg_failed_${code}`)
      err.stderr = stderr
      reject(err)
    })
  })
}

const formatFfmpegCommand = (args: string[]) => {
  const quoted = args.map((arg) => {
    if (arg === '') return '""'
    if (/[\\s"'`]/.test(arg)) {
      return `"${arg.replace(/"/g, '\\"')}"`
    }
    return arg
  })
  return [FFMPEG_BIN, ...quoted].join(' ')
}

const safeUnlink = (filePath?: string | null) => {
  if (!filePath) return
  try {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath)
  } catch (e) {
    // ignore
  }
}

const updateJob = async (jobId: string, data: any) => {
  const updated = await prisma.job.update({ where: { id: jobId }, data })
  broadcastJobUpdate(updated.userId, { job: updated })
  return updated
}

const getDurationSeconds = (filePath: string) => {
  try {
    if (hasFfprobe()) {
      const result = spawnSync(
        FFPROBE_BIN,
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
    const result = spawnSync(FFMPEG_BIN, ['-hide_banner', '-i', filePath], { encoding: 'utf8' })
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
  score: number
}
type EditPlan = {
  hook: { start: number; duration: number; score: number }
  segments: Segment[]
  silences: TimeRange[]
  removedSegments: TimeRange[]
  compressedSegments: TimeRange[]
  engagementWindows: EngagementWindow[]
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
}

const HOOK_MIN = 5
const HOOK_MAX = 10
const CUT_MIN = 3
const CUT_MAX = 5
const PACE_MIN = 3
const PACE_MAX = 6
const CUT_GUARD_SEC = 0.35
const CUT_LEN_PATTERN = [3.2, 4.4, 3.6, 4.8]
const CUT_GAP_PATTERN = [1.0, 1.6, 1.2, 0.8]
const MAX_CUT_RATIO = 0.6
const ZOOM_HARD_MAX = 1.15
const ZOOM_MAX_DURATION_RATIO = 0.1
const ZOOM_EASE_SEC = 0.2
const STITCH_FADE_SEC = 0.08
const SILENCE_DB = -30
const SILENCE_MIN = 0.8
const HOOK_ANALYZE_MAX = 600
const SCENE_THRESHOLD = 0.45
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
  autoZoomMax: 1.1
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

const runFfmpegCapture = (args: string[]) => {
  return new Promise<string>((resolve, reject) => {
    const proc = spawn(FFMPEG_BIN, args)
    let stderr = ''
    proc.stderr.on('data', (data) => {
      if (stderr.length >= FFMPEG_LOG_LIMIT) return
      stderr += data.toString()
    })
    proc.on('error', (err) => reject(err))
    proc.on('close', (code) => {
      if (code === 0) return resolve(stderr)
      reject(new Error(`ffmpeg_failed_${code}`))
    })
  })
}

const hasAudioStream = (filePath: string) => {
  try {
    if (hasFfprobe()) {
      const result = spawnSync(
        FFPROBE_BIN,
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
    const result = spawnSync(FFMPEG_BIN, ['-hide_banner', '-i', filePath], { encoding: 'utf8' })
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
      FFPROBE_BIN,
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

const normalizeEnergy = (rmsDb: number) => {
  if (!Number.isFinite(rmsDb)) return 0
  const clamped = Math.min(0, Math.max(-60, rmsDb))
  return (clamped + 60) / 60
}

const buildEngagementWindows = (
  durationSeconds: number,
  energySamples: { time: number; rms: number }[],
  sceneChanges: number[],
  faceSamples: { time: number; presence: number }[] = [],
  textSamples: { time: number; density: number }[] = []
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
    const emotionIntensity = Math.min(1, 0.6 * speechIntensity + 0.25 * vocalExcitement + 0.15 * emotionalSpike)
    const score =
      0.2 * audioEnergy +
      0.2 * speechIntensity +
      0.15 * motionScore +
      0.15 * facePresence +
      0.15 * emotionIntensity +
      0.08 * textDensity +
      0.07 * vocalExcitement
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
    const result = spawnSync(FFMPEG_BIN, ['-hide_banner', '-filters'], { encoding: 'utf8' })
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
  const relevant = windows.filter((w) => w.time >= start && w.time < end)
  if (!relevant.length) return -Infinity
  const avg = relevant.reduce((sum, w) => sum + w.score, 0) / relevant.length
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
    .slice(0, 8)
    .forEach((win) => candidates.add(win.time))
  candidates.add(0)

  const maxDuration = Math.min(HOOK_MAX, durationSeconds || HOOK_MAX)
  const minDuration = Math.min(HOOK_MIN, maxDuration)

  let bestStart = 0
  let bestDuration = Math.max(minDuration, maxDuration)
  let bestScore = -Infinity

  for (const start of candidates) {
    for (let duration = maxDuration; duration >= minDuration; duration -= 1) {
      const end = start + duration
      if (end > durationSeconds) continue
      if (!isRangeCoveredBySegments(start, end, segments)) continue
      const score = scoreWindow(start, duration, windows)
      if (score > bestScore) {
        bestScore = score
        bestStart = start
        bestDuration = duration
      }
    }
  }

  return { start: bestStart, duration: bestDuration, score: bestScore }
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

const detectFillerWindows = (windows: EngagementWindow[], silences: TimeRange[]) => {
  const isSilentAt = (time: number) => {
    const windowEnd = time + 1
    return silences.some((s) => time < s.end && windowEnd > s.start)
  }
  return windows.map((w) => {
    if (isSilentAt(w.time)) return false
    const lowSpeech = w.speechIntensity < 0.25
    const lowEnergy = w.audioEnergy < 0.25 && w.audioEnergy > 0.05
    return lowSpeech && lowEnergy
  })
}

const buildBoringFlags = (windows: EngagementWindow[], silences: TimeRange[]) => {
  const faceAbsent = buildFaceAbsenceFlags(windows, 2)
  const fillerFlags = detectFillerWindows(windows, silences)
  const isSilentAt = (time: number) => {
    const windowEnd = time + 1
    return silences.some((s) => time < s.end && windowEnd > s.start)
  }
  return windows.map((w, idx) => {
    const silent = isSilentAt(w.time) && w.audioEnergy < 0.15
    const lowSpeech = w.speechIntensity < 0.25 && w.audioEnergy < 0.2
    const lowMotion = w.motionScore < 0.2 && w.sceneChangeRate < 0.2
    const staticVisual = w.motionScore < 0.1 && w.sceneChangeRate < 0.1
    const emotionalMoment = w.emotionIntensity > 0.6 || w.vocalExcitement > 0.6 || w.emotionalSpike > 0
    if (emotionalMoment) return false
    if (silent) return true
    if (fillerFlags[idx]) return true
    if (faceAbsent[idx] && (lowSpeech || lowMotion)) return true
    if (lowSpeech && lowMotion) return true
    if (staticVisual && w.audioEnergy < 0.2) return true
    return false
  })
}

const buildBoringCuts = (flags: boolean[]) => {
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
          const maxRemove = runLen * MAX_CUT_RATIO
          let removed = 0
          let cursor = runStart + CUT_GUARD_SEC
          const endLimit = runEnd - CUT_GUARD_SEC
          let patternIdx = 0
          while (cursor + CUT_MIN <= endLimit) {
            let cutLen = CUT_LEN_PATTERN[patternIdx % CUT_LEN_PATTERN.length]
            cutLen = Math.max(CUT_MIN, Math.min(CUT_MAX, cutLen))
            let actualLen = Math.min(cutLen, endLimit - cursor)
            if (actualLen < CUT_MIN) break
            if (removed + actualLen > maxRemove) {
              actualLen = Math.max(CUT_MIN, maxRemove - removed)
              if (actualLen < CUT_MIN) break
            }
            ranges.push({ start: cursor, end: cursor + actualLen })
            removed += actualLen
            cursor += actualLen + CUT_GAP_PATTERN[patternIdx % CUT_GAP_PATTERN.length]
            patternIdx += 1
          }
        }
      }
      runStart = null
    }
  }
  return mergeRanges(ranges)
}

const applyPacingPattern = (segments: Segment[], minLen: number, maxLen: number) => {
  const out: Segment[] = []
  let patternIdx = 0
  for (const seg of segments) {
    let cursor = seg.start
    const end = seg.end
    while (end - cursor > maxLen) {
      const target = CUT_LEN_PATTERN[patternIdx % CUT_LEN_PATTERN.length]
      const jitter = (patternIdx % 2 === 0 ? -0.2 : 0.2)
      const desired = Math.max(minLen, Math.min(maxLen, target + jitter))
      const nextEnd = Math.min(end, cursor + desired)
      out.push({ ...seg, start: cursor, end: nextEnd })
      cursor = nextEnd
      patternIdx += 1
    }
    const remaining = end - cursor
    if (remaining > 0.1) {
      if (remaining < minLen && out.length) {
        out[out.length - 1].end = end
      } else {
        out.push({ ...seg, start: cursor, end })
      }
    }
  }
  return out
}

const buildEditPlan = async (
  filePath: string,
  durationSeconds: number,
  options: EditOptions = DEFAULT_EDIT_OPTIONS,
  onStage?: (stage: 'cutting' | 'hooking' | 'pacing') => void | Promise<void>
) => {
  if (onStage) await onStage('cutting')
  const silences = await detectSilences(filePath, durationSeconds).catch(() => [])
  const energySamples = await detectAudioEnergy(filePath, durationSeconds).catch(() => [])
  const sceneChanges = await detectSceneChanges(filePath, durationSeconds).catch(() => [])
  const faceSamples = await detectFacePresence(filePath, durationSeconds).catch(() => [])
  const textSamples = await detectTextDensity(filePath, durationSeconds).catch(() => [])
  const windows = buildEngagementWindows(durationSeconds, energySamples, sceneChanges, faceSamples, textSamples)

  const boringFlags = options.removeBoring ? buildBoringFlags(windows, silences) : windows.map(() => false)
  const removedSegments = options.removeBoring ? buildBoringCuts(boringFlags) : []
  const compressedSegments: TimeRange[] = []

  const baseSegments = [{ start: 0, end: durationSeconds, speed: 1 }]
  const keepSegments = removedSegments.length ? subtractRanges(baseSegments, removedSegments) : baseSegments

  const minLen = PACE_MIN
  const maxLen = PACE_MAX
  const pacingInput = keepSegments.length ? keepSegments : [{ start: 0, end: durationSeconds, speed: 1 }]
  const normalizedKeep = options.onlyCuts ? pacingInput : applyPacingPattern(pacingInput, minLen, maxLen)

  if (onStage) await onStage('hooking')
  const hook = pickBestHook(durationSeconds, normalizedKeep, windows)
  const hookRange: TimeRange = { start: hook.start, end: hook.start + hook.duration }

  if (onStage) await onStage('pacing')
  const shouldMoveHook = options.autoHookMove && !options.onlyCuts
  const withoutHook = shouldMoveHook ? subtractRange(normalizedKeep, hookRange) : normalizedKeep
  const finalSegments = withoutHook.map((seg) => ({ ...seg }))

  return {
    hook,
    segments: finalSegments,
    silences,
    removedSegments,
    compressedSegments,
    engagementWindows: windows
  }
}

const applySegmentEffects = (
  segments: Segment[],
  windows: EngagementWindow[],
  options: EditOptions,
  hookRange?: TimeRange | null
) => {
  const hardMaxZoom = Math.min(options.autoZoomMax || ZOOM_HARD_MAX, ZOOM_HARD_MAX)
  const maxZoomDelta = Math.max(0, hardMaxZoom - 1)
  const totalDuration = segments.reduce((sum, seg) => {
    const speed = seg.speed && seg.speed > 0 ? seg.speed : 1
    return sum + Math.max(0, (seg.end - seg.start) / speed)
  }, 0)
  const maxZoomDuration = totalDuration * ZOOM_MAX_DURATION_RATIO
  const hasFaceSignal = windows.some((w) => w.facePresence > 0.2)

  const segmentScores = segments.map((seg) => {
    const relevant = windows.filter((w) => w.time >= seg.start && w.time < seg.end)
    const avg = (key: keyof EngagementWindow) =>
      relevant.length ? relevant.reduce((sum, w) => sum + (w[key] as number), 0) / relevant.length : 0
    const facePresence = avg('facePresence')
    const emotionIntensity = avg('emotionIntensity')
    const vocalExcitement = avg('vocalExcitement')
    const speechIntensity = avg('speechIntensity')
    const emotionalSpike = avg('emotionalSpike')
    const isHook = hookRange ? seg.start < hookRange.end && seg.end > hookRange.start : false
    const emphasisScore = Math.min(1, emotionIntensity * 0.6 + vocalExcitement * 0.3 + emotionalSpike * 0.1)
    const scoreBoost = isHook ? 0.12 : 0
    const score = emphasisScore + scoreBoost
    return {
      seg,
      facePresence,
      speechIntensity,
      emotionIntensity,
      vocalExcitement,
      emotionalSpike,
      isHook,
      score
    }
  })

  const zoomCandidates = segmentScores
    .filter((entry) => hasFaceSignal && entry.facePresence >= 0.25)
    .filter((entry) => {
      if (options.aggressiveMode) return entry.score >= 0.35
      if (entry.isHook) return entry.score >= 0.45
      return entry.score >= 0.55 && entry.speechIntensity >= 0.25
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
    const baseZoom = 0.05 + 0.06 * entry.score + (entry.isHook ? 0.02 : 0)
    zoomMap.set(entry.seg, Math.min(maxZoomDelta, baseZoom))
    remainingZoom -= duration
  }

  return segments.map((seg) => {
    const score = segmentScores.find((entry) => entry.seg === seg)
    const hasSpike = (score?.emotionalSpike ?? 0) > 0.05
    const calmNarrative = (score?.emotionIntensity ?? 0) < 0.4 && (score?.speechIntensity ?? 0) < 0.3
    const hookBoost = score?.isHook ? 0.02 : 0
    let zoom = seg.zoom ?? 0
    let brightness = seg.brightness ?? 0
    if (hasFaceSignal && options.smartZoom && (!calmNarrative || options.aggressiveMode)) {
      const desired = zoomMap.get(seg) ?? 0
      zoom = Math.max(zoom, desired + hookBoost)
    }
    if (options.emotionalBoost && hasSpike) {
      brightness = Math.max(brightness, 0.03)
    }
    zoom = Math.min(maxZoomDelta || 0, zoom)
    return { ...seg, zoom, brightness, emphasize: hasSpike || score?.isHook }
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
  opts: { withAudio: boolean; hasAudioStream: boolean; targetWidth: number; targetHeight: number }
) => {
  const parts: string[] = []
  const scalePad = `scale=${opts.targetWidth}:${opts.targetHeight}:force_original_aspect_ratio=decrease,pad=${opts.targetWidth}:${opts.targetHeight}:(ow-iw)/2:(oh-ih)/2,setsar=1,format=yuv420p`
  const durations: number[] = []

  segments.forEach((seg, idx) => {
    const speed = seg.speed && seg.speed > 0 ? seg.speed : 1
    const zoom = seg.zoom && seg.zoom > 0 ? seg.zoom : 0
    const brightness = seg.brightness && seg.brightness !== 0 ? seg.brightness : 0
    const segDuration = Math.max(0.01, (seg.end - seg.start) / speed)
    durations.push(segDuration)
    const vTrim = `trim=start=${seg.start}:end=${seg.end}`
    const vSpeed = speed !== 1 ? `,setpts=(PTS-STARTPTS)/${speed}` : ',setpts=PTS-STARTPTS'
    const vZoom = zoom > 0 ? `,scale=iw*${1 + zoom}:ih*${1 + zoom},crop=iw:ih` : ''
    const vBright = brightness !== 0 ? `,eq=brightness=${brightness}:saturation=1.05` : ''
    parts.push(`[0:v]${vTrim}${vSpeed}${vZoom}${vBright},${scalePad}[v${idx}]`)

    if (opts.withAudio) {
      const aSpeed = speed !== 1 ? buildAtempoChain(speed) : ''
      const aNormalize = 'aformat=sample_rates=48000:channel_layouts=stereo'
      const fadeLen = 0.04
      const afadeIn = `afade=t=in:st=0:d=${fadeLen}`
      const afadeOut = `afade=t=out:st=${Math.max(0, segDuration - fadeLen)}:d=${fadeLen}`
      if (opts.hasAudioStream) {
        const guard = 0.04
        const aTrim = `atrim=start=${Math.max(0, seg.start - guard)}:end=${seg.end + guard}`
        const chain = [aTrim, 'asetpts=PTS-STARTPTS', aSpeed, afadeIn, afadeOut, aNormalize].filter(Boolean).join(',')
        parts.push(`[0:a]${chain}[a${idx}]`)
      } else {
        const chain = [`anullsrc=r=48000:cl=stereo`, `atrim=duration=${segDuration}`, 'asetpts=PTS-STARTPTS', aSpeed, afadeIn, afadeOut, aNormalize]
          .filter(Boolean)
          .join(',')
        parts.push(`${chain}[a${idx}]`)
      }
    }
  })

  if (segments.length <= 1 || STITCH_FADE_SEC <= 0) {
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
    const offset = Math.max(0, Number((cumulative - fade).toFixed(3)))
    const outLabel = `vx${i}`
    parts.push(`[${vPrev}][v${i}]xfade=transition=fade:duration=${fade}:offset=${offset}[${outLabel}]`)
    fades.push(fade)
    vPrev = outLabel
    cumulative += (durations[i] || 0) - fade
  }

  if (opts.withAudio) {
    let aPrev = `a0`
    for (let i = 1; i < segments.length; i += 1) {
      const fade = fades[i - 1] ?? STITCH_FADE_SEC
      const outLabel = `ax${i}`
      parts.push(`[${aPrev}][a${i}]acrossfade=d=${fade}:c1=tri:c2=tri[${outLabel}]`)
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
  durationSeconds: number
) => {
  if (segments.length <= 2) return segments
  const tailStart = Math.max(0, durationSeconds * 0.6)
  const tailCandidates = segments
    .map((seg, idx) => ({ seg, idx, score: scoreSegment(seg, windows) }))
    .filter((entry) => entry.seg.start >= tailStart)
  if (!tailCandidates.length) return segments
  const best = tailCandidates.sort((a, b) => b.score - a.score)[0]
  if (best.idx === segments.length - 1) return segments
  const reordered = segments.slice()
  reordered.splice(best.idx, 1)
  reordered.push(best.seg)
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

const buildAudioFilters = () => {
  return [
    'highpass=f=80',
    'lowpass=f=16000',
    'afftdn',
    'acompressor=threshold=-15dB:ratio=3:attack=20:release=250',
    'loudnorm=I=-14:TP=-1.5:LRA=11'
  ]
}

const computeRetentionScore = (segments: Segment[], windows: EngagementWindow[], hookScore: number, captionsEnabled: boolean) => {
  const lengths = segments.map((seg) => seg.end - seg.start).filter((len) => len > 0)
  const avgLen = lengths.length ? lengths.reduce((sum, len) => sum + len, 0) / lengths.length : 0
  const pacingScore = avgLen > 0 ? Math.max(0, 1 - Math.abs(avgLen - 4) / 6) : 0.5
  const energies = windows.map((w) => w.audioEnergy)
  const mean = energies.length ? energies.reduce((sum, v) => sum + v, 0) / energies.length : 0
  const variance = energies.length ? energies.reduce((sum, v) => sum + (v - mean) ** 2, 0) / energies.length : 0
  const consistency = mean > 0 ? Math.max(0, 1 - Math.sqrt(variance) / (mean + 0.01)) : 0.4
  const hook = Number.isFinite(hookScore) ? Math.max(0, Math.min(1, hookScore)) : 0.5
  const subtitleScore = captionsEnabled ? 1 : 0.6
  const audioScore = 0.85
  const score = Math.round(100 * (0.25 * hook + 0.2 * consistency + 0.2 * pacingScore + 0.15 * subtitleScore + 0.2 * audioScore))
  const notes: string[] = []
  if (avgLen > 6) notes.push('Pacing is slower than short-form optimal; consider aggressive mode.')
  if (!captionsEnabled) notes.push('Enable auto subtitles for stronger retention.')
  if (hook < 0.6) notes.push('Hook strength is moderate; consider re-recording the opening.')
  return { score: Math.max(0, Math.min(100, score)), notes }
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
  durationMinutes: number,
  tier: PlanTier,
  plan: { maxRendersPerMonth: number | null; maxMinutesPerMonth: number | null }
) => {
  const monthKey = getMonthKey()
  const renderUsage = await getRenderUsageForMonth(userId, monthKey)
  const maxRenders = plan.maxRendersPerMonth
  if (maxRenders !== null && maxRenders !== undefined) {
    if ((renderUsage?.rendersCount ?? 0) >= maxRenders) {
      const requiredPlan = getRequiredPlanForRenders(tier)
      throw new PlanLimitError(
        'Monthly render limit reached. Upgrade to continue.',
        'renders',
        requiredPlan,
        undefined,
        'RENDER_LIMIT_REACHED'
      )
    }
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

const getEditOptionsForUser = async (userId: string) => {
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
  return {
    options: {
      autoHookMove: onlyCuts ? false : (settings?.autoHookMove ?? DEFAULT_EDIT_OPTIONS.autoHookMove),
      removeBoring,
      onlyCuts,
      smartZoom: onlyCuts ? false : (settings?.smartZoom ?? DEFAULT_EDIT_OPTIONS.smartZoom),
      emotionalBoost: onlyCuts ? false : (features.advancedEffects ? (settings?.emotionalBoost ?? DEFAULT_EDIT_OPTIONS.emotionalBoost) : false),
      aggressiveMode: onlyCuts ? false : (features.advancedEffects ? (settings?.aggressiveMode ?? DEFAULT_EDIT_OPTIONS.aggressiveMode) : false),
      autoCaptions: onlyCuts ? false : (subtitlesEnabled ? (settings?.autoCaptions ?? DEFAULT_EDIT_OPTIONS.autoCaptions) : false),
      musicDuck: onlyCuts ? false : (settings?.musicDuck ?? DEFAULT_EDIT_OPTIONS.musicDuck),
      subtitleStyle,
      autoZoomMax: settings?.autoZoomMax ?? plan.autoZoomMax
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

  await ensureBucket(INPUT_BUCKET, true)
  await ensureBucket(OUTPUT_BUCKET, false)
  const { data, error } = await supabaseAdmin.storage.from(INPUT_BUCKET).download(job.inputPath)
  if (error) {
    await updateJob(jobId, { status: 'failed', error: 'download_failed' })
    throw new Error('download_failed')
  }

  const buf = Buffer.from(await data.arrayBuffer())
  const tmpIn = path.join(os.tmpdir(), `${jobId}-analysis`)
  fs.writeFileSync(tmpIn, buf)
  try {
    const duration = getDurationSeconds(tmpIn)
    if (!duration || !Number.isFinite(duration) || duration <= 0) {
      await updateJob(jobId, { status: 'failed', error: 'duration_unavailable' })
      throw new Error('duration_unavailable')
    }

    await updateJob(jobId, { status: 'analyzing', progress: 15, inputDurationSeconds: Math.round(duration) })

    let editPlan: EditPlan | null = null
    if (duration) {
      try {
        editPlan = await buildEditPlan(tmpIn, duration, options, async (stage) => {
          if (stage === 'cutting') {
            await updateJob(jobId, { status: 'cutting', progress: 25 })
          } else if (stage === 'hooking') {
            await updateJob(jobId, { status: 'hooking', progress: 35 })
          } else if (stage === 'pacing') {
            await updateJob(jobId, { status: 'pacing', progress: 45 })
          }
        })
      } catch (e) {
        editPlan = null
      }
    }

    const analysis = {
      duration: duration ?? 0,
      size: buf.length,
      filename: path.basename(job.inputPath),
      hook_start_time: editPlan?.hook?.start ?? null,
      hook_end_time: editPlan?.hook ? editPlan.hook.start + editPlan.hook.duration : null,
      hook_score: editPlan?.hook?.score ?? null,
      removed_segments: editPlan?.removedSegments ?? [],
      compressed_segments: editPlan?.compressedSegments ?? [],
      editPlan
    }
    const analysisPath = `${job.userId}/${jobId}/analysis.json`
    await supabaseAdmin.storage.from(OUTPUT_BUCKET).upload(analysisPath, Buffer.from(JSON.stringify(analysis)), { contentType: 'application/json', upsert: true })
    await updateJob(jobId, {
      status: editPlan ? 'pacing' : 'analyzing',
      progress: editPlan ? 50 : 30,
      inputDurationSeconds: duration ? Math.round(duration) : null,
      analysis: analysis
    })
    console.log(`[${requestId || 'noid'}] analyze complete ${jobId}`)
    return analysis
  } finally {
    safeUnlink(tmpIn)
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
  const rawSubtitleStyle = options.subtitleStyle ?? settings?.subtitleStyle ?? DEFAULT_SUBTITLE_PRESET
  const normalizedSubtitle = normalizeSubtitlePreset(rawSubtitleStyle) ?? DEFAULT_SUBTITLE_PRESET
  if (options.autoCaptions) {
    if (!features.subtitles.enabled) {
      throw new PlanLimitError('Subtitles are temporarily disabled.', 'subtitles', 'creator')
    }
    if (!isSubtitlePresetAllowed(normalizedSubtitle, tier)) {
      const requiredPlan = getRequiredPlanForSubtitlePreset(normalizedSubtitle)
      throw new PlanLimitError('Upgrade to unlock subtitle styles.', 'subtitles', requiredPlan)
    }
  }
  const subtitleStyle = rawSubtitleStyle
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
  const watermarkEnabled = features.watermark

  await updateJob(jobId, {
    requestedQuality: desiredQuality,
    finalQuality,
    watermarkApplied: watermarkEnabled,
    priority: features.priorityQueue,
    priorityLevel: features.priorityQueue ? 1 : 2
  })

  await ensureBucket(INPUT_BUCKET, true)
  await ensureBucket(OUTPUT_BUCKET, false)

  const input = await supabaseAdmin.storage.from(INPUT_BUCKET).download(job.inputPath)
  if (input.error) {
    await updateJob(jobId, { status: 'failed', error: 'download_failed' })
    throw new Error('download_failed')
  }
  const buf = Buffer.from(await input.data.arrayBuffer())
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), `${jobId}-`))
  const tmpIn = path.join(workDir, 'input')
  const tmpOut = path.join(workDir, 'output.mp4')
  fs.writeFileSync(tmpIn, buf)
  let subtitlePath: string | null = null
  try {
    const storedDuration = job.inputDurationSeconds && job.inputDurationSeconds > 0 ? job.inputDurationSeconds : null
    const durationSeconds = storedDuration ?? getDurationSeconds(tmpIn) ?? 0
    if (!durationSeconds || durationSeconds <= 0) {
      await updateJob(jobId, { status: 'failed', error: 'duration_unavailable' })
      throw new Error('duration_unavailable')
    }
    const durationMinutes = toMinutes(durationSeconds)
    await updateJob(jobId, { inputDurationSeconds: Math.round(durationSeconds) })

    await ensureUsageWithinLimits(user.id, durationMinutes, tier, plan)

    let processed = false
    let retentionScore: number | null = null
    let optimizationNotes: string[] = []
    if (hasFfmpeg()) {
      const target = getTargetDimensions(finalQuality)

      const storedPlan = (job.analysis as any)?.editPlan as EditPlan | undefined
      const editPlan = storedPlan?.segments ? storedPlan : (durationSeconds ? await buildEditPlan(tmpIn, durationSeconds, options) : null)

      await updateJob(jobId, { status: 'story', progress: 55 })

      const hookRange: TimeRange | null = editPlan
        ? { start: editPlan.hook.start, end: editPlan.hook.start + editPlan.hook.duration }
        : null
      const hookSegment: Segment | null = hookRange ? { ...hookRange, speed: 1 } : null
      const baseSegments: Segment[] = editPlan
        ? editPlan.segments
        : [{ start: 0, end: durationSeconds || 0, speed: 1 }]
      const storySegments = editPlan && !options.onlyCuts
        ? applyStoryStructure(baseSegments, editPlan.engagementWindows, durationSeconds)
        : baseSegments
      const orderedSegments = editPlan && options.autoHookMove && !options.onlyCuts && hookSegment
        ? [hookSegment, ...storySegments]
        : storySegments
      const filteredSegments = orderedSegments.filter((seg) => seg.end - seg.start > 0.25)
      const effectedSegments = editPlan && !options.onlyCuts
        ? applySegmentEffects(filteredSegments, editPlan.engagementWindows, options, hookRange)
        : filteredSegments
      const finalSegments = editPlan && !options.onlyCuts ? applyZoomEasing(effectedSegments) : effectedSegments

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
      if (editPlan) {
        const retention = computeRetentionScore(finalSegments, editPlan.engagementWindows, editPlan.hook.score, options.autoCaptions)
        retentionScore = retention.score
        optimizationNotes = [...optimizationNotes, ...retention.notes]
      }

      await updateJob(jobId, { status: 'rendering', progress: 80 })

      const hasSegments = finalSegments.length >= 1
      const argsBase = ['-y', '-nostdin', '-hide_banner', '-loglevel', 'error', '-i', tmpIn, '-movflags', '+faststart', '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23', '-pix_fmt', 'yuv420p']
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

      const probe = probeVideoStream(tmpIn)
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

      try {
        if (hasSegments) {
          const concatFilter = buildConcatFilter(finalSegments, {
            withAudio,
            hasAudioStream: hasAudio,
            targetWidth: target.width,
            targetHeight: target.height
          })
          const fullVideoChain = [subtitleFilter, watermarkFilter].filter(Boolean).join(',')
          // If using an image watermark we must add the watermark file as a second input
          // so ffmpeg can reference it as input index 1 in the overlay filter.
          const argsWithWatermark = [...argsBase]
          if (watermarkImageExists) argsWithWatermark.push('-i', watermarkImagePath)
          const videoChains = [fullVideoChain, ''].filter((value, idx, arr) => arr.indexOf(value) === idx)

          const runWithChain = async (videoChain: string) => {
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
            const args = [...argsWithWatermark, '-filter_complex', filter, '-map', videoMap]
            if (audioMap) args.push('-map', audioMap)
            const fullArgs = [...args, tmpOut]
            try {
              await runFfmpeg(fullArgs)
            } catch (err) {
              logFfmpegFailure('concat', fullArgs, err)
              throw err
            }
          }

          let lastErr: any = null
          let ran = false
          for (const chain of videoChains) {
            try {
              await runWithChain(chain)
              ran = true
              if (chain !== fullVideoChain) {
                const reason = lastErr ? summarizeFfmpegError(lastErr) : 'ffmpeg_failed'
                optimizationNotes.push(`Render fallback: without subtitles/watermark (${reason}).`)
              }
              break
            } catch (err) {
              lastErr = err
            }
          }
          if (!ran) throw lastErr || new Error('ffmpeg_failed')
        } else {
          const fallbackArgs = [
            ...argsBase,
            '-vf',
            `scale=${target.width}:${target.height}:force_original_aspect_ratio=decrease,pad=${target.width}:${target.height}:(ow-iw)/2:(oh-ih)/2,setsar=1,format=yuv420p`,
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
        throw err
      }
    }

    if (!processed) {
      throw new Error('render_failed')
    }

    await updateJob(jobId, { progress: 95 })
    const outBuf = fs.readFileSync(tmpOut)
    const outPath = `${job.userId}/${jobId}/output.mp4`
    const uploadResult = await supabaseAdmin.storage.from(OUTPUT_BUCKET).upload(outPath, outBuf, { contentType: 'video/mp4', upsert: true })
    if (uploadResult.error) {
      await updateJob(jobId, { status: 'failed', error: 'upload_failed' })
      throw new Error('upload_failed')
    }

    await updateJob(jobId, {
      status: 'completed',
      progress: 100,
      outputPath: outPath,
      finalQuality,
      watermarkApplied: watermarkEnabled,
      retentionScore,
      optimizationNotes: optimizationNotes.length ? optimizationNotes : null
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
  try {
    const { options } = await getEditOptionsForUser(user.id)
    await analyzeJob(jobId, options, requestId)
    await processJob(jobId, user, requestedQuality, options, requestId)
  } catch (err: any) {
    if (err instanceof PlanLimitError) {
      await updateJob(jobId, { status: 'failed', error: err.code })
      return
    }
    console.error(`[${requestId || 'noid'}] pipeline error`, err)
    await updateJob(jobId, { status: 'failed', error: String(err?.message || err) })
  }
}

type QueueItem = { jobId: string; user: { id: string; email?: string }; requestedQuality?: ExportQuality; requestId?: string; priorityLevel: number }
const pipelineQueue: QueueItem[] = []
let activePipelines = 0
const MAX_PIPELINES = Number(process.env.JOB_CONCURRENCY || 1)

const processQueue = () => {
  while (activePipelines < MAX_PIPELINES && pipelineQueue.length > 0) {
    const next = pipelineQueue.shift()
    if (!next) return
    activePipelines += 1
    void runPipeline(next.jobId, next.user, next.requestedQuality, next.requestId)
      .finally(() => {
        activePipelines = Math.max(0, activePipelines - 1)
        processQueue()
      })
  }
}

const enqueuePipeline = (item: QueueItem) => {
  const index = pipelineQueue.findIndex((queued) => queued.priorityLevel > item.priorityLevel)
  if (index === -1) {
    pipelineQueue.push(item)
  } else {
    pipelineQueue.splice(index, 0, item)
  }
  processQueue()
}

const handleCreateJob = async (req: any, res: any) => {
  try {
    const userId = req.user.id
    const { filename, inputPath: providedPath, requestedQuality } = req.body
    if (!filename && !providedPath) return res.status(400).json({ error: 'filename required' })
    const id = crypto.randomUUID()
    const safeName = filename ? path.basename(filename) : path.basename(providedPath)
    const inputPath = providedPath || `${userId}/${id}/${safeName}`

    await getOrCreateUser(userId, req.user?.email)
    const { plan, tier } = await getUserPlan(userId)
    const monthKey = getMonthKey()
    const renderUsage = await getRenderUsageForMonth(userId, monthKey)
    if (plan.maxRendersPerMonth !== null && plan.maxRendersPerMonth !== undefined) {
      if ((renderUsage?.rendersCount ?? 0) >= plan.maxRendersPerMonth) {
        return res.status(403).json(buildRenderLimitPayload(plan, renderUsage))
      }
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
        priorityLevel: plan.priority ? 1 : 2
      }
    })

    try {
      const expires = 60 * 15
      const result: any = await (supabaseAdmin.storage.from(INPUT_BUCKET) as any).createSignedUploadUrl(inputPath, expires)
      const uploadUrl = result?.data?.signedUploadUrl ?? result?.data?.signedUrl ?? null
      return res.json({ job, uploadUrl, inputPath, bucket: INPUT_BUCKET })
    } catch (e) {
      console.warn('createSignedUploadUrl not available or failed, returning job only', e)
      return res.json({ job, uploadUrl: null, inputPath, bucket: INPUT_BUCKET })
    }
  } catch (err) {
    console.error('create job', err)
    res.status(500).json({ error: 'server_error' })
  }
}

// Create job and return upload URL or null
router.post('/', handleCreateJob)
router.post('/create', handleCreateJob)

// List jobs
router.get('/', async (req: any, res) => {
  try {
    const userId = req.user.id
    const jobs = await prisma.job.findMany({ where: { userId }, orderBy: { createdAt: 'desc' } })
    const payload = jobs.map((job) => ({
      id: job.id,
      status: job.status === 'completed' ? 'ready' : job.status,
      createdAt: job.createdAt,
      requestedQuality: job.requestedQuality,
      watermark: job.watermarkApplied,
      inputPath: job.inputPath,
      progress: job.progress
    }))
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
    const jobPayload: any = {
      ...job,
      status: job.status === 'completed' ? 'ready' : job.status,
      watermark: job.watermarkApplied,
      steps: [
        { key: 'queued', label: 'Queued' },
        { key: 'uploading', label: 'Uploading' },
        { key: 'analyzing', label: 'Analyzing' },
        { key: 'hooking', label: 'Hook' },
        { key: 'cutting', label: 'Cuts' },
        { key: 'pacing', label: 'Pacing' },
        { key: 'story', label: 'Story' },
        { key: 'subtitling', label: 'Subtitles' },
        { key: 'rendering', label: 'Rendering' },
        { key: 'ready', label: 'Ready' }
      ]
    }
    if (job.status === 'completed' && job.outputPath) {
      try {
        await ensureBucket(OUTPUT_BUCKET, false)
        const expires = 60 * 10
        const { data, error } = await supabaseAdmin.storage.from(OUTPUT_BUCKET).createSignedUrl(job.outputPath, expires)
        if (!error && data?.signedUrl) {
          jobPayload.outputUrl = data.signedUrl
        }
      } catch (err) {
        // ignore signed URL failures; client can fallback to output-url endpoint
      }
    }
    res.json({ job: jobPayload })
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

const handleCompleteUpload = async (req: any, res: any) => {
  try {
    const id = req.params.id
    const job = await prisma.job.findUnique({ where: { id } })
    if (!job || job.userId !== req.user.id) return res.status(404).json({ error: 'not_found' })
    const inputPath = req.body?.inputPath || job.inputPath
    const requestedQuality = req.body?.requestedQuality ? normalizeQuality(req.body.requestedQuality) : job.requestedQuality

    const { plan } = await getUserPlan(req.user.id)
    const monthKey = getMonthKey()
    const renderUsage = await getRenderUsageForMonth(req.user.id, monthKey)
    if (plan.maxRendersPerMonth !== null && plan.maxRendersPerMonth !== undefined) {
      if ((renderUsage?.rendersCount ?? 0) >= plan.maxRendersPerMonth) {
        return res.status(403).json(buildRenderLimitPayload(plan, renderUsage))
      }
    }

    await updateJob(id, { inputPath, status: 'analyzing', progress: 10, requestedQuality: requestedQuality || job.requestedQuality })

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
    const { options } = await getEditOptionsForUser(req.user.id)
    const analysis = await analyzeJob(id, options, req.requestId)
    res.json({ ok: true, analysis })
  } catch (err) {
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
    const { options } = await getEditOptionsForUser(req.user.id)
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
    console.error('process error', err)
    try {
      await updateJob(req.params.id, { status: 'failed', error: String(err) })
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
    if (!job || job.userId !== req.user.id || !job.outputPath) return res.status(404).json({ error: 'not_found' })
    await ensureBucket(OUTPUT_BUCKET, false)
    const expires = 60 * 10
    const { data, error } = await supabaseAdmin.storage.from(OUTPUT_BUCKET).createSignedUrl(job.outputPath, expires)
    if (error) return res.status(500).json({ error: 'signed_url_failed' })
    res.json({ url: data.signedUrl })
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

export default router
