import express from 'express'
import fs from 'fs'
import os from 'os'
import path from 'path'
import crypto from 'crypto'
import { spawn, spawnSync } from 'child_process'
import { prisma } from '../db/prisma'
import { supabaseAdmin } from '../supabaseClient'
import { clampQualityForTier, normalizeQuality, qualityToHeight, type ExportQuality } from '../lib/gating'
import { createCheckoutUrlForUser } from '../services/billing'
import { getOrCreateUser } from '../services/users'
import { getUserPlan } from '../services/plans'
import { getUsageForMonth, incrementUsageForMonth } from '../services/usage'
import { getMonthKey } from '../shared/planConfig'

const router = express.Router()

const INPUT_BUCKET = process.env.SUPABASE_BUCKET_INPUT || process.env.SUPABASE_BUCKET_UPLOADS || 'uploads'
const OUTPUT_BUCKET = process.env.SUPABASE_BUCKET_OUTPUT || process.env.SUPABASE_BUCKET_OUTPUTS || 'outputs'

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
    const result = spawnSync('ffmpeg', ['-version'], { stdio: 'ignore' })
    return result.status === 0
  } catch (e) {
    return false
  }
}

const hasFfprobe = () => {
  try {
    const result = spawnSync('ffprobe', ['-version'], { stdio: 'ignore' })
    return result.status === 0
  } catch (e) {
    return false
  }
}

const runFfmpeg = (args: string[]) => {
  return new Promise<void>((resolve, reject) => {
    const proc = spawn('ffmpeg', args)
    proc.on('error', (err) => reject(err))
    proc.on('close', (code) => {
      if (code === 0) return resolve()
      reject(new Error(`ffmpeg_failed_${code}`))
    })
  })
}

const getDurationSeconds = (filePath: string) => {
  if (!hasFfprobe()) return null
  try {
    const result = spawnSync(
      'ffprobe',
      ['-v', 'error', '-show_entries', 'format=duration', '-of', 'default=nk=1:nw=1', filePath],
      { encoding: 'utf8' }
    )
    if (result.status !== 0) return null
    const value = String(result.stdout || '').trim()
    const parsed = Number.parseFloat(value)
    return Number.isFinite(parsed) ? parsed : null
  } catch (e) {
    return null
  }
}

const toMinutes = (seconds?: number | null) => {
  if (!seconds || seconds <= 0) return 0
  return Math.ceil(seconds / 60)
}

type TimeRange = { start: number; end: number }
type EditPlan = {
  hook: { start: number; duration: number }
  segments: TimeRange[]
  silences: TimeRange[]
}

const HOOK_MIN = 3
const HOOK_MAX = 5
const CUT_MIN = 3
const CUT_MAX = 10
const SILENCE_DB = -30
const SILENCE_MIN = 0.6
const HOOK_ANALYZE_MAX = 600
const SCENE_THRESHOLD = 0.45

const runFfmpegCapture = (args: string[]) => {
  return new Promise<string>((resolve, reject) => {
    const proc = spawn('ffmpeg', args)
    let stderr = ''
    proc.stderr.on('data', (data) => {
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
    const result = spawnSync(
      'ffprobe',
      ['-v', 'error', '-select_streams', 'a:0', '-show_entries', 'stream=codec_type', '-of', 'csv=p=0', filePath],
      { encoding: 'utf8' }
    )
    if (result.status !== 0) return false
    return String(result.stdout || '').trim().length > 0
  } catch (e) {
    return false
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

const detectSilences = async (filePath: string, durationSeconds: number) => {
  if (!hasFfprobe()) return [] as TimeRange[]
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

const isWindowInsideSegments = (start: number, end: number, segments: TimeRange[]) => {
  return segments.some((seg) => start >= seg.start && end <= seg.end)
}

const scoreWindow = (
  start: number,
  duration: number,
  energySamples: { time: number; rms: number }[],
  sceneChanges: number[]
) => {
  const end = start + duration
  if (!energySamples.length) return -100
  let sum = 0
  let count = 0
  for (const sample of energySamples) {
    if (sample.time < start || sample.time > end) continue
    sum += sample.rms
    count += 1
  }
  const avg = count ? sum / count : -100
  const hasScene = sceneChanges.some((t) => Math.abs(t - start) <= 0.5)
  return avg + (hasScene ? 3 : 0)
}

const pickBestHook = (
  durationSeconds: number,
  segments: TimeRange[],
  energySamples: { time: number; rms: number }[],
  sceneChanges: number[]
) => {
  const candidates = new Set<number>()
  segments.forEach((seg) => candidates.add(seg.start))
  sceneChanges.forEach((t) => candidates.add(t))
  energySamples
    .slice()
    .sort((a, b) => b.rms - a.rms)
    .slice(0, 8)
    .forEach((sample) => candidates.add(sample.time))
  candidates.add(0)

  let bestStart = 0
  let bestDuration = Math.min(HOOK_MAX, Math.max(HOOK_MIN, durationSeconds || HOOK_MIN))
  let bestScore = -Infinity

  for (const start of candidates) {
    for (let duration = HOOK_MAX; duration >= HOOK_MIN; duration -= 1) {
      const end = start + duration
      if (end > durationSeconds) continue
      if (!isWindowInsideSegments(start, end, segments)) continue
      const score = scoreWindow(start, duration, energySamples, sceneChanges)
      if (score > bestScore) {
        bestScore = score
        bestStart = start
        bestDuration = duration
      }
    }
  }

  return { start: bestStart, duration: bestDuration }
}

const subtractRange = (segments: TimeRange[], range: TimeRange) => {
  const result: TimeRange[] = []
  for (const seg of segments) {
    if (range.end <= seg.start || range.start >= seg.end) {
      result.push(seg)
      continue
    }
    if (range.start > seg.start) result.push({ start: seg.start, end: Math.max(seg.start, range.start) })
    if (range.end < seg.end) result.push({ start: Math.min(seg.end, range.end), end: seg.end })
  }
  return result.filter((seg) => seg.end - seg.start > 0.25)
}

const splitSegments = (segments: TimeRange[], minLen: number, maxLen: number) => {
  const out: TimeRange[] = []
  for (const seg of segments) {
    let start = seg.start
    let end = seg.end
    while (end - start > maxLen) {
      out.push({ start, end: start + maxLen })
      start += maxLen
    }
    if (end - start < minLen && out.length > 0) {
      out[out.length - 1].end = end
    } else if (end - start > 0.1) {
      out.push({ start, end })
    }
  }
  return out
}

const buildEditPlan = async (
  filePath: string,
  durationSeconds: number,
  onStage?: (stage: 'cutting' | 'hooking' | 'pacing') => void | Promise<void>
) => {
  if (onStage) await onStage('cutting')
  const silences = await detectSilences(filePath, durationSeconds)
  const cuts = silences.filter((s) => (s.end - s.start) >= CUT_MIN)
  const keep: TimeRange[] = []
  let cursor = 0
  for (const silence of cuts) {
    if (silence.start > cursor) keep.push({ start: cursor, end: silence.start })
    cursor = Math.max(cursor, silence.end)
  }
  if (cursor < durationSeconds) keep.push({ start: cursor, end: durationSeconds })

  const normalizedKeep = splitSegments(keep.length ? keep : [{ start: 0, end: durationSeconds }], CUT_MIN, CUT_MAX)
  if (onStage) await onStage('hooking')
  const energySamples = await detectAudioEnergy(filePath, durationSeconds).catch(() => [])
  const sceneChanges = await detectSceneChanges(filePath, durationSeconds).catch(() => [])
  const hook = pickBestHook(durationSeconds, normalizedKeep, energySamples, sceneChanges)
  const hookRange: TimeRange = { start: hook.start, end: hook.start + hook.duration }
  if (onStage) await onStage('pacing')
  const withoutHook = subtractRange(normalizedKeep, hookRange)

  return { hook, segments: withoutHook, silences }
}

const buildConcatFilter = (segments: TimeRange[], withAudio: boolean) => {
  const parts: string[] = []
  segments.forEach((seg, idx) => {
    parts.push(`[0:v]trim=start=${seg.start}:end=${seg.end},setpts=PTS-STARTPTS[v${idx}]`)
    if (withAudio) {
      parts.push(`[0:a]atrim=start=${seg.start}:end=${seg.end},asetpts=PTS-STARTPTS[a${idx}]`)
    }
  })
  if (withAudio) {
    const inputs = segments.map((_, idx) => `[v${idx}][a${idx}]`).join('')
    parts.push(`${inputs}concat=n=${segments.length}:v=1:a=1[vcat][acat]`)
  } else {
    const inputs = segments.map((_, idx) => `[v${idx}]`).join('')
    parts.push(`${inputs}concat=n=${segments.length}:v=1:a=0[vcat]`)
  }
  return parts.join(';')
}

class PlanLimitError extends Error {
  status: number
  code: string
  feature: string
  checkoutUrl?: string | null
  constructor(message: string, feature: string, checkoutUrl?: string | null) {
    super(message)
    this.status = 402
    this.code = 'plan_limit'
    this.feature = feature
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
  plan: { maxRendersPerMonth: number | null; maxMinutesPerMonth: number | null }
) => {
  const monthKey = getMonthKey()
  const usage = await getUsageForMonth(userId, monthKey)
  if (plan.maxRendersPerMonth !== null && (usage?.rendersUsed ?? 0) >= plan.maxRendersPerMonth) {
    const checkoutUrl = await createCheckoutUrlForUser(userId, 'starter', userEmail).catch(() => null)
    throw new PlanLimitError('Monthly render limit reached. Upgrade to continue.', 'renders', checkoutUrl)
  }
  if (plan.maxMinutesPerMonth !== null && (usage?.minutesUsed ?? 0) + durationMinutes > plan.maxMinutesPerMonth) {
    const checkoutUrl = await createCheckoutUrlForUser(userId, 'starter', userEmail).catch(() => null)
    throw new PlanLimitError('Monthly minutes limit reached. Upgrade to continue.', 'minutes', checkoutUrl)
  }
  return { usage, monthKey }
}

const analyzeJob = async (jobId: string, requestId?: string) => {
  console.log(`[${requestId || 'noid'}] analyze start ${jobId}`)
  const job = await prisma.job.findUnique({ where: { id: jobId } })
  if (!job) throw new Error('not_found')

  await ensureBucket(INPUT_BUCKET, true)
  await ensureBucket(OUTPUT_BUCKET, false)
  const { data, error } = await supabaseAdmin.storage.from(INPUT_BUCKET).download(job.inputPath)
  if (error) {
    await prisma.job.update({ where: { id: jobId }, data: { status: 'failed', error: 'download_failed' } })
    throw new Error('download_failed')
  }

  const buf = Buffer.from(await data.arrayBuffer())
  const tmpIn = path.join(os.tmpdir(), `${jobId}-analysis`)
  fs.writeFileSync(tmpIn, buf)
  const duration = getDurationSeconds(tmpIn)

  await prisma.job.update({
    where: { id: jobId },
    data: { status: 'analyzing', progress: 15, inputDurationSeconds: duration ? Math.round(duration) : null }
  })

  let editPlan: EditPlan | null = null
  if (duration) {
    try {
      editPlan = await buildEditPlan(tmpIn, duration, async (stage) => {
        if (stage === 'cutting') {
          await prisma.job.update({ where: { id: jobId }, data: { status: 'cutting', progress: 25 } })
        } else if (stage === 'hooking') {
          await prisma.job.update({ where: { id: jobId }, data: { status: 'hooking', progress: 35 } })
        } else if (stage === 'pacing') {
          await prisma.job.update({ where: { id: jobId }, data: { status: 'pacing', progress: 45 } })
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
    editPlan
  }
  const analysisPath = `${job.userId}/${jobId}/analysis.json`
  await supabaseAdmin.storage.from(OUTPUT_BUCKET).upload(analysisPath, Buffer.from(JSON.stringify(analysis)), { contentType: 'application/json', upsert: true })
  await prisma.job.update({
    where: { id: jobId },
    data: {
      status: editPlan ? 'pacing' : 'analyzing',
      progress: editPlan ? 50 : 30,
      inputDurationSeconds: duration ? Math.round(duration) : null
    }
  })
  console.log(`[${requestId || 'noid'}] analyze complete ${jobId}`)
  return analysis
}

const processJob = async (jobId: string, user: { id: string; email?: string }, requestedQuality?: ExportQuality, requestId?: string) => {
  console.log(`[${requestId || 'noid'}] process start ${jobId}`)
  const job = await prisma.job.findUnique({ where: { id: jobId } })
  if (!job) throw new Error('not_found')

  const settings = await prisma.userSettings.findUnique({ where: { userId: user.id } })
  const { tier, plan } = await getUserPlan(user.id)
  const desiredQuality = requestedQuality ?? getRequestedQuality(job.requestedQuality, settings?.exportQuality)
  const finalQuality = clampQualityForTier(desiredQuality, tier)
  const watermarkEnabled = plan.watermark

  await prisma.job.update({
    where: { id: jobId },
    data: {
      status: 'rendering',
      progress: 60,
      requestedQuality: desiredQuality,
      finalQuality,
      watermarkApplied: watermarkEnabled,
      priority: plan.priority
    }
  })

  await ensureBucket(INPUT_BUCKET, true)
  await ensureBucket(OUTPUT_BUCKET, false)

  const input = await supabaseAdmin.storage.from(INPUT_BUCKET).download(job.inputPath)
  if (input.error) {
    await prisma.job.update({ where: { id: jobId }, data: { status: 'failed', error: 'download_failed' } })
    throw new Error('download_failed')
  }
  const buf = Buffer.from(await input.data.arrayBuffer())
  const tmpIn = path.join(os.tmpdir(), `${jobId}-in`)
  const tmpOut = path.join(os.tmpdir(), `${jobId}-out.mp4`)
  fs.writeFileSync(tmpIn, buf)

  const durationSeconds = job.inputDurationSeconds ?? getDurationSeconds(tmpIn) ?? 0
  const durationMinutes = toMinutes(durationSeconds)
  await prisma.job.update({ where: { id: jobId }, data: { inputDurationSeconds: durationSeconds ? Math.round(durationSeconds) : null } })

  await ensureUsageWithinLimits(user.id, user.email, durationMinutes, plan)

  let processed = false
  if (hasFfmpeg()) {
    const height = qualityToHeight(finalQuality)
    const filters: string[] = []
    if (height) filters.push(`scale=-2:${height}`)
    if (watermarkEnabled) {
      filters.push("drawtext=text='AutoEditor':x=w-tw-12:y=h-th-12:fontsize=18:fontcolor=white@0.45:box=1:boxcolor=black@0.25:boxborderw=6")
    }

    const plan = durationSeconds ? await buildEditPlan(tmpIn, durationSeconds) : null
    const hookRange = plan ? { start: plan.hook.start, end: plan.hook.start + plan.hook.duration } : null
    const segments = plan
      ? [hookRange as TimeRange, ...plan.segments]
      : [{ start: 0, end: durationSeconds || 0 }]

    const validSegments = segments.filter((seg) => seg.end - seg.start > 0.25)
    const hasAudio = hasAudioStream(tmpIn)

    const argsBase = ['-y', '-nostdin', '-hide_banner', '-loglevel', 'error', '-i', tmpIn, '-movflags', '+faststart', '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23']
    if (hasAudio) argsBase.push('-c:a', 'aac')

    try {
      if (validSegments.length >= 1) {
        const filterParts: string[] = []
        const concatFilter = buildConcatFilter(validSegments, hasAudio)
        filterParts.push(concatFilter)
        if (filters.length > 0) {
          if (hasAudio) {
            filterParts.push(`[vcat]${filters.join(',')}[vout]`)
            const filter = `${filterParts.join(';')}`
            await runFfmpeg([...argsBase, '-filter_complex', filter, '-map', '[vout]', '-map', '[acat]', tmpOut])
          } else {
            filterParts.push(`[vcat]${filters.join(',')}[vout]`)
            const filter = `${filterParts.join(';')}`
            await runFfmpeg([...argsBase, '-filter_complex', filter, '-map', '[vout]', tmpOut])
          }
        } else {
          const filter = filterParts.join(';')
          if (hasAudio) {
            await runFfmpeg([...argsBase, '-filter_complex', filter, '-map', '[vcat]', '-map', '[acat]', tmpOut])
          } else {
            await runFfmpeg([...argsBase, '-filter_complex', filter, '-map', '[vcat]', tmpOut])
          }
        }
      } else {
        await runFfmpeg([...argsBase, tmpOut])
      }
      processed = true
    } catch (err) {
      if (watermarkEnabled) {
        const noWatermarkFilters = filters.filter((f) => !f.startsWith('drawtext='))
        const retryArgs = ['-y', '-nostdin', '-hide_banner', '-loglevel', 'error', '-i', tmpIn, '-movflags', '+faststart', '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23']
        if (hasAudioStream(tmpIn)) retryArgs.push('-c:a', 'aac')
        try {
          if (noWatermarkFilters.length > 0) {
            await runFfmpeg([...retryArgs, '-vf', noWatermarkFilters.join(','), tmpOut])
          } else {
            await runFfmpeg([...retryArgs, tmpOut])
          }
          processed = true
        } catch (retryErr) {
          processed = false
        }
      }
    }
  }

  if (!processed) {
    fs.writeFileSync(tmpOut, buf)
  }

  await prisma.job.update({ where: { id: jobId }, data: { progress: 95 } })
  const outBuf = fs.readFileSync(tmpOut)
  const outPath = `${job.userId}/${jobId}/output.mp4`
  const uploadResult = await supabaseAdmin.storage.from(OUTPUT_BUCKET).upload(outPath, outBuf, { contentType: 'video/mp4', upsert: true })
  if (uploadResult.error) {
    await prisma.job.update({ where: { id: jobId }, data: { status: 'failed', error: 'upload_failed' } })
    throw new Error('upload_failed')
  }

  await prisma.job.update({
    where: { id: jobId },
    data: { status: 'completed', progress: 100, outputPath: outPath, finalQuality, watermarkApplied: watermarkEnabled }
  })

  const monthKey = getMonthKey()
  await incrementUsageForMonth(user.id, monthKey, 1, durationMinutes)
  console.log(`[${requestId || 'noid'}] process complete ${jobId}`)
}

const runPipeline = async (jobId: string, user: { id: string; email?: string }, requestedQuality?: ExportQuality, requestId?: string) => {
  try {
    await analyzeJob(jobId, requestId)
    await processJob(jobId, user, requestedQuality, requestId)
  } catch (err: any) {
    if (err instanceof PlanLimitError) {
      await prisma.job.update({ where: { id: jobId }, data: { status: 'failed', error: err.code } })
      return
    }
    console.error(`[${requestId || 'noid'}] pipeline error`, err)
    await prisma.job.update({ where: { id: jobId }, data: { status: 'failed', error: String(err?.message || err) } })
  }
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
        requestedQuality: desiredQuality
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
    res.json({ jobs })
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
    res.json({ job })
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

    await prisma.job.update({
      where: { id },
      data: { inputPath, status: 'analyzing', progress: 10, requestedQuality: requestedQuality || job.requestedQuality }
    })

    res.json({ ok: true })
    void runPipeline(id, { id: req.user.id, email: req.user?.email }, requestedQuality as ExportQuality | undefined, req.requestId)
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
    const analysis = await analyzeJob(id, req.requestId)
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
    await processJob(id, { id: user.id, email: user.email }, requestedQuality as ExportQuality | undefined, req.requestId)
    res.json({ ok: true })
  } catch (err: any) {
    if (err instanceof PlanLimitError) {
      return res.status(err.status).json({ error: err.code, message: err.message, feature: err.feature, checkoutUrl: err.checkoutUrl })
    }
    console.error('process error', err)
    try {
      await prisma.job.update({ where: { id: req.params.id }, data: { status: 'failed', error: String(err) } })
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
