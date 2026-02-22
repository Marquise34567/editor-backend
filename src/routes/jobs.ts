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

  const analysis = { duration: duration ?? 0, size: buf.length, filename: path.basename(job.inputPath) }
  const analysisPath = `${job.userId}/${jobId}/analysis.json`
  await supabaseAdmin.storage.from(OUTPUT_BUCKET).upload(analysisPath, Buffer.from(JSON.stringify(analysis)), { contentType: 'application/json', upsert: true })
  await prisma.job.update({
    where: { id: jobId },
    data: { status: 'analyzing', progress: 35, inputDurationSeconds: duration ? Math.round(duration) : null }
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
      progress: 40,
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
    const argsBase = ['-y', '-nostdin', '-hide_banner', '-loglevel', 'error', '-i', tmpIn, '-movflags', '+faststart', '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23', '-c:a', 'aac']
    const withFilters = filters.length > 0 ? [...argsBase, '-vf', filters.join(','), tmpOut] : [...argsBase, tmpOut]
    try {
      await runFfmpeg(withFilters)
      processed = true
    } catch (err) {
      if (watermarkEnabled) {
        const noWatermarkFilters = filters.filter((f) => !f.startsWith('drawtext='))
        const retryArgs = noWatermarkFilters.length > 0 ? [...argsBase, '-vf', noWatermarkFilters.join(','), tmpOut] : [...argsBase, tmpOut]
        try {
          await runFfmpeg(retryArgs)
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
