import express from 'express'
import { prisma } from '../db/prisma'
import { r2 } from '../lib/r2'
import crypto from 'crypto'
import { enqueuePipeline, updateJob } from './jobs'
import bodyParser from 'body-parser'
import { supabaseAdmin } from '../supabaseClient'

const router = express.Router()
const INPUT_BUCKET = process.env.SUPABASE_BUCKET_INPUT || process.env.SUPABASE_BUCKET_UPLOADS || 'uploads'

const logAwsError = (label: string, err: any) => {
  console.error(label, {
    name: err?.name,
    message: err?.message,
    code: err?.code || err?.Code,
    stack: err?.stack,
    metadata: err?.$metadata,
    raw: err
  })
}

const r2NotConfigured = (res: any) => {
  return res.status(503).json({ error: 'R2_NOT_CONFIGURED', missing: r2.missingEnvVars || [] })
}

const parseJobIdFromPath = (value: unknown, userId: string) => {
  if (!value) return null
  const raw = String(value).trim()
  if (!raw) return null
  const clean = raw.replace(/^\/+|\/+$/g, '')
  const parts = clean.split('/')
  // uploads/{userId}/{jobId}/...
  if (parts.length >= 4 && parts[0] === 'uploads' && parts[1] === userId) return parts[2]
  // {userId}/{jobId}/...
  if (parts.length >= 3 && parts[0] === userId) return parts[1]
  return null
}

const resolveJobIdForCreate = async (userId: string, req: any) => {
  const explicitJobId = typeof req.body?.jobId === 'string' ? req.body.jobId.trim() : ''
  if (explicitJobId) return explicitJobId

  const fromInputPath = parseJobIdFromPath(req.body?.inputPath, userId)
  if (fromInputPath) return fromInputPath

  // Legacy frontend payloads may omit jobId; attach upload to the latest non-terminal job for this user.
  const list = await prisma.job.findMany({
    where: { userId },
    orderBy: { createdAt: 'desc' },
    take: 20
  })
  const sorted = (Array.isArray(list) ? list : [])
    .slice()
    .sort((a: any, b: any) => {
      const at = new Date(a?.createdAt || 0).getTime()
      const bt = new Date(b?.createdAt || 0).getTime()
      return bt - at
    })
  const open = sorted.find((job: any) => {
    const status = String(job?.status || '').toLowerCase()
    return status !== 'completed' && status !== 'failed'
  })
  return open?.id || null
}

// POST /api/uploads/presign
router.post('/presign', async (req: any, res) => {
  try {
    if (!r2.isConfigured) return r2NotConfigured(res)
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const { jobId, filename, contentType } = req.body
    if (!jobId || !filename) return res.status(400).json({ error: 'missing_params' })
    const job = await prisma.job.findUnique({ where: { id: jobId } })
    if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })

    const safeName = filename.replace(/[^a-zA-Z0-9._-]/g, '_')
    const key = `uploads/${userId}/${jobId}/${Date.now()}-${safeName}`
    try {
      const uploadUrl = await r2.generateUploadUrl(key, contentType || 'application/octet-stream')
      return res.json({ uploadUrl, key, bucket: r2.bucket })
    } catch (e: any) {
      logAwsError('presign failed', e)
      return res.status(500).json({ error: 'PRESIGN_FAILED', details: String(e?.message || e) })
    }
  } catch (err: any) {
    console.error('uploads.presign error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

// POST /api/uploads/complete
router.post('/complete', async (req: any, res) => {
  try {
    if (!r2.isConfigured) return r2NotConfigured(res)
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const { uploadId, parts } = req.body
    const key = req.body?.key || req.body?.objectKey
    const resolvedJobId =
      (typeof req.body?.jobId === 'string' ? req.body.jobId.trim() : '') ||
      parseJobIdFromPath(req.body?.inputPath, userId) ||
      parseJobIdFromPath(key, userId)
    if (!resolvedJobId || !key) return res.status(400).json({ error: 'missing_params' })
    const job = await prisma.job.findUnique({ where: { id: resolvedJobId } })
    if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })

    // If multipart completion requested, call CompleteMultipartUpload
    if (uploadId && Array.isArray(parts)) {
      try {
        await r2.completeMultipartUpload({ Key: key, UploadId: uploadId, Parts: parts })
      } catch (e: any) {
        logAwsError('completeMultipartUpload failed', e)
        return res.status(500).json({ error: 'R2_COMPLETE_FAILED', details: String(e?.message || e) })
      }
    } else {
      // Verify object exists for single-put flows
      try {
        const exists = await r2.objectExists(key)
        if (!exists) return res.status(404).json({ error: 'R2_OBJECT_MISSING' })
      } catch (e: any) {
        logAwsError('headObject failed', e)
        return res.status(500).json({ error: 'R2_HEAD_FAILED', details: String(e?.message || e) })
      }
    }

    await updateJob(resolvedJobId, { inputPath: key, status: 'queued', progress: 1 })
    // trigger processing asynchronously
    setImmediate(() => {
      try {
        enqueuePipeline({ jobId: resolvedJobId, user: { id: userId, email: req.user?.email }, priorityLevel: job.priorityLevel ?? 2 })
      } catch (e) {
        console.error('enqueuePipeline failed', e)
      }
    })
    return res.json({ ok: true, jobId: resolvedJobId })
  } catch (err: any) {
    console.error('uploads.complete error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

// POST /api/uploads/create - initiate multipart upload and return presigned part URLs
router.post('/create', async (req: any, res) => {
  try {
    if (!r2.isConfigured) return r2NotConfigured(res)
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const resolvedJobId = await resolveJobIdForCreate(userId, req)
    if (!resolvedJobId) return res.status(400).json({ error: 'missing_jobId' })
    // Accept legacy field names used by older frontend bundles.
    const filename = String(req.body?.filename ?? req.body?.fileName ?? '')
    const contentType = String(req.body?.contentType ?? req.body?.mimeType ?? 'application/octet-stream')
    const sizeBytes = Number(req.body?.sizeBytes ?? req.body?.fileSizeBytes ?? 0)
    if (!filename || !Number.isFinite(sizeBytes) || sizeBytes <= 0) {
      return res.status(400).json({ error: 'missing_params' })
    }
    const job = await prisma.job.findUnique({ where: { id: resolvedJobId } })
    if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })

    const safeName = String(filename).replace(/[^a-zA-Z0-9._-]/g, '_')
    const idSegment = resolvedJobId || crypto.randomUUID()
    const key = `uploads/${userId}/${idSegment}/${Date.now()}-${safeName}`

    // Choose part size (start 15MB) and ensure parts <= 10000
    const MIN_PART_SIZE = 5 * 1024 * 1024
    let partSize = 15 * 1024 * 1024
    let partsCount = Math.ceil(sizeBytes / partSize)
    while (partsCount > 10000) {
      partSize = Math.max(partSize * 2, MIN_PART_SIZE)
      partsCount = Math.ceil(sizeBytes / partSize)
    }

    // Initiate multipart upload
    let uploadId: string
    try {
      const create = await r2.createMultipartUpload({ Key: key, ContentType: contentType || 'application/octet-stream' })
      uploadId = create.UploadId as string
      if (!uploadId) throw new Error('no_upload_id')
    } catch (e: any) {
      logAwsError('createMultipartUpload failed', e)
      return res.status(500).json({ error: 'R2_CREATE_FAILED', details: String(e?.message || e) })
    }

    // Generate presigned URLs for each part
    const presignedParts: { partNumber: number; url: string }[] = []
    for (let partNumber = 1; partNumber <= partsCount; partNumber++) {
      try {
        const url = await r2.getPresignedUploadPartUrl({ Key: key, UploadId: uploadId, PartNumber: partNumber })
        presignedParts.push({ partNumber, url })
      } catch (e: any) {
        logAwsError('getPresignedUploadPartUrl failed', e)
        // Abort on failure
        try { await r2.abortMultipartUpload({ Key: key, UploadId: uploadId }) } catch (e) {}
        return res.status(500).json({ error: 'R2_SIGN_PARTS_FAILED', details: String(e?.message || e) })
      }
    }

    return res.json({
      uploadId,
      key,
      partSize,
      presignedParts,
      // Backward-compatible aliases for older frontend bundles.
      objectKey: key,
      partSizeBytes: partSize,
      jobId: resolvedJobId,
      completeUrl: '/api/uploads/complete',
      abortUrl: '/api/uploads/abort'
    })
  } catch (err: any) {
    console.error('uploads.create error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

// POST /api/uploads/sign-part - compatibility endpoint for legacy clients
router.post('/sign-part', async (req: any, res) => {
  try {
    if (!r2.isConfigured) return r2NotConfigured(res)
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const jobId = req.body?.jobId as string | undefined
    const key = (req.body?.key || req.body?.objectKey) as string | undefined
    const uploadId = req.body?.uploadId as string | undefined
    const partNumber = Number(req.body?.partNumber)
    if (!key || !uploadId || !Number.isFinite(partNumber) || partNumber < 1) {
      return res.status(400).json({ error: 'missing_params' })
    }
    if (jobId) {
      const job = await prisma.job.findUnique({ where: { id: jobId } })
      if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })
    }
    const url = await r2.getPresignedUploadPartUrl({
      Key: key,
      UploadId: uploadId,
      PartNumber: partNumber
    })
    return res.json({ url })
  } catch (err: any) {
    logAwsError('uploads.sign-part error', err)
    return res.status(500).json({ error: 'server_error' })
  }
})

// POST /api/uploads/abort
router.post('/abort', async (req: any, res) => {
  try {
    if (!r2.isConfigured) return r2NotConfigured(res)
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const { key, uploadId } = req.body
    if (!key || !uploadId) return res.status(400).json({ error: 'missing_params' })
    try {
      await r2.abortMultipartUpload({ Key: key, UploadId: uploadId })
      return res.json({ ok: true })
    } catch (e: any) {
      logAwsError('abortMultipartUpload failed', e)
      return res.status(500).json({ error: 'R2_ABORT_FAILED', details: String(e?.message || e) })
    }
  } catch (err: any) {
    console.error('uploads.abort error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

// Optional proxy fallback: POST /api/uploads/proxy?jobId=...&key=...
router.post('/proxy', bodyParser.raw({ type: '*/*', limit: '3gb' }), async (req: any, res) => {
  try {
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const jobId = req.query.jobId as string | undefined
    let key = (req.query.key as string) || undefined
    if (!jobId) return res.status(400).json({ error: 'missing_jobId' })
    const job = await prisma.job.findUnique({ where: { id: jobId } })
    if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })
    if (!key) {
      key = `uploads/${userId}/${jobId}/${Date.now()}-upload`
    }
    const contentType = req.headers['content-type'] || 'application/octet-stream'
    const body = req.body as Buffer
    if (!body || body.length === 0) return res.status(400).json({ error: 'missing_body' })
    if (r2.isConfigured) {
      try {
        await r2.uploadBuffer({ Key: key, Body: Buffer.from(body), ContentType: String(contentType) })
      } catch (e: any) {
        logAwsError('proxy upload failed', e)
        return res.status(500).json({ error: 'PROXY_UPLOAD_FAILED', details: String(e?.message || e) })
      }
    } else {
      const { error } = await supabaseAdmin.storage
        .from(INPUT_BUCKET)
        .upload(key, Buffer.from(body), { contentType: String(contentType), upsert: true })
      if (error) {
        console.error('proxy upload failed (supabase fallback)', error)
        return res.status(500).json({ error: 'PROXY_UPLOAD_FAILED', details: String(error.message || error) })
      }
    }
    await updateJob(jobId, { inputPath: key, status: 'queued', progress: 1 })
    setImmediate(() => enqueuePipeline({ jobId, user: { id: userId, email: req.user?.email }, priorityLevel: job.priorityLevel ?? 2 }))
    return res.json({ ok: true, key })
  } catch (err: any) {
    console.error('uploads.proxy error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

export default router
