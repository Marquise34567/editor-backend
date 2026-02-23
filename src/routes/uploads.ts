import express from 'express'
import { prisma } from '../db/prisma'
import { r2 } from '../lib/r2'
import crypto from 'crypto'
import { enqueuePipeline, updateJob } from './jobs'
import bodyParser from 'body-parser'

const router = express.Router()

// POST /api/uploads/presign
router.post('/presign', async (req: any, res) => {
  try {
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
      console.error('presign failed', e?.message || e)
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
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const { jobId, uploadId, parts } = req.body
    const key = req.body?.key || req.body?.objectKey
    if (!jobId || !key) return res.status(400).json({ error: 'missing_params' })
    const job = await prisma.job.findUnique({ where: { id: jobId } })
    if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })

    // If multipart completion requested, call CompleteMultipartUpload
    if (uploadId && Array.isArray(parts)) {
      try {
        await r2.completeMultipartUpload({ Key: key, UploadId: uploadId, Parts: parts })
      } catch (e: any) {
        console.error('completeMultipartUpload failed', e?.message || e)
        return res.status(500).json({ error: 'R2_COMPLETE_FAILED', details: String(e?.message || e) })
      }
    } else {
      // Verify object exists for single-put flows
      try {
        const exists = await r2.objectExists(key)
        if (!exists) return res.status(404).json({ error: 'R2_OBJECT_MISSING' })
      } catch (e: any) {
        console.error('headObject failed', e?.message || e)
        return res.status(500).json({ error: 'R2_HEAD_FAILED', details: String(e?.message || e) })
      }
    }

    // Construct public URL if possible
    const account = process.env.R2_ACCOUNT_ID || ''
    const bucket = process.env.R2_BUCKET || r2.bucket || ''
    let publicUrl = ''
    if (account && bucket) {
      publicUrl = `https://${bucket}.${account}.r2.cloudflarestorage.com/${key}`
    } else if (process.env.R2_PUBLIC_BASE_URL) {
      publicUrl = `${process.env.R2_PUBLIC_BASE_URL.replace(/\/$/, '')}/${key}`
    } else if (r2.endpoint) {
      publicUrl = `${r2.endpoint.replace(/\/$/, '')}/${key}`
    } else {
      publicUrl = key
    }

    const bucketEnv = process.env.R2_BUCKET || r2.bucket || ''
    await updateJob(jobId, { storageProvider: 'r2', inputKey: key, inputBucket: bucketEnv, inputPath: key, inputUrl: publicUrl, status: 'queued', progress: 1 })
    // trigger processing asynchronously
    setImmediate(() => {
      try {
        enqueuePipeline({ jobId, user: { id: userId, email: req.user?.email }, priorityLevel: job.priorityLevel ?? 2 })
      } catch (e) {
        console.error('enqueuePipeline failed', e)
      }
    })
    return res.json({ ok: true })
  } catch (err: any) {
    console.error('uploads.complete error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

// POST /api/uploads/create - initiate multipart upload and return presigned part URLs
router.post('/create', async (req: any, res) => {
  try {
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const { jobId } = req.body
    // Accept legacy field names used by older frontend bundles.
    const filename = String(req.body?.filename ?? req.body?.fileName ?? '')
    const contentType = String(req.body?.contentType ?? req.body?.mimeType ?? 'application/octet-stream')
    const sizeBytes = Number(req.body?.sizeBytes ?? req.body?.fileSizeBytes ?? 0)
    if (!filename || !Number.isFinite(sizeBytes) || sizeBytes <= 0) {
      return res.status(400).json({ error: 'missing_params' })
    }
    const job = jobId ? await prisma.job.findUnique({ where: { id: jobId } }) : null
    if (jobId && (!job || job.userId !== userId)) return res.status(404).json({ error: 'not_found' })

    const safeName = String(filename).replace(/[^a-zA-Z0-9._-]/g, '_')
    const idSegment = jobId || crypto.randomUUID()
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
      console.error('createMultipartUpload failed', e?.message || e)
      return res.status(500).json({ error: 'R2_CREATE_FAILED', details: String(e?.message || e) })
    }

    // Generate presigned URLs for each part
    const presignedParts: { partNumber: number; url: string }[] = []
    for (let partNumber = 1; partNumber <= partsCount; partNumber++) {
      try {
        const url = await r2.getPresignedUploadPartUrl({ Key: key, UploadId: uploadId, PartNumber: partNumber })
        presignedParts.push({ partNumber, url })
      } catch (e: any) {
        console.error('getPresignedUploadPartUrl failed', e?.message || e)
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
      jobId: jobId || null,
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
    console.error('uploads.sign-part error', err)
    return res.status(500).json({ error: 'server_error' })
  }
})

// POST /api/uploads/abort
router.post('/abort', async (req: any, res) => {
  try {
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const { key, uploadId } = req.body
    if (!key || !uploadId) return res.status(400).json({ error: 'missing_params' })
    try {
      await r2.abortMultipartUpload({ Key: key, UploadId: uploadId })
      return res.json({ ok: true })
    } catch (e: any) {
      console.error('abortMultipartUpload failed', e?.message || e)
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
    try {
      await r2.uploadBuffer({ Key: key, Body: Buffer.from(body), ContentType: String(contentType) })
    } catch (e: any) {
      console.error('proxy upload failed', e?.message || e)
      return res.status(500).json({ error: 'PROXY_UPLOAD_FAILED', details: String(e?.message || e) })
    }
    // update job and enqueue
    const account = process.env.R2_ACCOUNT_ID || ''
    const bucket = process.env.R2_BUCKET || r2.bucket || ''
    const publicUrl = account && bucket ? `https://${bucket}.${account}.r2.cloudflarestorage.com/${key}` : `${r2.endpoint.replace(/\/$/, '')}/${key}`
    const bucketEnv = process.env.R2_BUCKET || r2.bucket || ''
    await updateJob(jobId, { storageProvider: 'r2', inputKey: key, inputBucket: bucketEnv, inputPath: key, inputUrl: publicUrl, status: 'queued', progress: 1 })
    setImmediate(() => enqueuePipeline({ jobId, user: { id: userId, email: req.user?.email }, priorityLevel: job.priorityLevel ?? 2 }))
    return res.json({ ok: true, key })
  } catch (err: any) {
    console.error('uploads.proxy error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

export default router
