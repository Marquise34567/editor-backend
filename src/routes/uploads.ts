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
    const { jobId, key } = req.body
    if (!jobId || !key) return res.status(400).json({ error: 'missing_params' })
    const job = await prisma.job.findUnique({ where: { id: jobId } })
    if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })

    // Verify object exists
    try {
      const exists = await r2.objectExists(key)
      if (!exists) return res.status(404).json({ error: 'R2_OBJECT_MISSING' })
    } catch (e: any) {
      console.error('headObject failed', e?.message || e)
      return res.status(500).json({ error: 'R2_HEAD_FAILED', details: String(e?.message || e) })
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

    await updateJob(jobId, { inputPath: key, inputUrl: publicUrl, status: 'queued', progress: 1 })
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
    await updateJob(jobId, { inputPath: key, inputUrl: publicUrl, status: 'queued', progress: 1 })
    setImmediate(() => enqueuePipeline({ jobId, user: { id: userId, email: req.user?.email }, priorityLevel: job.priorityLevel ?? 2 }))
    return res.json({ ok: true, key })
  } catch (err: any) {
    console.error('uploads.proxy error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

export default router
