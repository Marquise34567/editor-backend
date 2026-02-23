import express from 'express'
import { prisma } from '../db/prisma'
import { r2 } from '../lib/r2'
import path from 'path'
import crypto from 'crypto'
import { enqueuePipeline, updateJob } from './jobs'

const router = express.Router()

const MAX_BYTES = 2 * 1024 * 1024 * 1024 // 2GB
const PART_SIZE = 10 * 1024 * 1024 // 10MB

router.post('/create', async (req: any, res) => {
  try {
    const userId = req.user.id
    const { fileName, fileSizeBytes, mimeType } = req.body
    if (!fileName || !fileSizeBytes) return res.status(400).json({ error: 'missing_params' })
    if (fileSizeBytes > MAX_BYTES) return res.status(400).json({ error: 'file_too_large', maxBytes: MAX_BYTES })
    const id = crypto.randomUUID()
    const safeName = path.basename(fileName)
    const objectKey = `${userId}/${id}/${safeName}`

    // create job record immediately
    const job = await prisma.job.create({ data: { id, userId, status: 'uploading', inputPath: objectKey, progress: 0 } })

    // create multipart upload on R2
    const createRes: any = await r2.createMultipartUpload({ Key: objectKey, ContentType: mimeType })
    const uploadId = createRes.UploadId
    return res.json({ jobId: job.id, objectKey, uploadId, partSizeBytes: PART_SIZE, bucket: r2.bucket })
  } catch (err) {
    console.error('uploads.create error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

router.post('/sign-part', async (req: any, res) => {
  try {
    const userId = req.user.id
    const { jobId, objectKey, uploadId, partNumber } = req.body
    if (!jobId || !objectKey || !uploadId || !partNumber) return res.status(400).json({ error: 'missing_params' })
    const job = await prisma.job.findUnique({ where: { id: jobId } })
    if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })
    const url = await r2.getPresignedUploadPartUrl({ Key: objectKey, UploadId: uploadId, PartNumber: Number(partNumber), expiresIn: 60 * 15 })
    res.json({ url })
  } catch (err) {
    console.error('uploads.sign-part error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

router.post('/complete', async (req: any, res) => {
  try {
    const userId = req.user.id
    const { jobId, objectKey, uploadId, parts } = req.body
    if (!jobId || !objectKey || !uploadId || !parts) return res.status(400).json({ error: 'missing_params' })
    const job = await prisma.job.findUnique({ where: { id: jobId } })
    if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })
    // complete multipart
    const completeRes = await r2.completeMultipartUpload({ Key: objectKey, UploadId: uploadId, Parts: parts })
    // update job and queue pipeline
    await updateJob(jobId, { status: 'queued', progress: 0, inputPath: objectKey })
    enqueuePipeline({ jobId, user: { id: userId, email: req.user?.email }, priorityLevel: job.priorityLevel ?? 2 })
    res.json({ ok: true, result: completeRes })
  } catch (err) {
    console.error('uploads.complete error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

router.post('/abort', async (req: any, res) => {
  try {
    const userId = req.user.id
    const { jobId, objectKey, uploadId } = req.body
    if (!jobId || !objectKey || !uploadId) return res.status(400).json({ error: 'missing_params' })
    const job = await prisma.job.findUnique({ where: { id: jobId } })
    if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })
    await r2.abortMultipartUpload({ Key: objectKey, UploadId: uploadId })
    await updateJob(jobId, { status: 'failed', error: 'upload_aborted' })
    res.json({ ok: true })
  } catch (err) {
    console.error('uploads.abort error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

export default router
