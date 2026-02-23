import { S3Client, CreateMultipartUploadCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand, PutObjectCommand, GetObjectCommand, UploadPartCommand } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import fs from 'fs'
import stream from 'stream'
import { promisify } from 'util'

const pipeline = promisify(stream.pipeline)

const endpoint = process.env.S3_ENDPOINT || ''
const region = process.env.S3_REGION || 'auto'
const accessKeyId = process.env.S3_ACCESS_KEY_ID || ''
const secretAccessKey = process.env.S3_SECRET_ACCESS_KEY || ''
const bucket = process.env.S3_BUCKET || ''

if (!endpoint || !accessKeyId || !secretAccessKey || !bucket) {
  // warn but don't throw so devs can run parts that don't need R2
  console.warn('R2 envs missing: S3_ENDPOINT,S3_ACCESS_KEY_ID,S3_SECRET_ACCESS_KEY,S3_BUCKET')
}

const client = new S3Client({
  endpoint: endpoint || undefined,
  region,
  credentials: {
    accessKeyId,
    secretAccessKey
  },
  forcePathStyle: false
})

export const r2 = {
  client,
  bucket,
  createMultipartUpload: async ({ Key, ContentType }: { Key: string; ContentType?: string }) => {
    const cmd = new CreateMultipartUploadCommand({ Bucket: bucket, Key, ContentType })
    return client.send(cmd)
  },
  getPresignedUploadPartUrl: async ({ Key, UploadId, PartNumber, expiresIn = 900 }: { Key: string; UploadId: string; PartNumber: number; expiresIn?: number }) => {
    const cmd = new UploadPartCommand({ Bucket: bucket, Key, UploadId, PartNumber })
    return getSignedUrl(client, cmd, { expiresIn })
  },
  completeMultipartUpload: async ({ Key, UploadId, Parts }: { Key: string; UploadId: string; Parts: { ETag: string; PartNumber: number }[] }) => {
    const cmd = new CompleteMultipartUploadCommand({ Bucket: bucket, Key, UploadId, MultipartUpload: { Parts } })
    return client.send(cmd)
  },
  abortMultipartUpload: async ({ Key, UploadId }: { Key: string; UploadId: string }) => {
    const cmd = new AbortMultipartUploadCommand({ Bucket: bucket, Key, UploadId })
    return client.send(cmd)
  },
  getObjectToFile: async ({ Key, destPath }: { Key: string; destPath: string }) => {
    const cmd = new GetObjectCommand({ Bucket: bucket, Key })
    const res = await client.send(cmd)
    if (!res.Body) throw new Error('no_body')
    await pipeline(res.Body as any, fs.createWriteStream(destPath))
    return destPath
  },
  uploadBuffer: async ({ Key, Body, ContentType }: { Key: string; Body: Buffer; ContentType?: string }) => {
    const cmd = new PutObjectCommand({ Bucket: bucket, Key, Body, ContentType })
    return client.send(cmd)
  },
  getPresignedGetUrl: async ({ Key, expiresIn = 600 }: { Key: string; expiresIn?: number }) => {
    const cmd = new GetObjectCommand({ Bucket: bucket, Key })
    return getSignedUrl(client, cmd, { expiresIn })
  }
}

export default r2
