import { S3Client, CreateMultipartUploadCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand, PutObjectCommand, GetObjectCommand, UploadPartCommand, DeleteObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import fs from 'fs'
import stream from 'stream'
import { promisify } from 'util'

const pipeline = promisify(stream.pipeline)

const accountId = process.env.R2_ACCOUNT_ID || process.env.S3_ACCOUNT_ID || ''
const endpoint = process.env.R2_ENDPOINT || process.env.S3_ENDPOINT || (accountId ? `https://${accountId}.r2.cloudflarestorage.com` : '')
const region = process.env.R2_REGION || process.env.S3_REGION || 'auto'
const accessKeyId = process.env.R2_ACCESS_KEY_ID || process.env.S3_ACCESS_KEY_ID || ''
const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY || process.env.S3_SECRET_ACCESS_KEY || ''
const bucket = process.env.R2_BUCKET || process.env.S3_BUCKET || ''

if (!endpoint || !accessKeyId || !secretAccessKey || !bucket) {
  throw new Error('Missing R2 environment variables. Please set R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, and R2_BUCKET')
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

export const r2Client = client

export const r2 = {
  client,
  bucket,
  accountId,
  endpoint,
  createMultipartUpload: async ({ Key, ContentType }: { Key: string; ContentType?: string }) => {
    const cmd = new CreateMultipartUploadCommand({ Bucket: bucket, Key, ContentType })
    return client.send(cmd)
  },
  generateUploadUrl: async (Key: string, ContentType?: string, expiresIn = 60 * 10) => {
    const cmd = new PutObjectCommand({ Bucket: bucket, Key, ContentType })
    return getSignedUrl(client, cmd, { expiresIn })
  },
  objectExists: async (Key: string) => {
    try {
      const cmd = new HeadObjectCommand({ Bucket: bucket, Key })
      await client.send(cmd)
      return true
    } catch (e: any) {
      if (e?.$metadata && e.$metadata.httpStatusCode === 404) return false
      // For other errors, rethrow
      throw e
    }
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

  ,
  deleteObject: async ({ Key }: { Key: string }) => {
    const cmd = new DeleteObjectCommand({ Bucket: bucket, Key })
    return client.send(cmd)
  }
}

export default r2
