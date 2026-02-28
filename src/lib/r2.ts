import {
  S3Client,
  CreateMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  PutObjectCommand,
  GetObjectCommand,
  UploadPartCommand,
  DeleteObjectCommand,
  HeadObjectCommand
} from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import fs from 'fs'
import stream from 'stream'
import { promisify } from 'util'

const pipeline = promisify(stream.pipeline)

const requiredR2Vars = ['R2_ENDPOINT', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_BUCKET'] as const
export const missingR2EnvVars = requiredR2Vars.filter((name) => {
  const value = process.env[name]
  return !value || !String(value).trim()
})
export const isR2Configured = missingR2EnvVars.length === 0

const accountId = process.env.R2_ACCOUNT_ID || ''
const endpoint = process.env.R2_ENDPOINT || ''
const accessKeyId = process.env.R2_ACCESS_KEY_ID || ''
const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY || ''
const bucket = process.env.R2_BUCKET || ''

let r2ClientInstance: S3Client | null = null

const notConfiguredError = () => {
  const err: any = new Error(`R2_NOT_CONFIGURED: missing ${missingR2EnvVars.join(', ')}`)
  err.code = 'R2_NOT_CONFIGURED'
  err.missing = missingR2EnvVars.slice()
  return err
}

const getClient = () => {
  if (!isR2Configured) throw notConfiguredError()
  if (!r2ClientInstance) {
    r2ClientInstance = new S3Client({
      region: 'auto',
      endpoint,
      credentials: { accessKeyId, secretAccessKey },
      forcePathStyle: false
    })
  }
  return r2ClientInstance
}

export const r2 = {
  get client() {
    return getClient()
  },
  bucket,
  accountId,
  endpoint,
  isConfigured: isR2Configured,
  missingEnvVars: missingR2EnvVars,
  createMultipartUpload: async ({ Key, ContentType }: { Key: string; ContentType?: string }) => {
    const cmd = new CreateMultipartUploadCommand({ Bucket: bucket, Key, ContentType })
    return getClient().send(cmd)
  },
  generateUploadUrl: async (Key: string, ContentType?: string, expiresIn = 60 * 10) => {
    const cmd = new PutObjectCommand({ Bucket: bucket, Key, ContentType })
    return getSignedUrl(getClient(), cmd, { expiresIn })
  },
  objectExists: async (Key: string) => {
    try {
      const cmd = new HeadObjectCommand({ Bucket: bucket, Key })
      await getClient().send(cmd)
      return true
    } catch (e: any) {
      if (e?.$metadata && e.$metadata.httpStatusCode === 404) return false
      throw e
    }
  },
  getPresignedUploadPartUrl: async ({
    Key,
    UploadId,
    PartNumber,
    expiresIn = 900
  }: {
    Key: string
    UploadId: string
    PartNumber: number
    expiresIn?: number
  }) => {
    const cmd = new UploadPartCommand({ Bucket: bucket, Key, UploadId, PartNumber })
    return getSignedUrl(getClient(), cmd, { expiresIn })
  },
  completeMultipartUpload: async ({
    Key,
    UploadId,
    Parts
  }: {
    Key: string
    UploadId: string
    Parts: { ETag: string; PartNumber: number }[]
  }) => {
    const cmd = new CompleteMultipartUploadCommand({
      Bucket: bucket,
      Key,
      UploadId,
      MultipartUpload: { Parts }
    })
    return getClient().send(cmd)
  },
  abortMultipartUpload: async ({ Key, UploadId }: { Key: string; UploadId: string }) => {
    const cmd = new AbortMultipartUploadCommand({ Bucket: bucket, Key, UploadId })
    return getClient().send(cmd)
  },
  getObjectToFile: async ({ Key, destPath }: { Key: string; destPath: string }) => {
    const cmd = new GetObjectCommand({ Bucket: bucket, Key })
    const res = await getClient().send(cmd)
    if (!res.Body) throw new Error('no_body')
    await pipeline(res.Body as any, fs.createWriteStream(destPath))
    return destPath
  },
  uploadBuffer: async ({ Key, Body, ContentType }: { Key: string; Body: Buffer; ContentType?: string }) => {
    const cmd = new PutObjectCommand({ Bucket: bucket, Key, Body, ContentType })
    return getClient().send(cmd)
  },
  uploadFile: async ({ Key, filePath, ContentType }: { Key: string; filePath: string; ContentType?: string }) => {
    const body = fs.createReadStream(filePath)
    const cmd = new PutObjectCommand({ Bucket: bucket, Key, Body: body, ContentType })
    return getClient().send(cmd)
  },
  getPresignedGetUrl: async ({
    Key,
    expiresIn = 600,
    responseContentDisposition
  }: {
    Key: string
    expiresIn?: number
    responseContentDisposition?: string
  }) => {
    const cmd = new GetObjectCommand({
      Bucket: bucket,
      Key,
      ResponseContentDisposition: responseContentDisposition
    })
    return getSignedUrl(getClient(), cmd, { expiresIn })
  },
  deleteObject: async ({ Key }: { Key: string }) => {
    const cmd = new DeleteObjectCommand({ Bucket: bucket, Key })
    return getClient().send(cmd)
  }
}

export default r2
