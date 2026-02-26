import express from 'express'
import cors from 'cors'
import bodyParser from 'body-parser'
import crypto from 'crypto'
import fs from 'fs'
import path from 'path'
import { loadEnv } from './lib/loadEnv'
import billingRoutes from './routes/billing'
import checkoutRoutes from './routes/checkout'
import jobsRoutes from './routes/jobs'
import uploadsRoutes from './routes/uploads'
import webhookRoutes from './webhooks/stripe'
import meRoutes from './routes/me'
import settingsRoutes from './routes/settings'
import publicRoutes from './routes/public'
import audioAssetsRoutes from './routes/audioAssets'
import adminRoutes from './routes/admin'
import debugRoutes from './routes/debug'
import analyticsRoutes from './routes/analytics'
import algorithmDevRoutes from './dev/algorithm/routes/algorithm'
import { requireAuth } from './middleware/requireAuth'
import { checkDb, isStubDb } from './db/prisma'
import { rateLimit } from './middleware/rateLimit'
import { recordAdminErrorLog } from './services/adminTelemetry'
import { blockBannedIp } from './middleware/blockBannedIp'
import { recordRequestMetric } from './services/requestMetrics'
import { getCaptionEngineStatus } from './lib/captionEngine'

loadEnv()
const app = express()
app.set('trust proxy', 1)
const localOutputsDir = path.join(process.cwd(), 'outputs')
if (!fs.existsSync(localOutputsDir)) {
  fs.mkdirSync(localOutputsDir, { recursive: true })
}

const parseOriginList = (value?: string | null) =>
  String(value || '')
    .split(',')
    .map((item) => item.trim().replace(/\/+$/, ''))
    .filter(Boolean)

const normalizeOrigin = (value?: string | null) => {
  if (!value) return null
  const trimmed = String(value).trim().replace(/\/+$/, '')
  if (!trimmed) return null
  try {
    const parsed = new URL(trimmed)
    const protocol = parsed.protocol.toLowerCase()
    const hostname = parsed.hostname.toLowerCase()
    const port = parsed.port
    const hasDefaultPort = (protocol === 'https:' && (!port || port === '443'))
      || (protocol === 'http:' && (!port || port === '80'))
    return `${protocol}//${hostname}${hasDefaultPort ? '' : `:${port}`}`
  } catch {
    return trimmed.toLowerCase()
  }
}

const normalizeHostSuffix = (value?: string | null) =>
  String(value || '')
    .trim()
    .replace(/^https?:\/\//i, '')
    .replace(/\/+$/, '')
    .toLowerCase()

const envOrigins = parseOriginList(process.env.CORS_ALLOWED_ORIGINS)
  .concat(parseOriginList(process.env.FRONTEND_URL))
  .concat(parseOriginList(process.env.APP_URL))

const allowedOrigins = [
  'https://www.autoeditor.app',
  'https://autoeditor.app',
  'https://autoeddd.vercel.app',
  'http://localhost:5173',
  'http://127.0.0.1:5173',
  'http://localhost:3000',
  'http://127.0.0.1:3000',
  'http://localhost:8080',
  'http://127.0.0.1:8080',
  ...envOrigins
]
const allowedOriginSet = new Set(
  allowedOrigins
    .map((origin) => normalizeOrigin(origin))
    .filter((origin): origin is string => Boolean(origin))
)

const vercelSuffixes = parseOriginList(process.env.CORS_VERCEL_SUFFIXES)
  .concat(['-quises-projects-89577714.vercel.app'])
  .map((suffix) => normalizeHostSuffix(suffix))
  .filter(Boolean)
const AUTOEDITOR_ROOT_DOMAIN = 'autoeditor.app'

const isAllowedVercelOrigin = (origin: string) => {
  try {
    const url = new URL(origin)
    if (url.protocol !== 'https:') return false
    const host = url.hostname.toLowerCase()
    return vercelSuffixes.some((suffix) => host.endsWith(suffix))
  } catch {
    return false
  }
}

const isAllowedAutoeditorOrigin = (origin: string) => {
  try {
    const url = new URL(origin)
    if (url.protocol !== 'https:') return false
    const host = url.hostname.toLowerCase()
    return host === AUTOEDITOR_ROOT_DOMAIN || host.endsWith(`.${AUTOEDITOR_ROOT_DOMAIN}`)
  } catch {
    return false
  }
}

const isAllowedLocalDevOrigin = (origin: string) => {
  try {
    const url = new URL(origin)
    const protocol = url.protocol.toLowerCase()
    if (protocol !== 'http:' && protocol !== 'https:') return false
    const host = url.hostname.toLowerCase()
    return host === 'localhost' || host === '127.0.0.1' || host === '::1'
  } catch {
    return false
  }
}

const isAllowedOrigin = (origin?: string | null) => {
  if (!origin) return true
  const normalized = normalizeOrigin(origin)
  if (!normalized) return false
  if (allowedOriginSet.has(normalized)) return true
  if (isAllowedLocalDevOrigin(normalized)) return true
  if (isAllowedAutoeditorOrigin(normalized)) return true
  if (isAllowedVercelOrigin(normalized)) return true
  return false
}

// Central origin check used by CORS middleware (handles null/undefined origin safely)
const originCallback = (origin: any, cb: any) => {
  try {
    // Allow requests with no origin (server-to-server, curl, etc.)
    if (isAllowedOrigin(origin)) return cb(null, true)
    // Build a CORS-specific error so downstream error middleware can respond safely
    const err: any = new Error('Not allowed by CORS')
    err.type = 'cors'
    err.origin = origin
    console.warn(`CORS blocked origin: ${origin}`)
    return cb(err)
  } catch (e) {
    console.error('Error while checking CORS origin', e)
    return cb(e)
  }
}

// Register CORS globally before routes
const CORS_METHODS = ['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'OPTIONS']
const CORS_EXPOSE_HEADERS = ['x-request-id', 'etag']
app.use(cors({
  origin: originCallback,
  credentials: true,
  methods: CORS_METHODS,
  exposedHeaders: CORS_EXPOSE_HEADERS,
  maxAge: 86400,
  optionsSuccessStatus: 204
}))

// Ensure OPTIONS preflight responses are handled quickly for all routes using same origin logic
app.options('*', cors({
  origin: originCallback,
  credentials: true,
  methods: CORS_METHODS,
  maxAge: 86400,
  optionsSuccessStatus: 204
}))

// Ensure CORS headers are present on downstream 401/4xx responses for allowed browser origins.
app.use((req, res, next) => {
  const origin = req.get('origin')
  if (origin && isAllowedOrigin(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin)
    res.setHeader('Access-Control-Allow-Credentials', 'true')
    res.setHeader('Access-Control-Expose-Headers', CORS_EXPOSE_HEADERS.join(','))
    res.setHeader('Vary', 'Origin')
  }
  next()
})

app.use((req, res, next) => {
  const id = crypto.randomUUID().slice(0, 12)
  ;(req as any).requestId = id
  res.setHeader('x-request-id', id)
  const started = Date.now()
  res.on('finish', () => {
    const ms = Date.now() - started
    const ip = String(req.headers['x-forwarded-for'] || '').split(',')[0]?.trim() || req.ip || null
    const userAgent = typeof req.headers['user-agent'] === 'string' ? req.headers['user-agent'] : null
    recordRequestMetric({
      t: new Date().toISOString(),
      path: req.originalUrl || req.path || '/',
      method: req.method || 'GET',
      statusCode: Number(res.statusCode || 0),
      latencyMs: ms,
      userId: req?.user?.id || null,
      ip,
      userAgent
    })
    console.log(`[${id}] ${req.method} ${req.originalUrl} ${res.statusCode} ${ms}ms`)
  })
  next()
})

app.use(blockBannedIp)

app.post('/webhooks/stripe', rateLimit({ windowMs: 60_000, max: 120 }), bodyParser.raw({ type: '*/*' }), (req, res, next) => {
  ;(req as any).rawBody = req.body
  next()
}, webhookRoutes)

app.post('/api/billing/webhook', rateLimit({ windowMs: 60_000, max: 120 }), bodyParser.raw({ type: '*/*' }), (req, res, next) => {
  ;(req as any).rawBody = req.body
  next()
}, webhookRoutes)

app.post('/api/stripe/webhook', rateLimit({ windowMs: 60_000, max: 120 }), bodyParser.raw({ type: '*/*' }), (req, res, next) => {
  ;(req as any).rawBody = req.body
  next()
}, webhookRoutes)

app.use(express.json({ limit: '10mb' }))

app.use('/api/public', publicRoutes)
app.use('/api/audio-assets', audioAssetsRoutes)
app.use('/api/debug', debugRoutes)
app.use('/outputs', express.static(localOutputsDir))
app.use('/api/billing', requireAuth, billingRoutes)
app.use('/api', requireAuth, checkoutRoutes)
app.use('/api/jobs', requireAuth, jobsRoutes)
app.use('/api/uploads', requireAuth, uploadsRoutes)
app.use('/api/me', requireAuth, meRoutes)
app.use('/api/settings', requireAuth, settingsRoutes)
app.use('/api/analytics', requireAuth, analyticsRoutes)
app.use('/api/dev/algorithm', algorithmDevRoutes)
app.use('/api/admin', adminRoutes)

app.get('/api/health', rateLimit({ windowMs: 60_000, max: 60 }), async (req, res) => {
  const version = process.env.APP_VERSION || process.env.npm_package_version || '0.0.0'
  const time = new Date().toISOString()
  const captions = getCaptionEngineStatus()
  await checkDb()
  res.json({
    ok: true,
    version,
    time,
    db: isStubDb() ? 'stub' : 'prisma',
    captions: {
      available: captions.available,
      provider: captions.provider,
      mode: captions.mode,
      reason: captions.reason
    }
  })
})

app.get('/health', rateLimit({ windowMs: 60_000, max: 60 }), async (req, res) => {
  const version = process.env.APP_VERSION || process.env.npm_package_version || '0.0.0'
  const time = new Date().toISOString()
  const captions = getCaptionEngineStatus()
  await checkDb()
  res.json({
    ok: true,
    version,
    time,
    db: isStubDb() ? 'stub' : 'prisma',
    captions: {
      available: captions.available,
      provider: captions.provider,
      mode: captions.mode,
      reason: captions.reason
    }
  })
})

app.use((err: any, req: any, res: any, next: any) => {
  const requestId = req?.requestId
  // If this is a CORS rejection, return a friendly 403 JSON response
  if (err && (err.type === 'cors' || err.message === 'Not allowed by CORS')) {
    const origin = err.origin || req.get('origin') || null
    console.warn('[CORS] Blocked request', { origin, path: req?.originalUrl, requestId })
    return res.status(403).json({ error: 'CORS_BLOCKED', origin })
  }

  // Log full stack for debugging in Railway logs (avoid logging sensitive headers)
  console.error('Unhandled error', requestId, err?.stack || err)
  void recordAdminErrorLog({
    severity: Number(err?.status || 500) >= 500 ? 'high' : 'medium',
    message: String(err?.message || 'internal_error'),
    stackSnippet: err?.stack ? String(err.stack).split('\n').slice(0, 4).join('\n') : null,
    route: req?.path || req?.originalUrl || null,
    endpoint: req?.originalUrl || null,
    userId: req?.user?.id || null,
    jobId: req?.params?.id || null,
    userAgent: typeof req?.headers?.['user-agent'] === 'string' ? req.headers['user-agent'] : null
  })
  const message = err?.message || 'Internal error'
  res.status(err?.status || 500).json({ error: 'internal_error', message, path: req?.originalUrl, requestId })
})

// Lightweight health endpoint for debug use
app.get('/api/debug/health', rateLimit({ windowMs: 60_000, max: 60 }), async (req, res) => {
  res.json({ ok: true })
})

export default app
