import express from 'express'
import cors from 'cors'
import bodyParser from 'body-parser'
import crypto from 'crypto'
import { loadEnv } from './lib/loadEnv'
import billingRoutes from './routes/billing'
import checkoutRoutes from './routes/checkout'
import jobsRoutes from './routes/jobs'
import uploadsRoutes from './routes/uploads'
import webhookRoutes from './webhooks/stripe'
import meRoutes from './routes/me'
import settingsRoutes from './routes/settings'
import publicRoutes from './routes/public'
import adminRoutes from './routes/admin'
import debugRoutes from './routes/debug'
import { requireAuth } from './middleware/requireAuth'
import { checkDb, isStubDb } from './db/prisma'
import { rateLimit } from './middleware/rateLimit'

loadEnv()
const app = express()
app.set('trust proxy', 1)

const allowedOrigins = [
  'https://www.autoeditor.app',
  'https://autoeditor.app',
  'http://localhost:5173',
  'http://localhost:3000'
]

// Central origin check used by CORS middleware (handles null/undefined origin safely)
const originCallback = (origin: any, cb: any) => {
  try {
    // Allow requests with no origin (server-to-server, curl, etc.)
    if (!origin) return cb(null, true)
    if (allowedOrigins.includes(origin)) return cb(null, true)
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
app.use(cors({
  origin: originCallback,
  credentials: true,
  allowedHeaders: ['Authorization', 'Content-Type', 'Accept'],
  methods: ['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'OPTIONS'],
  optionsSuccessStatus: 204
}))

// Ensure OPTIONS preflight responses are handled quickly for all routes using same origin logic
app.options('*', cors({ origin: originCallback, credentials: true, optionsSuccessStatus: 204 }))

app.use((req, res, next) => {
  const id = crypto.randomUUID().slice(0, 12)
  ;(req as any).requestId = id
  res.setHeader('x-request-id', id)
  const started = Date.now()
  res.on('finish', () => {
    const ms = Date.now() - started
    console.log(`[${id}] ${req.method} ${req.originalUrl} ${res.statusCode} ${ms}ms`)
  })
  next()
})

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
app.use('/api/debug', debugRoutes)
app.use('/api/billing', requireAuth, billingRoutes)
app.use('/api', requireAuth, checkoutRoutes)
app.use('/api/jobs', requireAuth, jobsRoutes)
app.use('/api/uploads', requireAuth, uploadsRoutes)
app.use('/api/me', requireAuth, meRoutes)
app.use('/api/settings', requireAuth, settingsRoutes)
app.use('/api/admin', adminRoutes)

app.get('/api/health', rateLimit({ windowMs: 60_000, max: 60 }), async (req, res) => {
  const version = process.env.APP_VERSION || process.env.npm_package_version || '0.0.0'
  const time = new Date().toISOString()
  await checkDb()
  res.json({ ok: true, version, time, db: isStubDb() ? 'stub' : 'prisma' })
})

app.get('/health', rateLimit({ windowMs: 60_000, max: 60 }), async (req, res) => {
  const version = process.env.APP_VERSION || process.env.npm_package_version || '0.0.0'
  const time = new Date().toISOString()
  await checkDb()
  res.json({ ok: true, version, time, db: isStubDb() ? 'stub' : 'prisma' })
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
  const message = err?.message || 'Internal error'
  res.status(err?.status || 500).json({ error: 'internal_error', message, path: req?.originalUrl, requestId })
})

// Lightweight health endpoint for debug use
app.get('/api/debug/health', rateLimit({ windowMs: 60_000, max: 60 }), async (req, res) => {
  res.json({ ok: true })
})

export default app
