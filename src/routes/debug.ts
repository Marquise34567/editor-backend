import express from 'express'
import { supabaseAdmin } from '../supabaseClient'
import { r2 } from '../lib/r2'
import { ListObjectsV2Command } from '@aws-sdk/client-s3'

const router = express.Router()

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

// Temporary debug endpoint to validate an incoming Supabase access token.
// Returns { ok: true, user: { id, email } } when valid, or { error, message } when invalid.
// IMPORTANT: This endpoint does NOT log or return the token itself.
router.post('/validate-token', async (req: any, res) => {
  try {
    const auth = req.headers.authorization
    if (!auth || !auth.startsWith('Bearer ')) return res.status(400).json({ error: 'missing_token', message: 'Authorization header required' })
    const token = auth.split(' ')[1]
    const { data, error } = await supabaseAdmin.auth.getUser(token)
    if (error || !data?.user) {
      return res.status(401).json({ error: 'invalid_token', message: error?.message || 'Invalid token' })
    }
    return res.json({ ok: true, user: { id: data.user.id, email: data.user.email ?? null } })
  } catch (err: any) {
    console.error('debug.validate-token error', err?.stack || err)
    return res.status(500).json({ error: 'server_error', message: err?.message || String(err) })
  }
})

// GET /api/debug/r2-health
router.get('/r2-health', async (req: any, res) => {
  try {
    if (!r2.isConfigured) {
      return res.status(503).json({ ok: false, error: 'R2_NOT_CONFIGURED', missing: r2.missingEnvVars || [] })
    }
    const bucket = process.env.R2_BUCKET || r2.bucket
    if (!bucket) return res.status(500).json({ ok: false, error: 'R2_BUCKET_NOT_CONFIGURED' })
    try {
      await r2.client.send(new ListObjectsV2Command({ Bucket: bucket, MaxKeys: 1 }))
      return res.json({ ok: true })
    } catch (e: any) {
      logAwsError('r2-health list objects failed', e)
      return res.status(500).json({ ok: false, error: String(e?.message || e) })
    }
  } catch (err: any) {
    logAwsError('r2-health error', err)
    res.status(500).json({ ok: false, error: String(err?.message || err) })
  }
})

export default router
