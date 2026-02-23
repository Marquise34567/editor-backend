import express from 'express'
import { supabaseAdmin } from '../supabaseClient'

const router = express.Router()

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

export default router
