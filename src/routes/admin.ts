import express from 'express'
import { prisma } from '../db/prisma'
import { supabaseAdmin } from '../supabaseClient'
import { getOrCreateUser } from '../services/users'

const router = express.Router()

const isAdminRequest = async (req: any) => {
  const headerKey = req.headers['x-admin-key'] || req.headers['x_admin_key']
  const envKey = process.env.ADMIN_API_KEY
  if (envKey && headerKey && String(headerKey) === String(envKey)) return { ok: true, via: 'header' }
  // if authenticated user exists, check DB field isAdmin if present
  if (req.user && req.user.id) {
    try {
      const userRec = await prisma.user.findUnique({ where: { id: req.user.id } })
      if (userRec && (userRec as any).isAdmin) return { ok: true, via: 'db' }
    } catch (e) {
      // ignore
    }
  }
  return { ok: false }
}

// Comp upgrade endpoint
router.post('/comp-upgrade', async (req: any, res) => {
  try {
    const check = await isAdminRequest(req)
    if (!check.ok) return res.status(403).json({ error: 'forbidden' })
    const email = String(req.body?.email || '').trim().toLowerCase()
    const planKey = String(req.body?.planKey || 'founder')
    const reason = req.body?.reason || null
    if (!email) return res.status(400).json({ error: 'missing_email' })
    if (planKey !== 'founder') return res.status(400).json({ error: 'invalid_plan' })

    let user = await prisma.user.findUnique({ where: { email } })
    if (!user) {
      // create minimal user record
      user = await prisma.user.create({ data: { email, planStatus: 'active' } })
    }

    // update user plan status
    await prisma.user.update({ where: { id: user.id }, data: { planStatus: 'active' } })

    // update settings (watermark off, export 4k, autoZoomMax)
    try {
      await prisma.userSettings.upsert({
        where: { userId: user.id },
        update: { watermarkEnabled: false, exportQuality: '4k', autoZoomMax: 1.15 },
        create: { userId: user.id, watermarkEnabled: false, exportQuality: '4k', autoZoomMax: 1.15 }
      })
    } catch (e) {
      // ignore if schema doesn't support some fields
    }

    // upsert subscription
    try {
      await prisma.subscription.upsert({
        where: { userId: user.id },
        update: { status: 'active', planTier: 'founder', stripeSubscriptionId: 'comped', stripeCustomerId: null, currentPeriodEnd: null },
        create: { userId: user.id, status: 'active', planTier: 'founder', stripeSubscriptionId: 'comped', stripeCustomerId: null }
      })
    } catch (e) {
      // ignore
    }

    // record audit trail (best-effort)
    try {
      await prisma.adminAudit.create({ data: { actor: req.user?.email || (req.headers['x-admin-key'] ? 'api_key' : null), action: 'comp_upgrade', targetEmail: email, planKey, reason } })
    } catch (e) {
      console.warn('admin audit failed', e)
    }

    res.json({ ok: true, userId: user.id, email: user.email, planKey: 'founder' })
  } catch (err) {
    console.error('comp-upgrade error', err)
    res.status(500).json({ error: 'server_error' })
  }
})

export default router
