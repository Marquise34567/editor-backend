import { Request, Response, NextFunction } from 'express'
import { supabaseAdmin } from '../supabaseClient'
import { prisma } from '../db/prisma'
import { isControlPanelOwnerEmail } from '../lib/devAccounts'
import { getLocalhostBypassUser, shouldBypassAuthForLocalhost } from '../lib/localhostAuthBypass'

declare global {
  namespace Express {
    interface Request {
      user?: { id: string; email?: string }
    }
  }
}

const unauthorized = (res: Response, reason: 'password_required' | 'invalid_password' | 'unauthorized') =>
  res.status(401).json({ error: reason })
const forbidden = (res: Response, reason: 'unauthorized_email') => res.status(403).json({ error: reason })

const getClientIp = (req: Request) => {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0]?.trim()
  return forwarded || req.ip || 'unknown'
}

const CONTROL_PANEL_PASSWORD = String(process.env.CONTROL_PANEL_PASSWORD || 'Quise').trim()

const getTokenFromRequest = (req: Request) => {
  const authHeader = req.headers.authorization
  if (authHeader && authHeader.startsWith('Bearer ')) {
    return authHeader.slice(7)
  }
  const tokenQuery = typeof req.query?.token === 'string' ? req.query.token : ''
  if (tokenQuery) return tokenQuery
  const tokenHeader = typeof req.headers['x-admin-token'] === 'string' ? req.headers['x-admin-token'] : ''
  return tokenHeader || ''
}

const getPasswordFromRequest = (req: Request) => {
  const headerPassword = req.headers['x-dev-password']
  if (typeof headerPassword === 'string' && headerPassword.trim()) return headerPassword.trim()
  const queryPassword = typeof req.query?.password === 'string' ? req.query.password.trim() : ''
  if (queryPassword) return queryPassword
  const bodyPassword = typeof (req.body as any)?.password === 'string' ? String((req.body as any).password).trim() : ''
  return bodyPassword
}

const auditAccessAttempt = async ({
  req,
  allowed,
  email,
  userId,
  reason
}: {
  req: Request
  allowed: boolean
  email?: string | null
  userId?: string | null
  reason?: string | null
}) => {
  const ip = getClientIp(req)
  const time = new Date().toISOString()
  const actor = email || userId || 'unknown'
  const action = allowed ? 'admin_access_granted' : 'admin_access_denied'
  const detail = reason ? `${reason}; path=${req.originalUrl}; ip=${ip}; time=${time}` : `path=${req.originalUrl}; ip=${ip}; time=${time}`
  console.log('[admin-access]', { action, actor, ip, path: req.originalUrl, time, reason: reason || null })
  try {
    await prisma.adminAudit.create({
      data: {
        actor,
        action,
        targetEmail: email || 'unknown',
        planKey: userId || null,
        reason: detail
      }
    })
  } catch {
    // best-effort audit
  }
}

export const requireDevAdmin = async (req: Request, res: Response, next: NextFunction) => {
  try {
    if (shouldBypassAuthForLocalhost(req)) {
      req.user = getLocalhostBypassUser()
      await auditAccessAttempt({
        req,
        allowed: true,
        email: req.user.email || null,
        userId: req.user.id,
        reason: 'localhost_bypass'
      })
      return next()
    }

    const token = getTokenFromRequest(req)
    if (!token) {
      await auditAccessAttempt({ req, allowed: false, reason: 'missing_token' })
      return unauthorized(res, 'unauthorized')
    }
    const { data, error } = await supabaseAdmin.auth.getUser(token)
    if (error || !data?.user) {
      await auditAccessAttempt({ req, allowed: false, reason: 'invalid_token' })
      return unauthorized(res, 'unauthorized')
    }

    const userId = data.user.id
    const email = data.user.email ?? null
    req.user = { id: userId, email: email ?? undefined }

    if (!isControlPanelOwnerEmail(email)) {
      await auditAccessAttempt({ req, allowed: false, email, userId, reason: 'unauthorized_email' })
      return forbidden(res, 'unauthorized_email')
    }

    const password = getPasswordFromRequest(req)
    if (!password) {
      await auditAccessAttempt({ req, allowed: false, email, userId, reason: 'password_required' })
      return unauthorized(res, 'password_required')
    }
    if (password !== CONTROL_PANEL_PASSWORD) {
      await auditAccessAttempt({ req, allowed: false, email, userId, reason: 'invalid_password' })
      return unauthorized(res, 'invalid_password')
    }

    await auditAccessAttempt({
      req,
      allowed: true,
      email,
      userId,
      reason: 'password_ok'
    })
    return next()
  } catch (err) {
    await auditAccessAttempt({
      req,
      allowed: false,
      email: req.user?.email || null,
      userId: req.user?.id || null,
      reason: 'middleware_error'
    })
    return unauthorized(res, 'unauthorized')
  }
}
