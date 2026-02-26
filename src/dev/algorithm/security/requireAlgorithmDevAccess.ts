import { NextFunction, Request, Response } from 'express'
import { supabaseAdmin } from '../../../supabaseClient'
import { recordSecurityEvent } from './securityEvents'
import { isControlPanelOwnerEmail } from '../../../lib/devAccounts'

const unauthorized = (res: Response, reason: 'password_required' | 'invalid_password' | 'unauthorized') =>
  res.status(401).json({ error: reason })
const forbidden = (res: Response, reason: 'unauthorized_email') => res.status(403).json({ error: reason })

const DEV_ALGORITHM_PASSWORD = String(process.env.DEV_ALGORITHM_PASSWORD || 'Quise').trim()

const getClientIp = (req: Request) => {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0]?.trim()
  return forwarded || req.ip || 'unknown'
}

const getPasswordFromRequest = (req: Request) => {
  const headerPassword = req.headers['x-dev-password']
  if (typeof headerPassword === 'string' && headerPassword.trim()) {
    return headerPassword.trim()
  }
  const queryPassword = req.query?.password
  if (typeof queryPassword === 'string' && queryPassword.trim()) {
    return queryPassword.trim()
  }
  const bodyPassword = (req.body as any)?.password
  if (typeof bodyPassword === 'string' && bodyPassword.trim()) {
    return bodyPassword.trim()
  }
  return ''
}

const resolveAuthUser = async (req: Request): Promise<{ id: string; email?: string } | null> => {
  if (req.user?.id) return req.user

  const authHeader = String(req.headers.authorization || '').trim()
  if (!authHeader.toLowerCase().startsWith('bearer ')) return null

  const token = authHeader.slice(7).trim()
  if (!token) return null

  try {
    const { data, error } = await supabaseAdmin.auth.getUser(token)
    if (error || !data?.user?.id) return null
    return {
      id: data.user.id,
      email: data.user.email || undefined
    }
  } catch {
    return null
  }
}

export const requireAlgorithmDevAccess = async (req: Request, res: Response, next: NextFunction) => {
  const authUser = await resolveAuthUser(req)
  if (!authUser) {
    await recordSecurityEvent({
      type: 'dev_algorithm_access_denied',
      meta: {
        path: req.originalUrl,
        method: req.method,
        ip: getClientIp(req),
        user_id: null,
        allowed: false,
        reason: 'unauthorized'
      }
    })
    return unauthorized(res, 'unauthorized')
  }
  req.user = authUser

  const userId = req.user?.id || null
  const email = req.user?.email || null
  if (!isControlPanelOwnerEmail(email)) {
    await recordSecurityEvent({
      type: 'dev_algorithm_access_denied',
      meta: {
        path: req.originalUrl,
        method: req.method,
        ip: getClientIp(req),
        user_id: userId,
        allowed: false,
        reason: 'unauthorized_email'
      }
    })
    return forbidden(res, 'unauthorized_email')
  }

  const password = getPasswordFromRequest(req)
  const hasPassword = Boolean(password)
  const allowed = hasPassword && password === DEV_ALGORITHM_PASSWORD
  const failureReason = hasPassword ? 'invalid_password' : 'password_required'

  await recordSecurityEvent({
    type: allowed ? 'dev_algorithm_access_granted' : 'dev_algorithm_access_denied',
    meta: {
      path: req.originalUrl,
      method: req.method,
      ip: getClientIp(req),
      user_id: userId,
      allowed,
      reason: allowed ? 'password_ok' : failureReason
    }
  })

  if (!allowed) return unauthorized(res, failureReason)
  return next()
}
