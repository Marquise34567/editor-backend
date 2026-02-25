import { Request, Response, NextFunction } from 'express'
import { supabaseAdmin } from '../supabaseClient'
import { prisma } from '../db/prisma'
import { getAllowedDevAdminEmails, resolveDevAdminAccess } from '../lib/devAccounts'

declare global {
  namespace Express {
    interface Request {
      user?: { id: string; email?: string }
    }
  }
}

const stealthNotFound = (res: Response) => res.status(404).json({ error: 'not_found' })

const getClientIp = (req: Request) => {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0]?.trim()
  return forwarded || req.ip || 'unknown'
}

const safeLower = (value?: string | null) => String(value || '').trim().toLowerCase()

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
    const token = getTokenFromRequest(req)
    if (!token) {
      await auditAccessAttempt({ req, allowed: false, reason: 'missing_token' })
      return stealthNotFound(res)
    }
    const { data, error } = await supabaseAdmin.auth.getUser(token)
    if (error || !data?.user) {
      await auditAccessAttempt({ req, allowed: false, reason: 'invalid_token' })
      return stealthNotFound(res)
    }

    const userId = data.user.id
    const email = data.user.email ?? null
    const normalizedEmail = safeLower(email)
    req.user = { id: userId, email: email ?? undefined }

    const access = await resolveDevAdminAccess(userId, normalizedEmail)
    if (!access.allowed) {
      await auditAccessAttempt({
        req,
        allowed: false,
        email,
        userId,
        reason: access.emailAuthorized
          ? `admin_role_required; role=${access.role}; isDevAdmin=${access.isDevAdmin}`
          : `email_not_authorized; allowed=${getAllowedDevAdminEmails().join('|')}`
      })
      return stealthNotFound(res)
    }

    await auditAccessAttempt({
      req,
      allowed: true,
      email: normalizedEmail,
      userId,
      reason: 'email_authorized'
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
    return stealthNotFound(res)
  }
}
