import { NextFunction, Request, Response } from 'express'
import { getAllowedDevAdminEmails } from '../../../lib/devAccounts'
import { supabaseAdmin } from '../../../supabaseClient'
import { recordSecurityEvent } from './securityEvents'

const stealthNotFound = (res: Response) => res.status(404).json({ error: 'not_found' })

const normalizeEmail = (value: unknown) => String(value || '').trim().toLowerCase()

const parseAllowlist = () => {
  const raw =
    String(process.env.DEV_EMAIL_ALLOWLIST || '').trim() ||
    String(process.env.DEV_ADMIN_EMAILS || process.env.ADMIN_EMAILS || '').trim()

  const fromEnv = raw
    .split(',')
    .map((entry) => normalizeEmail(entry))
    .filter(Boolean)

  if (fromEnv.length > 0) return new Set(fromEnv)
  return new Set(getAllowedDevAdminEmails().map((email) => normalizeEmail(email)))
}

const DEV_EMAIL_ALLOWLIST = parseAllowlist()

const getClientIp = (req: Request) => {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0]?.trim()
  return forwarded || req.ip || 'unknown'
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
  if (authUser) {
    req.user = authUser
  }

  const userId = req.user?.id || null
  const email = normalizeEmail(req.user?.email)
  const allowed = Boolean(userId && email && DEV_EMAIL_ALLOWLIST.has(email))

  await recordSecurityEvent({
    type: allowed ? 'dev_algorithm_access_granted' : 'dev_algorithm_access_denied',
    meta: {
      path: req.originalUrl,
      method: req.method,
      ip: getClientIp(req),
      user_id: userId,
      email: email || null,
      allowed,
      reason: allowed ? 'allowlisted_email' : !userId ? 'missing_or_invalid_session' : 'email_not_allowlisted'
    }
  })

  if (!allowed) return stealthNotFound(res)
  return next()
}
