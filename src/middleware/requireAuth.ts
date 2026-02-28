import { Request, Response, NextFunction } from 'express'
import { supabaseAdmin } from '../supabaseClient'
import { getLocalhostBypassUser, shouldBypassAuthForLocalhost } from '../lib/localhostAuthBypass'

declare global {
  namespace Express {
    interface Request {
      user?: { id: string; email?: string }
    }
  }
}

export async function requireAuth(req: Request, res: Response, next: NextFunction) {
  try {
    if (shouldBypassAuthForLocalhost(req)) {
      req.user = getLocalhostBypassUser()
      return next()
    }

    const auth = req.headers.authorization
    let token = ''
    if (auth && auth.startsWith('Bearer ')) {
      token = auth.split(' ')[1]
    } else if (typeof req.query?.token === 'string' && req.query.token.trim()) {
      token = req.query.token.trim()
    } else if (typeof req.headers['x-auth-token'] === 'string' && req.headers['x-auth-token'].trim()) {
      token = req.headers['x-auth-token'].trim()
    }
    if (!token) return res.status(401).json({ error: 'Unauthorized' })
    const { data, error } = await supabaseAdmin.auth.getUser(token)
    if (error || !data?.user) return res.status(401).json({ error: 'Invalid token' })
    req.user = { id: data.user.id, email: data.user.email ?? undefined }
    next()
  } catch (err) {
    console.error('Auth middleware error', err)
    // Return 401 to indicate auth failure rather than a generic 500.
    return res.status(401).json({ error: 'auth_error', message: String(err) })
  }
}
