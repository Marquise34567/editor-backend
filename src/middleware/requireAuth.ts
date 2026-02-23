import { Request, Response, NextFunction } from 'express'
import { supabaseAdmin } from '../supabaseClient'

declare global {
  namespace Express {
    interface Request {
      user?: { id: string; email?: string }
    }
  }
}

export async function requireAuth(req: Request, res: Response, next: NextFunction) {
  try {
    const auth = req.headers.authorization
    if (!auth || !auth.startsWith('Bearer ')) return res.status(401).json({ error: 'Unauthorized' })
    const token = auth.split(' ')[1]
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
