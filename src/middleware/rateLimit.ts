import { Request, Response, NextFunction } from 'express'

type RateLimitOptions = {
  windowMs: number
  max: number
  keyFn?: (req: Request) => string
}

const buckets = new Map<string, { count: number; resetAt: number }>()

const getKey = (req: Request) => {
  const forwarded = (req.headers['x-forwarded-for'] as string | undefined) || ''
  const ip = forwarded.split(',')[0]?.trim() || req.ip || 'unknown'
  return ip
}

export const rateLimit = (options: RateLimitOptions) => {
  const windowMs = options.windowMs
  const max = options.max
  return (req: Request, res: Response, next: NextFunction) => {
    const key = options.keyFn ? options.keyFn(req) : getKey(req)
    const now = Date.now()
    const bucket = buckets.get(key)
    if (!bucket || bucket.resetAt <= now) {
      buckets.set(key, { count: 1, resetAt: now + windowMs })
      return next()
    }
    if (bucket.count >= max) {
      res.status(429).json({ error: 'rate_limited', message: 'Too many requests' })
      return
    }
    bucket.count += 1
    return next()
  }
}

