import { Request, Response, NextFunction } from 'express'
import { prisma } from '../db/prisma'

export const guardFeature = (feature: 'chat' | 'rizz') => {
  return async (req: Request & { user?: any }, res: Response, next: NextFunction) => {
    const userId = req.user?.sub
    if (!userId) return res.status(401).json({ error: 'unauthenticated' })
    const user = await prisma.user.findUnique({ where: { id: userId } })
    const plan = user?.planStatus ?? 'free'
    if (plan === 'active') return next()
    // free limits
    const today = new Date().toISOString().slice(0,10)
    const usage = await prisma.usageDaily.findUnique({ where: { userId_date: { userId, date: today } } })
    const counts = { chat: usage?.chatCount ?? 0, rizz: usage?.rizzCount ?? 0 }
    const limits = { chat: 5, rizz: 3 }
    if (counts[feature] >= limits[feature]) {
      return res.status(402).json({ error: 'payment_required', message: 'Upgrade to Premium', feature, upgrade: { checkoutUrl: null } })
    }
    return next()
  }
}
