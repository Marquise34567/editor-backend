import { NextFunction, Request, Response } from 'express'
import { getActiveIpBan, getRequestIpAddress } from '../services/ipBan'

export const blockBannedIp = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const ip = getRequestIpAddress(req)
    if (!ip) return next()
    const ban = await getActiveIpBan(ip)
    if (!ban) return next()
    return res.status(403).json({
      error: 'ip_banned',
      message: 'Access denied.',
      ip,
      reason: ban.reason || null,
      expiresAt: ban.expiresAt || null
    })
  } catch {
    return next()
  }
}
