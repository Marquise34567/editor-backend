import express from 'express'
import { getConnectedRealtimeClientCount, getRealtimeActiveUsersCount, getRealtimeActiveUsersSeries } from '../realtime'
import { buildLiveStatsResponse } from '../services/liveStats'

const router = express.Router()

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const parseBool = (value: unknown) => {
  const normalized = String(value || '').trim().toLowerCase()
  return normalized === '1' || normalized === 'true' || normalized === 'yes'
}

const parseInterval = (value: unknown) => {
  const parsed = Number.parseInt(String(value || '5000'), 10)
  if (!Number.isFinite(parsed)) return 5000
  return clamp(parsed, 1000, 5000)
}

const readContext = () => ({
  activeUsers: getRealtimeActiveUsersCount(),
  activeUsersSeries: getRealtimeActiveUsersSeries(),
  connectedClients: getConnectedRealtimeClientCount()
})

router.get('/', async (req: any, res) => {
  const userId = String(req?.user?.id || '').trim()
  if (!userId) {
    return res.status(401).json({ error: 'unauthorized' })
  }
  const user = {
    userId,
    email: req?.user?.email || null
  }

  const stream = parseBool(req.query?.stream)
  if (!stream) {
    try {
      const payload = await buildLiveStatsResponse({
        user,
        context: readContext()
      })
      return res.json(payload)
    } catch (err: any) {
      return res.status(500).json({
        error: 'live_stats_failed',
        message: String(err?.message || 'Failed to build live stats payload')
      })
    }
  }

  const intervalMs = parseInterval(req.query?.intervalMs)
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache, no-transform')
  res.setHeader('Connection', 'keep-alive')
  res.setHeader('X-Accel-Buffering', 'no')
  ;(res as any).flushHeaders?.()

  let closed = false
  const send = (eventName: string, payload: any) => {
    if (closed || res.writableEnded || (res as any).destroyed) return
    try {
      res.write(`event: ${eventName}\n`)
      res.write(`data: ${JSON.stringify(payload)}\n\n`)
    } catch {
      // ignore failed stream write
    }
  }

  const sendStats = async () => {
    try {
      const payload = await buildLiveStatsResponse({
        user,
        context: readContext()
      })
      send('stats', payload)
    } catch (err: any) {
      send('warning', {
        message: String(err?.message || 'live_stats_stream_failed'),
        t: new Date().toISOString()
      })
    }
  }

  send('ready', {
    ok: true,
    intervalMs,
    t: new Date().toISOString()
  })
  await sendStats()

  const statsTimer = setInterval(() => {
    void sendStats()
  }, intervalMs)
  const keepaliveTimer = setInterval(() => {
    if (!closed && !res.writableEnded && !(res as any).destroyed) {
      res.write(':keepalive\n\n')
    }
  }, 15_000)
  statsTimer.unref()
  keepaliveTimer.unref()

  req.on('close', () => {
    closed = true
    clearInterval(statsTimer)
    clearInterval(keepaliveTimer)
    res.end()
  })
})

export default router
