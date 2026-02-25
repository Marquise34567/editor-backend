import type { Server } from 'http'
import crypto from 'crypto'
import { WebSocketServer, WebSocket } from 'ws'
import { supabaseAdmin } from './supabaseClient'

type SocketWithMeta = WebSocket & { userId?: string; sessionId?: string; userEmail?: string }

const clientsByUser = new Map<string, Set<SocketWithMeta>>()
const ACTIVE_WINDOW_MS = 60_000
const PRESENCE_SNAPSHOT_INTERVAL_MS = 60_000
const PRESENCE_HEARTBEAT_INTERVAL_MS = 20_000
const PRESENCE_STALE_TIMEOUT_MS = 120_000
const PRESENCE_HISTORY_LIMIT = 24 * 60 + 5

type PresenceSession = {
  sessionId: string
  userId: string
  email: string | null
  connectedAt: string
  lastSeen: string
  ip: string | null
}

type PresencePoint = {
  t: string
  v: number
}

const presenceBySession = new Map<string, PresenceSession>()
const activeUsersSeries: PresencePoint[] = []

const getRequestIp = (req: any) => {
  const forwarded = String(req?.headers?.['x-forwarded-for'] || '').split(',')[0]?.trim()
  return forwarded || req?.socket?.remoteAddress || null
}

const nowIso = () => new Date().toISOString()

const touchPresenceSession = (sessionId: string) => {
  const existing = presenceBySession.get(sessionId)
  if (!existing) return
  existing.lastSeen = nowIso()
  presenceBySession.set(sessionId, existing)
}

const deletePresenceSession = (sessionId: string) => {
  presenceBySession.delete(sessionId)
}

const computeActiveUsersCount = (windowMs: number = ACTIVE_WINDOW_MS) => {
  const minSeenMs = Date.now() - Math.max(1_000, windowMs)
  const activeUsers = new Set<string>()
  for (const session of presenceBySession.values()) {
    const seenMs = new Date(session.lastSeen).getTime()
    if (Number.isFinite(seenMs) && seenMs >= minSeenMs) {
      activeUsers.add(session.userId)
    }
  }
  return activeUsers.size
}

const captureActiveUsersSnapshot = () => {
  activeUsersSeries.push({
    t: nowIso(),
    v: computeActiveUsersCount()
  })
  if (activeUsersSeries.length > PRESENCE_HISTORY_LIMIT) {
    activeUsersSeries.splice(0, activeUsersSeries.length - PRESENCE_HISTORY_LIMIT)
  }
}

setInterval(captureActiveUsersSnapshot, PRESENCE_SNAPSHOT_INTERVAL_MS).unref()
captureActiveUsersSnapshot()

export const initRealtime = (server: Server) => {
  const wss = new WebSocketServer({ server, path: '/ws' })

  wss.on('connection', async (socket: SocketWithMeta, req) => {
    try {
      const url = new URL(req.url || '', 'http://localhost')
      const token = url.searchParams.get('token')
      if (!token) {
        socket.close(1008, 'missing_token')
        return
      }
      const { data, error } = await supabaseAdmin.auth.getUser(token)
      if (error || !data?.user) {
        socket.close(1008, 'invalid_token')
        return
      }
      const userId = data.user.id
      const userEmail = data.user.email ?? null
      const sessionId = crypto.randomUUID()
      socket.userId = userId
      socket.userEmail = userEmail ?? undefined
      socket.sessionId = sessionId
      const bucket = clientsByUser.get(userId) ?? new Set()
      bucket.add(socket)
      clientsByUser.set(userId, bucket)

      presenceBySession.set(sessionId, {
        sessionId,
        userId,
        email: userEmail,
        connectedAt: nowIso(),
        lastSeen: nowIso(),
        ip: getRequestIp(req)
      })

      socket.send(JSON.stringify({
        type: 'presence:ready',
        payload: {
          sessionId,
          heartbeatMs: PRESENCE_HEARTBEAT_INTERVAL_MS,
          activeWindowMs: ACTIVE_WINDOW_MS
        }
      }))

      socket.on('pong', () => {
        touchPresenceSession(sessionId)
      })

      const heartbeatTimer = setInterval(() => {
        if (socket.readyState !== WebSocket.OPEN) return
        const snapshot = presenceBySession.get(sessionId)
        const lastSeenMs = snapshot ? new Date(snapshot.lastSeen).getTime() : 0
        if (!snapshot || !Number.isFinite(lastSeenMs) || Date.now() - lastSeenMs > PRESENCE_STALE_TIMEOUT_MS) {
          socket.close(1000, 'presence_timeout')
          return
        }
        try {
          socket.ping()
        } catch {
          socket.close(1011, 'presence_ping_failed')
        }
      }, PRESENCE_HEARTBEAT_INTERVAL_MS)
      heartbeatTimer.unref()

      socket.on('message', () => {
        touchPresenceSession(sessionId)
      })

      socket.on('close', () => {
        clearInterval(heartbeatTimer)
        const set = clientsByUser.get(userId)
        if (!set) return
        set.delete(socket)
        if (set.size === 0) clientsByUser.delete(userId)
        deletePresenceSession(sessionId)
      })
    } catch (err) {
      socket.close(1011, 'server_error')
    }
  })
}

export const broadcastJobUpdate = (userId: string, payload: any) => {
  const sockets = clientsByUser.get(userId)
  if (!sockets || sockets.size === 0) return
  const message = JSON.stringify({ type: 'job:update', payload })
  for (const socket of sockets) {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(message)
    }
  }
}

export const getRealtimeActiveUsersCount = () => {
  return computeActiveUsersCount(ACTIVE_WINDOW_MS)
}

export const getRealtimePresenceSessions = () => {
  return Array.from(presenceBySession.values())
    .sort((a, b) => new Date(b.lastSeen).getTime() - new Date(a.lastSeen).getTime())
}

export const getRealtimeActiveUsersSeries = () => {
  return activeUsersSeries.slice()
}
