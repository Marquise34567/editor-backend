import type { Server } from 'http'
import { WebSocketServer, WebSocket } from 'ws'
import { supabaseAdmin } from './supabaseClient'

type SocketWithMeta = WebSocket & { userId?: string }

const clientsByUser = new Map<string, Set<SocketWithMeta>>()

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
      socket.userId = userId
      const bucket = clientsByUser.get(userId) ?? new Set()
      bucket.add(socket)
      clientsByUser.set(userId, bucket)

      socket.on('close', () => {
        const set = clientsByUser.get(userId)
        if (!set) return
        set.delete(socket)
        if (set.size === 0) clientsByUser.delete(userId)
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
