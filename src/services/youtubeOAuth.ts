import crypto from 'crypto'
import fs from 'fs'
import path from 'path'
import { prisma } from '../db/prisma'

const YOUTUBE_ANALYTICS_SCOPES = [
  'https://www.googleapis.com/auth/yt-analytics.readonly',
  'https://www.googleapis.com/auth/youtube.readonly'
] as const

const OAUTH_STATE_TTL_MS = 10 * 60 * 1000
const ACCESS_TOKEN_EXPIRY_SAFETY_MS = 90 * 1000

type YouTubeOAuthCredentials = {
  clientId: string
  clientSecret: string
  redirectUri: string
  authUri: string
  tokenUri: string
}

const GOOGLE_OAUTH_AUTH_URI = 'https://accounts.google.com/o/oauth2/v2/auth'
const GOOGLE_OAUTH_AUTH_URI_LEGACY = 'https://accounts.google.com/o/oauth2/auth'

type StoredConnection = {
  userId: string
  channelId: string | null
  channelTitle: string | null
  accessToken: string
  refreshToken: string | null
  tokenType: string | null
  scope: string | null
  expiryDate: string | null
  createdAt: string
  updatedAt: string
}

type StoredState = {
  state: string
  userId: string
  expiresAt: string
}

export type YouTubeOAuthConnectionPublic = {
  connected: boolean
  channelId: string | null
  channelTitle: string | null
  expiryDate: string | null
  hasRefreshToken: boolean
  scopes: string[]
}

export type YouTubeOAuthConfigStatus = {
  configured: boolean
  missing: string[]
}

let infraEnsured = false
let lastEnsureAttemptAt = 0
const inMemoryConnections = new Map<string, StoredConnection>()
const inMemoryStates = new Map<string, StoredState>()

const canRunRawSql = () =>
  typeof (prisma as any)?.$executeRawUnsafe === 'function' &&
  typeof (prisma as any)?.$queryRawUnsafe === 'function'

const nowIso = () => new Date().toISOString()

const toBase64Url = (value: Buffer) =>
  value
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '')

const fromBase64Url = (value: string) => {
  const normalized = String(value || '').trim().replace(/-/g, '+').replace(/_/g, '/')
  if (!normalized) return Buffer.alloc(0)
  const padding = normalized.length % 4
  const padded = padding === 0 ? normalized : `${normalized}${'='.repeat(4 - padding)}`
  return Buffer.from(padded, 'base64')
}

const parseDateIso = (value: unknown): string | null => {
  if (!value) return null
  const parsed = new Date(String(value))
  if (!Number.isFinite(parsed.getTime())) return null
  return parsed.toISOString()
}

const toScopeList = (scope: string | null | undefined) =>
  String(scope || '')
    .trim()
    .split(/\s+/)
    .map((entry) => entry.trim())
    .filter(Boolean)

const normalizeGoogleAuthUri = (value: unknown) => {
  const normalized = String(value || '').trim()
  if (!normalized) return GOOGLE_OAUTH_AUTH_URI
  if (normalized === GOOGLE_OAUTH_AUTH_URI_LEGACY) return GOOGLE_OAUTH_AUTH_URI
  return normalized
}

const parseOAuthConfigObject = (raw: any): YouTubeOAuthCredentials | null => {
  const source = raw?.web && typeof raw.web === 'object' ? raw.web : raw
  if (!source || typeof source !== 'object') return null

  const clientId = String(source.client_id || source.clientId || '').trim()
  const clientSecret = String(source.client_secret || source.clientSecret || '').trim()
  const redirectUriRaw =
    source.redirect_uri ||
    source.redirectUri ||
    (Array.isArray(source.redirect_uris) ? source.redirect_uris[0] : null)
  const redirectUri = String(redirectUriRaw || '').trim()
  const authUri = normalizeGoogleAuthUri(source.auth_uri || source.authUri)
  const tokenUri = String(source.token_uri || source.tokenUri || 'https://oauth2.googleapis.com/token').trim()

  if (!clientId || !clientSecret || !redirectUri) return null
  return {
    clientId,
    clientSecret,
    redirectUri,
    authUri: authUri || GOOGLE_OAUTH_AUTH_URI,
    tokenUri: tokenUri || 'https://oauth2.googleapis.com/token'
  }
}

const readOAuthConfigFromFile = () => {
  const candidateRaw =
    process.env.GOOGLE_OAUTH_CLIENT_FILE ||
    process.env.YOUTUBE_OAUTH_CLIENT_FILE ||
    process.env.GOOGLE_OAUTH_CREDENTIALS_FILE ||
    ''
  const candidate = String(candidateRaw || '').trim()
  if (!candidate) return null
  try {
    const resolved = path.isAbsolute(candidate)
      ? candidate
      : path.resolve(process.cwd(), candidate)
    if (!fs.existsSync(resolved)) return null
    const payload = JSON.parse(fs.readFileSync(resolved, 'utf8'))
    return parseOAuthConfigObject(payload)
  } catch {
    return null
  }
}

const resolveOAuthCredentials = (): YouTubeOAuthCredentials | null => {
  const envObjectCandidate = parseOAuthConfigObject({
    client_id: process.env.GOOGLE_OAUTH_CLIENT_ID || process.env.YOUTUBE_OAUTH_CLIENT_ID,
    client_secret: process.env.GOOGLE_OAUTH_CLIENT_SECRET || process.env.YOUTUBE_OAUTH_CLIENT_SECRET,
    redirect_uri:
      process.env.GOOGLE_OAUTH_REDIRECT_URI ||
      process.env.YOUTUBE_OAUTH_REDIRECT_URI,
    auth_uri: process.env.GOOGLE_OAUTH_AUTH_URI,
    token_uri: process.env.GOOGLE_OAUTH_TOKEN_URI
  })
  if (envObjectCandidate) return envObjectCandidate
  return readOAuthConfigFromFile()
}

export const getYouTubeOAuthConfigStatus = (): YouTubeOAuthConfigStatus => {
  const credentials = resolveOAuthCredentials()
  if (credentials) return { configured: true, missing: [] }
  return {
    configured: false,
    missing: [
      'GOOGLE_OAUTH_CLIENT_ID',
      'GOOGLE_OAUTH_CLIENT_SECRET',
      'GOOGLE_OAUTH_REDIRECT_URI'
    ]
  }
}

const getTokenCipherKey = () => {
  const secret = String(
    process.env.YOUTUBE_OAUTH_TOKEN_SECRET ||
      process.env.APP_SECRET ||
      ''
  ).trim()
  if (!secret) return null
  return crypto.createHash('sha256').update(secret).digest()
}

const encryptToken = (value: string): string => {
  const key = getTokenCipherKey()
  if (!key) return value
  const iv = crypto.randomBytes(12)
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv)
  const encrypted = Buffer.concat([cipher.update(value, 'utf8'), cipher.final()])
  const tag = cipher.getAuthTag()
  return `enc:v1:${toBase64Url(iv)}:${toBase64Url(tag)}:${toBase64Url(encrypted)}`
}

const decryptToken = (value: string): string => {
  if (!value.startsWith('enc:v1:')) return value
  const key = getTokenCipherKey()
  if (!key) return ''
  const chunks = value.split(':')
  if (chunks.length !== 5) return ''
  try {
    const iv = fromBase64Url(chunks[2])
    const tag = fromBase64Url(chunks[3])
    const payload = fromBase64Url(chunks[4])
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv)
    decipher.setAuthTag(tag)
    const decrypted = Buffer.concat([decipher.update(payload), decipher.final()])
    return decrypted.toString('utf8')
  } catch {
    return ''
  }
}

const ensureInfra = async () => {
  const now = Date.now()
  if (infraEnsured) return true
  if (now - lastEnsureAttemptAt < 10_000) return false
  lastEnsureAttemptAt = now
  if (!canRunRawSql()) return false
  try {
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS youtube_oauth_connections (
        user_id TEXT PRIMARY KEY,
        channel_id TEXT NULL,
        channel_title TEXT NULL,
        access_token TEXT NOT NULL,
        refresh_token TEXT NULL,
        token_type TEXT NULL,
        scope TEXT NULL,
        expiry_date TIMESTAMPTZ NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    await (prisma as any).$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS youtube_oauth_states (
        state TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    infraEnsured = true
    return true
  } catch (error) {
    console.warn('youtube oauth infra ensure failed, using in-memory fallback', error)
    return false
  }
}

const cleanupExpiredStates = async () => {
  const nowMs = Date.now()
  if (canRunRawSql()) {
    try {
      await ensureInfra()
      await (prisma as any).$executeRawUnsafe(
        'DELETE FROM youtube_oauth_states WHERE expires_at < NOW()'
      )
      return
    } catch {
      // ignore and fallback to memory cleanup
    }
  }
  for (const [state, entry] of inMemoryStates.entries()) {
    if (new Date(entry.expiresAt).getTime() <= nowMs) {
      inMemoryStates.delete(state)
    }
  }
}

const saveState = async (entry: StoredState) => {
  if (canRunRawSql()) {
    try {
      await ensureInfra()
      await (prisma as any).$executeRawUnsafe(
        `
          INSERT INTO youtube_oauth_states (state, user_id, expires_at, created_at)
          VALUES ($1, $2, $3, NOW())
          ON CONFLICT (state) DO UPDATE
          SET user_id = EXCLUDED.user_id,
              expires_at = EXCLUDED.expires_at
        `,
        entry.state,
        entry.userId,
        entry.expiresAt
      )
      return
    } catch (error) {
      console.warn('youtube oauth state save failed, using in-memory fallback', error)
    }
  }
  inMemoryStates.set(entry.state, entry)
}

const consumeState = async ({
  userId,
  state
}: {
  userId: string
  state: string
}) => {
  if (!state) return false
  if (canRunRawSql()) {
    try {
      await ensureInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        `
          DELETE FROM youtube_oauth_states
          WHERE state = $1
          RETURNING user_id AS "userId", expires_at AS "expiresAt"
        `,
        state
      )
      const row = Array.isArray(rows) && rows.length ? rows[0] : null
      const owner = String(row?.userId || '')
      const expiresAt = parseDateIso(row?.expiresAt)
      if (!owner || owner !== userId || !expiresAt) return false
      return new Date(expiresAt).getTime() > Date.now()
    } catch (error) {
      console.warn('youtube oauth state consume failed, using in-memory fallback', error)
    }
  }
  const cached = inMemoryStates.get(state)
  if (!cached) return false
  inMemoryStates.delete(state)
  if (cached.userId !== userId) return false
  return new Date(cached.expiresAt).getTime() > Date.now()
}

const parseConnectionRow = (row: any): StoredConnection | null => {
  if (!row || typeof row !== 'object') return null
  const userId = String(row.userId || row.user_id || '').trim()
  if (!userId) return null
  const accessRaw = String(row.accessToken || row.access_token || '').trim()
  const accessToken = decryptToken(accessRaw)
  if (!accessToken) return null
  const refreshRaw = String(row.refreshToken || row.refresh_token || '').trim()
  const refreshToken = refreshRaw ? decryptToken(refreshRaw) : null
  return {
    userId,
    channelId: String(row.channelId || row.channel_id || '').trim() || null,
    channelTitle: String(row.channelTitle || row.channel_title || '').trim() || null,
    accessToken,
    refreshToken: refreshToken || null,
    tokenType: String(row.tokenType || row.token_type || '').trim() || null,
    scope: String(row.scope || '').trim() || null,
    expiryDate: parseDateIso(row.expiryDate || row.expiry_date),
    createdAt: parseDateIso(row.createdAt || row.created_at) || nowIso(),
    updatedAt: parseDateIso(row.updatedAt || row.updated_at) || nowIso()
  }
}

const loadConnection = async (userId: string): Promise<StoredConnection | null> => {
  if (!userId) return null
  if (canRunRawSql()) {
    try {
      await ensureInfra()
      const rows = await (prisma as any).$queryRawUnsafe(
        `
          SELECT
            user_id AS "userId",
            channel_id AS "channelId",
            channel_title AS "channelTitle",
            access_token AS "accessToken",
            refresh_token AS "refreshToken",
            token_type AS "tokenType",
            scope AS "scope",
            expiry_date AS "expiryDate",
            created_at AS "createdAt",
            updated_at AS "updatedAt"
          FROM youtube_oauth_connections
          WHERE user_id = $1
          LIMIT 1
        `,
        userId
      )
      return parseConnectionRow(Array.isArray(rows) && rows.length ? rows[0] : null)
    } catch (error) {
      console.warn('youtube oauth load failed, using in-memory fallback', error)
    }
  }
  return inMemoryConnections.get(userId) || null
}

const saveConnection = async (connection: StoredConnection) => {
  const accessEncrypted = encryptToken(connection.accessToken)
  const refreshEncrypted = connection.refreshToken ? encryptToken(connection.refreshToken) : null
  const now = nowIso()
  if (canRunRawSql()) {
    try {
      await ensureInfra()
      await (prisma as any).$executeRawUnsafe(
        `
          INSERT INTO youtube_oauth_connections (
            user_id,
            channel_id,
            channel_title,
            access_token,
            refresh_token,
            token_type,
            scope,
            expiry_date,
            created_at,
            updated_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
          ON CONFLICT (user_id) DO UPDATE
          SET channel_id = EXCLUDED.channel_id,
              channel_title = EXCLUDED.channel_title,
              access_token = EXCLUDED.access_token,
              refresh_token = COALESCE(EXCLUDED.refresh_token, youtube_oauth_connections.refresh_token),
              token_type = EXCLUDED.token_type,
              scope = EXCLUDED.scope,
              expiry_date = EXCLUDED.expiry_date,
              updated_at = NOW()
        `,
        connection.userId,
        connection.channelId,
        connection.channelTitle,
        accessEncrypted,
        refreshEncrypted,
        connection.tokenType,
        connection.scope,
        connection.expiryDate
      )
      return
    } catch (error) {
      console.warn('youtube oauth save failed, using in-memory fallback', error)
    }
  }
  inMemoryConnections.set(connection.userId, {
    ...connection,
    createdAt: connection.createdAt || now,
    updatedAt: now
  })
}

const deleteConnection = async (userId: string) => {
  if (!userId) return
  if (canRunRawSql()) {
    try {
      await ensureInfra()
      await (prisma as any).$executeRawUnsafe(
        'DELETE FROM youtube_oauth_connections WHERE user_id = $1',
        userId
      )
      return
    } catch (error) {
      console.warn('youtube oauth delete failed, using in-memory fallback', error)
    }
  }
  inMemoryConnections.delete(userId)
}

const toPublicConnection = (connection: StoredConnection | null): YouTubeOAuthConnectionPublic => ({
  connected: Boolean(connection),
  channelId: connection?.channelId || null,
  channelTitle: connection?.channelTitle || null,
  expiryDate: connection?.expiryDate || null,
  hasRefreshToken: Boolean(connection?.refreshToken),
  scopes: toScopeList(connection?.scope)
})

const fetchJson = async (url: string, init: RequestInit = {}) => {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 15_000)
  try {
    const response = await fetch(url, {
      ...init,
      signal: controller.signal
    })
    const payload = await response.json().catch(() => ({} as any))
    if (!response.ok) {
      const reason = payload?.error?.message || `request_failed_${response.status}`
      throw new Error(reason)
    }
    return payload
  } finally {
    clearTimeout(timer)
  }
}

const fetchChannelIdentity = async (accessToken: string) => {
  const endpoint = new URL('https://www.googleapis.com/youtube/v3/channels')
  endpoint.searchParams.set('part', 'id,snippet')
  endpoint.searchParams.set('mine', 'true')
  const payload = await fetchJson(endpoint.toString(), {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${accessToken}`
    }
  })
  const item = Array.isArray(payload?.items) ? payload.items[0] : null
  return {
    channelId: item?.id ? String(item.id) : null,
    channelTitle: item?.snippet?.title ? String(item.snippet.title) : null
  }
}

const buildTokenExpiryDate = (expiresInRaw: unknown) => {
  const expiresIn = Number(expiresInRaw)
  if (!Number.isFinite(expiresIn) || expiresIn <= 0) return null
  return new Date(Date.now() + Math.round(expiresIn * 1000)).toISOString()
}

const exchangeTokenRequest = async ({
  grantType,
  code,
  refreshToken,
  credentials
}: {
  grantType: 'authorization_code' | 'refresh_token'
  code?: string
  refreshToken?: string
  credentials: YouTubeOAuthCredentials
}) => {
  const body = new URLSearchParams()
  body.set('client_id', credentials.clientId)
  body.set('client_secret', credentials.clientSecret)
  body.set('grant_type', grantType)
  if (grantType === 'authorization_code') {
    body.set('code', String(code || ''))
    body.set('redirect_uri', credentials.redirectUri)
  } else {
    body.set('refresh_token', String(refreshToken || ''))
  }
  const payload = await fetchJson(credentials.tokenUri, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded'
    },
    body
  })
  return {
    accessToken: String(payload?.access_token || '').trim(),
    refreshToken: String(payload?.refresh_token || '').trim() || null,
    tokenType: String(payload?.token_type || '').trim() || 'Bearer',
    scope: String(payload?.scope || '').trim() || null,
    expiryDate: buildTokenExpiryDate(payload?.expires_in)
  }
}

const normalizeStoredConnection = (connection: StoredConnection): StoredConnection => ({
  ...connection,
  channelId: connection.channelId || null,
  channelTitle: connection.channelTitle || null,
  tokenType: connection.tokenType || null,
  scope: connection.scope || null,
  expiryDate: connection.expiryDate || null,
  refreshToken: connection.refreshToken || null
})

export const buildYouTubeOAuthAuthorizeUrl = async (userId: string) => {
  const credentials = resolveOAuthCredentials()
  if (!credentials) {
    const error = new Error('youtube_oauth_not_configured')
    ;(error as any).code = 'youtube_oauth_not_configured'
    throw error
  }
  await cleanupExpiredStates()
  const state = toBase64Url(crypto.randomBytes(24))
  const expiresAt = new Date(Date.now() + OAUTH_STATE_TTL_MS).toISOString()
  await saveState({
    state,
    userId,
    expiresAt
  })
  const authUrl = new URL(credentials.authUri)
  authUrl.searchParams.set('client_id', credentials.clientId)
  authUrl.searchParams.set('redirect_uri', credentials.redirectUri)
  authUrl.searchParams.set('response_type', 'code')
  authUrl.searchParams.set('access_type', 'offline')
  authUrl.searchParams.set('prompt', 'consent')
  authUrl.searchParams.set('include_granted_scopes', 'true')
  authUrl.searchParams.set('scope', YOUTUBE_ANALYTICS_SCOPES.join(' '))
  authUrl.searchParams.set('state', state)
  return {
    authUrl: authUrl.toString(),
    state,
    expiresAt
  }
}

export const exchangeYouTubeOAuthCodeForUser = async ({
  userId,
  code,
  state
}: {
  userId: string
  code: string
  state?: string | null
}) => {
  const credentials = resolveOAuthCredentials()
  if (!credentials) {
    const error = new Error('youtube_oauth_not_configured')
    ;(error as any).code = 'youtube_oauth_not_configured'
    throw error
  }
  if (!code) {
    const error = new Error('missing_oauth_code')
    ;(error as any).code = 'missing_oauth_code'
    throw error
  }
  if (state) {
    const valid = await consumeState({ userId, state })
    if (!valid) {
      const error = new Error('invalid_oauth_state')
      ;(error as any).code = 'invalid_oauth_state'
      throw error
    }
  }

  const existing = await loadConnection(userId)
  const exchanged = await exchangeTokenRequest({
    grantType: 'authorization_code',
    code,
    credentials
  })
  if (!exchanged.accessToken) {
    const error = new Error('oauth_exchange_missing_access_token')
    ;(error as any).code = 'oauth_exchange_missing_access_token'
    throw error
  }
  const channel = await fetchChannelIdentity(exchanged.accessToken).catch(() => ({
    channelId: null,
    channelTitle: null
  }))
  const now = nowIso()
  const nextConnection = normalizeStoredConnection({
    userId,
    channelId: channel.channelId || existing?.channelId || null,
    channelTitle: channel.channelTitle || existing?.channelTitle || null,
    accessToken: exchanged.accessToken,
    refreshToken: exchanged.refreshToken || existing?.refreshToken || null,
    tokenType: exchanged.tokenType || existing?.tokenType || 'Bearer',
    scope: exchanged.scope || existing?.scope || YOUTUBE_ANALYTICS_SCOPES.join(' '),
    expiryDate: exchanged.expiryDate || existing?.expiryDate || null,
    createdAt: existing?.createdAt || now,
    updatedAt: now
  })
  await saveConnection(nextConnection)
  return toPublicConnection(nextConnection)
}

const isAccessTokenUsable = (connection: StoredConnection) => {
  const expiryMs = connection.expiryDate ? new Date(connection.expiryDate).getTime() : Number.POSITIVE_INFINITY
  return expiryMs - ACCESS_TOKEN_EXPIRY_SAFETY_MS > Date.now()
}

const refreshConnectionAccessToken = async (connection: StoredConnection) => {
  const credentials = resolveOAuthCredentials()
  if (!credentials) {
    const error = new Error('youtube_oauth_not_configured')
    ;(error as any).code = 'youtube_oauth_not_configured'
    throw error
  }
  if (!connection.refreshToken) {
    const error = new Error('missing_refresh_token')
    ;(error as any).code = 'missing_refresh_token'
    throw error
  }
  const refreshed = await exchangeTokenRequest({
    grantType: 'refresh_token',
    refreshToken: connection.refreshToken,
    credentials
  })
  if (!refreshed.accessToken) {
    const error = new Error('refresh_missing_access_token')
    ;(error as any).code = 'refresh_missing_access_token'
    throw error
  }
  const nextConnection = normalizeStoredConnection({
    ...connection,
    accessToken: refreshed.accessToken,
    refreshToken: refreshed.refreshToken || connection.refreshToken,
    tokenType: refreshed.tokenType || connection.tokenType || 'Bearer',
    scope: refreshed.scope || connection.scope,
    expiryDate: refreshed.expiryDate || connection.expiryDate,
    updatedAt: nowIso()
  })
  await saveConnection(nextConnection)
  return nextConnection
}

export const getYouTubeOAuthConnectionForUser = async (userId: string) => {
  const connection = await loadConnection(userId)
  return toPublicConnection(connection)
}

export const disconnectYouTubeOAuthForUser = async (userId: string) => {
  await deleteConnection(userId)
  return { ok: true }
}

export const getYouTubeAccessTokenForUser = async (userId: string) => {
  const connection = await loadConnection(userId)
  if (!connection) return null
  if (isAccessTokenUsable(connection)) return connection.accessToken
  try {
    const refreshed = await refreshConnectionAccessToken(connection)
    return refreshed.accessToken
  } catch {
    return connection.accessToken || null
  }
}
