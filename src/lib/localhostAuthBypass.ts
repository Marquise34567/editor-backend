type HeaderValue = string | string[] | undefined

type RequestLike = {
  headers?: Record<string, HeaderValue>
  ip?: string | null
  hostname?: string | null
  socket?: {
    remoteAddress?: string | null
  } | null
}

const TRUE_PATTERN = /^(1|true|yes|on)$/i
const FALSE_PATTERN = /^(0|false|no|off)$/i

const normalize = (value?: string | null) => String(value || '').trim()

const firstHeaderValue = (value: HeaderValue) => {
  if (Array.isArray(value)) return normalize(value[0] || '')
  return normalize(value)
}

const parseHost = (value?: string | null) => {
  const raw = normalize(value).toLowerCase()
  if (!raw) return ''
  if (raw.startsWith('[') && raw.includes(']')) {
    return raw.slice(1, raw.indexOf(']'))
  }
  return raw.split(':')[0]
}

const parseHostFromUrl = (value?: string | null) => {
  const raw = normalize(value)
  if (!raw) return ''
  try {
    const url = new URL(raw)
    return parseHost(url.hostname)
  } catch {
    return ''
  }
}

const parseForwardedFor = (value?: string | null) => {
  const raw = normalize(value)
  if (!raw) return ''
  return raw.split(',')[0]?.trim() || ''
}

const isLoopbackHost = (value?: string | null) => {
  const host = parseHost(value)
  return host === 'localhost' || host === '127.0.0.1' || host === '::1'
}

const isLoopbackIp = (value?: string | null) => {
  const ip = normalize(value).replace(/^::ffff:/i, '').toLowerCase()
  if (!ip) return false
  return ip === '::1' || ip === '127.0.0.1' || ip.startsWith('127.') || ip === 'localhost'
}

export const isLocalhostAuthBypassEnabled = () => {
  const raw = normalize(process.env.LOCALHOST_AUTH_BYPASS)
  if (!raw) return true
  if (FALSE_PATTERN.test(raw)) return false
  return TRUE_PATTERN.test(raw)
}

export const isLocalhostRequestLike = (req: RequestLike) => {
  const host = parseHost(firstHeaderValue(req.headers?.host))
  const originHost = parseHostFromUrl(firstHeaderValue(req.headers?.origin))
  const refererHost = parseHostFromUrl(firstHeaderValue(req.headers?.referer))
  const hostname = parseHost(req.hostname || '')

  if (isLoopbackHost(host) || isLoopbackHost(originHost) || isLoopbackHost(refererHost) || isLoopbackHost(hostname)) {
    return true
  }

  const forwardedIp = parseForwardedFor(firstHeaderValue(req.headers?.['x-forwarded-for']))
  const socketIp = normalize(req.socket?.remoteAddress || '')
  const reqIp = normalize(req.ip || '')
  if (isLoopbackIp(forwardedIp) || isLoopbackIp(socketIp) || isLoopbackIp(reqIp)) {
    return true
  }

  return false
}

export const shouldBypassAuthForLocalhost = (req: RequestLike) => {
  if (!isLocalhostAuthBypassEnabled()) return false
  return isLocalhostRequestLike(req)
}

export const getLocalhostBypassUser = () => ({
  id: normalize(process.env.LOCALHOST_BYPASS_USER_ID) || 'localhost-dev-user',
  email: normalize(process.env.LOCALHOST_BYPASS_EMAIL) || 'localhost-dev@autoeditor.local'
})

