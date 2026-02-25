import { resolveProfileAdminFlags } from '../services/adminTelemetry'

const DEFAULT_DEV_ADMIN_EMAIL = 'fyequise03@gmail.com'
const ADMIN_ROLE_SET = new Set(['ADMIN', 'OWNER', 'FOUNDER', 'SUPERADMIN'])

const parseAllowedEmails = () => {
  const configured = String(process.env.DEV_ADMIN_EMAILS || process.env.ADMIN_EMAILS || '')
    .split(',')
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean)
  if (configured.length > 0) return new Set(configured)
  return new Set([DEFAULT_DEV_ADMIN_EMAIL])
}

const ALLOWED_DEV_ADMIN_EMAILS = parseAllowedEmails()

const normalizeEmail = (email?: string | null) => String(email || '').trim().toLowerCase()

export const isDevAccount = (_userId?: string | null, email?: string | null) => {
  const normalized = normalizeEmail(email)
  return Boolean(normalized && ALLOWED_DEV_ADMIN_EMAILS.has(normalized))
}

export const resolveDevAdminAccess = async (userId?: string | null, email?: string | null) => {
  const normalizedEmail = normalizeEmail(email)
  const emailAuthorized = Boolean(normalizedEmail && ALLOWED_DEV_ADMIN_EMAILS.has(normalizedEmail))

  if (!emailAuthorized || !userId) {
    return {
      allowed: false,
      emailAuthorized,
      role: 'USER',
      isDevAdmin: false
    }
  }

  const flags = await resolveProfileAdminFlags(String(userId))
  const role = String(flags?.role || 'USER').trim().toUpperCase() || 'USER'
  const roleAuthorized = Boolean(flags?.isDevAdmin || ADMIN_ROLE_SET.has(role))

  return {
    allowed: emailAuthorized && roleAuthorized,
    emailAuthorized,
    role,
    isDevAdmin: Boolean(flags?.isDevAdmin)
  }
}

export const getAllowedDevAdminEmails = () => Array.from(ALLOWED_DEV_ADMIN_EMAILS.values())
