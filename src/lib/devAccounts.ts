import { resolveProfileAdminFlags } from '../services/adminTelemetry'
import { getLocalhostBypassUser } from './localhostAuthBypass'

export const CONTROL_PANEL_OWNER_EMAIL = 'fyequise03@gmail.com'

const parseBooleanEnv = (value?: string | null) => {
  const raw = String(value || '').trim().toLowerCase()
  return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on'
}

const parseCsvValues = (value?: string | null) => {
  return String(value || '')
    .split(',')
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean)
}

const GLOBAL_DEV_ACCOUNT_BYPASS = parseBooleanEnv(process.env.DEV_ACCOUNT_BYPASS_ALL)
  || parseBooleanEnv(process.env.BILLING_BYPASS_ALL)
  || parseBooleanEnv(process.env.PLAN_LIMIT_BYPASS_ALL)

const ALLOWED_DEV_ADMIN_EMAILS = new Set([
  CONTROL_PANEL_OWNER_EMAIL,
  ...parseCsvValues(process.env.DEV_ACCOUNT_EMAILS)
])
const ALLOWED_DEV_ACCOUNT_USER_IDS = new Set(parseCsvValues(process.env.DEV_ACCOUNT_USER_IDS))
const LOCALHOST_BYPASS_USER = getLocalhostBypassUser()

const normalizeEmail = (email?: string | null) => String(email || '').trim().toLowerCase()
const normalizeUserId = (userId?: string | null) => String(userId || '').trim().toLowerCase()

export const isControlPanelOwnerEmail = (email?: string | null) => {
  if (GLOBAL_DEV_ACCOUNT_BYPASS) return true
  const normalized = normalizeEmail(email)
  if (normalized && ALLOWED_DEV_ADMIN_EMAILS.has('*')) return true
  return Boolean(normalized && ALLOWED_DEV_ADMIN_EMAILS.has(normalized))
}

export const isDevAccount = (userId?: string | null, email?: string | null) => {
  if (GLOBAL_DEV_ACCOUNT_BYPASS) return true
  if (isControlPanelOwnerEmail(email)) return true
  const normalizedEmail = normalizeEmail(email)
  if (normalizedEmail && normalizedEmail === normalizeEmail(LOCALHOST_BYPASS_USER.email)) return true
  const normalizedUserId = normalizeUserId(userId)
  if (normalizedUserId && normalizedUserId === normalizeUserId(LOCALHOST_BYPASS_USER.id)) return true
  if (normalizedUserId && ALLOWED_DEV_ACCOUNT_USER_IDS.has('*')) return true
  return Boolean(normalizedUserId && ALLOWED_DEV_ACCOUNT_USER_IDS.has(normalizedUserId))
}

export const resolveDevAdminAccess = async (userId?: string | null, email?: string | null) => {
  const devAuthorized = isDevAccount(userId, email)

  if (!devAuthorized || !userId) {
    return {
      allowed: false,
      emailAuthorized: false,
      role: 'USER',
      isDevAdmin: false
    }
  }

  const flags = await resolveProfileAdminFlags(String(userId))
  const role = String(flags?.role || 'USER').trim().toUpperCase() || 'USER'

  return {
    allowed: devAuthorized,
    emailAuthorized: true,
    role,
    isDevAdmin: Boolean(flags?.isDevAdmin)
  }
}

export const getAllowedDevAdminEmails = () => Array.from(ALLOWED_DEV_ADMIN_EMAILS.values())
