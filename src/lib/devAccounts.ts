import { resolveProfileAdminFlags } from '../services/adminTelemetry'

export const CONTROL_PANEL_OWNER_EMAIL = 'fyequise03@gmail.com'
const parseCsvValues = (value?: string | null) => {
  return String(value || '')
    .split(',')
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean)
}
const ALLOWED_DEV_ADMIN_EMAILS = new Set([
  CONTROL_PANEL_OWNER_EMAIL,
  ...parseCsvValues(process.env.DEV_ACCOUNT_EMAILS)
])
const ALLOWED_DEV_ACCOUNT_USER_IDS = new Set(parseCsvValues(process.env.DEV_ACCOUNT_USER_IDS))

const normalizeEmail = (email?: string | null) => String(email || '').trim().toLowerCase()
const normalizeUserId = (userId?: string | null) => String(userId || '').trim().toLowerCase()

export const isControlPanelOwnerEmail = (email?: string | null) => {
  const normalized = normalizeEmail(email)
  return Boolean(normalized && ALLOWED_DEV_ADMIN_EMAILS.has(normalized))
}

export const isDevAccount = (userId?: string | null, email?: string | null) => {
  if (isControlPanelOwnerEmail(email)) return true
  const normalizedUserId = normalizeUserId(userId)
  return Boolean(normalizedUserId && ALLOWED_DEV_ACCOUNT_USER_IDS.has(normalizedUserId))
}

export const resolveDevAdminAccess = async (userId?: string | null, email?: string | null) => {
  const emailAuthorized = isControlPanelOwnerEmail(email)

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

  return {
    allowed: emailAuthorized,
    emailAuthorized,
    role,
    isDevAdmin: Boolean(flags?.isDevAdmin)
  }
}

export const getAllowedDevAdminEmails = () => Array.from(ALLOWED_DEV_ADMIN_EMAILS.values())
