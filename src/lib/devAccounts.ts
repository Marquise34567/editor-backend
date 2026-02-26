import { resolveProfileAdminFlags } from '../services/adminTelemetry'

export const CONTROL_PANEL_OWNER_EMAIL = 'fyequise03@gmail.com'
const ALLOWED_DEV_ADMIN_EMAILS = new Set([CONTROL_PANEL_OWNER_EMAIL])

const normalizeEmail = (email?: string | null) => String(email || '').trim().toLowerCase()

export const isControlPanelOwnerEmail = (email?: string | null) => {
  const normalized = normalizeEmail(email)
  return Boolean(normalized && ALLOWED_DEV_ADMIN_EMAILS.has(normalized))
}

export const isDevAccount = (_userId?: string | null, email?: string | null) => {
  return isControlPanelOwnerEmail(email)
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
