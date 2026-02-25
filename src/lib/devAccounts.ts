const ONLY_DEV_PANEL_EMAIL = 'fyequise03@gmail.com'
const ONLY_DEV_PANEL_EMAIL_NORMALIZED = ONLY_DEV_PANEL_EMAIL.trim().toLowerCase()

export const isDevAccount = (userId?: string | null, email?: string | null) => {
  const normalizedEmail = String(email || '').trim().toLowerCase()
  return Boolean(normalizedEmail && normalizedEmail === ONLY_DEV_PANEL_EMAIL_NORMALIZED)
}
