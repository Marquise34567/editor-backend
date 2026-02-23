const DEV_ACCOUNT_EMAILS = (process.env.DEV_ACCOUNT_EMAILS || process.env.DEV_ACCOUNT_EMAIL || '')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean)

const DEV_ACCOUNT_USER_IDS = (process.env.DEV_ACCOUNT_USER_IDS || '')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean)

export const isDevAccount = (userId?: string | null, email?: string | null) => {
  const normalizedEmail = String(email || '').trim().toLowerCase()
  if (normalizedEmail && DEV_ACCOUNT_EMAILS.includes(normalizedEmail)) return true
  if (userId && DEV_ACCOUNT_USER_IDS.includes(userId)) return true
  return false
}
