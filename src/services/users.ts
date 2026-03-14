import { prisma } from '../db/prisma'

const REFERRAL_CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
const REFERRAL_CODE_LENGTH = 8
const MAX_REFERRAL_CODE_ATTEMPTS = 16

const randomReferralCode = () => {
  let code = ''
  for (let index = 0; index < REFERRAL_CODE_LENGTH; index += 1) {
    const pick = Math.floor(Math.random() * REFERRAL_CODE_ALPHABET.length)
    code += REFERRAL_CODE_ALPHABET[pick]
  }
  return code
}

const normalizeReferralCode = (value: unknown) =>
  String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')

export const parseReferralCode = (value: unknown): string | null => {
  const normalized = normalizeReferralCode(value)
  if (!normalized) return null
  if (normalized.length < 6 || normalized.length > 16) return null
  return normalized
}

export const createUniqueReferralCode = async (excludeUserId?: string): Promise<string> => {
  for (let attempt = 0; attempt < MAX_REFERRAL_CODE_ATTEMPTS; attempt += 1) {
    const candidate = randomReferralCode()
    const existing = await prisma.user.findUnique({ where: { referralCode: candidate } })
    if (!existing || existing.id === excludeUserId) return candidate
  }
  const fallback = normalizeReferralCode(`R${Math.random().toString(36).slice(2, 12)}`)
  if (fallback.length >= 6) return fallback.slice(0, 16)
  return `REF${Date.now().toString(36).toUpperCase()}`.slice(0, 16)
}

export const ensureUserReferralCode = async (userId: string): Promise<string> => {
  const existing = await prisma.user.findUnique({ where: { id: userId } })
  if (existing?.referralCode) return String(existing.referralCode)
  const referralCode = await createUniqueReferralCode(userId)
  const updated = await prisma.user.update({
    where: { id: userId },
    data: { referralCode }
  })
  return String(updated.referralCode || referralCode)
}

export const getOrCreateUser = async (userId: string, email?: string | null) => {
  const normalizedEmail = String(email || '').trim().toLowerCase()
  let user = await prisma.user.findUnique({ where: { id: userId } })
  if (!user) {
    const referralCode = await createUniqueReferralCode(userId)
    user = await prisma.user.create({
      data: {
        id: userId,
        email: normalizedEmail || `${userId}@autoeditor.local`,
        planStatus: 'free',
        referralCode,
      }
    })
  } else if (normalizedEmail) {
    const existingEmail = String(user.email || '').trim().toLowerCase()
    const hasPlaceholderEmail = existingEmail.endsWith('@autoeditor.local')
    if (hasPlaceholderEmail || !existingEmail || existingEmail !== normalizedEmail) {
      user = await prisma.user.update({
        where: { id: userId },
        data: { email: normalizedEmail }
      })
    }
  }
  if (!user?.referralCode) {
    const referralCode = await createUniqueReferralCode(userId)
    user = await prisma.user.update({
      where: { id: userId },
      data: { referralCode }
    })
  }
  return user
}
