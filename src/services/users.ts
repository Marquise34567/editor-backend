import { prisma } from '../db/prisma'

export const getOrCreateUser = async (userId: string, email?: string | null) => {
  const normalizedEmail = String(email || '').trim().toLowerCase()
  let user = await prisma.user.findUnique({ where: { id: userId } })
  if (!user) {
    user = await prisma.user.create({
      data: {
        id: userId,
        email: normalizedEmail || `${userId}@autoeditor.local`,
        planStatus: 'free',
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
  return user
}
