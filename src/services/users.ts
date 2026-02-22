import { prisma } from '../db/prisma'

export const getOrCreateUser = async (userId: string, email?: string | null) => {
  let user = await prisma.user.findUnique({ where: { id: userId } })
  if (!user) {
    user = await prisma.user.create({
      data: {
        id: userId,
        email: email || `${userId}@autoeditor.local`,
        planStatus: 'free',
      }
    })
  }
  return user
}
