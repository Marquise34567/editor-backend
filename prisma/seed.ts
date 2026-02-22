import { PrismaClient } from '@prisma/client'

const prisma = new PrismaClient()

async function main() {
  console.log('Seeding lessons...')
  const lessons = [
    { title: 'Welcome to Sparkd Coach', category: 'intro', contentJson: JSON.stringify({ blocks: [] }), isPremium: false, order: 1 },
    { title: 'Advanced Prompting', category: 'advanced', contentJson: JSON.stringify({ blocks: [] }), isPremium: true, order: 2 },
  ]
  for (const l of lessons) {
    await prisma.lesson.upsert({ where: { title: l.title }, update: {}, create: l as any })
  }
  console.log('Seed complete')
}

main()
  .catch(e => { console.error(e); process.exit(1) })
  .finally(() => prisma.$disconnect())
