const fs = require('fs')
const path = require('path')
const { PrismaClient } = require('@prisma/client')

const loadDotEnvIfNeeded = () => {
  if (process.env.DATABASE_URL && String(process.env.DATABASE_URL).trim()) return
  const envPath = path.join(__dirname, '..', '.env')
  if (!fs.existsSync(envPath)) return
  const raw = fs.readFileSync(envPath, 'utf8')
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#')) continue
    const eq = trimmed.indexOf('=')
    if (eq <= 0) continue
    const key = trimmed.slice(0, eq).trim()
    let value = trimmed.slice(eq + 1).trim()
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1)
    }
    if (!process.env[key]) process.env[key] = value
  }
}

loadDotEnvIfNeeded()

const prisma = new PrismaClient()

const args = process.argv.slice(2)
const getArg = (flag) => {
  const idx = args.indexOf(flag)
  if (idx === -1) return null
  return args[idx + 1] ?? null
}

const readNumber = (raw, fallback) => {
  const value = Number(raw)
  return Number.isFinite(value) ? value : fallback
}

const minutes = readNumber(
  getArg('--minutes') || process.env.REQUEUE_ANALYZING_MINUTES,
  30
)
const maxProgress = readNumber(
  getArg('--max-progress') || process.env.REQUEUE_ANALYZING_MAX_PROGRESS,
  12
)
const limit = Math.max(
  1,
  Math.round(
    readNumber(getArg('--limit') || process.env.REQUEUE_ANALYZING_LIMIT, 500)
  )
)
const apply = args.includes('--apply')

async function main() {
  const cutoff = new Date(Date.now() - minutes * 60 * 1000)
  const jobs = await prisma.job.findMany({
    where: {
      status: 'analyzing',
      progress: { lte: maxProgress },
      updatedAt: { lt: cutoff }
    },
    orderBy: { updatedAt: 'asc' },
    take: limit,
    select: {
      id: true,
      status: true,
      progress: true,
      updatedAt: true,
      userId: true
    }
  })

  console.log(
    JSON.stringify(
      {
        mode: apply ? 'apply' : 'dry-run',
        cutoff,
        minutes,
        maxProgress,
        limit,
        count: jobs.length,
        sample: jobs.slice(0, 10)
      },
      null,
      2
    )
  )

  if (!apply || jobs.length === 0) return

  const ids = jobs.map((job) => job.id)
  const result = await prisma.job.updateMany({
    where: { id: { in: ids } },
    data: { status: 'queued', progress: 1, error: null }
  })
  console.log(`Requeued ${result.count} jobs.`)
}

main()
  .catch((err) => {
    console.error(err)
    process.exitCode = 1
  })
  .finally(async () => {
    await prisma.$disconnect()
  })
