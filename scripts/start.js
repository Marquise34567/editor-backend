const { execSync } = require('child_process')

const parseBool = (value) => /^(1|true|yes)$/i.test(String(value || '').trim())

const run = (label, command, options = {}) => {
  const { allowFailure = false } = options
  console.log(`[startup] ${label}`)
  try {
    execSync(command, { stdio: 'inherit' })
    return true
  } catch (error) {
    if (!allowFailure) throw error
    const message = error instanceof Error ? error.message : String(error)
    console.warn(`[startup] ${label} failed, continuing startup`)
    console.warn(`[startup] ${message}`)
    return false
  }
}

const hasDatabaseUrl = Boolean(String(process.env.DATABASE_URL || '').trim())
const skipMigrations = parseBool(process.env.SKIP_PRISMA_MIGRATIONS)

run('Building backend', 'npm run build')
run('Generating Prisma client', 'prisma generate')

if (!hasDatabaseUrl) {
  console.warn('[startup] DATABASE_URL is empty, skipping prisma migrate deploy')
} else if (skipMigrations) {
  console.warn('[startup] SKIP_PRISMA_MIGRATIONS is enabled, skipping prisma migrate deploy')
} else {
  run('Applying Prisma migrations', 'prisma migrate deploy', { allowFailure: true })
}

console.log('[startup] Starting API server')
require('../dist/index.js')
