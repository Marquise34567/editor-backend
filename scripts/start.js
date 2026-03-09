const { execSync } = require('child_process')
const { existsSync, readdirSync, statSync } = require('fs')
const path = require('path')

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

const getLatestMtimeMs = (targetPath) => {
  if (!existsSync(targetPath)) return 0
  try {
    const targetStats = statSync(targetPath)
    if (!targetStats.isDirectory()) return Number(targetStats.mtimeMs || 0)
  } catch {
    return 0
  }
  let latest = 0
  const stack = [targetPath]
  while (stack.length > 0) {
    const current = stack.pop()
    if (!current) continue
    let entries = []
    try {
      entries = readdirSync(current, { withFileTypes: true })
    } catch {
      continue
    }
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        if (entry.name === 'node_modules' || entry.name === 'dist' || entry.name.startsWith('.')) continue
        stack.push(fullPath)
        continue
      }
      if (!entry.isFile()) continue
      if (!/\.(ts|js|json|prisma)$/i.test(entry.name)) continue
      try {
        const mtimeMs = Number(statSync(fullPath).mtimeMs || 0)
        if (mtimeMs > latest) latest = mtimeMs
      } catch {
        // ignore stat failures for transient files
      }
    }
  }
  return latest
}

const hasDatabaseUrl = Boolean(String(process.env.DATABASE_URL || '').trim())
const skipMigrations = parseBool(process.env.SKIP_PRISMA_MIGRATIONS)
const shouldBuildOnStartup = parseBool(process.env.BUILD_ON_STARTUP)
const shouldGeneratePrismaOnStartup = parseBool(process.env.PRISMA_GENERATE_ON_STARTUP)
const shouldInstallCaptionRuntimeOnStartup = parseBool(process.env.INSTALL_CAPTION_RUNTIME_ON_STARTUP)
const distEntryPath = path.resolve(process.cwd(), 'dist', 'index.js')
const sourceMtimeMs = Math.max(
  getLatestMtimeMs(path.resolve(process.cwd(), 'src')),
  getLatestMtimeMs(path.resolve(process.cwd(), 'prisma')),
  getLatestMtimeMs(path.resolve(process.cwd(), 'tsconfig.json'))
)
const distMtimeMs = getLatestMtimeMs(distEntryPath)
const distStale = sourceMtimeMs > distMtimeMs + 1000

if (shouldBuildOnStartup || !existsSync(distEntryPath) || distStale) {
  run('Building backend', 'npm run build')
} else {
  console.log('[startup] Skipping startup build; dist/index.js is up to date')
}

if (shouldGeneratePrismaOnStartup) {
  run('Generating Prisma client', 'prisma generate')
} else {
  console.log('[startup] Skipping prisma generate on startup')
}

if (shouldInstallCaptionRuntimeOnStartup) {
  run('Installing caption runtime', 'node scripts/install-caption-runtime.js')
} else {
  console.log('[startup] Skipping caption runtime install on startup')
}

if (!hasDatabaseUrl) {
  console.warn('[startup] DATABASE_URL is empty, skipping prisma migrate deploy')
} else if (skipMigrations) {
  console.warn('[startup] SKIP_PRISMA_MIGRATIONS is enabled, skipping prisma migrate deploy')
} else {
  run('Applying Prisma migrations', 'prisma migrate deploy', { allowFailure: true })
}

console.log('[startup] Starting API server')
require('../dist/index.js')
