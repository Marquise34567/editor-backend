const { execSync } = require('child_process')
const { existsSync, readdirSync, statSync, watch } = require('fs')
const path = require('path')
const { startWorkerSupervisor } = require('./worker-supervisor')

const parseBool = (value) => /^(1|true|yes)$/i.test(String(value || '').trim())
const parseIntEnv = (value, fallback) => {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback
  return Math.round(parsed)
}

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
const rawCaptionRuntimeInstallToggle = String(
  process.env.INSTALL_CAPTION_RUNTIME_ON_STARTUP ??
  process.env.INSTALL_CAPTION_RUNTIME ??
  ''
).trim()
const shouldInstallCaptionRuntimeOnStartup = !/^(0|false|no|off)$/i.test(rawCaptionRuntimeInstallToggle)
const distEntryPath = path.resolve(process.cwd(), 'dist', 'worker.js')
const sourceMtimeMs = Math.max(
  getLatestMtimeMs(path.resolve(process.cwd(), 'src')),
  getLatestMtimeMs(path.resolve(process.cwd(), 'prisma')),
  getLatestMtimeMs(path.resolve(process.cwd(), 'tsconfig.json'))
)
const distMtimeMs = getLatestMtimeMs(distEntryPath)
const distStale = sourceMtimeMs > distMtimeMs + 1000
const shouldWatchWorkerChanges = (() => {
  if (parseBool(process.env.WATCH_WORKER_CHANGES)) return true
  if (String(process.env.WATCH_WORKER_CHANGES || '').trim() !== '') return false
  const env = String(process.env.NODE_ENV || '').trim().toLowerCase()
  return env !== 'production'
})()
const watchDebounceMs = Math.max(100, parseIntEnv(process.env.WATCH_WORKER_DEBOUNCE_MS, 300))

if (shouldBuildOnStartup || !existsSync(distEntryPath) || distStale) {
  run('Building backend', 'npm run build')
} else {
  console.log('[startup] Skipping startup build; dist/worker.js is up to date')
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

console.log('[startup] Starting pipeline worker supervisor')
const supervisor = startWorkerSupervisor({
  workerEntryPath: distEntryPath,
  label: 'pipeline worker'
})

if (shouldWatchWorkerChanges) {
  const distDir = path.dirname(distEntryPath)
  const distBasename = path.basename(distEntryPath)
  let lastMtimeMs = getLatestMtimeMs(distEntryPath)
  let restartTimer = null

  const scheduleRestart = (reason) => {
    if (restartTimer) return
    restartTimer = setTimeout(() => {
      restartTimer = null
      console.log(`[startup] Detected ${reason} change in dist/worker.js; restarting worker replicas`)
      if (supervisor && typeof supervisor.restart === 'function') {
        supervisor.restart()
      }
    }, watchDebounceMs)
    if (restartTimer && typeof restartTimer.unref === 'function') {
      restartTimer.unref()
    }
  }

  try {
    const watcher = watch(distDir, { persistent: true }, (eventType, filename) => {
      if (filename && String(filename) !== distBasename) return
      const nextMtimeMs = getLatestMtimeMs(distEntryPath)
      if (nextMtimeMs <= lastMtimeMs + 10) return
      lastMtimeMs = nextMtimeMs
      scheduleRestart(eventType || 'change')
    })

    watcher.on('error', (error) => {
      console.warn(`[startup] Worker watch error: ${error instanceof Error ? error.message : String(error)}`)
    })

    process.on('exit', () => watcher.close())
    console.log('[startup] Watching dist/worker.js for changes; set WATCH_WORKER_CHANGES=0 to disable')
  } catch (error) {
    console.warn(`[startup] Failed to watch dist/worker.js: ${error instanceof Error ? error.message : String(error)}`)
  }
}
