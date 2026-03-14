const { execSync } = require('child_process')

const run = (label, command, options = {}) => {
  const { allowFailure = false } = options
  console.log(`[postinstall] ${label}`)
  try {
    execSync(command, { stdio: 'inherit' })
    return true
  } catch (error) {
    if (!allowFailure) throw error
    const message = error instanceof Error ? error.message : String(error)
    console.warn(`[postinstall] ${label} failed, continuing`)
    console.warn(`[postinstall] ${message}`)
    return false
  }
}

const prismaOk = run('Generating Prisma client', 'prisma generate')
if (!prismaOk) {
  process.exit(1)
}

run('Installing caption runtime', 'node scripts/install-caption-runtime.js', { allowFailure: true })
