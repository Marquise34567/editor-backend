const { spawnSync } = require('child_process')

const ENABLED_PATTERN = /^(1|true|yes|on)$/i
const DISABLED_PATTERN = /^(0|false|no|off)$/i

const rawInstallToggle = String(process.env.INSTALL_CAPTION_RUNTIME || '').trim()
const shouldInstallCaptionRuntime = (() => {
  if (ENABLED_PATTERN.test(rawInstallToggle)) return true
  if (DISABLED_PATTERN.test(rawInstallToggle)) return false
  return Boolean(String(process.env.RAILWAY_ENVIRONMENT || '').trim())
})()

if (!shouldInstallCaptionRuntime) {
  console.log('[caption-runtime] skipped')
  process.exit(0)
}

const resolvePythonCommand = () => {
  const candidates = [
    String(process.env.FASTER_WHISPER_PYTHON || '').trim(),
    'python3',
    'python'
  ].filter(Boolean)
  for (const command of candidates) {
    const probe = spawnSync(command, ['--version'], {
      encoding: 'utf8',
      stdio: 'pipe',
      windowsHide: true
    })
    if (!probe.error && probe.status === 0) {
      return command
    }
  }
  return null
}

const pythonCommand = resolvePythonCommand()
if (!pythonCommand) {
  console.error('[caption-runtime] python runtime not found; install python3/python3-pip in deploy image')
  process.exit(1)
}

const pipBaseArgs = ['-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir']

const runPipInstall = (extraArgs) => {
  const result = spawnSync(pythonCommand, [...pipBaseArgs, ...extraArgs], {
    stdio: 'inherit',
    windowsHide: true,
    env: {
      ...process.env,
      PIP_ROOT_USER_ACTION: process.env.PIP_ROOT_USER_ACTION || 'ignore'
    }
  })
  return result.status === 0
}

const installWithFallback = (extraArgs) => {
  if (runPipInstall(extraArgs)) return true
  return runPipInstall(['--break-system-packages', ...extraArgs])
}

const packages = String(process.env.CAPTION_RUNTIME_PIP_PACKAGES || 'faster-whisper')
  .split(/\s+/)
  .map((entry) => entry.trim())
  .filter(Boolean)

if (!packages.length) {
  console.log('[caption-runtime] no packages requested')
  process.exit(0)
}

console.log(`[caption-runtime] using ${pythonCommand}`)

if (!installWithFallback(['--upgrade', 'pip'])) {
  console.error('[caption-runtime] failed to upgrade pip')
  process.exit(1)
}

if (!installWithFallback(packages)) {
  console.error(`[caption-runtime] failed to install packages: ${packages.join(', ')}`)
  process.exit(1)
}

console.log(`[caption-runtime] installed packages: ${packages.join(', ')}`)
