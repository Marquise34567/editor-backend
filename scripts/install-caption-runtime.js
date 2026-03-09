const { spawnSync } = require('child_process')
const { existsSync, writeFileSync } = require('fs')
const path = require('path')

const ENABLED_PATTERN = /^(1|true|yes|on)$/i
const DISABLED_PATTERN = /^(0|false|no|off)$/i

const rawInstallToggle = String(
  process.env.INSTALL_CAPTION_RUNTIME ??
  process.env.INSTALL_CAPTION_RUNTIME_ON_STARTUP ??
  ''
).trim()
const shouldInstallCaptionRuntime = (() => {
  if (ENABLED_PATTERN.test(rawInstallToggle)) return true
  if (DISABLED_PATTERN.test(rawInstallToggle)) return false
  return true
})()

const CAPTION_RUNTIME_POINTER_FILE = path.resolve(process.cwd(), '.caption-runtime-python-path')

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

const runPipInstall = (pythonBin, extraArgs) => {
  const result = spawnSync(pythonBin, [...pipBaseArgs, ...extraArgs], {
    stdio: 'inherit',
    windowsHide: true,
    env: {
      ...process.env,
      PIP_ROOT_USER_ACTION: process.env.PIP_ROOT_USER_ACTION || 'ignore'
    }
  })
  return result.status === 0
}

const isCaptionRuntimeAlreadyInstalled = (pythonBin) => {
  const probe = spawnSync(
    pythonBin,
    ['-c', 'import faster_whisper;print("ok")'],
    {
      encoding: 'utf8',
      stdio: 'pipe',
      windowsHide: true
    }
  )
  return !probe.error && probe.status === 0
}

const installWithFallback = (pythonBin, extraArgs, opts = {}) => {
  const {
    allowBreakSystemPackages = true
  } = opts
  if (runPipInstall(pythonBin, extraArgs)) return true
  if (!allowBreakSystemPackages) return false
  return runPipInstall(pythonBin, ['--break-system-packages', ...extraArgs])
}

const resolveVenvPythonPath = (venvDir) => (
  process.platform === 'win32'
    ? path.join(venvDir, 'Scripts', 'python.exe')
    : path.join(venvDir, 'bin', 'python')
)

const shouldUseVenv = !DISABLED_PATTERN.test(String(process.env.CAPTION_RUNTIME_USE_VENV || 'true').trim())
const venvDir = path.resolve(
  process.cwd(),
  String(process.env.CAPTION_RUNTIME_VENV_DIR || '.caption-runtime-venv').trim() || '.caption-runtime-venv'
)
const venvPython = resolveVenvPythonPath(venvDir)

const ensureVenv = () => {
  if (existsSync(venvPython)) return true
  console.log(`[caption-runtime] creating virtualenv at ${venvDir}`)
  const result = spawnSync(pythonCommand, ['-m', 'venv', venvDir], {
    stdio: 'inherit',
    windowsHide: true
  })
  return result.status === 0 && existsSync(venvPython)
}

const persistPythonPointer = (pythonBin) => {
  try {
    writeFileSync(CAPTION_RUNTIME_POINTER_FILE, `${pythonBin}\n`, 'utf8')
  } catch {
    // non-fatal
  }
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

let installerPython = pythonCommand
let installerIsVenv = false
if (shouldUseVenv) {
  if (ensureVenv()) {
    installerPython = venvPython
    installerIsVenv = true
    console.log(`[caption-runtime] using virtualenv python ${installerPython}`)
  } else {
    console.warn('[caption-runtime] virtualenv setup failed; falling back to system python install')
  }
}

if (packages.length === 1 && packages[0] === 'faster-whisper' && isCaptionRuntimeAlreadyInstalled(installerPython)) {
  console.log('[caption-runtime] faster-whisper already available')
  persistPythonPointer(installerPython)
  process.exit(0)
}

const allowBreakSystemPackages = !installerIsVenv
if (!installWithFallback(installerPython, ['--upgrade', 'pip'], { allowBreakSystemPackages })) {
  if (installerIsVenv && installWithFallback(pythonCommand, ['--upgrade', 'pip'])) {
    installerPython = pythonCommand
    installerIsVenv = false
  } else {
    console.error('[caption-runtime] failed to upgrade pip')
    process.exit(1)
  }
}

if (!installWithFallback(installerPython, packages, { allowBreakSystemPackages: !installerIsVenv })) {
  if (installerIsVenv && installWithFallback(pythonCommand, packages)) {
    installerPython = pythonCommand
    installerIsVenv = false
  } else {
    console.error(`[caption-runtime] failed to install packages: ${packages.join(', ')}`)
    process.exit(1)
  }
}

persistPythonPointer(installerPython)
if (installerIsVenv) {
  console.log(`[caption-runtime] set FASTER_WHISPER_PYTHON=${installerPython}`)
}
console.log(`[caption-runtime] installed packages: ${packages.join(', ')}`)
process.exit(0)
