import { spawnSync } from 'child_process'
import { existsSync } from 'fs'
import path from 'path'

type CaptionProbeMode = 'faster_whisper' | 'whisper' | 'python_module' | 'api'

type CaptionProbeAttempt = {
  label: string
  command: string
  args: string[]
  mode: CaptionProbeMode
}

export type CaptionEngineStatus = {
  available: boolean
  provider: 'faster_whisper' | 'whisper' | 'openai_api' | 'none'
  command: string | null
  mode: CaptionProbeMode | null
  reason: string
  checkedAt: string
}

const CAPTION_ENGINE_CACHE_TTL_MS = 20_000
const DEFAULT_CAPTION_PROBE_TIMEOUT_MS = 25_000
const MIN_CAPTION_PROBE_TIMEOUT_MS = 3_000
const MAX_CAPTION_PROBE_TIMEOUT_MS = 120_000
const CAPTION_PROBE_TIMEOUT_MS = (() => {
  const raw = String(process.env.CAPTION_PROBE_TIMEOUT_MS || '').trim()
  if (!raw) return DEFAULT_CAPTION_PROBE_TIMEOUT_MS
  const parsed = Number(raw)
  if (!Number.isFinite(parsed)) return DEFAULT_CAPTION_PROBE_TIMEOUT_MS
  return Math.round(
    Math.max(MIN_CAPTION_PROBE_TIMEOUT_MS, Math.min(parsed, MAX_CAPTION_PROBE_TIMEOUT_MS))
  )
})()
const CAPTION_USAGE_SIGNATURE = /usage|--output_format|--model|transcribe/i
const FAST_WHISPER_SIGNATURE = /(faster_whisper|whispermodel|ok)/i
const DEFAULT_PYTHON_UTF8_ENV = '1'
const DEFAULT_PYTHON_IO_ENCODING = 'utf-8'

let cachedStatus: CaptionEngineStatus | null = null
let cacheUpdatedAtMs = 0

const trimToEmpty = (value?: string | null) => String(value || '').trim()

const getLikelyWindowsWhisperBins = () => {
  if (process.platform !== 'win32') return []
  const roots = [
    trimToEmpty(process.env.LOCALAPPDATA),
    trimToEmpty(path.join(process.env.USERPROFILE || '', 'AppData', 'Local'))
  ].filter(Boolean)
  const versions = ['314', '313', '312', '311', '310', '39']
  const bins: string[] = []
  for (const root of roots) {
    for (const version of versions) {
      bins.push(path.join(root, 'Programs', 'Python', `Python${version}`, 'Scripts', 'whisper.exe'))
    }
  }
  return bins.filter((candidate, idx) => bins.indexOf(candidate) === idx && existsSync(candidate))
}

const buildProbeAttempts = () => {
  const attempts: CaptionProbeAttempt[] = []
  const seen = new Set<string>()
  const addAttempt = (
    label: string,
    command: string | undefined,
    args: string[],
    mode: CaptionProbeMode
  ) => {
    const normalized = trimToEmpty(command)
    if (!normalized) return
    const key = `${normalized}\u001f${args.join('\u001f')}`
    if (seen.has(key)) return
    seen.add(key)
    attempts.push({ label, command: normalized, args, mode })
  }

  addAttempt('FASTER_WHISPER_PYTHON', process.env.FASTER_WHISPER_PYTHON, ['-c', 'import faster_whisper;print("ok")'], 'faster_whisper')
  addAttempt('python -c faster_whisper', 'python', ['-c', 'import faster_whisper;print("ok")'], 'faster_whisper')
  addAttempt('python3 -c faster_whisper', 'python3', ['-c', 'import faster_whisper;print("ok")'], 'faster_whisper')
  addAttempt('py -3 -c faster_whisper', 'py', ['-3', '-c', 'import faster_whisper;print("ok")'], 'faster_whisper')
  addAttempt('py -c faster_whisper', 'py', ['-c', 'import faster_whisper;print("ok")'], 'faster_whisper')
  addAttempt('WHISPER_BIN', process.env.WHISPER_BIN, ['--help'], 'whisper')
  for (const candidate of getLikelyWindowsWhisperBins()) {
    addAttempt(`WINDOWS_WHISPER_BIN(${candidate})`, candidate, ['--help'], 'whisper')
  }
  addAttempt('whisper', 'whisper', ['--help'], 'whisper')
  addAttempt('python -m whisper', 'python', ['-m', 'whisper', '--help'], 'python_module')
  addAttempt('python3 -m whisper', 'python3', ['-m', 'whisper', '--help'], 'python_module')
  addAttempt('py -3.11 -m whisper', 'py', ['-3.11', '-m', 'whisper', '--help'], 'python_module')
  addAttempt('py -3.12 -m whisper', 'py', ['-3.12', '-m', 'whisper', '--help'], 'python_module')
  addAttempt('py -3.10 -m whisper', 'py', ['-3.10', '-m', 'whisper', '--help'], 'python_module')
  addAttempt('py -3 -m whisper', 'py', ['-3', '-m', 'whisper', '--help'], 'python_module')
  addAttempt('py -m whisper', 'py', ['-m', 'whisper', '--help'], 'python_module')
  return attempts
}

const probeAttempt = (attempt: CaptionProbeAttempt) => {
  try {
    const probeEnv: NodeJS.ProcessEnv = {
      ...process.env,
      PYTHONUTF8: process.env.PYTHONUTF8 || DEFAULT_PYTHON_UTF8_ENV,
      PYTHONIOENCODING: process.env.PYTHONIOENCODING || DEFAULT_PYTHON_IO_ENCODING
    }
    const result = spawnSync(attempt.command, attempt.args, {
      timeout: CAPTION_PROBE_TIMEOUT_MS,
      encoding: 'utf8',
      windowsHide: true,
      env: probeEnv
    })
    if (result.error) return false
    const combinedOutput = `${result.stdout || ''}\n${result.stderr || ''}`
    if (attempt.mode === 'faster_whisper') {
      return result.status === 0 && FAST_WHISPER_SIGNATURE.test(combinedOutput)
    }
    if (CAPTION_USAGE_SIGNATURE.test(combinedOutput)) return true
    return result.status === 0
  } catch {
    return false
  }
}

const resolveCaptionEngineStatus = (): CaptionEngineStatus => {
  const attempts = buildProbeAttempts()
  for (const attempt of attempts) {
    if (!probeAttempt(attempt)) continue
    if (attempt.mode === 'faster_whisper') {
      return {
        available: true,
        provider: 'faster_whisper',
        command: attempt.command,
        mode: attempt.mode,
        reason: `Caption engine available via ${attempt.label}.`,
        checkedAt: new Date().toISOString()
      }
    }
    return {
      available: true,
      provider: 'whisper',
      command: attempt.command,
      mode: attempt.mode,
      reason: `Caption engine available via ${attempt.label}.`,
      checkedAt: new Date().toISOString()
    }
  }
  const allowOpenAiApi = /^(1|true|yes)$/i.test(String(process.env.ALLOW_OPENAI_CAPTION_API || '').trim())
  if (allowOpenAiApi && trimToEmpty(process.env.OPENAI_API_KEY)) {
    return {
      available: true,
      provider: 'openai_api',
      command: 'https://api.openai.com/v1/audio/transcriptions',
      mode: 'api',
      reason: 'Caption engine available via OpenAI audio transcription API.',
      checkedAt: new Date().toISOString()
    }
  }
  return {
    available: false,
    provider: 'none',
    command: null,
    mode: null,
    reason: 'Caption engine unavailable: faster-whisper/whisper runtime not found.',
    checkedAt: new Date().toISOString()
  }
}

export const getCaptionEngineStatus = (opts?: { force?: boolean }) => {
  const force = Boolean(opts?.force)
  const now = Date.now()
  if (!force && cachedStatus && now - cacheUpdatedAtMs < CAPTION_ENGINE_CACHE_TTL_MS) {
    return cachedStatus
  }
  cachedStatus = resolveCaptionEngineStatus()
  cacheUpdatedAtMs = now
  return cachedStatus
}
