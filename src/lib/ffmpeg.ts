const windowsAbsolutePathPattern = /^[a-zA-Z]:\\/

const resolveBinaryPath = (configuredPath: string | undefined, fallback: string, label: string) => {
  const value = String(configuredPath || '').trim()
  if (!value) return fallback

  if (process.platform !== 'win32' && windowsAbsolutePathPattern.test(value)) {
    console.warn(`[startup] ${label} path looks Windows-specific on ${process.platform}; using ${fallback}`)
    return fallback
  }

  return value
}

export const FFMPEG_PATH = resolveBinaryPath(
  process.env.FFMPEG_PATH || process.env.FFMPEG_BIN,
  'ffmpeg',
  'FFMPEG'
)
export const FFPROBE_PATH = resolveBinaryPath(
  process.env.FFPROBE_PATH || process.env.FFPROBE_BIN,
  'ffprobe',
  'FFPROBE'
)

export const quoteCliArg = (value: string) => {
  if (value === '') return '""'
  if (/[^\w./:\\-]/.test(value)) {
    return `"${value.replace(/"/g, '\\"')}"`
  }
  return value
}

export const formatCommand = (binaryPath: string, args: string[]) => {
  return [quoteCliArg(binaryPath), ...args.map((arg) => quoteCliArg(arg))].join(' ')
}
