export const FFMPEG_PATH = process.env.FFMPEG_PATH || process.env.FFMPEG_BIN || 'ffmpeg'
export const FFPROBE_PATH = process.env.FFPROBE_PATH || process.env.FFPROBE_BIN || 'ffprobe'

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
