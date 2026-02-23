import { createServer } from 'http'
import { exec } from 'child_process'
import app from './app'
import { initRealtime } from './realtime'
import { FFMPEG_PATH, formatCommand } from './lib/ffmpeg'

const PORT = Number(process.env.PORT || 4000)
const STARTUP_LOG_LIMIT = 64 * 1024

const server = createServer(app)
initRealtime(server)

const verifyFfmpegOrExit = async () => {
  const args = ['-version']
  const cmd = formatCommand(FFMPEG_PATH, args)
  await new Promise<void>((resolve, reject) => {
    exec(cmd, { maxBuffer: STARTUP_LOG_LIMIT }, (error, stdout, stderr) => {
      const output = `${stdout || ''}${stderr || ''}`.trim()
      if (error) {
        console.error('[startup] Fatal: FFmpeg check failed')
        console.error('[startup] Command:', cmd)
        if (output) console.error(output)
        return reject(error)
      }
      if (!output) {
        console.error('[startup] Fatal: FFmpeg check returned no output')
        console.error('[startup] Command:', cmd)
        return reject(new Error('ffmpeg_version_output_missing'))
      }
      console.log('[startup] FFmpeg version output:')
      console.log(output)
      resolve()
    })
  }).catch(() => {
    process.exit(1)
  })
}

const start = async () => {
  await verifyFfmpegOrExit()
  server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`)
  })
}

void start()
