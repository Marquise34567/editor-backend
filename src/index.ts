import { createServer } from 'http'
import { exec } from 'child_process'
import app from './app'
import { initRealtime } from './realtime'
import { FFMPEG_PATH, formatCommand } from './lib/ffmpeg'
import { getCaptionEngineStatus } from './lib/captionEngine'
import { initWeeklyReportScheduler } from './services/weeklyReports'
import { warmIpBanCache } from './services/ipBan'

const PORT = Number(process.env.PORT || 4000)
const STARTUP_LOG_LIMIT = 64 * 1024
const REQUIRE_FFMPEG_ON_STARTUP = /^(1|true|yes)$/i.test(
  String(process.env.REQUIRE_FFMPEG_ON_STARTUP || '').trim()
)

const server = createServer(app)
initRealtime(server)

const reportFfmpegStartupIssue = (message: string, cmd: string, output: string) => {
  if (REQUIRE_FFMPEG_ON_STARTUP) {
    console.error(message)
    console.error('[startup] Command:', cmd)
    if (output) console.error(output)
    console.error('[startup] REQUIRE_FFMPEG_ON_STARTUP is set, exiting')
    return false
  }

  console.warn(message)
  console.warn('[startup] Command:', cmd)
  if (output) console.warn(output)
  console.warn('[startup] Continuing without FFmpeg; processing routes may fail until FFmpeg is available')
  return true
}

const verifyFfmpegOnStartup = async () => {
  const args = ['-version']
  const cmd = formatCommand(FFMPEG_PATH, args)
  return new Promise<boolean>((resolve) => {
    exec(cmd, { maxBuffer: STARTUP_LOG_LIMIT }, (error, stdout, stderr) => {
      const output = `${stdout || ''}${stderr || ''}`.trim()
      if (error) {
        return resolve(reportFfmpegStartupIssue('[startup] FFmpeg check failed', cmd, output))
      }
      if (!output) {
        return resolve(
          reportFfmpegStartupIssue('[startup] FFmpeg check returned no output', cmd, output)
        )
      }
      console.log('[startup] FFmpeg version output:')
      console.log(output)
      resolve(true)
    })
  })
}

const start = async () => {
  const ffmpegOk = await verifyFfmpegOnStartup()
  if (!ffmpegOk) process.exit(1)
  const captions = getCaptionEngineStatus({ force: true })
  if (captions.available) {
    console.log(`[startup] Caption engine ready: ${captions.provider} (${captions.command})`)
  } else {
    console.warn(`[startup] Caption engine unavailable: ${captions.reason}`)
  }
  await warmIpBanCache().catch(() => null)
  initWeeklyReportScheduler()
  server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`)
  })
}

void start()
