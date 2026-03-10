import { createServer } from 'http'
import { exec, spawn } from 'child_process'
import path from 'path'
import app from './app'
import { initRealtime } from './realtime'
import { FFMPEG_PATH, formatCommand } from './lib/ffmpeg'
import { getCaptionEngineStatus } from './lib/captionEngine'
import { initWeeklyReportScheduler } from './services/weeklyReports'
import { initDailyEngagementScheduler } from './services/dailyEngagement'
import { warmIpBanCache } from './services/ipBan'

const PORT = Number(process.env.PORT || 4000)
const STARTUP_LOG_LIMIT = 64 * 1024
const STARTUP_CHECK_TIMEOUT_MS = 5_000
const REQUIRE_FFMPEG_ON_STARTUP = /^(1|true|yes)$/i.test(
  String(process.env.REQUIRE_FFMPEG_ON_STARTUP || '').trim()
)
const RAW_CAPTION_RUNTIME_INSTALL_TOGGLE = String(
  process.env.INSTALL_CAPTION_RUNTIME_ON_STARTUP ??
  process.env.INSTALL_CAPTION_RUNTIME ??
  ''
).trim()
const SHOULD_INSTALL_CAPTION_RUNTIME_ON_STARTUP = !/^(0|false|no|off)$/i.test(
  RAW_CAPTION_RUNTIME_INSTALL_TOGGLE
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
    exec(cmd, { maxBuffer: STARTUP_LOG_LIMIT, timeout: STARTUP_CHECK_TIMEOUT_MS }, (error, stdout, stderr) => {
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

const ensureCaptionRuntimeOnStartup = () => {
  if (!SHOULD_INSTALL_CAPTION_RUNTIME_ON_STARTUP) return Promise.resolve(true)
  const installerPath = path.resolve(process.cwd(), 'scripts/install-caption-runtime.js')
  console.log('[startup] Ensuring caption runtime in background')
  return new Promise<boolean>((resolve) => {
    const child = spawn(process.execPath, [installerPath], {
      env: process.env,
      stdio: 'inherit',
      windowsHide: true
    })
    child.once('error', (error) => {
      console.error('[startup] caption runtime installer failed to start', error)
      resolve(false)
    })
    child.once('exit', (code) => {
      if (code !== 0) {
        console.error(`[startup] caption runtime installer exited with status ${code}`)
        return resolve(false)
      }
      resolve(true)
    })
  })
}

const runStartupWarmups = async () => {
  const ffmpegOk = await verifyFfmpegOnStartup()
  if (!ffmpegOk) {
    if (REQUIRE_FFMPEG_ON_STARTUP) {
      console.error('[startup] FFmpeg is required; shutting down after failed startup check')
      server.close(() => process.exit(1))
    }
    return
  }
  await ensureCaptionRuntimeOnStartup().catch((error) => {
    console.error('[startup] caption runtime warmup failed', error)
    return false
  })
  const captions = getCaptionEngineStatus({ force: true })
  if (captions.available) {
    console.log(`[startup] Caption engine ready: ${captions.provider} (${captions.command})`)
  } else {
    console.warn(`[startup] Caption engine unavailable: ${captions.reason}`)
  }
  await warmIpBanCache().catch(() => null)
  initWeeklyReportScheduler()
  initDailyEngagementScheduler()
}

const start = async () => {
  server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`)
    void runStartupWarmups()
  })
}

void start()
