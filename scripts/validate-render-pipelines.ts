import fs from 'fs'
import os from 'os'
import path from 'path'
import { spawnSync } from 'child_process'

const ffmpeg = process.env.FFMPEG_PATH || 'ffmpeg'
const ffprobe = process.env.FFPROBE_PATH || 'ffprobe'

const run = (bin: string, args: string[]) => {
  const result = spawnSync(bin, args, { encoding: 'utf8' })
  if (result.error) {
    throw new Error(`${bin} unavailable: ${result.error.message}`)
  }
  if (result.status === 0) return
  const stderr = String(result.stderr || '').trim()
  const stdout = String(result.stdout || '').trim()
  throw new Error(`${bin} failed (${result.status})\n${stderr || stdout}`)
}

const probeDims = (filePath: string) => {
  const result = spawnSync(
    ffprobe,
    ['-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=width,height', '-of', 'json', filePath],
    { encoding: 'utf8' }
  )
  if (result.status !== 0) {
    throw new Error(`ffprobe failed for ${filePath}`)
  }
  const parsed = JSON.parse(String(result.stdout || '{}'))
  const stream = Array.isArray(parsed?.streams) ? parsed.streams[0] : null
  if (!stream?.width || !stream?.height) {
    throw new Error(`Missing dimensions for ${filePath}`)
  }
  return { width: Number(stream.width), height: Number(stream.height) }
}

const assert = (condition: boolean, message: string) => {
  if (!condition) throw new Error(message)
}

const main = () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'auto-editor-render-validate-'))
  const sourcePath = path.join(tempDir, 'source.mp4')
  const verticalPath = path.join(tempDir, 'vertical.mp4')
  const horizontalPath = path.join(tempDir, 'horizontal.mp4')

  // Synthetic 16:9 source with audio, used for deterministic dimension checks.
  run(ffmpeg, [
    '-y',
    '-hide_banner',
    '-loglevel',
    'error',
    '-f',
    'lavfi',
    '-i',
    'testsrc2=size=1920x1080:rate=30',
    '-f',
    'lavfi',
    '-i',
    'sine=frequency=440:sample_rate=48000',
    '-t',
    '6',
    '-c:v',
    'libx264',
    '-pix_fmt',
    'yuv420p',
    '-c:a',
    'aac',
    sourcePath
  ])

  const ow = 1080
  const oh = 1920
  const th = 760
  const bh = oh - th
  const cx = 200
  const cy = 80
  const cw = 1500
  const ch = 420
  const verticalFilter = [
    '[0:v]trim=start=0:end=6,setpts=PTS-STARTPTS,split=2[vfull][vweb]',
    `[vweb]crop=w=${cw}:h=${ch}:x=${cx}:y=${cy},scale=w=${ow}:h=${th}:force_original_aspect_ratio=increase,crop=w=${ow}:h=${th},setsar=1,format=yuv420p[top]`,
    `[vfull]scale=w=${ow}:h=${bh}:force_original_aspect_ratio=increase,crop=w=${ow}:h=${bh},setsar=1,format=yuv420p[bottom]`,
    '[top][bottom]vstack=inputs=2[outv]',
    '[0:a]atrim=start=0:end=6,asetpts=PTS-STARTPTS,aformat=sample_rates=48000:channel_layouts=stereo[outa]'
  ].join(';')

  run(ffmpeg, [
    '-y',
    '-hide_banner',
    '-loglevel',
    'error',
    '-i',
    sourcePath,
    '-filter_complex',
    verticalFilter,
    '-map',
    '[outv]',
    '-map',
    '[outa]',
    '-c:v',
    'libx264',
    '-pix_fmt',
    'yuv420p',
    '-c:a',
    'aac',
    verticalPath
  ])

  // Horizontal baseline path: preserve original 16:9 framing (non-stacked output).
  run(ffmpeg, [
    '-y',
    '-hide_banner',
    '-loglevel',
    'error',
    '-i',
    sourcePath,
    '-vf',
    'scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,format=yuv420p',
    '-c:v',
    'libx264',
    '-pix_fmt',
    'yuv420p',
    '-c:a',
    'aac',
    horizontalPath
  ])

  const sourceDims = probeDims(sourcePath)
  const verticalDims = probeDims(verticalPath)
  const horizontalDims = probeDims(horizontalPath)

  assert(verticalDims.width === 1080 && verticalDims.height === 1920, `Vertical dims mismatch: ${verticalDims.width}x${verticalDims.height}`)
  assert(!(horizontalDims.width === 1080 && horizontalDims.height === 1920), 'Horizontal render is incorrectly stacked 9:16')
  const sourceAspect = sourceDims.width / sourceDims.height
  const horizontalAspect = horizontalDims.width / horizontalDims.height
  assert(Math.abs(horizontalAspect - sourceAspect) < 0.02, `Horizontal aspect drifted: got ${horizontalAspect.toFixed(4)}, expected ${sourceAspect.toFixed(4)}`)
  assert(horizontalDims.width > horizontalDims.height, `Horizontal output should be landscape: ${horizontalDims.width}x${horizontalDims.height}`)

  console.log('PASS vertical pipeline: 1080x1920 stacked output confirmed')
  console.log('PASS horizontal pipeline: original-aspect non-stacked output confirmed')
  console.log(`Artifacts: ${tempDir}`)
}

try {
  main()
} catch (err: any) {
  console.error('FAIL render pipeline validation')
  console.error(err?.message || err)
  process.exit(1)
}
