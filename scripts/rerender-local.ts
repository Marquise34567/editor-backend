import fs from 'fs'
import os from 'os'
import path from 'path'
import { spawnSync } from 'child_process'
import { __retentionTestUtils } from '../src/routes/jobs'

const ffmpegBin = process.env.FFMPEG_PATH || 'ffmpeg'
const ffprobeBin = process.env.FFPROBE_PATH || 'ffprobe'
const AUTO_HOOK_MIN_SECONDS = 5
const AUTO_HOOK_MAX_SECONDS = 8
const AUTO_HOOK_LOCK_SECONDS = 8
const LOCAL_RERENDER_DEFAULTS = {
  aggressionLevel: 'medium',
  strategyProfile: 'balanced',
  longFormPreset: 'balanced',
  longFormAggression: 42,
  longFormClarityVsSpeed: 72,
  maxCuts: 22,
  autoTranscribe: true
} as const

const clamp = (value: number, min: number, max: number) => {
  if (!Number.isFinite(value)) return min
  return Math.min(max, Math.max(min, value))
}

type CliOptions = {
  input: string
  output: string
  aggressionLevel?: string
  strategyProfile?: string
  editorMode?: string
  longFormPreset?: string
  longFormAggression?: number
  longFormClarityVsSpeed?: number
  maxCuts?: number
  autoTranscribe: boolean
  transcriptSrtPath?: string
}

const parseBooleanOption = (value: string | undefined, fallback: boolean) => {
  if (value === undefined) return fallback
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return fallback
  if (['1', 'true', 'yes', 'on', 'enabled'].includes(normalized)) return true
  if (['0', 'false', 'no', 'off', 'disabled'].includes(normalized)) return false
  return fallback
}

const parseCli = (): CliOptions => {
  const args = process.argv.slice(2)
  const positionals: string[] = []
  const options: Record<string, string> = {}
  for (let i = 0; i < args.length; i += 1) {
    const token = String(args[i] || '')
    if (!token.startsWith('--')) {
      positionals.push(token)
      continue
    }
    const key = token.replace(/^--+/, '').trim()
    const next = String(args[i + 1] || '')
    if (!key) continue
    if (!next || next.startsWith('--')) {
      options[key] = 'true'
      continue
    }
    options[key] = next
    i += 1
  }
  if (!positionals[0]) {
    throw new Error(
      'usage: ts-node scripts/rerender-local.ts "<input.mp4>" "<output.mp4>" ' +
      '[--aggression medium] [--strategy balanced] [--longFormPreset balanced] ' +
      '[--longFormAggression 42] [--longFormClarity 72] [--maxCuts 22] ' +
      '[--transcribe true] [--transcriptSrt "<captions.srt>"]'
    )
  }
  const input = path.resolve(positionals[0])
  const output = path.resolve(positionals[1] || `${input.replace(/\.[^/.]+$/, '')}_rerender.mp4`)
  const longFormAggression = Number(options.longFormAggression ?? options.longformAggression ?? '')
  const longFormClarityVsSpeed = Number(options.longFormClarity ?? options.longformClarity ?? '')
  const maxCuts = Number(options.maxCuts ?? options.maxcuts ?? '')
  const autoTranscribe = parseBooleanOption(options.transcribe, LOCAL_RERENDER_DEFAULTS.autoTranscribe)
  const transcriptSrtPathRaw = String(options.transcriptSrt ?? options.transcript_srt ?? '').trim()
  return {
    input,
    output,
    aggressionLevel: options.aggression || LOCAL_RERENDER_DEFAULTS.aggressionLevel,
    strategyProfile: options.strategy || LOCAL_RERENDER_DEFAULTS.strategyProfile,
    editorMode: options.editorMode,
    longFormPreset: options.longFormPreset || LOCAL_RERENDER_DEFAULTS.longFormPreset,
    longFormAggression: Number.isFinite(longFormAggression)
      ? longFormAggression
      : LOCAL_RERENDER_DEFAULTS.longFormAggression,
    longFormClarityVsSpeed: Number.isFinite(longFormClarityVsSpeed)
      ? longFormClarityVsSpeed
      : LOCAL_RERENDER_DEFAULTS.longFormClarityVsSpeed,
    maxCuts: Number.isFinite(maxCuts) ? maxCuts : LOCAL_RERENDER_DEFAULTS.maxCuts,
    autoTranscribe,
    transcriptSrtPath: transcriptSrtPathRaw ? path.resolve(transcriptSrtPathRaw) : undefined
  }
}

const run = (bin: string, args: string[]) => {
  const result = spawnSync(bin, args, { encoding: 'utf8', maxBuffer: 1024 * 1024 * 8 })
  if (result.error) {
    throw new Error(`${bin} unavailable: ${result.error.message}`)
  }
  if (result.status !== 0) {
    throw new Error(`${bin} failed (${result.status}): ${String(result.stderr || result.stdout || '').trim()}`)
  }
  return result
}

const probeDurationSeconds = (filePath: string) => {
  const result = run(ffprobeBin, [
    '-v', 'error',
    '-show_entries', 'format=duration',
    '-of', 'default=noprint_wrappers=1:nokey=1',
    filePath
  ])
  const value = Number(String(result.stdout || '').trim())
  if (!Number.isFinite(value) || value <= 0) throw new Error(`unable to probe duration: ${filePath}`)
  return value
}

const buildAtempoChain = (speed: number) => {
  const safe = Number.isFinite(speed) && speed > 0 ? speed : 1
  if (Math.abs(safe - 1) < 0.001) return ''
  const parts: string[] = []
  let remaining = safe
  while (remaining > 2.0 + 1e-6) {
    parts.push('atempo=2')
    remaining /= 2
  }
  while (remaining < 0.5 - 1e-6) {
    parts.push('atempo=0.5')
    remaining /= 0.5
  }
  parts.push(`atempo=${remaining.toFixed(5)}`)
  return parts.join(',')
}

const stabilizeSegmentsForStoryRender = (
  segments: Array<{ start: number; end: number; speed: number }>,
  style: string | null | undefined
) => {
  if (!Array.isArray(segments) || !segments.length) return [] as Array<{ start: number; end: number; speed: number }>
  const normalizedStyle = String(style || '').toLowerCase()
  const isNarrative = normalizedStyle === 'story' || normalizedStyle === 'vlog'
  if (!isNarrative) return segments
  const speedCap = normalizedStyle === 'vlog' ? 1.2 : 1.18
  const base = segments
    .map((segment) => ({
      start: Number(segment.start),
      end: Number(segment.end),
      speed: Number(clamp(Number(segment.speed || 1), 1, speedCap).toFixed(3))
    }))
    .sort((a, b) => a.start - b.start || a.end - b.end)
  const merged: Array<{ start: number; end: number; speed: number }> = []
  for (const segment of base) {
    const duration = segment.end - segment.start
    if (!Number.isFinite(duration) || duration <= 0.08) continue
    const prev = merged[merged.length - 1]
    if (!prev) {
      merged.push({ ...segment })
      continue
    }
    const gap = segment.start - prev.end
    const prevDuration = prev.end - prev.start
    if (gap >= 0 && gap <= 0.32 && (prevDuration < 1.45 || duration < 1.45)) {
      prev.end = Number(Math.max(prev.end, segment.end).toFixed(3))
      prev.speed = Number(clamp(((prev.speed || 1) + (segment.speed || 1)) / 2, 1, speedCap).toFixed(3))
      continue
    }
    merged.push({ ...segment })
  }
  const filtered = merged.filter((segment) => (segment.end - segment.start) >= 0.62)
  return filtered.length ? filtered : base
}

const capNarrativeSegmentCount = (
  segments: Array<{ start: number; end: number; speed: number }>,
  sourceDurationSeconds: number,
  style: string | null | undefined
) => {
  const normalizedStyle = String(style || '').toLowerCase()
  const isNarrative = normalizedStyle === 'story' || normalizedStyle === 'vlog'
  if (!isNarrative || !Array.isArray(segments) || segments.length <= 1) return segments
  const targetCount = sourceDurationSeconds >= 220 ? 30 : sourceDurationSeconds >= 150 ? 24 : 20
  if (segments.length <= targetCount) return segments
  const out = segments.map((segment) => ({ ...segment }))
  const maxMergedDuration = normalizedStyle === 'vlog' ? 11.5 : 10.5
  while (out.length > targetCount) {
    let bestIndex = -1
    let bestCost = Number.POSITIVE_INFINITY
    for (let index = 0; index < out.length - 1; index += 1) {
      const left = out[index]
      const right = out[index + 1]
      const gap = Math.max(0, right.start - left.end)
      const combinedDuration = right.end - left.start
      if (!Number.isFinite(combinedDuration) || combinedDuration > maxMergedDuration) continue
      const speedDelta = Math.abs((left.speed || 1) - (right.speed || 1))
      const cost = gap * 1.35 + speedDelta * 0.65 + combinedDuration * 0.04
      if (cost < bestCost) {
        bestCost = cost
        bestIndex = index
      }
    }
    if (bestIndex < 0) break
    const left = out[bestIndex]
    const right = out[bestIndex + 1]
    const merged = {
      start: Number(left.start.toFixed(3)),
      end: Number(Math.max(left.end, right.end).toFixed(3)),
      speed: Number(clamp(((left.speed || 1) + (right.speed || 1)) / 2, 1, normalizedStyle === 'vlog' ? 1.2 : 1.18).toFixed(3))
    }
    out.splice(bestIndex, 2, merged)
  }
  return out
}

const capSegmentsToMaxCuts = (
  segments: Array<{ start: number; end: number; speed: number }>,
  maxCuts: number | undefined
) => {
  if (!Array.isArray(segments) || !segments.length) return [] as Array<{ start: number; end: number; speed: number }>
  const parsedMaxCuts = Number(maxCuts)
  if (!Number.isFinite(parsedMaxCuts) || parsedMaxCuts < 1) return segments
  const maxSegments = Math.max(2, Math.round(parsedMaxCuts) + 1)
  if (segments.length <= maxSegments) return segments
  const out = segments
    .map((segment) => ({ ...segment }))
    .sort((a, b) => a.start - b.start || a.end - b.end)
  while (out.length > maxSegments) {
    let bestIndex = -1
    let bestCost = Number.POSITIVE_INFINITY
    for (let index = 0; index < out.length - 1; index += 1) {
      const left = out[index]
      const right = out[index + 1]
      const gap = Math.max(0, right.start - left.end)
      const mergedDuration = Math.max(0.1, right.end - left.start)
      const speedDelta = Math.abs((left.speed || 1) - (right.speed || 1))
      const cost = gap * 1.2 + speedDelta * 0.6 + mergedDuration * 0.05
      if (cost < bestCost) {
        bestCost = cost
        bestIndex = index
      }
    }
    if (bestIndex < 0) break
    const left = out[bestIndex]
    const right = out[bestIndex + 1]
    out.splice(bestIndex, 2, {
      start: Number(left.start.toFixed(3)),
      end: Number(Math.max(left.end, right.end).toFixed(3)),
      speed: Number(clamp(((left.speed || 1) + (right.speed || 1)) / 2, 1, 1.2).toFixed(3))
    })
  }
  return out
}

const main = async () => {
  const cli = parseCli()
  if (!fs.existsSync(cli.input)) throw new Error(`input_missing:${cli.input}`)
  fs.mkdirSync(path.dirname(cli.output), { recursive: true })
  const sourceDuration = probeDurationSeconds(cli.input)

  const planner = (__retentionTestUtils as any).buildEditPlanForTest as (args: any) => Promise<any>
  const plan = await planner({
    filePath: cli.input,
    aggressionLevel: cli.aggressionLevel || 'medium',
    strategyProfile: cli.strategyProfile,
    editorMode: cli.editorMode,
    longFormPreset: cli.longFormPreset,
    longFormAggression: cli.longFormAggression,
    longFormClarityVsSpeed: cli.longFormClarityVsSpeed,
    maxCuts: cli.maxCuts,
    autoTranscribe: cli.autoTranscribe,
    transcriptSrtPath: cli.transcriptSrtPath
  })

  const moveHookToStart = (__retentionTestUtils as any).buildTimelineWithHookAtStartForTest as
    | ((segments: any[], hook: any) => any[])
    | undefined
  const baseSegments = Array.isArray(plan?.segments) ? plan.segments : []
  const lockedHookDuration = Number(clamp(
    AUTO_HOOK_LOCK_SECONDS,
    AUTO_HOOK_MIN_SECONDS,
    Math.min(AUTO_HOOK_MAX_SECONDS, sourceDuration)
  ).toFixed(3))
  const normalizedHook = (plan?.hook && Number.isFinite(Number(plan.hook.start)))
    ? {
      ...plan.hook,
      start: Number(plan.hook.start),
      duration: lockedHookDuration
    }
    : null
  if (normalizedHook) {
    const maxStart = Math.max(0, sourceDuration - normalizedHook.duration)
    normalizedHook.start = Number(clamp(normalizedHook.start, 0, maxStart).toFixed(3))
    plan.hook = {
      ...plan.hook,
      start: normalizedHook.start,
      duration: normalizedHook.duration
    }
  }
  const timelineWithHookAtStart = (
    normalizedHook &&
    typeof moveHookToStart === 'function'
  )
    ? moveHookToStart(baseSegments, normalizedHook)
    : baseSegments
  const preppedSegments = timelineWithHookAtStart
    .map((segment: any) => ({
      start: Number(segment?.start),
      end: Number(segment?.end),
      speed: Number(segment?.speed && segment.speed > 0 ? segment.speed : 1)
    }))
    .filter((segment: any) => (
      Number.isFinite(segment.start) &&
      Number.isFinite(segment.end) &&
      Number.isFinite(segment.speed) &&
      segment.end - segment.start > 0.12
    ))
    .sort((a: any, b: any) => a.start - b.start || a.end - b.end)
  const segments = stabilizeSegmentsForStoryRender(
    preppedSegments,
    String(plan?.styleProfile?.style || '')
  )
  const cappedSegments = capNarrativeSegmentCount(
    segments,
    sourceDuration,
    String(plan?.styleProfile?.style || '')
  )
  const densityCappedSegments = capSegmentsToMaxCuts(cappedSegments, cli.maxCuts)

  if (!densityCappedSegments.length) throw new Error('plan_has_no_segments')

  const filterParts: string[] = []
  for (let idx = 0; idx < densityCappedSegments.length; idx += 1) {
    const segment = densityCappedSegments[idx]
    const start = segment.start.toFixed(3)
    const end = segment.end.toFixed(3)
    const speed = Number(segment.speed || 1)
    const videoPtsExpr = Math.abs(speed - 1) < 0.001
      ? 'PTS-STARTPTS'
      : `(PTS-STARTPTS)/${speed.toFixed(6)}`
    filterParts.push(
      `[0:v]trim=start=${start}:end=${end},setpts=${videoPtsExpr},` +
      'scale=1280:720:force_original_aspect_ratio=decrease,' +
      'pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1,format=yuv420p' +
      `[v${idx}]`
    )

    const outDuration = (segment.end - segment.start) / Math.max(speed, 0.001)
    const audioFilters = [
      `atrim=start=${start}:end=${end}`,
      'asetpts=PTS-STARTPTS'
    ]
    const atempo = buildAtempoChain(speed)
    if (atempo) audioFilters.push(atempo)
    if (outDuration > 0.12) {
      const fadeDuration = Math.min(0.035, outDuration / 3)
      const fadeOutStart = Math.max(0, outDuration - fadeDuration)
      audioFilters.push(`afade=t=in:st=0:d=${fadeDuration.toFixed(3)}`)
      audioFilters.push(`afade=t=out:st=${fadeOutStart.toFixed(3)}:d=${fadeDuration.toFixed(3)}`)
    }
    audioFilters.push('aformat=sample_rates=48000:channel_layouts=stereo')
    filterParts.push(`[0:a]${audioFilters.join(',')}[a${idx}]`)
  }

  const concatInputs = densityCappedSegments
    .map((_, idx) => `[v${idx}][a${idx}]`)
    .join('')
  filterParts.push(`${concatInputs}concat=n=${densityCappedSegments.length}:v=1:a=1[outv][outa]`)
  const filterComplex = filterParts.join(';')
  const filterScriptPath = path.join(
    os.tmpdir(),
    `rerender-local-filter-${Date.now()}-${Math.random().toString(36).slice(2, 9)}.txt`
  )
  fs.writeFileSync(filterScriptPath, filterComplex, 'utf8')
  try {
    run(ffmpegBin, [
      '-y',
      '-hide_banner',
      '-loglevel', 'error',
      '-i', cli.input,
      '-filter_complex_script', filterScriptPath,
      '-map', '[outv]',
      '-map', '[outa]',
      '-movflags', '+faststart',
      '-c:v', 'libx264',
      '-preset', 'superfast',
      '-crf', '22',
      '-pix_fmt', 'yuv420p',
      '-c:a', 'aac',
      '-b:a', '192k',
      '-ar', '48000',
      '-ac', '2',
      cli.output
    ])
  } finally {
    try {
      fs.unlinkSync(filterScriptPath)
    } catch {
      // ignore
    }
  }

  const outputDuration = probeDurationSeconds(cli.output)
  const outputPlanPath = `${cli.output}.plan.json`
  fs.writeFileSync(outputPlanPath, JSON.stringify({
    generatedAt: new Date().toISOString(),
    input: cli.input,
    output: cli.output,
    sourceDurationSeconds: Number(sourceDuration.toFixed(3)),
    outputDurationSeconds: Number(outputDuration.toFixed(3)),
    segmentCount: densityCappedSegments.length,
    hook: plan?.hook || null,
    transcriptSignals: plan?.transcriptSignals || null,
    buildOptions: {
      aggressionLevel: cli.aggressionLevel || 'medium',
      strategyProfile: cli.strategyProfile || null,
      editorMode: cli.editorMode || null,
      longFormPreset: cli.longFormPreset || null,
      longFormAggression: cli.longFormAggression ?? null,
      longFormClarityVsSpeed: cli.longFormClarityVsSpeed ?? null,
      maxCuts: cli.maxCuts ?? null,
      autoTranscribe: cli.autoTranscribe,
      transcriptSrtPath: cli.transcriptSrtPath || null
    }
  }, null, 2))

  console.log(`INPUT: ${cli.input}`)
  console.log(`OUTPUT: ${cli.output}`)
  console.log(`SOURCE_SECONDS: ${sourceDuration.toFixed(3)}`)
  console.log(`OUTPUT_SECONDS: ${outputDuration.toFixed(3)}`)
  console.log(`SEGMENTS: ${densityCappedSegments.length}`)
  console.log(`TRANSCRIPT_CUES: ${Number(plan?.transcriptSignals?.cueCount || 0)}`)
  console.log(`HOOK: ${JSON.stringify(plan?.hook || null)}`)
  console.log(`PLAN_JSON: ${outputPlanPath}`)
}

main().catch((error: any) => {
  console.error(error?.message || error)
  process.exit(1)
})
