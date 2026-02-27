import express from 'express'
import path from 'path'
import { prisma } from '../db/prisma'
import { getUserPlan } from '../services/plans'
import { isPaidTier } from '../shared/planConfig'
import { resolveDevAdminAccess } from '../lib/devAccounts'
import { buildVideoFeedbackAnalysis, type VideoFeedbackInput } from '../services/videoFeedback'

const router = express.Router()

type RealtimePredictionPotential = 'low' | 'moderate' | 'high'
type RealtimePredictionTrend = 'rising' | 'steady' | 'falling'

type RealtimePredictionVideo = {
  videoId: string
  uploadKey: string
  jobId: string
  sourceType: 'classic' | 'vibecut'
  title: string
  status: string
  createdAt: string
  updatedAt: string
  durationSeconds: number | null
  prediction: {
    score: number | null
    confidencePercent: number
    potential: RealtimePredictionPotential
    trend: RealtimePredictionTrend
    predictedCompletionPercent: number | null
    expectedLiftPercent: number | null
    hookStrengthPercent: number | null
    pacingScorePercent: number | null
    summary: string
    reasoning: string[]
    updatedAt: string | null
  }
}

const asNumber = (value: any, fallback = 0) => {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))
const clamp01 = (value: number) => clamp(value, 0, 1)

const parseBoolean = (value: any) => {
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0
  if (typeof value !== 'string') return false
  const normalized = value.trim().toLowerCase()
  return normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on' || normalized === 'enabled'
}

const parseRatio = (value: any): number | null => {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return null
  if (numeric >= 0 && numeric <= 1) return clamp01(numeric)
  return clamp01(numeric / 100)
}

const parsePercent = (value: any): number | null => {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return null
  if (numeric >= 0 && numeric <= 1) return clamp(numeric * 100, 0, 100)
  return clamp(numeric, 0, 100)
}

const parsePercentFromText = (value: any): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) return clamp(value, 0, 100)
  const text = String(value || '')
  const directMatch = text.match(/(-?\d+(?:\.\d+)?)\s*%/)
  if (directMatch) return clamp(Number(directMatch[1]), 0, 100)
  const plainMatch = text.match(/(-?\d+(?:\.\d+)?)/)
  if (!plainMatch) return null
  const numeric = Number(plainMatch[1])
  if (!Number.isFinite(numeric)) return null
  return numeric <= 1 ? clamp(numeric * 100, 0, 100) : clamp(numeric, 0, 100)
}

const parseTrendBoost = (value: any): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    if (value <= 0) return null
    if (value <= 5) return value
    return clamp(value / 100, 0.5, 3)
  }

  const text = String(value || '').trim()
  if (!text) return null

  const xMatch = text.match(/(-?\d+(?:\.\d+)?)\s*x/i)
  if (xMatch) {
    const parsed = Number(xMatch[1])
    if (!Number.isFinite(parsed) || parsed <= 0) return null
    return parsed
  }

  const percentMatch = text.match(/(-?\d+(?:\.\d+)?)\s*%/)
  if (percentMatch) {
    const parsed = Number(percentMatch[1])
    if (!Number.isFinite(parsed)) return null
    return clamp(1 + (parsed / 100), 0.5, 3)
  }

  const numeric = Number(text)
  if (!Number.isFinite(numeric) || numeric <= 0) return null
  if (numeric <= 5) return numeric
  return clamp(numeric / 100, 0.5, 3)
}

const parseSourceType = (value: any): 'classic' | 'vibecut' | null => {
  const normalized = String(value || '').trim().toLowerCase()
  if (normalized === 'classic' || normalized === 'job' || normalized === 'jobs') return 'classic'
  if (normalized === 'vibecut' || normalized === 'vibe') return 'vibecut'
  return null
}

const extractNoiseReductionLevel = (filters: string[]) => {
  for (const filter of filters) {
    const match = filter.match(/afftdn[^\s,]*nf=([-+]?\d+(?:\.\d+)?)/i)
    if (match) {
      const nf = Math.abs(Number(match[1]))
      if (Number.isFinite(nf)) {
        return clamp(Math.round(((nf - 14) / 18) * 100), 0, 100)
      }
    }
  }
  if (filters.some((entry) => /arnndn|denoise/i.test(entry))) return 78
  return 0
}

const parseAudioFiltersFromCommands = (commands: string[]) => {
  const collected: string[] = []
  for (const rawCommand of commands) {
    const command = String(rawCommand || '')
    const index = command.indexOf(' -af ')
    if (index === -1) continue
    let remainder = command.slice(index + 5).trim()
    const stop = remainder.search(/\s-(?:c:a|an|movflags|b:a|shortest|map|preset|crf|vf|f|y)\b/i)
    if (stop > 0) remainder = remainder.slice(0, stop).trim()
    remainder = remainder.replace(/^['"]|['"]$/g, '')
    for (const part of remainder.split(',')) {
      const cleaned = String(part || '').trim()
      if (cleaned) collected.push(cleaned)
    }
  }
  return Array.from(new Set(collected))
}

const inferKeptDurationFromCommands = (commands: string[], rawDurationSeconds: number) => {
  const ranges = new Set<string>()

  for (const rawCommand of commands) {
    const command = String(rawCommand || '')

    for (const match of command.matchAll(/(?:a)?trim=start=([0-9.]+):end=([0-9.]+)/gi)) {
      const start = Number(match[1])
      const end = Number(match[2])
      if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) continue
      ranges.add(`${start.toFixed(3)}:${end.toFixed(3)}`)
    }

    const ssMatch = command.match(/-ss\s+([0-9.]+)/i)
    const toMatch = command.match(/-to\s+([0-9.]+)/i)
    if (ssMatch && toMatch && /clip_/i.test(command)) {
      const start = Number(ssMatch[1])
      const end = Number(toMatch[1])
      if (Number.isFinite(start) && Number.isFinite(end) && end > start) {
        ranges.add(`${start.toFixed(3)}:${end.toFixed(3)}`)
      }
    }
  }

  if (!ranges.size) return null
  const kept = Array.from(ranges).reduce((sum, range) => {
    const [startRaw, endRaw] = range.split(':')
    const start = Number(startRaw)
    const end = Number(endRaw)
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) return sum
    return sum + (end - start)
  }, 0)

  if (!Number.isFinite(kept) || kept <= 0) return null
  return clamp(kept, 1, Math.max(1, rawDurationSeconds))
}

const readObject = (value: any) =>
  value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, any>)
    : {}

const parseSignedPercent = (value: any): number | null => {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return null
  if (numeric >= -1 && numeric <= 1) return Number(clamp(numeric * 100, -100, 100).toFixed(2))
  return Number(clamp(numeric, -100, 100).toFixed(2))
}

const pickFirstNumber = (values: Array<number | null | undefined>) => {
  for (const value of values) {
    if (typeof value === 'number' && Number.isFinite(value)) return value
  }
  return null
}

const normalizeRealtimeStatus = (value: any) => {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return 'queued'
  if (normalized === 'completed') return 'ready'
  return normalized
}

const resolveConfidencePercentFromStatus = ({
  status,
  hasScore
}: {
  status: string
  hasScore: boolean
}) => {
  if (status === 'ready') return hasScore ? 84 : 72
  if (status === 'rendering' || status === 'retention' || status === 'audio' || status === 'subtitling') return hasScore ? 74 : 60
  if (status === 'story' || status === 'pacing' || status === 'cutting' || status === 'hooking' || status === 'analyzing') return hasScore ? 64 : 48
  if (status === 'failed') return hasScore ? 40 : 26
  return hasScore ? 50 : 34
}

const resolvePotentialFromScore = (score: number | null): RealtimePredictionPotential => {
  if (score === null) return 'moderate'
  if (score >= 72) return 'high'
  if (score >= 46) return 'moderate'
  return 'low'
}

const resolveTrendFromLift = (liftPercent: number | null): RealtimePredictionTrend => {
  if (liftPercent === null) return 'steady'
  if (liftPercent >= 1.5) return 'rising'
  if (liftPercent <= -1.5) return 'falling'
  return 'steady'
}

const buildClassicRealtimePrediction = (job: any): RealtimePredictionVideo => {
  const analysis = readObject(job?.analysis)
  const metadata = readObject(analysis?.metadata_summary)
  const retention = readObject(metadata?.retention)
  const automation = readObject(retention?.automation)
  const qualityGate = readObject(metadata?.qualityGate)
  const qualityScores = readObject(qualityGate?.scores)
  const timeline = readObject(metadata?.timeline)
  const pacing = readObject(metadata?.pacing)
  const clips = Array.isArray(metadata?.clips) ? metadata.clips : []

  const clipPredictions = clips
    .map((clip: any) => parsePercent(clip?.predictedCompletion))
    .filter((value: number | null): value is number => value !== null)
  const clipAverage = clipPredictions.length
    ? Number((clipPredictions.reduce((sum, value) => sum + value, 0) / clipPredictions.length).toFixed(2))
    : null

  const score = pickFirstNumber([
    parsePercent(retention?.afterScore),
    parsePercent(retention?.score),
    parsePercent(job?.retentionScore),
    parsePercent(analysis?.retention_score_after),
    parsePercent(analysis?.retentionScore)
  ])
  const predictedCompletion = pickFirstNumber([
    parsePercent(metadata?.predictedAverage),
    clipAverage
  ])
  const expectedLift = pickFirstNumber([
    parseSignedPercent(automation?.expectedLift),
    parseSignedPercent(retention?.delta)
  ])
  const hookStrength = pickFirstNumber([
    parsePercent(qualityScores?.hook),
    parsePercent(analysis?.hook_score)
  ])
  const pacingScore = pickFirstNumber([
    parsePercent(qualityScores?.pacing),
    parsePercent(analysis?.retention_judge?.pacing_score),
    parsePercent(pacing?.withinTargetRatio !== undefined ? Number(pacing.withinTargetRatio) * 100 : null)
  ])
  const confidencePercent = pickFirstNumber([
    parsePercent(automation?.confidence),
    resolveConfidencePercentFromStatus({
      status: normalizeRealtimeStatus(job?.status),
      hasScore: score !== null || predictedCompletion !== null
    })
  ]) || 40

  const reasoning = Array.from(
    new Set(
      [
        ...((Array.isArray(retention?.improvements) ? retention.improvements : []) as string[]),
        ...((Array.isArray(automation?.reasons) ? automation.reasons : []) as string[]),
      ]
        .map((entry) => String(entry || '').trim())
        .filter(Boolean)
    )
  ).slice(0, 4)

  const scoreLabel = score !== null ? `${Math.round(score)}%` : 'n/a'
  const completionLabel = predictedCompletion !== null ? `${Math.round(predictedCompletion)}%` : null
  const liftLabel = expectedLift !== null ? `${expectedLift > 0 ? '+' : ''}${expectedLift.toFixed(1)}%` : null
  const summary = reasoning[0] || (
    completionLabel
      ? `Projected score ${scoreLabel}, completion ${completionLabel}${liftLabel ? `, expected lift ${liftLabel}` : ''}.`
      : `Projected score ${scoreLabel}${liftLabel ? ` with expected lift ${liftLabel}` : ''}.`
  )

  const duration = pickFirstNumber([
    asNumber(job?.inputDurationSeconds, Number.NaN),
    asNumber(timeline?.sourceDurationSeconds, Number.NaN),
    asNumber(analysis?.duration, Number.NaN)
  ])

  return {
    videoId: `classic:${job.id}`,
    uploadKey: String(job?.inputPath || job?.id || ''),
    jobId: String(job?.id || ''),
    sourceType: 'classic',
    title: path.basename(String(job?.inputPath || `job-${job?.id || 'video'}`)),
    status: normalizeRealtimeStatus(job?.status),
    createdAt: new Date(job?.createdAt || Date.now()).toISOString(),
    updatedAt: new Date(job?.updatedAt || Date.now()).toISOString(),
    durationSeconds: duration !== null ? Number(duration) : null,
    prediction: {
      score,
      confidencePercent: Number(clamp(confidencePercent, 0, 100).toFixed(2)),
      potential: resolvePotentialFromScore(score),
      trend: resolveTrendFromLift(expectedLift),
      predictedCompletionPercent: predictedCompletion,
      expectedLiftPercent: expectedLift,
      hookStrengthPercent: hookStrength,
      pacingScorePercent: pacingScore,
      summary,
      reasoning,
      updatedAt: metadata?.generatedAt ? String(metadata.generatedAt) : new Date(job?.updatedAt || Date.now()).toISOString()
    }
  }
}

const buildVibecutRealtimePrediction = (job: any): RealtimePredictionVideo => {
  const retention = readObject(job?.retention)
  const points = Array.isArray(retention?.points) ? retention.points : []
  const avgScore = points.length
    ? Number((points.reduce((sum: number, point: any) => sum + asNumber(point?.watchedPct, 0), 0) / points.length).toFixed(2))
    : null
  const firstPoint = points[0] || null
  const hookStrength = firstPoint ? parsePercent(firstPoint?.watchedPct) : null
  const status = normalizeRealtimeStatus(job?.status)
  const confidencePercent = resolveConfidencePercentFromStatus({
    status,
    hasScore: avgScore !== null
  })

  const summaryText = String(retention?.summary || '').trim()
  const reasoning = summaryText ? [summaryText] : []
  const summary = summaryText || (
    avgScore !== null
      ? `Projected completion around ${Math.round(avgScore)}% for this VibeCut export.`
      : `Realtime prediction is still collecting clip-level data for this VibeCut export.`
  )

  const upload = readObject(job?.upload)
  const uploadMetadata = readObject(upload?.metadata)
  const duration = Number.isFinite(Number(uploadMetadata?.duration))
    ? Number(uploadMetadata.duration)
    : null

  return {
    videoId: `vibecut:${job.id}`,
    uploadKey: String(job?.uploadId || job?.id || ''),
    jobId: String(job?.id || ''),
    sourceType: 'vibecut',
    title: String(job?.fileName || upload?.fileName || `vibecut-${job?.id || 'video'}`),
    status,
    createdAt: new Date(job?.createdAt || Date.now()).toISOString(),
    updatedAt: new Date(job?.updatedAt || Date.now()).toISOString(),
    durationSeconds: duration,
    prediction: {
      score: avgScore,
      confidencePercent: Number(clamp(confidencePercent, 0, 100).toFixed(2)),
      potential: resolvePotentialFromScore(avgScore),
      trend: 'steady',
      predictedCompletionPercent: avgScore,
      expectedLiftPercent: null,
      hookStrengthPercent: hookStrength,
      pacingScorePercent: null,
      summary,
      reasoning,
      updatedAt: new Date(job?.updatedAt || Date.now()).toISOString()
    }
  }
}

const ensurePremiumFeedbackAccess = async (userId: string, email?: string | null) => {
  const { tier } = await getUserPlan(userId)
  const devAccess = await resolveDevAdminAccess(userId, email)
  const isDev = devAccess.emailAuthorized
  const isPremium = isDev || isPaidTier(tier)
  return { tier, isDev, isPremium }
}

const buildClassicInput = (job: any): VideoFeedbackInput => {
  const analysis = (job?.analysis && typeof job.analysis === 'object') ? (job.analysis as any) : {}
  const renderSettings = (job?.renderSettings && typeof job.renderSettings === 'object') ? (job.renderSettings as any) : {}
  const metadata = (analysis?.metadata_summary && typeof analysis.metadata_summary === 'object') ? analysis.metadata_summary : {}
  const timeline = (metadata?.timeline && typeof metadata.timeline === 'object') ? metadata.timeline : {}
  const pacing = (metadata?.pacing && typeof metadata.pacing === 'object') ? metadata.pacing : {}
  const preScan = (analysis?.long_form_prescan && typeof analysis.long_form_prescan === 'object') ? analysis.long_form_prescan : {}

  const rawDurationSeconds = Math.max(1, asNumber(job?.inputDurationSeconds, asNumber(timeline?.sourceDurationSeconds, asNumber(analysis?.duration, 0))))
  const finalDurationFallback = rawDurationSeconds - asNumber(timeline?.removedSeconds, 0)
  const finalDurationSeconds = clamp(
    asNumber(pacing?.editedRuntimeSeconds, asNumber(pacing?.keptTimelineSeconds, finalDurationFallback)),
    1,
    rawDurationSeconds
  )

  const removedFromRatio = rawDurationSeconds * clamp01(asNumber(analysis?.boredom_removed_ratio, 0))
  const deadAirRemovedSeconds = clamp(
    asNumber(timeline?.removedSeconds, Math.max(0, rawDurationSeconds - finalDurationSeconds || removedFromRatio)),
    0,
    rawDurationSeconds
  )

  const hookStart = asNumber(metadata?.hook?.start, asNumber(analysis?.hook_start_time, Number.NaN))
  const hookEnd = asNumber(metadata?.hook?.end, asNumber(analysis?.hook_end_time, Number.NaN))
  const hookScore = parseRatio(metadata?.hook?.score ?? analysis?.hook_score)
  const hookAudit = parseRatio(metadata?.hook?.auditScore ?? analysis?.hook_audit_score)
  const hasHook = Number.isFinite(hookStart) && Number.isFinite(hookEnd) && hookEnd > hookStart

  const audioChain = Array.isArray(analysis?.audio_polish_chain)
    ? analysis.audio_polish_chain.map((entry: any) => String(entry || '').trim()).filter(Boolean)
    : []
  const noiseReductionLevel = extractNoiseReductionLevel(audioChain)
  const eqApplied = audioChain.some((entry) => /eq|treble|highpass|lowpass|equalizer/i.test(entry))

  const captionsEnabled = (
    parseBoolean(analysis?.vertical_caption_enabled) ||
    parseBoolean(renderSettings?.vertical_caption_enabled) ||
    String(analysis?.captionMode || '').toLowerCase() === 'ai'
  )
  const cueCount = Math.max(
    0,
    asNumber(analysis?.transcript_signals?.cueCount, Array.isArray(analysis?.transcript_cues) ? analysis.transcript_cues.length : 0)
  )
  const captionAccuracy = captionsEnabled
    ? clamp(60 + Math.min(30, cueCount * 1.1) + Math.min(8, cueCount / Math.max(1, rawDurationSeconds / 8 + 1) * 2), 55, 99)
    : null

  const manualConfig = (analysis?.manualTimestamp && typeof analysis.manualTimestamp === 'object')
    ? analysis.manualTimestamp
    : (analysis?.manual_timestamp && typeof analysis.manual_timestamp === 'object')
      ? analysis.manual_timestamp
      : {}
  const markers = Array.isArray(manualConfig?.markers)
    ? manualConfig.markers
    : Array.isArray(analysis?.manual_markers)
      ? analysis.manual_markers
      : []
  const suggestions = Array.isArray(manualConfig?.suggestions)
    ? manualConfig.suggestions
    : Array.isArray(analysis?.manual_suggestions)
      ? analysis.manual_suggestions
      : []
  const manualEditTimeMinutes = markers.length * 0.45 + suggestions.length * 0.25

  const chapterCount = Array.isArray(preScan?.highEnergyRanges)
    ? preScan.highEnergyRanges.length
    : Number.isFinite(Number(preScan?.totalChunks))
      ? Math.max(0, Math.round(Number(preScan.totalChunks) / 2))
      : 0

  const retentionFeedback = (analysis?.retention_feedback && typeof analysis.retention_feedback === 'object')
    ? analysis.retention_feedback
    : {}

  const modeSource = String(analysis?.renderMode || renderSettings?.renderMode || '').toLowerCase()
  const orientation: 'vertical' | 'horizontal' | 'unknown' = modeSource === 'vertical' ? 'vertical' : modeSource === 'horizontal' ? 'horizontal' : 'unknown'

  return {
    sourceType: 'classic',
    title: path.basename(String(job?.inputPath || `job-${job?.id || 'video'}`)),
    rawDurationSeconds,
    finalDurationSeconds,
    deadAirRemovedSeconds,
    hook: hasHook
      ? {
          startSeconds: hookStart,
          endSeconds: hookEnd,
          score: hookScore,
          confidence: hookAudit ?? hookScore,
          source: String(metadata?.retention?.hookSelectionSource || analysis?.hook_selection_source || 'auto')
        }
      : null,
    audioEnhancements: {
      chain: audioChain,
      noiseReductionLevel,
      eqApplied
    },
    captions: {
      enabled: captionsEnabled,
      accuracyPercent: captionAccuracy,
      timestamped: captionsEnabled && rawDurationSeconds >= 120
    },
    templateUsed: String(analysis?.longFormPreset || analysis?.longformPreset || renderSettings?.longFormPreset || renderSettings?.longformPreset || ''),
    exportFormat: {
      container: 'mp4',
      resolution: String(job?.finalQuality || job?.requestedQuality || 'Auto'),
      orientation,
      aspectRatio: orientation === 'vertical' ? '9:16' : orientation === 'horizontal' ? '16:9' : 'unknown'
    },
    manualEditTimeMinutes,
    aiProcessTimeMinutes: Math.max(0, (new Date(job?.updatedAt || Date.now()).getTime() - new Date(job?.createdAt || Date.now()).getTime()) / 60000),
    estimatedManualWithoutAiMinutes: 0,
    chapterCount,
    engagement: {
      views: Number(retentionFeedback?.views || retentionFeedback?.viewCount || retentionFeedback?.view_count),
      retentionRatePercent: parsePercent(retentionFeedback?.watchPercent ?? retentionFeedback?.completionPercent),
      likes: Number(retentionFeedback?.likes || retentionFeedback?.likeCount || retentionFeedback?.like_count),
      comments: Number(retentionFeedback?.comments || retentionFeedback?.commentCount || retentionFeedback?.comment_count),
      shares: Number(retentionFeedback?.shares || retentionFeedback?.shareCount || retentionFeedback?.share_count),
      likesPerView: parseRatio(retentionFeedback?.likesPerView ?? retentionFeedback?.likes_per_view),
      commentsPerView: parseRatio(retentionFeedback?.commentsPerView ?? retentionFeedback?.comments_per_view),
      sharesPerView: parseRatio(retentionFeedback?.sharesPerView ?? retentionFeedback?.shares_per_view)
    }
  }
}

const buildVibecutInput = (job: any): VideoFeedbackInput => {
  const upload = (job?.upload && typeof job.upload === 'object') ? job.upload : {}
  const uploadMetadata = (upload?.metadata && typeof upload.metadata === 'object') ? upload.metadata : {}
  const autoDetection = (upload?.autoDetection && typeof upload.autoDetection === 'object') ? upload.autoDetection : {}
  const editorProfile = (autoDetection?.editorProfile && typeof autoDetection.editorProfile === 'object') ? autoDetection.editorProfile : {}

  const rawDurationSeconds = Math.max(1, asNumber(uploadMetadata?.duration, 0))
  const ffmpegCommands = Array.isArray(job?.ffmpegCommands)
    ? job.ffmpegCommands.map((entry: any) => String(entry || '').trim()).filter(Boolean)
    : Array.isArray(job?.ffmpeg_commands)
      ? job.ffmpeg_commands.map((entry: any) => String(entry || '').trim()).filter(Boolean)
      : []

  const inferredKept = inferKeptDurationFromCommands(ffmpegCommands, rawDurationSeconds)
  const finalDurationSeconds = clamp(asNumber(inferredKept, rawDurationSeconds), 1, rawDurationSeconds)
  const deadAirRemovedSeconds = clamp(rawDurationSeconds - finalDurationSeconds, 0, rawDurationSeconds)

  const retention = (job?.retention && typeof job.retention === 'object') ? job.retention : {}
  const points = Array.isArray(retention?.points) ? retention.points : []
  const earlyWindowEnd = Math.max(6, rawDurationSeconds * 0.15)
  const hookCandidate = points
    .filter((point: any) => asNumber(point?.timestamp, Number.NaN) <= earlyWindowEnd)
    .sort((a: any, b: any) => asNumber(b?.watchedPct, 0) - asNumber(a?.watchedPct, 0))[0] || points[0] || null

  const hookScore = hookCandidate ? clamp01(asNumber(hookCandidate?.watchedPct, 0) / 100) : 0

  const audioFilters = parseAudioFiltersFromCommands(ffmpegCommands)
  const summary = String(retention?.summary || '')
  const captionMatch = summary.match(/Captions:\s*([^\s\/]+)\/?([^\s.]+)?/i)
  const captionMode = captionMatch ? String(captionMatch[1] || '').trim().toLowerCase() : String(editorProfile?.captionMode || '').toLowerCase()
  const captionsEnabled = captionMode === 'ai' || captionMode === 'auto'
  const captionAccuracy = captionsEnabled
    ? clamp(66 + Math.min(24, points.length * 1.4) + hookScore * 12, 55, 97)
    : null

  const cutMatch = summary.match(/Cuts:\s*(\d+)\s*segment/i)
  const segmentCount = cutMatch ? Math.max(0, Number(cutMatch[1])) : 0
  const manualOverride = /manual override/i.test(summary)

  const mode = String(job?.mode || '').toLowerCase() === 'vertical' ? 'vertical' : 'horizontal'
  const width = asNumber(uploadMetadata?.width, 0)
  const height = asNumber(uploadMetadata?.height, 0)

  const avgRetention = points.length
    ? points.reduce((sum: number, point: any) => sum + asNumber(point?.watchedPct, 0), 0) / points.length
    : null

  return {
    sourceType: 'vibecut',
    title: String(job?.fileName || upload?.fileName || `vibecut-${job?.id || 'video'}`),
    rawDurationSeconds,
    finalDurationSeconds,
    deadAirRemovedSeconds,
    hook: hookCandidate
      ? {
          startSeconds: asNumber(hookCandidate?.timestamp, 0),
          endSeconds: Math.min(rawDurationSeconds, asNumber(hookCandidate?.timestamp, 0) + Math.max(2, Math.min(8, rawDurationSeconds * 0.18))),
          score: hookScore,
          confidence: hookScore,
          source: String(hookCandidate?.type || 'hook')
        }
      : null,
    audioEnhancements: {
      chain: audioFilters,
      noiseReductionLevel: extractNoiseReductionLevel(audioFilters),
      eqApplied: audioFilters.some((entry) => /eq|treble|highpass|lowpass|equalizer/i.test(entry))
    },
    captions: {
      enabled: captionsEnabled,
      accuracyPercent: captionAccuracy,
      timestamped: captionsEnabled && rawDurationSeconds >= 120
    },
    templateUsed: String(
      editorProfile?.suggestedSubMode ||
      autoDetection?.suggestedSubMode ||
      (mode === 'vertical' ? 'highlight_mode' : 'standard_mode')
    ),
    exportFormat: {
      container: 'mp4',
      resolution: width > 0 && height > 0 ? `${height}p` : 'Auto',
      orientation: mode,
      aspectRatio: mode === 'vertical' ? '9:16' : '16:9'
    },
    manualEditTimeMinutes: manualOverride ? Math.max(1, segmentCount * 0.55) : 0,
    aiProcessTimeMinutes: Math.max(0, (new Date(job?.updatedAt || Date.now()).getTime() - new Date(job?.createdAt || Date.now()).getTime()) / 60000),
    estimatedManualWithoutAiMinutes: 0,
    chapterCount: rawDurationSeconds >= 300 ? Math.max(1, Math.round(segmentCount / 2)) : 0,
    engagement: {
      retentionRatePercent: avgRetention,
      likesPerView: null,
      commentsPerView: null,
      sharesPerView: null
    }
  }
}

const buildUploadInput = (summary: any): VideoFeedbackInput => {
  const rawDurationSeconds = Math.max(1, asNumber(
    summary?.rawFootageLength ?? summary?.raw_footage_length ?? summary?.durationSeconds ?? summary?.duration_seconds,
    0
  ))

  const deadAirRemovedSeconds = clamp(asNumber(
    summary?.aiDetectedDeadAirRemoved ?? summary?.ai_detected_dead_air_removed ?? summary?.deadAirRemovedSeconds,
    0
  ), 0, rawDurationSeconds)

  const finalDurationSeconds = clamp(asNumber(
    summary?.finalLength ?? summary?.final_length ?? summary?.finalDurationSeconds,
    rawDurationSeconds - deadAirRemovedSeconds
  ), 1, rawDurationSeconds)

  const hookRaw = summary?.hookSelected || summary?.hook_selected || summary?.hook || null
  const hookStart = asNumber(hookRaw?.start ?? hookRaw?.startSeconds ?? hookRaw?.start_time, Number.NaN)
  const hookEnd = asNumber(hookRaw?.end ?? hookRaw?.endSeconds ?? hookRaw?.end_time, Number.NaN)
  const hookConfidence = parseRatio(hookRaw?.confidence ?? hookRaw?.confidencePercent ?? hookRaw?.score)

  const audioText = summary?.audioEnhancementsApplied ?? summary?.audio_enhancements_applied
  const captionRaw = summary?.captionsGenerated || summary?.captions_generated || {}
  const engagementRaw = summary?.postExportEngagement || summary?.post_export_engagement || {}
  const trendRaw = summary?.currentPlatformTrends || summary?.current_platform_trends || {}

  return {
    sourceType: 'upload',
    title: String(summary?.title || summary?.fileName || 'Uploaded video'),
    rawDurationSeconds,
    finalDurationSeconds,
    deadAirRemovedSeconds,
    hook: Number.isFinite(hookStart) && Number.isFinite(hookEnd) && hookEnd > hookStart
      ? {
          startSeconds: hookStart,
          endSeconds: hookEnd,
          score: hookConfidence,
          confidence: hookConfidence,
          source: 'upload_input'
        }
      : null,
    audioEnhancements: {
      chain: typeof audioText === 'string' ? [audioText] : Array.isArray(audioText) ? audioText : [],
      noiseReductionLevel: parsePercentFromText(audioText),
      eqApplied: /eq/i.test(String(audioText || ''))
    },
    captions: {
      enabled: parseBoolean(captionRaw?.enabled ?? captionRaw?.status ?? true),
      accuracyPercent: parsePercent(captionRaw?.accuracy ?? captionRaw?.accuracyPercent),
      timestamped: parseBoolean(captionRaw?.timestamped ?? captionRaw?.withTimestamps)
    },
    templateUsed: String(summary?.templateUsed || summary?.template_used || ''),
    exportFormat: {
      container: String(summary?.exportFormat?.container || 'mp4'),
      resolution: String(summary?.exportFormat?.resolution || summary?.exportResolution || 'Auto'),
      orientation: String(summary?.exportFormat?.orientation || '').toLowerCase() === 'vertical'
        ? 'vertical'
        : String(summary?.exportFormat?.orientation || '').toLowerCase() === 'horizontal'
          ? 'horizontal'
          : 'unknown',
      aspectRatio: String(summary?.exportFormat?.aspectRatio || summary?.exportAspectRatio || 'unknown')
    },
    manualEditTimeMinutes: asNumber(summary?.manualEditTime ?? summary?.manual_edit_time, 0),
    aiProcessTimeMinutes: asNumber(summary?.aiProcessTimeMinutes ?? summary?.ai_process_time_minutes, 0),
    estimatedManualWithoutAiMinutes: asNumber(
      summary?.estimatedManualEditTimeWithoutAI ?? summary?.estimated_manual_edit_time_without_ai,
      0
    ),
    chapterCount: asNumber(summary?.chapterCount ?? summary?.chapters ?? 0, 0),
    engagement: {
      views: asNumber(engagementRaw?.views, Number.NaN),
      retentionRatePercent: parsePercent(engagementRaw?.retention ?? engagementRaw?.retentionRatePercent),
      likes: asNumber(engagementRaw?.likes, Number.NaN),
      comments: asNumber(engagementRaw?.comments, Number.NaN),
      shares: asNumber(engagementRaw?.shares, Number.NaN)
    },
    trendSignals: {
      tiktokShortBoost: parseTrendBoost(trendRaw?.tiktokShortBoost ?? trendRaw?.tiktok),
      youtubeLongBoost: parseTrendBoost(trendRaw?.youtubeLongBoost ?? trendRaw?.youtube),
      instagramCaptionBoost: parseTrendBoost(trendRaw?.instagramCaptionBoost ?? trendRaw?.instagram)
    }
  }
}

router.get('/jobs', async (req: any, res) => {
  try {
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })

    const access = await ensurePremiumFeedbackAccess(userId, req.user?.email)
    if (!access.isPremium) {
      return res.status(403).json({
        error: 'PREMIUM_REQUIRED',
        message: 'Upgrade to unlock AI video feedback.',
        redirectTo: '/pricing'
      })
    }

    const classicRows = await prisma.job.findMany({
      where: { userId, status: 'completed' as any },
      orderBy: { createdAt: 'desc' },
      take: 40,
      select: {
        id: true,
        inputPath: true,
        createdAt: true,
        inputDurationSeconds: true,
        analysis: true
      }
    })

    const classicJobs = classicRows.map((job) => {
      const analysis = (job.analysis && typeof job.analysis === 'object') ? (job.analysis as any) : {}
      const metadata = (analysis?.metadata_summary && typeof analysis.metadata_summary === 'object') ? analysis.metadata_summary : {}
      const timeline = (metadata?.timeline && typeof metadata.timeline === 'object') ? metadata.timeline : {}
      const duration = asNumber(job.inputDurationSeconds, asNumber(timeline?.sourceDurationSeconds, asNumber(analysis?.duration, 0)))
      return {
        id: job.id,
        sourceType: 'classic' as const,
        title: path.basename(String(job.inputPath || `job-${job.id}`)),
        createdAt: job.createdAt,
        durationSeconds: Number.isFinite(duration) && duration > 0 ? duration : null
      }
    })

    let vibecutJobs: Array<{ id: string; sourceType: 'vibecut'; title: string; createdAt: Date; durationSeconds: number | null }> = []
    try {
      const rows = await (prisma as any).vibeCutJob.findMany({
        where: { userId, status: 'completed' },
        orderBy: { createdAt: 'desc' },
        take: 40,
        select: {
          id: true,
          fileName: true,
          createdAt: true,
          upload: { select: { metadata: true } }
        }
      })
      vibecutJobs = rows.map((row: any) => ({
        id: String(row.id),
        sourceType: 'vibecut' as const,
        title: String(row.fileName || `vibecut-${row.id}`),
        createdAt: new Date(row.createdAt),
        durationSeconds: Number.isFinite(Number(row?.upload?.metadata?.duration)) ? Number(row.upload.metadata.duration) : null
      }))
    } catch {
      vibecutJobs = []
    }

    const jobs = [...classicJobs, ...vibecutJobs]
      .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
      .slice(0, 40)

    return res.json({ jobs })
  } catch {
    return res.status(500).json({ error: 'server_error' })
  }
})

router.get('/realtime-predictions', async (req: any, res) => {
  try {
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })

    const access = await ensurePremiumFeedbackAccess(userId, req.user?.email)
    if (!access.isPremium) {
      return res.status(403).json({
        error: 'PREMIUM_REQUIRED',
        message: 'Upgrade to unlock realtime prediction analytics.',
        redirectTo: '/pricing'
      })
    }

    const limit = clamp(Math.round(asNumber(req.query?.limit, 24)), 1, 60)

    const classicRows = await prisma.job.findMany({
      where: { userId },
      orderBy: { updatedAt: 'desc' },
      take: 90,
      select: {
        id: true,
        status: true,
        inputPath: true,
        createdAt: true,
        updatedAt: true,
        inputDurationSeconds: true,
        retentionScore: true,
        analysis: true
      }
    })

    const classicPredictions: RealtimePredictionVideo[] = []
    const seenClassicUploads = new Set<string>()
    for (const row of classicRows) {
      const uploadKey = String(row.inputPath || '').trim() || String(row.id)
      if (seenClassicUploads.has(uploadKey)) continue
      seenClassicUploads.add(uploadKey)
      classicPredictions.push(buildClassicRealtimePrediction(row))
      if (classicPredictions.length >= limit) break
    }

    let vibecutPredictions: RealtimePredictionVideo[] = []
    try {
      const rows = await (prisma as any).vibeCutJob.findMany({
        where: { userId },
        orderBy: { updatedAt: 'desc' },
        take: 90,
        select: {
          id: true,
          uploadId: true,
          status: true,
          fileName: true,
          retention: true,
          createdAt: true,
          updatedAt: true,
          upload: {
            select: {
              fileName: true,
              metadata: true
            }
          }
        }
      })
      const seenVibecutUploads = new Set<string>()
      for (const row of rows) {
        const uploadKey = String(row?.uploadId || row?.id || '').trim()
        if (!uploadKey) continue
        if (seenVibecutUploads.has(uploadKey)) continue
        seenVibecutUploads.add(uploadKey)
        vibecutPredictions.push(buildVibecutRealtimePrediction(row))
        if (vibecutPredictions.length >= limit) break
      }
    } catch {
      vibecutPredictions = []
    }

    const merged = [...classicPredictions, ...vibecutPredictions]
      .sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime())
      .slice(0, limit)

    return res.json({
      generatedAt: new Date().toISOString(),
      videos: merged,
      totals: {
        uniqueUploads: merged.length
      }
    })
  } catch {
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/analyze', async (req: any, res) => {
  try {
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })

    const access = await ensurePremiumFeedbackAccess(userId, req.user?.email)
    if (!access.isPremium) {
      return res.status(403).json({
        error: 'PREMIUM_REQUIRED',
        message: 'Upgrade to unlock AI trends and feedback.',
        redirectTo: '/pricing'
      })
    }

    const jobId = String(req.body?.jobId || '').trim()
    const requestedSourceType = parseSourceType(req.body?.sourceType ?? req.body?.source)
    const uploadSummary = req.body?.uploadSummary && typeof req.body.uploadSummary === 'object'
      ? req.body.uploadSummary
      : null

    if (!jobId && !uploadSummary) {
      return res.status(400).json({ error: 'missing_source', message: 'Provide jobId or uploadSummary.' })
    }

    let input: VideoFeedbackInput | null = null
    let source: { type: string; jobId?: string } = { type: 'upload' }

    if (jobId) {
      if (requestedSourceType !== 'vibecut') {
        const classicJob = await prisma.job.findUnique({ where: { id: jobId } })
        if (classicJob && classicJob.userId === userId) {
          input = buildClassicInput(classicJob)
          source = { type: 'classic', jobId }
        }
      }

      if (!input && requestedSourceType !== 'classic') {
        try {
          const vibeJob = await (prisma as any).vibeCutJob.findUnique({
            where: { id: jobId },
            include: {
              upload: {
                select: {
                  metadata: true,
                  autoDetection: true,
                  fileName: true
                }
              }
            }
          })
          if (vibeJob && String(vibeJob.userId) === String(userId)) {
            input = buildVibecutInput(vibeJob)
            source = { type: 'vibecut', jobId }
          }
        } catch {
          // vibecut unavailable
        }
      }

      if (!input) {
        return res.status(404).json({ error: 'not_found', message: 'Video source not found for this account.' })
      }
    }

    if (!input && uploadSummary) {
      input = buildUploadInput(uploadSummary)
      source = { type: 'upload' }
    }

    if (!input) {
      return res.status(400).json({ error: 'invalid_source' })
    }

    const feedback = await buildVideoFeedbackAnalysis(input)

    return res.json({
      source,
      tier: access.tier,
      isDev: access.isDev,
      generatedAt: new Date().toISOString(),
      feedback
    })
  } catch {
    return res.status(500).json({ error: 'server_error' })
  }
})

export default router
