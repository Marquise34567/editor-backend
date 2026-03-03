import express from 'express'
import path from 'path'
import { prisma } from '../db/prisma'
import { getUserPlan } from '../services/plans'
import { isPaidTier } from '../shared/planConfig'
import { resolveDevAdminAccess } from '../lib/devAccounts'
import { buildVideoFeedbackAnalysis, type VideoFeedbackInput } from '../services/videoFeedback'
import { runFeedbackLoop } from '../dev/algorithm/feedbackLoop/feedbackLoopService'
import {
  buildYouTubeOAuthAuthorizeUrl,
  disconnectYouTubeOAuthForUser,
  exchangeYouTubeOAuthCodeForUser,
  getYouTubeAccessTokenForUser,
  getYouTubeOAuthConfigStatus,
  getYouTubeOAuthConnectionForUser
} from '../services/youtubeOAuth'
import {
  derivePerSecondRewardSignal,
  ingestPlatformRewardSignal,
  registerPolicyOutcomeForJob,
  upsertCreatorStyleProfileFromFeedback
} from '../services/editorIntelligence'

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

const parseYouTubeVideoId = (value: any): string | null => {
  const raw = String(value || '').trim()
  if (!raw) return null
  if (/^[a-zA-Z0-9_-]{11}$/.test(raw)) return raw
  try {
    const asUrl = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`
    const parsed = new URL(asUrl)
    const hostname = parsed.hostname.replace(/^www\./i, '').toLowerCase()
    if (hostname === 'youtu.be') {
      const id = parsed.pathname.split('/').filter(Boolean)[0] || ''
      return /^[a-zA-Z0-9_-]{11}$/.test(id) ? id : null
    }
    if (!hostname.endsWith('youtube.com')) return null
    const watchId = parsed.searchParams.get('v') || ''
    if (/^[a-zA-Z0-9_-]{11}$/.test(watchId)) return watchId
    const pathTokens = parsed.pathname.split('/').filter(Boolean)
    const pathId = pathTokens.length >= 2 && (
      pathTokens[0] === 'shorts' ||
      pathTokens[0] === 'embed' ||
      pathTokens[0] === 'live' ||
      pathTokens[0] === 'v'
    )
      ? pathTokens[1]
      : ''
    return /^[a-zA-Z0-9_-]{11}$/.test(pathId) ? pathId : null
  } catch {
    return null
  }
}

const parseIso8601DurationSeconds = (value: any): number | null => {
  const text = String(value || '').trim().toUpperCase()
  const match = text.match(/^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/)
  if (!match) return null
  const hours = Number(match[1] || 0)
  const minutes = Number(match[2] || 0)
  const seconds = Number(match[3] || 0)
  const total = hours * 3600 + minutes * 60 + seconds
  return Number.isFinite(total) ? total : null
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
  const clips = Array.isArray(metadata?.clips) ? metadata.clips : []
  const clipRetentionCurve = clips
    .map((clip: any, index: number) => {
      const predicted = parsePercent(clip?.predictedCompletion ?? clip?.predicted_completion)
      if (predicted === null) return null
      const start = asNumber(clip?.start, Number.NaN)
      const end = asNumber(clip?.end, Number.NaN)
      const fallbackStep = rawDurationSeconds / Math.max(1, clips.length)
      const fallbackTime = fallbackStep * index
      const timestampSeconds = Number.isFinite(start)
        ? start
        : Number.isFinite(end)
          ? Math.max(0, end - Math.max(2, Math.min(10, fallbackStep * 0.45)))
          : fallbackTime
      return {
        timestampSeconds: Number(clamp(timestampSeconds, 0, rawDurationSeconds).toFixed(2)),
        watchedPercent: Number(predicted.toFixed(2)),
        signal: Number((predicted / 100).toFixed(4)),
        category: String(clip?.type || clip?.label || '').toLowerCase() || null,
        label: String(clip?.title || clip?.label || `Clip ${index + 1}`),
        note: String(clip?.reason || clip?.description || '')
      }
    })
    .filter((item): item is {
      timestampSeconds: number
      watchedPercent: number
      signal: number
      category: string | null
      label: string
      note: string
    } => Boolean(item))

  const engagementWindows = Array.isArray(analysis?.engagementWindows)
    ? analysis.engagementWindows
    : Array.isArray(analysis?.editPlan?.engagementWindows)
      ? analysis.editPlan.engagementWindows
      : []
  const windowRetentionCurve = engagementWindows
    .map((window: any, index: number) => {
      const time = asNumber(window?.time, Number.NaN)
      const score = asNumber(window?.score, Number.NaN)
      if (!Number.isFinite(time) || !Number.isFinite(score)) return null
      const scorePct = clamp(score <= 1 ? score * 100 : score, 0, 100)
      const moodScore = asNumber(window?.emotionIntensity ?? window?.audioEnergy ?? window?.speechIntensity, Number.NaN)
      const category = scorePct >= 78
        ? 'best'
        : scorePct <= 36
          ? 'skip_risk'
          : scorePct <= 52
            ? 'low_energy'
            : index <= 1
              ? 'hook'
              : 'worst'
      return {
        timestampSeconds: Number(clamp(time, 0, rawDurationSeconds).toFixed(2)),
        watchedPercent: Number(scorePct.toFixed(2)),
        signal: Number((scorePct / 100).toFixed(4)),
        category,
        label: `Window ${index + 1}`,
        note: Number.isFinite(moodScore)
          ? `Composite intensity ${Math.round(clamp(moodScore <= 1 ? moodScore * 100 : moodScore, 0, 100))}%.`
          : ''
      }
    })
    .filter((item): item is {
      timestampSeconds: number
      watchedPercent: number
      signal: number
      category: string
      label: string
      note: string
    } => Boolean(item))

  const trendRaw = (
    (analysis?.currentPlatformTrends && typeof analysis.currentPlatformTrends === 'object' && analysis.currentPlatformTrends) ||
    (analysis?.current_platform_trends && typeof analysis.current_platform_trends === 'object' && analysis.current_platform_trends) ||
    (metadata?.currentPlatformTrends && typeof metadata.currentPlatformTrends === 'object' && metadata.currentPlatformTrends) ||
    (metadata?.current_platform_trends && typeof metadata.current_platform_trends === 'object' && metadata.current_platform_trends) ||
    {}
  ) as any
  const retentionCurve = (clipRetentionCurve.length ? clipRetentionCurve : windowRetentionCurve).slice(0, 96)

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
    },
    trendSignals: {
      tiktokShortBoost: parseTrendBoost(trendRaw?.tiktokShortBoost ?? trendRaw?.tiktok),
      youtubeLongBoost: parseTrendBoost(trendRaw?.youtubeLongBoost ?? trendRaw?.youtube),
      instagramCaptionBoost: parseTrendBoost(trendRaw?.instagramCaptionBoost ?? trendRaw?.instagram)
    },
    retentionCurve
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
  const retentionCurve = points
    .map((point: any, index: number) => ({
      timestampSeconds: Number(clamp(asNumber(point?.timestamp, 0), 0, rawDurationSeconds).toFixed(2)),
      watchedPercent: Number(clamp(asNumber(point?.watchedPct, 0), 0, 100).toFixed(2)),
      signal: Number((clamp(asNumber(point?.watchedPct, 0), 0, 100) / 100).toFixed(4)),
      category: String(point?.type || '').toLowerCase() || (index <= 1 ? 'hook' : ''),
      label: String(point?.label || `Moment ${index + 1}`),
      note: String(point?.description || '')
    }))
    .filter((point) => Number.isFinite(point.timestampSeconds) && Number.isFinite(point.watchedPercent))

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
    },
    retentionCurve
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

type YouTubeVideoMetrics = {
  dataSource: 'oauth' | 'api_key'
  videoId: string
  title: string
  channelId: string
  channelTitle: string
  publishedAt: string | null
  durationSeconds: number | null
  viewCount: number
  likeCount: number
  commentCount: number
  engagementRate: number
}

type YouTubeAnalyticsSummary = {
  views: number
  estimatedMinutesWatched: number
  averageViewDurationSeconds: number | null
  averageViewPercentage: number | null
  likes: number
  comments: number
  shares: number
}

type YouTubeRetentionPoint = {
  elapsedVideoTimeRatio: number
  audienceWatchRatio: number | null
  relativeRetentionPerformance: number | null
}

type YouTubeSignalState = {
  coldStartMode: boolean
  trustWeight: number
  qualifyingVideos: number
  requiredVideos: number
  averageViewsPerVideo: number | null
  currentVideoViews: number
  requiredAverageViewsPerVideo: number
  highTrustAverageViewsPerVideo: number
  recommendation: string
}

type YouTubePlatformFeedback = {
  watchPercent: number | null
  hookHoldPercent: number | null
  completionPercent: number | null
  rewatchRate: number | null
  first30Retention: number | null
  avgViewDurationSeconds: number | null
  clickThroughRate: number | null
  sharesPerView: number | null
  likesPerView: number | null
  commentsPerView: number | null
  manualScore: number | null
  source: string
  sourceType: 'platform'
  notes: string | null
  submittedAt: string
  youtubeSignal: YouTubeSignalState | null
}

const YOUTUBE_SIGNAL_REQUIRED_VIDEOS = 3
const YOUTUBE_SIGNAL_MIN_AVG_VIEWS = 200
const YOUTUBE_SIGNAL_HIGH_AVG_VIEWS = 500

const parseYmdDate = (value: any): string | null => {
  const raw = String(value || '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return null
  const parsed = new Date(`${raw}T00:00:00.000Z`)
  if (!Number.isFinite(parsed.getTime())) return null
  return raw
}

const toYmd = (date: Date) => date.toISOString().slice(0, 10)

const resolveYouTubeAnalyticsDateRange = (payload: any) => {
  const endRaw = parseYmdDate(payload?.endDate ?? payload?.end_date)
  const startRaw = parseYmdDate(payload?.startDate ?? payload?.start_date)
  const defaultEndDate = new Date(Date.now() - 24 * 60 * 60 * 1000)
  const defaultStartDate = new Date(defaultEndDate.getTime() - (27 * 24 * 60 * 60 * 1000))
  const end = endRaw || toYmd(defaultEndDate)
  const start = startRaw || toYmd(defaultStartDate)
  if (start > end) {
    return {
      startDate: end,
      endDate: start
    }
  }
  return {
    startDate: start,
    endDate: end
  }
}

const parseGoogleApiReason = (payload: any): string | null =>
  String(payload?.error?.message || payload?.error_description || '').trim() || null

const fetchGoogleJson = async ({
  url,
  accessToken,
  timeoutMs = 12_000
}: {
  url: string
  accessToken?: string | null
  timeoutMs?: number
}) => {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const headers: Record<string, string> = {}
    if (accessToken) headers.Authorization = `Bearer ${accessToken}`
    const response = await fetch(url, {
      method: 'GET',
      signal: controller.signal,
      headers
    })
    const payload = await response.json().catch(() => ({} as any))
    return {
      ok: response.ok,
      status: response.status,
      payload
    }
  } finally {
    clearTimeout(timer)
  }
}

const parseAnalyticsScalarMetric = ({
  report,
  metric
}: {
  report: any
  metric: string
}): number | null => {
  const headers = Array.isArray(report?.columnHeaders) ? report.columnHeaders : []
  const rows = Array.isArray(report?.rows) ? report.rows : []
  if (!rows.length) return null
  const index = headers.findIndex((entry: any) => String(entry?.name || '').trim() === metric)
  if (index === -1) return null
  const value = Number(rows[0]?.[index])
  return Number.isFinite(value) ? value : null
}

const parseYouTubeRetentionCurve = (report: any): YouTubeRetentionPoint[] => {
  const headers = Array.isArray(report?.columnHeaders) ? report.columnHeaders : []
  const rows = Array.isArray(report?.rows) ? report.rows : []
  if (!rows.length) return []
  const elapsedIndex = headers.findIndex((entry: any) => String(entry?.name || '').trim() === 'elapsedVideoTimeRatio')
  const watchIndex = headers.findIndex((entry: any) => String(entry?.name || '').trim() === 'audienceWatchRatio')
  const relativeIndex = headers.findIndex((entry: any) => String(entry?.name || '').trim() === 'relativeRetentionPerformance')
  if (elapsedIndex === -1) return []
  const parsed = rows
    .map((row: any) => {
      const elapsed = Number(row?.[elapsedIndex])
      if (!Number.isFinite(elapsed)) return null
      const watchRaw = Number(row?.[watchIndex])
      const relativeRaw = Number(row?.[relativeIndex])
      return {
        elapsedVideoTimeRatio: clamp(elapsed, 0, 1),
        audienceWatchRatio: Number.isFinite(watchRaw) ? watchRaw : null,
        relativeRetentionPerformance: Number.isFinite(relativeRaw) ? relativeRaw : null
      } satisfies YouTubeRetentionPoint
    })
    .filter((point: YouTubeRetentionPoint | null): point is YouTubeRetentionPoint => point !== null)
  return parsed.sort((left, right) => left.elapsedVideoTimeRatio - right.elapsedVideoTimeRatio)
}

const ratioMetricToPercent = (value: number | null) => {
  if (value === null || !Number.isFinite(value)) return null
  if (value >= 0 && value <= 1.5) return Number(clamp(value * 100, 0, 100).toFixed(3))
  return Number(clamp(value, 0, 100).toFixed(3))
}

const averagePercentForRange = (points: YouTubeRetentionPoint[], maxElapsedRatio: number) => {
  const sampled = points
    .filter((point) => point.elapsedVideoTimeRatio <= maxElapsedRatio)
    .map((point) => ratioMetricToPercent(point.audienceWatchRatio))
    .filter((value): value is number => value !== null)
  if (!sampled.length) return null
  return Number((sampled.reduce((sum, value) => sum + value, 0) / sampled.length).toFixed(3))
}

const derivePlatformFeedbackFromYouTube = ({
  summary,
  retentionCurve,
  videoId,
  startDate,
  endDate,
  youtubeSignal
}: {
  summary: YouTubeAnalyticsSummary
  retentionCurve: YouTubeRetentionPoint[]
  videoId: string
  startDate: string
  endDate: string
  youtubeSignal: YouTubeSignalState | null
}): YouTubePlatformFeedback => {
  const safeViews = Math.max(0, Number(summary.views || 0))
  const likesPerView = safeViews > 0 ? Number(clamp((summary.likes / safeViews) * 100, 0, 100).toFixed(4)) : null
  const commentsPerView = safeViews > 0 ? Number(clamp((summary.comments / safeViews) * 100, 0, 100).toFixed(4)) : null
  const sharesPerView = safeViews > 0 ? Number(clamp((summary.shares / safeViews) * 100, 0, 100).toFixed(4)) : null
  const hookHoldPercent = averagePercentForRange(retentionCurve, 0.08)
  const first30Retention = averagePercentForRange(retentionCurve, 0.3)
  const completionFromCurve = ratioMetricToPercent(
    retentionCurve.length ? retentionCurve[retentionCurve.length - 1].audienceWatchRatio : null
  )
  const rewatchRate = retentionCurve.length
    ? Number(
        (
          (retentionCurve.filter((point) => (point.relativeRetentionPerformance || 0) > 1.04).length / retentionCurve.length) *
          100
        ).toFixed(3)
      )
    : null

  const watchPercent = summary.averageViewPercentage !== null
    ? Number(clamp(summary.averageViewPercentage, 0, 100).toFixed(3))
    : null

  return {
    watchPercent,
    hookHoldPercent,
    completionPercent: completionFromCurve ?? watchPercent,
    rewatchRate,
    first30Retention,
    avgViewDurationSeconds: summary.averageViewDurationSeconds,
    clickThroughRate: null,
    sharesPerView,
    likesPerView,
    commentsPerView,
    manualScore: null,
    source: 'youtube_analytics_oauth',
    sourceType: 'platform',
    notes: `youtube_video:${videoId}; window:${startDate}..${endDate}`,
    submittedAt: new Date().toISOString(),
    youtubeSignal
  }
}

const extractMappedYouTubeVideoId = (analysis: any): string | null => {
  const mapped = parseYouTubeVideoId(
    analysis?.youtube_video_id ??
    analysis?.youtubeVideoId ??
    analysis?.youtube_sync?.videoId ??
    analysis?.youtubeSync?.videoId
  )
  return mapped || null
}

const extractYouTubeViewsFromAnalysis = (analysis: any): number | null => {
  const candidates = [
    analysis?.youtube_sync?.analyticsSummary?.views,
    analysis?.youtubeSync?.analyticsSummary?.views,
    analysis?.retention_feedback?.youtubeSignal?.currentVideoViews,
    analysis?.retention_feedback?.youtube_signal?.currentVideoViews
  ]
  for (const candidate of candidates) {
    const parsed = Number(candidate)
    if (Number.isFinite(parsed) && parsed >= 0) {
      return parsed
    }
  }
  return null
}

const collectHistoricalYouTubeViewsForUser = async ({
  userId,
  excludeJobId
}: {
  userId: string
  excludeJobId?: string | null
}) => {
  const rows = await prisma.job.findMany({
    where: {
      userId,
      status: 'completed' as any,
      ...(excludeJobId ? { NOT: { id: excludeJobId } } : {})
    },
    orderBy: { createdAt: 'desc' },
    take: 300,
    select: {
      analysis: true
    }
  })
  const views: number[] = []
  for (const row of rows) {
    const analysis = row.analysis && typeof row.analysis === 'object'
      ? (row.analysis as Record<string, any>)
      : {}
    const parsed = extractYouTubeViewsFromAnalysis(analysis)
    if (parsed !== null) views.push(parsed)
  }
  return views
}

const deriveYouTubeSignalState = ({
  historicalViews,
  currentVideoViews
}: {
  historicalViews: number[]
  currentVideoViews: number
}): YouTubeSignalState => {
  const mergedViews = [...historicalViews]
  if (Number.isFinite(currentVideoViews) && currentVideoViews >= 0) {
    mergedViews.push(currentVideoViews)
  }

  const qualifyingVideos = mergedViews.filter((value) => Number.isFinite(value) && value >= 0).length
  const averageViewsPerVideo = qualifyingVideos > 0
    ? Number((mergedViews.reduce((sum, value) => sum + value, 0) / qualifyingVideos).toFixed(3))
    : null
  const hasVideoVolume = qualifyingVideos >= YOUTUBE_SIGNAL_REQUIRED_VIDEOS
  const hasViewDepth = averageViewsPerVideo !== null && averageViewsPerVideo >= YOUTUBE_SIGNAL_MIN_AVG_VIEWS
  const coldStartMode = !(hasVideoVolume && hasViewDepth)

  const trustWeight = (() => {
    if (coldStartMode) {
      const volumeFactor = clamp01(qualifyingVideos / YOUTUBE_SIGNAL_REQUIRED_VIDEOS)
      const depthFactor = clamp01((averageViewsPerVideo ?? 0) / YOUTUBE_SIGNAL_MIN_AVG_VIEWS)
      return Number((0.15 + ((volumeFactor * 0.6 + depthFactor * 0.4) * 0.45)).toFixed(3))
    }
    const highTrustSpan = Math.max(1, YOUTUBE_SIGNAL_HIGH_AVG_VIEWS - YOUTUBE_SIGNAL_MIN_AVG_VIEWS)
    const highTrustFactor = clamp01(((averageViewsPerVideo ?? YOUTUBE_SIGNAL_MIN_AVG_VIEWS) - YOUTUBE_SIGNAL_MIN_AVG_VIEWS) / highTrustSpan)
    return Number((0.62 + highTrustFactor * 0.33).toFixed(3))
  })()

  const recommendation = coldStartMode
    ? 'Cold-start mode active: blend global defaults + boundary critic + in-app watch/skip/thumb signals.'
    : 'YouTube retention has enough signal for stronger policy weighting.'

  return {
    coldStartMode,
    trustWeight,
    qualifyingVideos,
    requiredVideos: YOUTUBE_SIGNAL_REQUIRED_VIDEOS,
    averageViewsPerVideo,
    currentVideoViews: Number.isFinite(currentVideoViews) ? Math.max(0, currentVideoViews) : 0,
    requiredAverageViewsPerVideo: YOUTUBE_SIGNAL_MIN_AVG_VIEWS,
    highTrustAverageViewsPerVideo: YOUTUBE_SIGNAL_HIGH_AVG_VIEWS,
    recommendation
  }
}

const fetchYouTubeVideoMetrics = async ({
  userId,
  apiKey,
  videoId
}: {
  userId: string
  apiKey: string
  videoId: string
}): Promise<YouTubeVideoMetrics> => {
  let dataSource: 'oauth' | 'api_key' = 'api_key'
  let accessToken: string | null = null
  if (!apiKey) {
    accessToken = await getYouTubeAccessTokenForUser(userId)
    if (!accessToken) {
      const error = new Error('youtube_auth_missing')
      ;(error as any).code = 'youtube_auth_missing'
      throw error
    }
    dataSource = 'oauth'
  }

  const endpoint = new URL('https://www.googleapis.com/youtube/v3/videos')
  endpoint.searchParams.set('part', 'snippet,statistics,contentDetails')
  endpoint.searchParams.set('id', videoId)
  if (apiKey) endpoint.searchParams.set('key', apiKey)

  const response = await fetchGoogleJson({
    url: endpoint.toString(),
    accessToken
  })
  if (!response.ok) {
    const error = new Error(parseGoogleApiReason(response.payload) || 'youtube_data_api_failed')
    ;(error as any).code = 'youtube_data_api_failed'
    ;(error as any).status = response.status
    throw error
  }

  const item = Array.isArray(response.payload?.items) ? response.payload.items[0] : null
  if (!item) {
    const error = new Error('youtube_video_not_found')
    ;(error as any).code = 'youtube_video_not_found'
    throw error
  }
  const viewCount = Math.max(0, Number(item?.statistics?.viewCount || 0))
  const likeCount = Math.max(0, Number(item?.statistics?.likeCount || 0))
  const commentCount = Math.max(0, Number(item?.statistics?.commentCount || 0))
  const engagementRate = viewCount > 0
    ? Number(clamp01((likeCount + commentCount) / viewCount).toFixed(4))
    : 0

  return {
    dataSource,
    videoId,
    title: String(item?.snippet?.title || ''),
    channelId: String(item?.snippet?.channelId || ''),
    channelTitle: String(item?.snippet?.channelTitle || ''),
    publishedAt: item?.snippet?.publishedAt || null,
    durationSeconds: parseIso8601DurationSeconds(item?.contentDetails?.duration),
    viewCount,
    likeCount,
    commentCount,
    engagementRate
  }
}

const fetchYouTubeAnalyticsReport = async ({
  userId,
  videoId,
  startDate,
  endDate
}: {
  userId: string
  videoId: string
  startDate: string
  endDate: string
}) => {
  const accessToken = await getYouTubeAccessTokenForUser(userId)
  if (!accessToken) {
    const error = new Error('youtube_auth_missing')
    ;(error as any).code = 'youtube_auth_missing'
    throw error
  }

  const summaryEndpoint = new URL('https://youtubeanalytics.googleapis.com/v2/reports')
  summaryEndpoint.searchParams.set('ids', 'channel==MINE')
  summaryEndpoint.searchParams.set('startDate', startDate)
  summaryEndpoint.searchParams.set('endDate', endDate)
  summaryEndpoint.searchParams.set(
    'metrics',
    [
      'views',
      'estimatedMinutesWatched',
      'averageViewDuration',
      'averageViewPercentage',
      'likes',
      'comments',
      'shares'
    ].join(',')
  )
  summaryEndpoint.searchParams.set('filters', `video==${videoId}`)

  const summaryResponse = await fetchGoogleJson({
    url: summaryEndpoint.toString(),
    accessToken
  })
  if (!summaryResponse.ok) {
    const error = new Error(parseGoogleApiReason(summaryResponse.payload) || 'youtube_analytics_summary_failed')
    ;(error as any).code = 'youtube_analytics_summary_failed'
    ;(error as any).status = summaryResponse.status
    throw error
  }

  const summary: YouTubeAnalyticsSummary = {
    views: Math.max(0, Number(parseAnalyticsScalarMetric({ report: summaryResponse.payload, metric: 'views' }) || 0)),
    estimatedMinutesWatched: Math.max(
      0,
      Number(parseAnalyticsScalarMetric({ report: summaryResponse.payload, metric: 'estimatedMinutesWatched' }) || 0)
    ),
    averageViewDurationSeconds: (() => {
      const metric = parseAnalyticsScalarMetric({ report: summaryResponse.payload, metric: 'averageViewDuration' })
      return metric === null ? null : Number(clamp(metric, 0, 86400).toFixed(3))
    })(),
    averageViewPercentage: (() => {
      const metric = parseAnalyticsScalarMetric({ report: summaryResponse.payload, metric: 'averageViewPercentage' })
      return metric === null ? null : Number(clamp(metric, 0, 100).toFixed(3))
    })(),
    likes: Math.max(0, Number(parseAnalyticsScalarMetric({ report: summaryResponse.payload, metric: 'likes' }) || 0)),
    comments: Math.max(
      0,
      Number(parseAnalyticsScalarMetric({ report: summaryResponse.payload, metric: 'comments' }) || 0)
    ),
    shares: Math.max(0, Number(parseAnalyticsScalarMetric({ report: summaryResponse.payload, metric: 'shares' }) || 0))
  }

  const retentionEndpoint = new URL('https://youtubeanalytics.googleapis.com/v2/reports')
  retentionEndpoint.searchParams.set('ids', 'channel==MINE')
  retentionEndpoint.searchParams.set('startDate', startDate)
  retentionEndpoint.searchParams.set('endDate', endDate)
  retentionEndpoint.searchParams.set('dimensions', 'elapsedVideoTimeRatio')
  retentionEndpoint.searchParams.set('metrics', 'audienceWatchRatio,relativeRetentionPerformance')
  retentionEndpoint.searchParams.set('filters', `video==${videoId}`)
  retentionEndpoint.searchParams.set('sort', 'elapsedVideoTimeRatio')

  const retentionResponse = await fetchGoogleJson({
    url: retentionEndpoint.toString(),
    accessToken
  })
  if (!retentionResponse.ok) {
    const error = new Error(parseGoogleApiReason(retentionResponse.payload) || 'youtube_analytics_retention_failed')
    ;(error as any).code = 'youtube_analytics_retention_failed'
    ;(error as any).status = retentionResponse.status
    throw error
  }

  return {
    summary,
    retentionCurve: parseYouTubeRetentionCurve(retentionResponse.payload)
  }
}

const persistPlatformFeedbackForJob = async ({
  userId,
  jobId,
  feedback,
  youtubeSync
}: {
  userId: string
  jobId: string
  feedback: YouTubePlatformFeedback
  youtubeSync?: {
    videoId: string
    startDate: string
    endDate: string
    analyticsSummary: YouTubeAnalyticsSummary
    signalState: YouTubeSignalState
  } | null
}) => {
  const job = await prisma.job.findUnique({ where: { id: jobId } })
  if (!job || String(job.userId) !== String(userId)) {
    const error = new Error('job_not_found')
    ;(error as any).code = 'job_not_found'
    throw error
  }
  if (String(job.status || '').toLowerCase() !== 'completed') {
    const error = new Error('job_not_ready')
    ;(error as any).code = 'job_not_ready'
    throw error
  }
  const existingAnalysis = (job.analysis && typeof job.analysis === 'object')
    ? (job.analysis as Record<string, any>)
    : {}
  const history = Array.isArray(existingAnalysis.retention_feedback_history)
    ? existingAnalysis.retention_feedback_history
    : []
  const priorYouTubeSync = existingAnalysis.youtube_sync && typeof existingAnalysis.youtube_sync === 'object'
    ? existingAnalysis.youtube_sync
    : {}
  const nextYouTubeSync = youtubeSync
    ? {
      ...priorYouTubeSync,
      videoId: youtubeSync.videoId,
      linkedAt: priorYouTubeSync.linkedAt || new Date().toISOString(),
      lastSyncedAt: new Date().toISOString(),
      dateRange: {
        startDate: youtubeSync.startDate,
        endDate: youtubeSync.endDate
      },
      analyticsSummary: youtubeSync.analyticsSummary,
      signalState: youtubeSync.signalState
    }
    : priorYouTubeSync
  const nextAnalysis = {
    ...existingAnalysis,
    retention_feedback: feedback,
    retention_feedback_history: [
      ...history.slice(-39),
      feedback
    ],
    retention_feedback_updated_at: new Date().toISOString(),
    ...(youtubeSync
      ? {
        youtube_video_id: youtubeSync.videoId,
        youtubeVideoId: youtubeSync.videoId,
        youtube_sync: nextYouTubeSync
      }
      : {})
  }
  await prisma.job.update({
    where: { id: jobId },
    data: {
      analysis: nextAnalysis
    }
  })
  await upsertCreatorStyleProfileFromFeedback({
    userId,
    feedback
  }).catch((error) => {
    console.warn('creator style profile update failed after youtube sync', error)
  })
  await registerPolicyOutcomeForJob({
    userId,
    jobId,
    feedback,
    source: 'youtube_analytics_sync',
    isPlatform: true,
    metadata: {
      route: 'feedback/youtube/analytics/sync-job-feedback'
    }
  }).catch((error) => {
    console.warn('policy outcome registration failed after youtube sync', error)
  })
  const durationForReward = Number(job.inputDurationSeconds || 0)
  if (Number.isFinite(durationForReward) && durationForReward > 0.1) {
    const reward = derivePerSecondRewardSignal({
      durationSeconds: durationForReward,
      retentionPoints: [
        { timestamp: 0, watchedPct: Number(feedback.hookHoldPercent ?? feedback.watchPercent ?? 0) },
        { timestamp: durationForReward * 0.3, watchedPct: Number(feedback.first30Retention ?? feedback.watchPercent ?? 0) },
        { timestamp: durationForReward * 0.98, watchedPct: Number(feedback.completionPercent ?? feedback.watchPercent ?? 0) }
      ]
    })
    await ingestPlatformRewardSignal({
      userId,
      jobId,
      source: 'youtube_analytics_oauth',
      videoId: null,
      perSecondRewards: reward.perSecondRewards,
      summary: {
        ...reward.summary,
        from: 'youtube_sync'
      }
    }).catch((error) => {
      console.warn('platform reward ingest failed after youtube sync', error)
    })
  }

  let feedbackLoop: {
    applied: boolean
    reason: string
    last_applied_at: string | null
    last_applied_config_version_id: string | null
  } | null = null
  try {
    const loop = await runFeedbackLoop({
      trigger: 'platform_feedback_submission',
      actorUserId: userId
    })
    feedbackLoop = {
      applied: loop.applied,
      reason: loop.reason,
      last_applied_at: loop.status.runtime.last_applied_at,
      last_applied_config_version_id: loop.status.runtime.last_applied_config_version_id
    }
  } catch (error) {
    console.warn('feedback loop run failed after youtube platform sync', error)
  }

  return {
    jobId,
    feedback,
    feedbackLoop
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

router.get('/youtube/oauth/status', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const config = getYouTubeOAuthConfigStatus()
    const connection = await getYouTubeOAuthConnectionForUser(userId)
    return res.json({
      ...connection,
      authConfigured: config.configured,
      missingConfig: config.missing
    })
  } catch {
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/youtube/oauth/authorize', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const config = getYouTubeOAuthConfigStatus()
    if (!config.configured) {
      return res.status(503).json({
        error: 'youtube_oauth_not_configured',
        missingConfig: config.missing
      })
    }
    const session = await buildYouTubeOAuthAuthorizeUrl(userId)
    return res.json({
      ok: true,
      authUrl: session.authUrl,
      state: session.state,
      expiresAt: session.expiresAt
    })
  } catch (error: any) {
    const code = String(error?.code || '')
    if (code === 'youtube_oauth_not_configured') {
      return res.status(503).json({
        error: code,
        message: 'Google OAuth client credentials are missing.'
      })
    }
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/youtube/oauth/exchange', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const code = String(req.body?.code || '').trim()
    const state = String(req.body?.state || '').trim() || null
    if (!code) {
      return res.status(400).json({
        error: 'missing_oauth_code',
        message: 'Provide the OAuth authorization code.'
      })
    }
    const connection = await exchangeYouTubeOAuthCodeForUser({
      userId,
      code,
      state
    })
    return res.json({
      ok: true,
      connection
    })
  } catch (error: any) {
    const code = String(error?.code || '')
    if (code === 'invalid_oauth_state') {
      return res.status(400).json({
        error: code,
        message: 'OAuth state is invalid or expired. Start authorization again.'
      })
    }
    if (code === 'youtube_oauth_not_configured') {
      return res.status(503).json({
        error: code,
        message: 'Google OAuth client credentials are missing.'
      })
    }
    if (code === 'oauth_exchange_missing_access_token' || code === 'missing_oauth_code') {
      return res.status(400).json({
        error: code
      })
    }
    return res.status(502).json({
      error: 'youtube_oauth_exchange_failed',
      reason: String(error?.message || 'oauth_exchange_failed')
    })
  }
})

router.post('/youtube/oauth/disconnect', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    await disconnectYouTubeOAuthForUser(userId)
    return res.json({
      ok: true
    })
  } catch {
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/youtube/job-video/link', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const jobId = String(req.body?.jobId || req.body?.job_id || '').trim()
    if (!jobId) {
      return res.status(400).json({
        error: 'missing_job_id',
        message: 'Provide a jobId to link with YouTube video ID.'
      })
    }
    const videoId = parseYouTubeVideoId(
      req.body?.videoId ??
      req.body?.video_id ??
      req.body?.videoUrl ??
      req.body?.video_url ??
      req.body?.video
    )
    if (!videoId) {
      return res.status(400).json({
        error: 'invalid_youtube_video',
        message: 'Provide a valid YouTube video ID or URL.'
      })
    }

    const job = await prisma.job.findUnique({ where: { id: jobId } })
    if (!job || String(job.userId) !== userId) {
      return res.status(404).json({ error: 'job_not_found' })
    }

    const existingAnalysis = job.analysis && typeof job.analysis === 'object'
      ? (job.analysis as Record<string, any>)
      : {}
    const priorSync = existingAnalysis.youtube_sync && typeof existingAnalysis.youtube_sync === 'object'
      ? existingAnalysis.youtube_sync
      : {}
    const nextAnalysis = {
      ...existingAnalysis,
      youtube_video_id: videoId,
      youtubeVideoId: videoId,
      youtube_sync: {
        ...priorSync,
        videoId,
        linkedAt: new Date().toISOString()
      }
    }

    await prisma.job.update({
      where: { id: jobId },
      data: {
        analysis: nextAnalysis
      }
    })

    return res.json({
      ok: true,
      jobId,
      videoId
    })
  } catch {
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/youtube/video-metrics', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const apiKey = String(process.env.YOUTUBE_API_KEY || '').trim()
    const rawVideoInput =
      req.body?.videoId ??
      req.body?.video_id ??
      req.body?.videoUrl ??
      req.body?.video_url ??
      req.body?.video ??
      req.body?.id
    const videoId = parseYouTubeVideoId(rawVideoInput)
    if (!videoId) {
      return res.status(400).json({
        error: 'invalid_youtube_video',
        message: 'Provide a valid YouTube video ID or URL.'
      })
    }
    const metrics = await fetchYouTubeVideoMetrics({
      userId,
      videoId,
      apiKey
    })

    return res.json({
      ok: true,
      dataSource: metrics.dataSource,
      videoId: metrics.videoId,
      title: metrics.title,
      channelId: metrics.channelId,
      channelTitle: metrics.channelTitle,
      publishedAt: metrics.publishedAt,
      durationSeconds: metrics.durationSeconds,
      stats: {
        viewCount: metrics.viewCount,
        likeCount: metrics.likeCount,
        commentCount: metrics.commentCount,
        engagementRate: metrics.engagementRate
      }
    })
  } catch (error: any) {
    const code = String(error?.code || '')
    const message = String(error?.message || '')
    if (code === 'youtube_auth_missing') {
      return res.status(503).json({
        error: code,
        message: 'Connect YouTube OAuth or configure YOUTUBE_API_KEY.'
      })
    }
    if (code === 'youtube_video_not_found') {
      return res.status(404).json({
        error: code,
        message: 'No public YouTube video found for that ID.'
      })
    }
    if (code === 'youtube_data_api_failed') {
      return res.status(502).json({
        error: code,
        message: 'YouTube Data API request failed.',
        reason: message || null,
        status: Number(error?.status || 0) || null
      })
    }
    if (message.toLowerCase().includes('aborted')) {
      return res.status(504).json({
        error: 'youtube_api_timeout',
        message: 'YouTube Data API request timed out.'
      })
    }
    return res.status(500).json({
      error: 'youtube_api_server_error',
      message: 'Could not fetch YouTube metrics.'
    })
  }
})

router.post('/youtube/analytics/video-report', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const rawVideoInput =
      req.body?.videoId ??
      req.body?.video_id ??
      req.body?.videoUrl ??
      req.body?.video_url ??
      req.body?.video ??
      req.body?.id
    const videoId = parseYouTubeVideoId(rawVideoInput)
    if (!videoId) {
      return res.status(400).json({
        error: 'invalid_youtube_video',
        message: 'Provide a valid YouTube video ID or URL.'
      })
    }
    const { startDate, endDate } = resolveYouTubeAnalyticsDateRange(req.body || {})
    const report = await fetchYouTubeAnalyticsReport({
      userId,
      videoId,
      startDate,
      endDate
    })
    const historicalViews = await collectHistoricalYouTubeViewsForUser({ userId })
    const youtubeSignal = deriveYouTubeSignalState({
      historicalViews,
      currentVideoViews: report.summary.views
    })
    const platformFeedback = derivePlatformFeedbackFromYouTube({
      summary: report.summary,
      retentionCurve: report.retentionCurve,
      videoId,
      startDate,
      endDate,
      youtubeSignal
    })
    const metrics = await fetchYouTubeVideoMetrics({
      userId,
      videoId,
      apiKey: String(process.env.YOUTUBE_API_KEY || '').trim()
    }).catch(() => null)

    return res.json({
      ok: true,
      videoId,
      dateRange: {
        startDate,
        endDate
      },
      video: metrics,
      analytics: report,
      platformFeedback,
      youtubeSignal
    })
  } catch (error: any) {
    const code = String(error?.code || '')
    const reason = String(error?.message || '')
    if (code === 'youtube_auth_missing') {
      return res.status(401).json({
        error: code,
        message: 'Connect YouTube OAuth first to read YouTube Analytics.'
      })
    }
    if (code === 'youtube_analytics_summary_failed' || code === 'youtube_analytics_retention_failed') {
      return res.status(502).json({
        error: code,
        message: 'YouTube Analytics API request failed.',
        reason: reason || null,
        status: Number(error?.status || 0) || null
      })
    }
    return res.status(500).json({
      error: 'youtube_analytics_server_error',
      message: 'Could not fetch YouTube analytics report.'
    })
  }
})

router.post('/youtube/analytics/sync-job-feedback', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const jobId = String(req.body?.jobId || req.body?.job_id || '').trim()
    if (!jobId) {
      return res.status(400).json({
        error: 'missing_job_id',
        message: 'Provide a completed jobId to sync platform feedback.'
      })
    }
    const rawVideoInput =
      req.body?.videoId ??
      req.body?.video_id ??
      req.body?.videoUrl ??
      req.body?.video_url ??
      req.body?.video ??
      req.body?.id
    let videoId = parseYouTubeVideoId(rawVideoInput)
    if (!videoId) {
      const existingJob = await prisma.job.findUnique({
        where: { id: jobId },
        select: {
          userId: true,
          analysis: true
        }
      })
      if (!existingJob || String(existingJob.userId) !== userId) {
        const error = new Error('job_not_found')
        ;(error as any).code = 'job_not_found'
        throw error
      }
      const existingAnalysis = existingJob.analysis && typeof existingJob.analysis === 'object'
        ? (existingJob.analysis as Record<string, any>)
        : {}
      videoId = extractMappedYouTubeVideoId(existingAnalysis)
    }
    if (!videoId) {
      return res.status(400).json({
        error: 'invalid_youtube_video',
        message: 'Provide a valid YouTube video ID/URL or link this job to a YouTube video first.'
      })
    }
    const { startDate, endDate } = resolveYouTubeAnalyticsDateRange(req.body || {})
    const report = await fetchYouTubeAnalyticsReport({
      userId,
      videoId,
      startDate,
      endDate
    })
    const historicalViews = await collectHistoricalYouTubeViewsForUser({
      userId,
      excludeJobId: jobId
    })
    const youtubeSignal = deriveYouTubeSignalState({
      historicalViews,
      currentVideoViews: report.summary.views
    })
    const platformFeedback = derivePlatformFeedbackFromYouTube({
      summary: report.summary,
      retentionCurve: report.retentionCurve,
      videoId,
      startDate,
      endDate,
      youtubeSignal
    })
    const persisted = await persistPlatformFeedbackForJob({
      userId,
      jobId,
      feedback: platformFeedback,
      youtubeSync: {
        videoId,
        startDate,
        endDate,
        analyticsSummary: report.summary,
        signalState: youtubeSignal
      }
    })
    return res.json({
      ok: true,
      jobId,
      videoId,
      dateRange: {
        startDate,
        endDate
      },
      analytics: report,
      feedback: persisted.feedback,
      feedbackLoop: persisted.feedbackLoop,
      youtubeSignal
    })
  } catch (error: any) {
    const code = String(error?.code || '')
    const reason = String(error?.message || '')
    if (code === 'youtube_auth_missing') {
      return res.status(401).json({
        error: code,
        message: 'Connect YouTube OAuth first to read YouTube Analytics.'
      })
    }
    if (code === 'job_not_found') {
      return res.status(404).json({
        error: code
      })
    }
    if (code === 'job_not_ready') {
      return res.status(403).json({
        error: code,
        message: 'Job must be completed before syncing platform feedback.'
      })
    }
    if (code === 'youtube_analytics_summary_failed' || code === 'youtube_analytics_retention_failed') {
      return res.status(502).json({
        error: code,
        message: 'YouTube Analytics API request failed.',
        reason: reason || null,
        status: Number(error?.status || 0) || null
      })
    }
    return res.status(500).json({
      error: 'youtube_feedback_sync_failed',
      message: 'Could not sync YouTube analytics feedback to job.'
    })
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

    const bodyTrendSignals = req.body?.trendSignals && typeof req.body.trendSignals === 'object'
      ? req.body.trendSignals
      : null
    if (bodyTrendSignals) {
      input = {
        ...input,
        trendSignals: {
          tiktokShortBoost: parseTrendBoost(bodyTrendSignals?.tiktokShortBoost ?? bodyTrendSignals?.tiktok),
          youtubeLongBoost: parseTrendBoost(bodyTrendSignals?.youtubeLongBoost ?? bodyTrendSignals?.youtube),
          instagramCaptionBoost: parseTrendBoost(bodyTrendSignals?.instagramCaptionBoost ?? bodyTrendSignals?.instagram)
        }
      }
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
