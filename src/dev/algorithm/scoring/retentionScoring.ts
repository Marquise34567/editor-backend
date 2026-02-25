import { DEFAULT_ALGORITHM_PARAMS } from '../presets'
import {
  AlgorithmConfigParams,
  RetentionFeatures,
  RetentionScoringResult,
  RetentionSubscores,
  ScoreFlags,
  SegmentDecision,
  SegmentSignal,
  algorithmConfigParamsSchema
} from '../types'

type TranscriptCue = {
  start: number
  end: number
  text: string
}

type EngagementWindow = {
  start: number
  end: number
  score: number
  emotion: number
  speech: number
  novelty: number
  energy: number
}

type RetentionWeights = {
  w1: number
  w2: number
  w3: number
  w4: number
  w5: number
  w6: number
  w7: number
}

const DEFAULT_RETENTION_WEIGHTS: RetentionWeights = {
  w1: 1.78,
  w2: 1.35,
  w3: 1.2,
  w4: 1.08,
  w5: 1.42,
  w6: 1.22,
  w7: 1.48
}

const FILLER_WORDS = new Set([
  'uh',
  'um',
  'like',
  'you know',
  'basically',
  'actually',
  'literally',
  'sort of',
  'kind of',
  'i mean',
  'right',
  'okay',
  'ok',
  'so'
])

const CONTEXT_TERMS = /\b(this|that|these|those|because|means|definition|context|earlier|before|after|therefore|which)\b/i

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))
const clamp01 = (value: number) => clamp(value, 0, 1)
const sigmoid = (value: number) => 1 / (1 + Math.exp(-value))

const toNumber = (value: unknown, fallback = 0) => {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

const parseDuration = (videoAnalysis: Record<string, unknown>, cutList: SegmentSignal[]) => {
  const candidates = [
    toNumber(videoAnalysis.duration),
    toNumber((videoAnalysis.metadata_summary as any)?.durationSeconds),
    toNumber((videoAnalysis.metadata_summary as any)?.duration_seconds),
    toNumber(videoAnalysis.input_duration_seconds),
    cutList.length ? Math.max(...cutList.map((segment) => segment.end_sec)) : 0
  ]
  const duration = candidates.find((value) => value > 0.05) || 30
  return clamp(duration, 1, 6 * 60 * 60)
}

const normalizeText = (value: unknown) => String(value || '').replace(/\s+/g, ' ').trim()

const parseTranscriptCues = (transcript: unknown, durationSec: number): TranscriptCue[] => {
  if (!transcript) return []
  if (typeof transcript === 'string') {
    const text = normalizeText(transcript)
    if (!text) return []
    return [{ start: 0, end: durationSec, text }]
  }
  if (Array.isArray(transcript)) {
    const cues = transcript
      .map((entry) => {
        if (!entry || typeof entry !== 'object') return null
        const row = entry as Record<string, unknown>
        const text = normalizeText(row.text || row.caption || row.value)
        if (!text) return null
        const start = clamp(toNumber(row.start ?? row.startSec ?? row.s, 0), 0, durationSec)
        const endCandidate = toNumber(row.end ?? row.endSec ?? row.e, start + 0.5)
        const end = clamp(endCandidate > start ? endCandidate : start + 0.5, start + 0.1, durationSec)
        return { start, end, text }
      })
      .filter((entry): entry is TranscriptCue => Boolean(entry))
    if (cues.length) return cues
  }
  if (typeof transcript === 'object') {
    const record = transcript as Record<string, unknown>
    const nested = parseTranscriptCues(record.cues || record.items || record.transcript, durationSec)
    if (nested.length) return nested
    const text = normalizeText(record.text || record.raw || record.content)
    if (text) return [{ start: 0, end: durationSec, text }]
  }
  return []
}

const parseEngagementWindows = (videoAnalysis: Record<string, unknown>, durationSec: number): EngagementWindow[] => {
  const raw =
    (videoAnalysis.engagement_windows as any) ||
    (videoAnalysis.engagementWindows as any) ||
    (videoAnalysis.editPlan as any)?.engagementWindows ||
    []
  if (!Array.isArray(raw) || !raw.length) {
    return [
      {
        start: 0,
        end: durationSec,
        score: 0.5,
        emotion: 0.5,
        speech: 0.5,
        novelty: 0.5,
        energy: 0.5
      }
    ]
  }
  const windows = raw
    .map((entry) => {
      if (!entry || typeof entry !== 'object') return null
      const row = entry as Record<string, unknown>
      const start = clamp(toNumber(row.start, 0), 0, durationSec)
      const endRaw = toNumber(row.end, start + 1)
      const end = clamp(endRaw > start ? endRaw : start + 0.5, start + 0.1, durationSec)
      const score = clamp01(toNumber(row.score, toNumber(row.hookScore, 0.5)))
      const emotion = clamp01(toNumber(row.emotionIntensity, toNumber(row.emotion, score)))
      const speech = clamp01(toNumber(row.speechIntensity, toNumber(row.speech, 0.45)))
      const novelty = clamp01(toNumber(row.novelty, toNumber(row.visualNovelty, 0.45)))
      const energy = clamp01(toNumber(row.energy, (score + emotion + speech) / 3))
      return { start, end, score, emotion, speech, novelty, energy }
    })
    .filter((entry): entry is EngagementWindow => Boolean(entry))
  return windows.length
    ? windows
    : [
        {
          start: 0,
          end: durationSec,
          score: 0.5,
          emotion: 0.5,
          speech: 0.5,
          novelty: 0.5,
          energy: 0.5
        }
      ]
}

const parseSegmentRanges = (videoAnalysis: Record<string, unknown>, cutList: unknown, durationSec: number) => {
  const fromInput = Array.isArray(cutList) ? cutList : []
  const fromAnalysis = Array.isArray((videoAnalysis.editPlan as any)?.segments)
    ? ((videoAnalysis.editPlan as any).segments as unknown[])
    : []
  const source = fromInput.length ? fromInput : fromAnalysis
  const segments = source
    .map((entry, index) => {
      if (!entry || typeof entry !== 'object') return null
      const row = entry as Record<string, unknown>
      const start = clamp(toNumber(row.start, 0), 0, durationSec)
      const endRaw = toNumber(row.end, start + 1)
      const end = clamp(endRaw > start ? endRaw : start + 0.1, start + 0.1, durationSec)
      const speed = clamp(toNumber(row.speed, 1), 0.65, 1.7)
      return { index, start, end, speed }
    })
    .filter((entry): entry is { index: number; start: number; end: number; speed: number } => Boolean(entry))

  if (segments.length) return segments

  const chunkSize = clamp(durationSec / 10, 1.5, 6.5)
  const autoSegments: Array<{ index: number; start: number; end: number; speed: number }> = []
  for (let i = 0; i < durationSec; i += chunkSize) {
    const start = Number(i.toFixed(3))
    const end = Number(clamp(i + chunkSize, start + 0.1, durationSec).toFixed(3))
    autoSegments.push({ index: autoSegments.length, start, end, speed: 1 })
    if (end >= durationSec) break
  }
  return autoSegments
}

const overlapDuration = (aStart: number, aEnd: number, bStart: number, bEnd: number) => {
  const start = Math.max(aStart, bStart)
  const end = Math.min(aEnd, bEnd)
  return Math.max(0, end - start)
}

const weightedWindowMetric = (
  windows: EngagementWindow[],
  start: number,
  end: number,
  getValue: (window: EngagementWindow) => number,
  fallback: number
) => {
  let weighted = 0
  let weight = 0
  for (const window of windows) {
    const overlap = overlapDuration(start, end, window.start, window.end)
    if (overlap <= 0) continue
    weighted += getValue(window) * overlap
    weight += overlap
  }
  if (weight <= 0) return fallback
  return weighted / weight
}

const countWords = (text: string) => {
  if (!text.trim()) return 0
  return text.trim().split(/\s+/).filter(Boolean).length
}

const countFillerWords = (text: string) => {
  const lower = normalizeText(text).toLowerCase()
  if (!lower) return 0
  let total = 0
  for (const filler of FILLER_WORDS) {
    const escaped = filler.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    const regex = new RegExp(`\\b${escaped}\\b`, 'g')
    const matches = lower.match(regex)
    if (matches) total += matches.length
  }
  return total
}

const computeRedundancyFromText = (text: string) => {
  const words = normalizeText(text)
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
  if (words.length < 6) return 0
  const counts = new Map<string, number>()
  for (let i = 0; i < words.length - 1; i += 1) {
    const bi = `${words[i]} ${words[i + 1]}`
    counts.set(bi, (counts.get(bi) || 0) + 1)
  }
  const repeated = Array.from(counts.values()).reduce((sum, count) => sum + Math.max(0, count - 1), 0)
  return clamp01(repeated / Math.max(1, words.length / 2))
}

const average = (values: number[]) => {
  if (!values.length) return 0
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

const variance = (values: number[], avg: number) => {
  if (!values.length) return 0
  return values.reduce((sum, value) => sum + (value - avg) ** 2, 0) / values.length
}

const percentile = (values: number[], ratio: number) => {
  if (!values.length) return 0
  const sorted = values.slice().sort((a, b) => a - b)
  const index = clamp(Math.floor((sorted.length - 1) * ratio), 0, sorted.length - 1)
  return sorted[index]
}

const extractConfigWeights = (configParams: AlgorithmConfigParams): RetentionWeights => {
  const maybeWeights = (configParams as any).scoring_weights
  if (!maybeWeights || typeof maybeWeights !== 'object') return DEFAULT_RETENTION_WEIGHTS
  const value = maybeWeights as Record<string, unknown>
  return {
    w1: clamp(toNumber(value.w1, DEFAULT_RETENTION_WEIGHTS.w1), 0.2, 3.5),
    w2: clamp(toNumber(value.w2, DEFAULT_RETENTION_WEIGHTS.w2), 0.2, 3.5),
    w3: clamp(toNumber(value.w3, DEFAULT_RETENTION_WEIGHTS.w3), 0.2, 3.5),
    w4: clamp(toNumber(value.w4, DEFAULT_RETENTION_WEIGHTS.w4), 0.2, 3.5),
    w5: clamp(toNumber(value.w5, DEFAULT_RETENTION_WEIGHTS.w5), 0.2, 3.5),
    w6: clamp(toNumber(value.w6, DEFAULT_RETENTION_WEIGHTS.w6), 0.2, 3.5),
    w7: clamp(toNumber(value.w7, DEFAULT_RETENTION_WEIGHTS.w7), 0.2, 3.5)
  }
}

const computeSegmentDecisions = (
  features: RetentionFeatures,
  configParams: AlgorithmConfigParams
): { decisions: SegmentDecision[]; flags: ScoreFlags; keepRatio: number; dropRatio: number; predictedJank: number } => {
  const decisions: SegmentDecision[] = []
  const minClipSec = configParams.min_clip_len_ms / 1000
  const maxClipSec = configParams.max_clip_len_ms / 1000
  const thresholdT = -0.85 + (configParams.cut_aggression / 100) * 1.7
  const lambda = 0.7 + (configParams.jank_guard / 100) * 1.4
  const contextScale = 0.6 + (configParams.story_coherence_guard / 100) * 1.6

  const segmentWeightConfig = ((configParams as any).segment_weights || {}) as Record<string, unknown>
  const a = clamp(toNumber(segmentWeightConfig.a, 1.35), 0.2, 2.8)
  const b = clamp(toNumber(segmentWeightConfig.b, 1.1), 0.2, 2.8)
  const c = clamp(toNumber(segmentWeightConfig.c, 1.02), 0.2, 2.8)
  const d = clamp(toNumber(segmentWeightConfig.d, 0.95), 0.2, 2.8)
  const e = clamp(toNumber(segmentWeightConfig.e, 1.08), 0.2, 2.8)
  const f = clamp(toNumber(segmentWeightConfig.f, 1.02), 0.2, 2.8)
  const g = clamp(toNumber(segmentWeightConfig.g, 1.18), 0.2, 2.8)
  const h = clamp(toNumber(segmentWeightConfig.h, 1.36), 0.2, 2.8)
  const j = clamp(toNumber(segmentWeightConfig.j, 1.31), 0.2, 2.8)

  let microCrossfadeRequired = false
  for (const segment of features.segment_signals) {
    const valueScore =
      a * segment.energy +
      b * segment.info_density +
      c * segment.novelty +
      d * segment.emotion -
      e * segment.filler -
      f * segment.redundancy
    const scaledContextRisk = clamp01(segment.context_loss_risk * contextScale)
    const riskScore = g * segment.continuity_risk + h * scaledContextRisk + j * segment.audio_jank_risk
    let keepProbability = sigmoid(valueScore - lambda * riskScore - thresholdT)
    const reasons: string[] = []

    if (segment.duration_sec < minClipSec) {
      keepProbability = Math.max(keepProbability, 0.72)
      reasons.push('forced_keep_min_clip')
    }
    if (segment.duration_sec > maxClipSec) {
      keepProbability = Math.min(keepProbability, 0.46)
      reasons.push('over_max_clip_len')
    }
    if (configParams.story_coherence_guard >= 70 && segment.is_context_segment && keepProbability < 0.63) {
      keepProbability = 0.63
      reasons.push('context_preservation_guard')
    }
    if (segment.audio_jank_risk > 0.78 && keepProbability < 0.58) {
      keepProbability = 0.58
      microCrossfadeRequired = true
      reasons.push('audio_jank_guard_crossfade')
    }

    decisions.push({
      index: segment.index,
      start_sec: segment.start_sec,
      end_sec: segment.end_sec,
      value_score: Number(valueScore.toFixed(4)),
      risk_score: Number(riskScore.toFixed(4)),
      keep_probability: Number(clamp01(keepProbability).toFixed(4)),
      keep_recommendation: keepProbability >= 0.5,
      continuity_risk: Number(segment.continuity_risk.toFixed(4)),
      context_loss_risk: Number(scaledContextRisk.toFixed(4)),
      audio_jank_risk: Number(segment.audio_jank_risk.toFixed(4)),
      reasons
    })
  }

  const riskyDrops = decisions
    .filter((decision) => !decision.keep_recommendation)
    .map((decision) => (decision.context_loss_risk + decision.audio_jank_risk + decision.continuity_risk) / 3)
  const avgRiskyDrop = average(riskyDrops)
  const predictedJank = clamp01(
    0.44 * features.jump_cut_severity +
      0.26 * clamp01(features.audio_discontinuity_events / Math.max(1, features.segment_signals.length)) +
      0.3 * avgRiskyDrop
  )
  const jankThreshold = 0.58 - (configParams.jank_guard / 100) * 0.25
  let autoSafetyAdjusted = false
  let adjustedCutAggression: number | undefined

  if (predictedJank > jankThreshold && decisions.length > 0) {
    autoSafetyAdjusted = true
    const riskGap = predictedJank - jankThreshold
    adjustedCutAggression = clamp(configParams.cut_aggression - Math.round(riskGap * 42 + 8), 0, 100)
    const lift = clamp01(riskGap * 0.48 + 0.1)
    for (const decision of decisions) {
      if (decision.keep_probability >= 0.8) continue
      const riskBias = clamp01(
        0.45 * decision.continuity_risk +
          0.3 * decision.context_loss_risk +
          0.25 * decision.audio_jank_risk
      )
      const nextKeep = clamp01(decision.keep_probability + lift * (0.86 - decision.keep_probability) * riskBias)
      decision.keep_probability = Number(nextKeep.toFixed(4))
      decision.keep_recommendation = nextKeep >= 0.5
      decision.reasons = Array.from(new Set([...decision.reasons, 'auto_safety_jank_adjust']))
    }
  }

  const keepRatio = decisions.length
    ? decisions.filter((decision) => decision.keep_recommendation).length / decisions.length
    : 1

  return {
    decisions,
    flags: {
      auto_safety_adjusted: autoSafetyAdjusted,
      reason: autoSafetyAdjusted ? 'predicted_jank_exceeded_threshold' : undefined,
      jank_risk: Number(predictedJank.toFixed(4)),
      micro_crossfade_required: microCrossfadeRequired,
      adjusted_cut_aggression: adjustedCutAggression
    },
    keepRatio: Number(keepRatio.toFixed(4)),
    dropRatio: Number((1 - keepRatio).toFixed(4)),
    predictedJank
  }
}

export const computeFeatures = (
  videoAnalysis: unknown,
  transcript: unknown,
  cutList: unknown
): RetentionFeatures => {
  const analysis = videoAnalysis && typeof videoAnalysis === 'object' ? (videoAnalysis as Record<string, unknown>) : {}

  const durationSeedSegments = parseSegmentRanges(analysis, cutList, 120)
  const durationSec = parseDuration(
    analysis,
    durationSeedSegments.map((segment) => ({
      index: segment.index,
      start_sec: segment.start,
      end_sec: segment.end,
      duration_sec: segment.end - segment.start,
      energy: 0,
      info_density: 0,
      novelty: 0,
      emotion: 0,
      filler: 0,
      redundancy: 0,
      continuity_risk: 0,
      context_loss_risk: 0,
      audio_jank_risk: 0,
      is_context_segment: false
    }))
  )
  const windows = parseEngagementWindows(analysis, durationSec)
  const cues = parseTranscriptCues(transcript || analysis.transcript || (analysis.editPlan as any)?.transcriptSignals, durationSec)
  const segments = parseSegmentRanges(analysis, cutList, durationSec)

  const transcriptText = cues.map((cue) => cue.text).join(' ').trim()
  const transcriptWordCount = countWords(transcriptText)
  const fillerWords = countFillerWords(transcriptText)
  const fillerWordsPerMin = durationSec > 0 ? fillerWords / (durationSec / 60) : 0

  const silenceRatioFromAnalysis = toNumber(analysis.silence_ratio, NaN)
  const silenceRatioFromWindows = (() => {
    let silent = 0
    for (const window of windows) {
      const length = Math.max(0, window.end - window.start)
      if (window.speech < 0.2) silent += length
    }
    return durationSec > 0 ? silent / durationSec : 0
  })()
  const silenceRatio = clamp01(Number.isFinite(silenceRatioFromAnalysis) ? silenceRatioFromAnalysis : silenceRatioFromWindows)

  const cutCount = Math.max(1, segments.length)
  const avgShotLenSec = durationSec / cutCount
  const cutRatePerMin = durationSec > 0 ? cutCount / (durationSec / 60) : 0

  const redundancyScoreRaw = toNumber(analysis.redundancy_score, NaN)
  const redundancyScore = clamp01(Number.isFinite(redundancyScoreRaw) ? redundancyScoreRaw : computeRedundancyFromText(transcriptText))

  const energySeries = windows.map((window) => window.energy)
  const energyMean = clamp01(average(energySeries) || 0.5)
  const energyVariance = clamp01(variance(energySeries, energyMean) * 6.4)
  const spikeThreshold = percentile(energySeries, 0.8)
  const spikeDensity = windows.length
    ? windows.filter((window) => window.energy >= spikeThreshold).length / windows.length
    : 0

  let flatSegmentSeconds = 0
  let rollingFlat = 0
  for (const window of windows) {
    const localVariance = Math.abs(window.energy - energyMean)
    const span = Math.max(0.1, window.end - window.start)
    if (localVariance < 0.08) {
      rollingFlat += span
      flatSegmentSeconds = Math.max(flatSegmentSeconds, rollingFlat)
    } else {
      rollingFlat = 0
    }
  }

  const jumpCutFromAnalysis = toNumber(analysis.jump_cut_severity, NaN)
  const ultraShortRatio = segments.length
    ? segments.filter((segment) => segment.end - segment.start <= 0.35).length / segments.length
    : 0
  const rapidBackToBackRatio = segments.length > 1
    ? segments
        .slice(1)
        .filter((segment, index) => {
          const prev = segments[index]
          return segment.start - prev.end < 0.03 || segment.end - segment.start < 0.5
        }).length / Math.max(1, segments.length - 1)
    : 0
  const jumpCutSeverity = clamp01(
    Number.isFinite(jumpCutFromAnalysis)
      ? jumpCutFromAnalysis
      : 0.55 * ultraShortRatio + 0.45 * rapidBackToBackRatio
  )

  const audioDiscontinuityEvents = Math.max(
    0,
    Math.round(
      toNumber(
        analysis.audio_discontinuity_events,
        toNumber((analysis.metadata_summary as any)?.audioDiscontinuityEvents, jumpCutSeverity * segments.length * 0.35)
      )
    )
  )
  const captionDesyncEvents = Math.max(
    0,
    Math.round(
      toNumber(
        analysis.caption_desync_events,
        toNumber((analysis.metadata_summary as any)?.captionDesyncEvents, 0)
      )
    )
  )

  let hookTimeToPayoff = durationSec
  let bestMomentInFirst8s = 0
  const earlyWindowEnd = Math.min(8, durationSec)
  for (const window of windows) {
    const infoBlend = clamp01(0.55 * window.score + 0.25 * window.speech + 0.2 * window.emotion)
    const signal = clamp01(0.5 * window.energy + 0.5 * infoBlend)
    if (window.start <= earlyWindowEnd) {
      bestMomentInFirst8s = Math.max(bestMomentInFirst8s, signal)
    }
    if (signal >= 0.82) {
      hookTimeToPayoff = Math.min(hookTimeToPayoff, Math.max(0, window.start))
    }
  }
  if (hookTimeToPayoff >= durationSec) {
    hookTimeToPayoff = clamp(durationSec * (1 - bestMomentInFirst8s), 0, durationSec)
  }

  const segmentSignals: SegmentSignal[] = segments.map((segment, index) => {
    const span = Math.max(0.1, segment.end - segment.start)
    const segmentText = cues
      .filter((cue) => overlapDuration(segment.start, segment.end, cue.start, cue.end) > 0)
      .map((cue) => cue.text)
      .join(' ')
      .trim()
    const words = countWords(segmentText)
    const segmentFiller = countFillerWords(segmentText)
    const infoDensity = clamp01(
      words > 0
        ? words / Math.max(1, 3.2 * span)
        : weightedWindowMetric(windows, segment.start, segment.end, (window) => window.speech, 0.35)
    )
    const energy = clamp01(weightedWindowMetric(windows, segment.start, segment.end, (window) => window.energy, energyMean))
    const emotion = clamp01(weightedWindowMetric(windows, segment.start, segment.end, (window) => window.emotion, energyMean))
    const novelty = clamp01(
      weightedWindowMetric(windows, segment.start, segment.end, (window) => window.novelty, 0.42) *
        (1 - redundancyScore * 0.45)
    )
    const localRedundancy = clamp01(
      computeRedundancyFromText(segmentText || transcriptText.slice(Math.floor((index / Math.max(1, segments.length)) * transcriptText.length), Math.floor(((index + 1) / Math.max(1, segments.length)) * transcriptText.length)))
    )
    const continuityRisk = clamp01(
      (span < 0.45 ? 0.5 : 0) +
        (segment.speed > 1.28 ? 0.32 : 0) +
        Math.abs(energy - emotion) * 0.45
    )
    const contextSegment = CONTEXT_TERMS.test(segmentText)
    const contextLossRisk = clamp01(
      contextSegment
        ? 0.72
        : 0.22 + (segment.start > durationSec * 0.66 ? 0.1 : 0) + infoDensity * 0.14
    )
    const audioJankRisk = clamp01(
      (span < 0.4 ? 0.42 : 0) +
        Math.abs(weightedWindowMetric(windows, segment.start, segment.end, (window) => window.speech, 0.5) - 0.5) * 0.5 +
        (audioDiscontinuityEvents > 0 ? Math.min(0.3, audioDiscontinuityEvents / Math.max(2, segments.length * 2)) : 0)
    )

    return {
      index,
      start_sec: Number(segment.start.toFixed(3)),
      end_sec: Number(segment.end.toFixed(3)),
      duration_sec: Number(span.toFixed(3)),
      energy: Number(energy.toFixed(4)),
      info_density: Number(infoDensity.toFixed(4)),
      novelty: Number(novelty.toFixed(4)),
      emotion: Number(emotion.toFixed(4)),
      filler: Number(clamp01(segmentFiller / Math.max(1, span * 4)).toFixed(4)),
      redundancy: Number(localRedundancy.toFixed(4)),
      continuity_risk: Number(continuityRisk.toFixed(4)),
      context_loss_risk: Number(contextLossRisk.toFixed(4)),
      audio_jank_risk: Number(audioJankRisk.toFixed(4)),
      is_context_segment: contextSegment
    }
  })

  const baselineDecisions: SegmentDecision[] = segmentSignals.map((segment) => {
    const raw = sigmoid(
      1.15 * segment.energy +
        1.05 * segment.info_density +
        0.9 * segment.novelty +
        0.8 * segment.emotion -
        1.1 * segment.filler -
        0.95 * segment.redundancy -
        0.4
    )
    return {
      index: segment.index,
      start_sec: segment.start_sec,
      end_sec: segment.end_sec,
      value_score: Number(raw.toFixed(4)),
      risk_score: Number(((segment.continuity_risk + segment.context_loss_risk + segment.audio_jank_risk) / 3).toFixed(4)),
      keep_probability: Number(raw.toFixed(4)),
      keep_recommendation: raw >= 0.5,
      continuity_risk: segment.continuity_risk,
      context_loss_risk: segment.context_loss_risk,
      audio_jank_risk: segment.audio_jank_risk,
      reasons: []
    }
  })
  const baselineKeepRatio = baselineDecisions.length
    ? baselineDecisions.filter((decision) => decision.keep_recommendation).length / baselineDecisions.length
    : 1

  const missingSignals: string[] = []
  if (!normalizeText(transcriptText)) missingSignals.push('transcript')
  if (!Array.isArray(cutList) || !cutList.length) missingSignals.push('cut_list')
  if (!Array.isArray((analysis.editPlan as any)?.engagementWindows) && !Array.isArray(analysis.engagement_windows)) {
    missingSignals.push('engagement_windows')
  }
  if (!Number.isFinite(toNumber(analysis.silence_ratio, NaN))) missingSignals.push('silence_ratio')
  if (!Number.isFinite(toNumber(analysis.jump_cut_severity, NaN))) missingSignals.push('jump_cut_severity')

  return {
    duration_sec: Number(durationSec.toFixed(3)),
    silence_ratio: Number(silenceRatio.toFixed(4)),
    filler_words_per_min: Number(fillerWordsPerMin.toFixed(4)),
    avg_shot_len_sec: Number(avgShotLenSec.toFixed(4)),
    cut_rate_per_min: Number(cutRatePerMin.toFixed(4)),
    redundancy_score: Number(redundancyScore.toFixed(4)),
    energy_mean: Number(energyMean.toFixed(4)),
    energy_variance: Number(energyVariance.toFixed(4)),
    spike_density: Number(clamp01(spikeDensity).toFixed(4)),
    flat_segment_seconds: Number(flatSegmentSeconds.toFixed(4)),
    jump_cut_severity: Number(jumpCutSeverity.toFixed(4)),
    audio_discontinuity_events: audioDiscontinuityEvents,
    caption_desync_events: captionDesyncEvents,
    hook_time_to_payoff: Number(clamp(hookTimeToPayoff, 0, durationSec).toFixed(4)),
    best_moment_in_first8s_score: Number(clamp01(bestMomentInFirst8s).toFixed(4)),
    segment_signals: segmentSignals,
    segment_decisions: baselineDecisions,
    keep_ratio: Number(baselineKeepRatio.toFixed(4)),
    drop_ratio: Number((1 - baselineKeepRatio).toFixed(4)),
    missing_signals: Array.from(new Set(missingSignals))
  }
}

export const computeSubscores = (
  features: RetentionFeatures,
  configParams: AlgorithmConfigParams
): RetentionSubscores => {
  const validated = algorithmConfigParamsSchema.parse(configParams)
  const decisionsResult = computeSegmentDecisions(features, validated)
  const decisions = decisionsResult.decisions
  const earlyDecisions = decisions.filter((decision) => decision.start_sec <= 8)
  const contextDecisions = decisions.filter((decision) => decision.context_loss_risk >= 0.55)

  const payoffNorm = features.duration_sec > 0 ? clamp01(features.hook_time_to_payoff / Math.max(1, features.duration_sec * 0.35)) : 1
  const earlyKeepQuality = earlyDecisions.length
    ? average(earlyDecisions.map((decision) => decision.keep_probability))
    : features.keep_ratio

  const pacingTarget = clamp(2.8 + validated.pacing_multiplier * 2.8 + validated.cut_aggression / 24, 2, 13)
  const pacingDeviation = Math.abs(features.cut_rate_per_min - pacingTarget) / Math.max(0.5, pacingTarget)

  const noveltyMean = features.segment_signals.length
    ? average(features.segment_signals.map((segment) => segment.novelty))
    : 0.4
  const contextDropRisk = contextDecisions.length
    ? average(
        contextDecisions.map((decision) =>
          decision.keep_recommendation ? 0 : decision.context_loss_risk
        )
      )
    : 0
  const keepContextRatio = contextDecisions.length
    ? contextDecisions.filter((decision) => decision.keep_recommendation).length / contextDecisions.length
    : 1

  const fillerNormalized = clamp01(features.filler_words_per_min / 12)
  const audioDiscNorm = clamp01(features.audio_discontinuity_events / Math.max(1, features.segment_signals.length))
  const captionDiscNorm = clamp01(features.caption_desync_events / Math.max(1, features.segment_signals.length))

  const H = clamp01(
    0.52 * features.best_moment_in_first8s_score +
      0.26 * (1 - payoffNorm) +
      0.22 * earlyKeepQuality * clamp(0.6 + validated.hook_priority_weight * 0.25, 0.4, 1.5)
  )

  const P = clamp01(
    0.56 * (1 - pacingDeviation) +
      0.2 * clamp01(1 - features.flat_segment_seconds / Math.max(1, features.duration_sec * 0.4)) +
      0.24 * decisionsResult.keepRatio
  )

  const E = clamp01(
    0.48 * clamp01(features.energy_variance * 1.35) +
      0.32 * features.spike_density * clamp(0.7 + validated.spike_boost * 0.22, 0.5, 1.4) +
      0.2 * clamp01((features.energy_mean - validated.energy_floor + 0.5) / 1.2)
  )

  const V = clamp01(
    0.52 * (1 - features.redundancy_score * validated.redundancy_weight / 2) +
      0.31 * noveltyMean +
      0.17 * clamp01(validated.pattern_interrupt_every_sec <= 0 ? 0.5 : 8 / validated.pattern_interrupt_every_sec)
  )

  const S = clamp01(
    0.44 * keepContextRatio +
      0.36 * (1 - contextDropRisk * clamp(0.5 + validated.story_coherence_guard / 100, 0.5, 1.8)) +
      0.2 * (1 - features.jump_cut_severity * 0.5)
  )

  const F = clamp01(
    0.4 * features.silence_ratio +
      0.35 * fillerNormalized * clamp(validated.filler_word_weight / 1.1, 0.5, 2.6) +
      0.25 * features.redundancy_score * clamp(validated.redundancy_weight / 1.1, 0.5, 2.6)
  )

  const J = clamp01(
    0.46 * features.jump_cut_severity +
      0.22 * audioDiscNorm +
      0.14 * captionDiscNorm +
      0.18 * decisionsResult.predictedJank
  )

  return {
    H: Number(H.toFixed(4)),
    P: Number(P.toFixed(4)),
    E: Number(E.toFixed(4)),
    V: Number(V.toFixed(4)),
    S: Number(S.toFixed(4)),
    F: Number(F.toFixed(4)),
    J: Number(J.toFixed(4))
  }
}

export const computeRetentionScore = (
  subscores: RetentionSubscores,
  weights?: Partial<RetentionWeights>
): number => {
  const resolvedWeights = {
    ...DEFAULT_RETENTION_WEIGHTS,
    ...(weights || {})
  }
  const x =
    resolvedWeights.w1 * subscores.H +
    resolvedWeights.w2 * subscores.P +
    resolvedWeights.w3 * subscores.E +
    resolvedWeights.w4 * subscores.V +
    resolvedWeights.w5 * subscores.S -
    resolvedWeights.w6 * subscores.F -
    resolvedWeights.w7 * subscores.J
  const score = 100 * sigmoid(x)
  return Number(clamp(score, 0, 100).toFixed(4))
}

export const evaluateRetentionScoring = (
  videoAnalysis: unknown,
  transcript: unknown,
  cutList: unknown,
  configParamsRaw?: Partial<AlgorithmConfigParams> | null
): RetentionScoringResult => {
  const configParams = algorithmConfigParamsSchema.parse({
    ...DEFAULT_ALGORITHM_PARAMS,
    ...(configParamsRaw || {})
  })
  const features = computeFeatures(videoAnalysis, transcript, cutList)
  const decisionsResult = computeSegmentDecisions(features, configParams)
  const scoredFeatures: RetentionFeatures = {
    ...features,
    segment_decisions: decisionsResult.decisions,
    keep_ratio: decisionsResult.keepRatio,
    drop_ratio: decisionsResult.dropRatio
  }
  const subscores = computeSubscores(scoredFeatures, configParams)
  const scoreTotal = computeRetentionScore(subscores, extractConfigWeights(configParams))

  return {
    score_total: scoreTotal,
    subscores,
    features: scoredFeatures,
    flags: decisionsResult.flags
  }
}
