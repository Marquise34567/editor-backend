const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

export type PlannerMode = 'horizontal' | 'vertical'

export type PlannerVideoMetadata = {
  width: number
  height: number
  duration: number
  fps: number
}

export type PlannerFrameScan = {
  portraitSignal: number
  landscapeSignal: number
  centeredFaceVerticalSignal: number
  horizontalMotionSignal: number
  highMotionShortClipSignal: number
  motionPeaks: number[]
}

export type PlannerTranscriptSegment = {
  start: number
  end: number
  text: string
  confidence: number | null
}

export type HookCandidateSource = 'motion_peak' | 'transcript' | 'intro_fallback' | 'hybrid'

export type HookCandidate = {
  id: string
  start: number
  end: number
  duration: number
  transcript: string
  source: HookCandidateSource
  reason: string
  scores: {
    motion: number
    audio: number
    sentiment: number
    llm: number
    combined: number
  }
}

export type PacingAdjustmentAction = 'trim' | 'speed_up' | 'transition_boost'

export type PacingAdjustment = {
  start: number
  end: number
  action: PacingAdjustmentAction
  intensity: number
  speedMultiplier?: number
  reason: string
}

export type HookComparison = {
  id: string
  start: number
  end: number
  predictedRetentionLift: number
  reason: string
}

export type SegmentRetentionInsight = {
  id: string
  start: number
  end: number
  predictedRetention: number
  reason: string
  fix?: string
}

export type RetentionTitleSuggestion = {
  id: string
  title: string
  explanation: string
  confidence: number
}

export type PredictionConfidenceLevel = 'low' | 'medium' | 'high'

export type FreeAiHookPlan = {
  provider: 'ruthless_retention_prompt' | 'heuristic'
  model: string | null
  selectedHook: HookCandidate | null
  rankedHooks: HookCandidate[]
  pacingAdjustments: PacingAdjustment[]
  hookComparison: HookComparison[]
  weakSegments: SegmentRetentionInsight[]
  strongSegments: SegmentRetentionInsight[]
  predictedAverageRetention: number
  predictionConfidence: number
  predictionConfidenceLevel: PredictionConfidenceLevel
  retentionProtectionChanges: string[]
  finalSummary: string
  titleSuggestions: RetentionTitleSuggestion[]
  notes: string[]
  prompts: {
    eligibility: string
    ranking: string
    pacing: string
  }
}

type PlannerInput = {
  mode: PlannerMode
  metadata: PlannerVideoMetadata
  frameScan: PlannerFrameScan
  transcriptSegments: PlannerTranscriptSegment[]
  transcriptExcerpt: string
}

const HYPE_WORDS = [
  'insane',
  'crazy',
  'wait',
  'watch',
  'what',
  'no way',
  'unbelievable',
  'wild',
  'shocking',
  'secret',
  'proof',
  'challenge',
  'failed',
  'won',
  'instant',
  'imagine',
  'question',
  'why',
  'how'
]

const POSITIVE_WORDS = [
  'amazing',
  'love',
  'great',
  'excited',
  'hype',
  'perfect',
  'best',
  'win',
  'awesome',
  'happy',
  'legendary',
  'fire'
]

const NEGATIVE_WORDS = [
  'boring',
  'sad',
  'hate',
  'bad',
  'slow',
  'confused',
  'awful',
  'problem',
  'annoying',
  'worse'
]

const FILLER_PHRASES = [
  'uh',
  'um',
  'erm',
  'ah',
  'you know',
  'i mean',
  'like',
  'sort of',
  'kind of',
  'basically',
  'actually',
  'literally'
]

const HESITATION_PATTERN = /\b(uh+|um+|er+|ah+|hmm+|mm+)\b/gi
const WORD_PATTERN = /[a-z0-9']+/gi
const SENTENCE_END_PATTERN = /[.!?]["')\]]*$/i
const FRAME_ALIGNMENT_FALLBACK_FPS = 30

const STRICT_RETENTION_PREFIX = 'Follow the exact retention-maximizing process above. Do NOT deviate. Output must be structured.'

const RUTHLESS_RETENTION_PLAYBOOK = `You are AutoEditor's ruthless retention-maximizing AI brain.
Mission: maximize average retention percent and full-watch completion, not runtime.
2026 ranking truth: a 5-minute video at 85% average retention usually beats a 15-minute video at 45%.
Protect consistent retention across the full runtime and avoid deep valleys.

Mandatory rules:
1) Remove or compress any segment with predicted drop-off >15-20%.
2) Prioritize tighter edits over longer edits when retention improves.
3) Build a can't-stop loop: curiosity -> tension -> payoff -> new curiosity every 15-30s.
4) First 8-15s must be the strongest hook candidate and force continuation.
5) End on a satisfying payoff and subtle next-step tease.
6) For dull zones, use trim + speed-up (1.3x-1.8x) + text tease when helpful.
7) Be brutally honest in segment evaluations:
   - Strong segment reason format: "Excellent - 92% retention hold here due to ... This keeps viewers locked in."
   - Weak segment reason format: "Danger zone - predicted 35% drop-off because ... Fix: ..."
   - Hook reason format: "Selected this 8-second opener over alternatives because ..."

Required output fields for every full-video analysis:
- predicted_average_retention_percent
- confidence_percent
- confidence_level (high|medium|low)
- retention_protection_changes (array of concrete edit actions)
- final_summary (one sentence: "This edit prioritizes 80%+ average retention over length - viewers are far more likely to finish this than the original.")

Output machine-parseable JSON only.`

const normalizeText = (value: string) =>
  String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9?!\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()

const clipText = (value: string, maxChars = 220) => String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxChars)

const countKeywordHits = (value: string, keywords: string[]) => {
  const text = ` ${normalizeText(value)} `
  let hits = 0
  for (const keyword of keywords) {
    const token = ` ${keyword.toLowerCase()} `
    if (text.includes(token)) hits += 1
  }
  return hits
}

const countWords = (text: string) => {
  const matches = normalizeText(text).match(WORD_PATTERN)
  return matches ? matches.length : 0
}

const countFillerHits = (text: string) => {
  const normalized = ` ${normalizeText(text)} `
  if (!normalized.trim()) return 0
  let total = 0
  for (const phrase of FILLER_PHRASES) {
    const escaped = phrase.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    const regex = new RegExp(`\\b${escaped}\\b`, 'gi')
    const matches = normalized.match(regex)
    if (matches) total += matches.length
  }
  return total
}

const computeRepetitionScore = (text: string) => {
  const words = normalizeText(text).split(/\s+/).filter(Boolean)
  if (words.length < 6) return 0
  const biGrams = new Map<string, number>()
  for (let i = 0; i < words.length - 1; i += 1) {
    const key = `${words[i]} ${words[i + 1]}`
    biGrams.set(key, (biGrams.get(key) || 0) + 1)
  }
  const repeated = Array.from(biGrams.values()).reduce((sum, count) => sum + Math.max(0, count - 1), 0)
  return clamp(repeated / Math.max(1, words.length * 0.45), 0, 1)
}

const normalizeTranscriptSignals = ({
  segments,
  duration
}: {
  segments: PlannerTranscriptSegment[]
  duration: number
}): TranscriptSignalSegment[] =>
  segments
    .map((segment) => {
      const safeStart = clamp(Number(segment.start || 0), 0, Math.max(0, duration - 0.15))
      const safeEnd = clamp(Number(segment.end || 0), safeStart + 0.1, duration)
      const text = clipText(String(segment.text || ''), 420)
      const words = countWords(text)
      const span = Math.max(0.1, safeEnd - safeStart)
      const wordsPerSecond = words / span
      const confidenceRaw = Number(segment.confidence)
      const confidenceNorm = clamp(Number.isFinite(confidenceRaw) ? confidenceRaw : 0.72, 0.05, 0.99)
      const fillerCount = countFillerHits(text)
      const fillerDensity = words > 0 ? fillerCount / words : 0
      const hesitationMatches = text.match(HESITATION_PATTERN)
      const hesitationScore = clamp(
        (hesitationMatches ? hesitationMatches.length : 0) / Math.max(1, Math.ceil(words / 5)),
        0,
        1
      )
      const repetitionScore = computeRepetitionScore(text)
      const sentenceTerminal = SENTENCE_END_PATTERN.test(String(segment.text || '').trim())
      return {
        ...segment,
        start: Number(safeStart.toFixed(3)),
        end: Number(safeEnd.toFixed(3)),
        text,
        duration: Number(span.toFixed(3)),
        words,
        wordsPerSecond: Number(wordsPerSecond.toFixed(3)),
        confidenceNorm: Number(confidenceNorm.toFixed(4)),
        fillerCount,
        fillerDensity: Number(clamp(fillerDensity, 0, 1).toFixed(4)),
        hesitationScore: Number(hesitationScore.toFixed(4)),
        repetitionScore: Number(repetitionScore.toFixed(4)),
        sentenceTerminal
      }
    })
    .filter((segment) => segment.end - segment.start >= 0.1)
    .sort((left, right) => left.start - right.start)

const roundToMsPrecision = (value: number) => Number(value.toFixed(3))

const snapToFrame = ({
  time,
  fps,
  mode
}: {
  time: number
  fps: number
  mode: 'floor' | 'ceil' | 'nearest'
}) => {
  const safeFps = clamp(Number.isFinite(fps) ? fps : FRAME_ALIGNMENT_FALLBACK_FPS, 12, 120)
  const rawFrame = time * safeFps
  let frameIndex =
    mode === 'floor'
      ? Math.floor(rawFrame)
      : mode === 'ceil'
        ? Math.ceil(rawFrame)
        : Math.round(rawFrame)
  if (frameIndex % 2 !== 0) frameIndex += mode === 'floor' ? -1 : 1
  if (frameIndex < 0) frameIndex = 0
  return Number((frameIndex / safeFps).toFixed(3))
}

const snapTimeToAnchors = ({
  time,
  anchors,
  tolerance
}: {
  time: number
  anchors: number[]
  tolerance: number
}) => {
  if (!anchors.length) return time
  let best = time
  let bestDistance = Number.POSITIVE_INFINITY
  for (const anchor of anchors) {
    const distance = Math.abs(anchor - time)
    if (distance < bestDistance) {
      bestDistance = distance
      best = anchor
    }
  }
  return bestDistance <= tolerance ? best : time
}

const buildBeatAndBoundaryAnchors = ({
  transcriptSignals,
  motionPeaks,
  duration,
  fps
}: {
  transcriptSignals: TranscriptSignalSegment[]
  motionPeaks: number[]
  duration: number
  fps: number
}) => {
  const rawAnchors = new Set<number>()
  for (const peak of motionPeaks) {
    const value = Number(peak)
    if (!Number.isFinite(value)) continue
    rawAnchors.add(roundToMsPrecision(clamp(value, 0, duration)))
  }
  for (const segment of transcriptSignals) {
    if (segment.sentenceTerminal || segment.hesitationScore >= 0.42 || segment.fillerDensity >= 0.15) {
      rawAnchors.add(roundToMsPrecision(segment.end))
    }
    if (segment.words <= 2 && segment.duration <= 0.9) {
      rawAnchors.add(roundToMsPrecision((segment.start + segment.end) / 2))
    }
  }
  const sorted = Array.from(rawAnchors.values())
    .filter((value) => Number.isFinite(value) && value >= 0 && value <= duration)
    .sort((left, right) => left - right)
  return sorted.map((value) => snapToFrame({ time: value, fps, mode: 'nearest' }))
}

const scoreMotionContinuity = ({
  start,
  end,
  motionPeaks
}: {
  start: number
  end: number
  motionPeaks: number[]
}) => {
  if (!motionPeaks.length) return 0
  const span = Math.max(0.25, end - start)
  const count = motionPeaks.filter((peak) => peak >= start - 0.16 && peak <= end + 0.16).length
  return clamp((count * 0.65) / Math.max(1, span * 1.9), 0, 1)
}

const buildCandidateId = (prefix: string, index: number) => `${prefix}_${String(index + 1).padStart(2, '0')}`

const computeTranscriptEnergy = (text: string) => {
  const normalized = normalizeText(text)
  if (!normalized) return 0
  const words = normalized.split(' ').filter(Boolean)
  const wordCount = words.length
  if (wordCount === 0) return 0
  const hypeHits = countKeywordHits(normalized, HYPE_WORDS)
  const questionBoost = normalized.includes('?') ? 0.22 : 0
  const exclaimBoost = normalized.includes('!') ? 0.16 : 0
  const paceBoost = clamp(wordCount / 16, 0, 1) * 0.24
  return clamp(hypeHits * 0.14 + questionBoost + exclaimBoost + paceBoost, 0, 1)
}

const computeLexiconSentiment = (text: string) => {
  const normalized = normalizeText(text)
  if (!normalized) return 0.5
  const positiveHits = countKeywordHits(normalized, POSITIVE_WORDS)
  const negativeHits = countKeywordHits(normalized, NEGATIVE_WORDS)
  const delta = positiveHits - negativeHits
  return clamp(0.5 + delta * 0.08, 0.05, 0.95)
}

const avg = (values: number[], fallback = 0) => {
  if (!values.length) return fallback
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

const deriveConfidenceLevel = (confidencePercent: number): PredictionConfidenceLevel => {
  if (confidencePercent >= 75) return 'high'
  if (confidencePercent >= 52) return 'medium'
  return 'low'
}

const parseConfidenceLevel = (value: unknown): PredictionConfidenceLevel | null => {
  const raw = String(value || '').trim().toLowerCase()
  if (raw === 'high' || raw === 'medium' || raw === 'low') return raw
  return null
}

const buildDefaultFinalSummary = () =>
  'This edit prioritizes 80%+ average retention over length - viewers are far more likely to finish this than the original.'

type TimelineWindow = {
  id: string
  start: number
  end: number
  transcript: string
  wordsPerSecond: number
  transcriptEnergy: number
  motionScore: number
  confidenceScore: number
  fillerDensity: number
}

type TranscriptSignalSegment = PlannerTranscriptSegment & {
  duration: number
  words: number
  wordsPerSecond: number
  confidenceNorm: number
  fillerCount: number
  fillerDensity: number
  hesitationScore: number
  repetitionScore: number
  sentenceTerminal: boolean
}

const dedupeCandidates = (candidates: HookCandidate[]) => {
  const seen = new Set<string>()
  const output: HookCandidate[] = []
  for (const candidate of candidates) {
    const key = `${Math.round(candidate.start * 2) / 2}|${Math.round(candidate.end * 2) / 2}`
    if (seen.has(key)) continue
    seen.add(key)
    output.push(candidate)
  }
  return output
}

const mergeTranscriptForWindow = (segments: PlannerTranscriptSegment[], start: number, end: number) => {
  return clipText(
    segments
      .filter((segment) => segment.end > start && segment.start < end)
      .map((segment) => segment.text)
      .join(' ')
  )
}

const buildInitialCandidates = ({
  mode,
  duration,
  frameScan,
  transcriptSegments
}: {
  mode: PlannerMode
  duration: number
  frameScan: PlannerFrameScan
  transcriptSegments: PlannerTranscriptSegment[]
}) => {
  const targetHookLength = mode === 'vertical' ? 8 : 8
  const safeDuration = Math.max(1, duration)
  const candidates: HookCandidate[] = []

  const motionPeaks = (Array.isArray(frameScan.motionPeaks) ? frameScan.motionPeaks : [])
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value) && value >= 0 && value <= safeDuration + 0.1)
    .slice(0, 10)

  motionPeaks.forEach((peak, index) => {
    const start = clamp(peak - 2.1, 0, Math.max(0, safeDuration - 0.4))
    const end = clamp(start + targetHookLength, start + 0.4, safeDuration)
    const transcript = mergeTranscriptForWindow(transcriptSegments, start, end)
    candidates.push({
      id: buildCandidateId('motion', index),
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      duration: Number((end - start).toFixed(3)),
      transcript,
      source: 'motion_peak',
      reason: 'OpenCV peak motion window.',
      scores: { motion: 0, audio: 0, sentiment: 0.5, llm: 0, combined: 0 }
    })
  })

  transcriptSegments
    .slice(0, 28)
    .forEach((segment, index) => {
      const start = clamp(Number(segment.start || 0) - 0.55, 0, Math.max(0, safeDuration - 0.4))
      const end = clamp(start + targetHookLength, start + 0.4, safeDuration)
      const text = clipText(segment.text || '')
      const energy = computeTranscriptEnergy(text)
      const include = energy >= 0.22 || start <= 12
      if (!include) return
      candidates.push({
        id: buildCandidateId('speech', index),
        start: Number(start.toFixed(3)),
        end: Number(end.toFixed(3)),
        duration: Number((end - start).toFixed(3)),
        transcript: text,
        source: 'transcript',
        reason: 'Whisper speech segment with engagement language.',
        scores: { motion: 0, audio: energy, sentiment: computeLexiconSentiment(text), llm: 0, combined: 0 }
      })
    })

  const introEnd = clamp(targetHookLength, 0.6, safeDuration)
  candidates.push({
    id: 'intro_fallback',
    start: 0,
    end: Number(introEnd.toFixed(3)),
    duration: Number(introEnd.toFixed(3)),
    transcript: mergeTranscriptForWindow(transcriptSegments, 0, introEnd),
    source: 'intro_fallback',
    reason: 'Guaranteed opener fallback candidate.',
    scores: { motion: 0, audio: 0, sentiment: 0.5, llm: 0, combined: 0 }
  })

  return dedupeCandidates(candidates).slice(0, 18)
}

const computeMotionScoreForWindow = (start: number, end: number, peaks: number[]) => {
  if (!peaks.length) return 0.25
  const center = (start + end) / 2
  const distances = peaks.map((peak) => Math.abs(peak - center))
  const minDistance = Math.min(...distances)
  return clamp(1 - minDistance / 9, 0, 1)
}

const computeAudioDensityForWindow = (
  start: number,
  end: number,
  transcriptSegments: PlannerTranscriptSegment[]
) => {
  const overlapping = transcriptSegments.filter((segment) => segment.end > start && segment.start < end)
  if (!overlapping.length) return 0.18
  const totalWords = overlapping
    .map((segment) => String(segment.text || '').trim().split(/\s+/).filter(Boolean).length)
    .reduce((sum, count) => sum + count, 0)
  const seconds = Math.max(0.6, end - start)
  const wordsPerSecond = totalWords / seconds
  return clamp(wordsPerSecond / 3.8, 0.05, 1)
}

const buildTimelineWindows = ({
  duration,
  transcriptSignals,
  motionPeaks
}: {
  duration: number
  transcriptSignals: TranscriptSignalSegment[]
  motionPeaks: number[]
}): TimelineWindow[] => {
  const safeDuration = Math.max(1, Number(duration || 0))
  const span = clamp(safeDuration / 18, 8, 20)
  const windows: TimelineWindow[] = []
  let cursor = 0
  let index = 0

  while (cursor < safeDuration && index < 36) {
    const start = clamp(cursor, 0, Math.max(0, safeDuration - 0.4))
    const end = clamp(start + span, start + 0.4, safeDuration)
    const transcript = mergeTranscriptForWindow(transcriptSignals, start, end)
    const overlappingSignals = transcriptSignals.filter((segment) => segment.end > start && segment.start < end)
    const transcriptEnergy = computeTranscriptEnergy(transcript)
    const words = transcript ? transcript.split(/\s+/).filter(Boolean).length : 0
    const wordsPerSecond = words / Math.max(0.6, end - start)
    const motionScore = computeMotionScoreForWindow(start, end, motionPeaks)
    const confidenceScore = avg(overlappingSignals.map((segment) => segment.confidenceNorm), 0.72)
    const fillerDensity = avg(overlappingSignals.map((segment) => segment.fillerDensity), 0)
    windows.push({
      id: `window_${String(index + 1).padStart(2, '0')}`,
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      transcript: clipText(transcript, 140),
      wordsPerSecond: Number(wordsPerSecond.toFixed(3)),
      transcriptEnergy: Number(transcriptEnergy.toFixed(3)),
      motionScore: Number(motionScore.toFixed(3)),
      confidenceScore: Number(confidenceScore.toFixed(3)),
      fillerDensity: Number(fillerDensity.toFixed(4))
    })
    cursor += span * 0.88
    index += 1
  }

  return windows
}

const buildScoredCandidates = ({
  candidates,
  frameScan,
  transcriptSignals
}: {
  candidates: HookCandidate[]
  frameScan: PlannerFrameScan
  transcriptSignals: TranscriptSignalSegment[]
}) => {
  const peaks = Array.isArray(frameScan.motionPeaks) ? frameScan.motionPeaks : []
  return candidates.map((candidate) => {
    const motionScore = computeMotionScoreForWindow(candidate.start, candidate.end, peaks)
    const audioScore = Math.max(candidate.scores.audio, computeAudioDensityForWindow(candidate.start, candidate.end, transcriptSignals))
    const overlappingSignals = transcriptSignals.filter((segment) => segment.end > candidate.start && segment.start < candidate.end)
    const confidenceScore = avg(overlappingSignals.map((segment) => segment.confidenceNorm), 0.72)
    const fillerPenalty = clamp(avg(overlappingSignals.map((segment) => segment.fillerDensity), 0) * 0.45, 0, 0.25)
    const sentimentScore = candidate.scores.sentiment || computeLexiconSentiment(candidate.transcript)
    const faceCenterBoost = clamp(frameScan.centeredFaceVerticalSignal, 0, 1) * 0.08
    const openerBias = candidate.start <= 2.5 ? 0.08 : 0
    const heuristicScore = clamp(
      motionScore * 0.36 +
        audioScore * 0.28 +
        sentimentScore * 0.16 +
        confidenceScore * 0.18 +
        faceCenterBoost +
        openerBias -
        fillerPenalty,
      0,
      1
    )
    return {
      ...candidate,
      scores: {
        ...candidate.scores,
        motion: Number(motionScore.toFixed(4)),
        audio: Number(audioScore.toFixed(4)),
        sentiment: Number(sentimentScore.toFixed(4)),
        combined: Number(heuristicScore.toFixed(4))
      }
    }
  })
}

const buildCandidateContext = (candidates: HookCandidate[]) =>
  candidates.map((candidate) => ({
    id: candidate.id,
    start: Number(candidate.start.toFixed(3)),
    end: Number(candidate.end.toFixed(3)),
    transcript: clipText(candidate.transcript, 180),
    motion_score: Number(candidate.scores.motion.toFixed(3)),
    audio_score: Number(candidate.scores.audio.toFixed(3)),
    sentiment_score: Number(candidate.scores.sentiment.toFixed(3)),
    heuristic_score: Number(candidate.scores.combined.toFixed(3)),
    reason: candidate.reason
  }))

const parsePacingAdjustments = ({
  payload,
  duration,
  fps,
  anchors
}: {
  payload: any
  duration: number
  fps: number
  anchors: number[]
}): PacingAdjustment[] => {
  const rows = Array.isArray(payload?.cuts)
    ? payload.cuts
    : Array.isArray(payload?.pacing_adjustments)
      ? payload.pacing_adjustments
      : []
  return rows
    .map((row: any) => {
      const rawStart = clamp(Number(row?.start || 0), 0, Math.max(0, duration - 0.4))
      const rawEnd = clamp(Number(row?.end || rawStart + 0.4), rawStart + 0.4, duration)
      const snappedStart = snapToFrame({
        time: snapTimeToAnchors({ time: rawStart, anchors, tolerance: 0.14 }),
        fps,
        mode: 'floor'
      })
      const snappedEnd = snapToFrame({
        time: snapTimeToAnchors({ time: rawEnd, anchors, tolerance: 0.14 }),
        fps,
        mode: 'ceil'
      })
      const start = clamp(snappedStart, 0, Math.max(0, duration - 0.35))
      const end = clamp(snappedEnd, start + 0.35, duration)
      const actionRaw = String(row?.action || '').trim().toLowerCase()
      const action: PacingAdjustmentAction =
        actionRaw === 'trim' ? 'trim' : actionRaw === 'speed_up' ? 'speed_up' : 'transition_boost'
      const intensity = clamp(Number(row?.intensity || 0.45), 0.05, 1)
      const speedRaw = Number(row?.speed_multiplier ?? row?.speedMultiplier ?? row?.speed ?? 0)
      const speedMultiplier = action === 'speed_up'
        ? Number(clamp(Number.isFinite(speedRaw) && speedRaw > 1 ? speedRaw : 1 + intensity * 0.75, 1.2, 1.8).toFixed(3))
        : undefined
      const reason = clipText(String(row?.reason || row?.why || 'Retention pacing optimization.'), 120)
      return {
        start: Number(start.toFixed(3)),
        end: Number(end.toFixed(3)),
        action,
        intensity,
        speedMultiplier,
        reason
      }
    })
    .filter((row) => row.end - row.start >= 0.35)
    .slice(0, 12)
}

const overlapSeconds = (left: PacingAdjustment, right: PacingAdjustment) =>
  Math.max(0, Math.min(left.end, right.end) - Math.max(left.start, right.start))

const dedupeAdjustmentsByWindow = (adjustments: PacingAdjustment[]) => {
  if (!adjustments.length) return adjustments
  const ranked = adjustments
    .slice()
    .sort((left, right) => left.start - right.start || right.intensity - left.intensity)
  const output: PacingAdjustment[] = []
  for (const candidate of ranked) {
    const duplicate = output.find((existing) => {
      if (existing.action !== candidate.action) return false
      const overlap = overlapSeconds(existing, candidate)
      if (overlap <= 0) return false
      const shortest = Math.max(0.25, Math.min(existing.end - existing.start, candidate.end - candidate.start))
      return overlap / shortest >= 0.62
    })
    if (!duplicate) {
      output.push(candidate)
      continue
    }
    if (candidate.intensity > duplicate.intensity) {
      duplicate.start = Number(Math.min(duplicate.start, candidate.start).toFixed(3))
      duplicate.end = Number(Math.max(duplicate.end, candidate.end).toFixed(3))
      duplicate.intensity = Number(candidate.intensity.toFixed(3))
      duplicate.speedMultiplier = candidate.speedMultiplier ?? duplicate.speedMultiplier
      duplicate.reason = candidate.reason
    }
  }
  return output
}

const alignAdjustmentToAnchorsAndFrames = ({
  adjustment,
  duration,
  anchors,
  fps
}: {
  adjustment: PacingAdjustment
  duration: number
  anchors: number[]
  fps: number
}): PacingAdjustment | null => {
  const snappedStart = snapToFrame({
    time: snapTimeToAnchors({ time: adjustment.start, anchors, tolerance: 0.16 }),
    fps,
    mode: 'floor'
  })
  const snappedEnd = snapToFrame({
    time: snapTimeToAnchors({ time: adjustment.end, anchors, tolerance: 0.16 }),
    fps,
    mode: 'ceil'
  })
  const start = clamp(snappedStart, 0, Math.max(0, duration - 0.3))
  const end = clamp(snappedEnd, start + 0.3, duration)
  if (end - start < 0.3) return null
  return {
    ...adjustment,
    start: Number(start.toFixed(3)),
    end: Number(end.toFixed(3)),
    intensity: Number(clamp(adjustment.intensity, 0.05, 1).toFixed(3)),
    speedMultiplier:
      adjustment.action === 'speed_up'
        ? Number(clamp(Number(adjustment.speedMultiplier || 1.3), 1.08, 1.8).toFixed(3))
        : undefined
  }
}

const derivePauseGapAdjustments = ({
  transcriptSignals,
  duration
}: {
  transcriptSignals: TranscriptSignalSegment[]
  duration: number
}) => {
  const adjustments: PacingAdjustment[] = []
  for (let index = 1; index < transcriptSignals.length; index += 1) {
    const previous = transcriptSignals[index - 1]
    const current = transcriptSignals[index]
    const gap = current.start - previous.end
    if (!Number.isFinite(gap)) continue
    const confidenceAverage = (previous.confidenceNorm + current.confidenceNorm) / 2
    const strictMinGap = confidenceAverage >= 0.82 ? 0.82 : 0.56
    if (gap < strictMinGap) continue
    if (gap < 0.95 && previous.sentenceTerminal && current.sentenceTerminal && confidenceAverage > 0.86) continue
    const buffer = gap >= 1.2 ? 0.1 : 0.06
    const start = clamp(previous.end + buffer, 0, Math.max(0, duration - 0.3))
    const end = clamp(current.start - buffer, start + 0.3, duration)
    if (end - start < 0.3) continue
    const confidenceWeight = clamp(confidenceAverage, 0.15, 1)
    const intensity = clamp((gap / 1.9) * confidenceWeight, 0.26, 0.88)
    adjustments.push({
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      action: 'trim',
      intensity: Number(intensity.toFixed(3)),
      reason:
        gap >= 1
          ? 'Speech pause >1s detected; trim with sentence-safe buffers.'
          : 'Sub-second hesitation gap detected; micro-trim dead air with buffer.'
    })
    if (adjustments.length >= 6) break
  }
  return adjustments
}

const deriveHesitationTrimAdjustments = ({
  transcriptSignals,
  duration
}: {
  transcriptSignals: TranscriptSignalSegment[]
  duration: number
}) => {
  const output: PacingAdjustment[] = []
  for (const segment of transcriptSignals.slice(0, 160)) {
    if (segment.duration < 0.35) continue
    const confidencePenalty = clamp(1 - segment.confidenceNorm, 0, 1)
    const fillerSignal = clamp(segment.fillerDensity * 2.8 + segment.hesitationScore * 0.7, 0, 1)
    const repetitionSignal = clamp(segment.repetitionScore * 0.92, 0, 1)
    const hesitationScore = clamp(
      confidencePenalty * 0.42 + fillerSignal * 0.42 + repetitionSignal * 0.16,
      0,
      1
    )
    if (hesitationScore < 0.58) continue
    const buffer = 0.06
    let start = segment.start + buffer
    let end = segment.end - buffer
    if (fillerSignal < 0.6 && segment.duration > 1.1) {
      const center = (segment.start + segment.end) / 2
      const span = clamp(segment.duration * (0.28 + hesitationScore * 0.24), 0.28, 1.35)
      start = center - span / 2
      end = center + span / 2
    }
    start = clamp(start, 0, Math.max(0, duration - 0.3))
    end = clamp(end, start + 0.3, duration)
    if (end - start < 0.3) continue
    output.push({
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      action: 'trim',
      intensity: Number(clamp(0.3 + hesitationScore * 0.58, 0.28, 0.94).toFixed(3)),
      reason: 'Low-confidence hesitation/filler phrase detected by transcript confidence + NLP signal.'
    })
    if (output.length >= 6) break
  }
  return output
}

const deriveFillerPhraseTrimAdjustments = ({
  transcriptSignals,
  duration
}: {
  transcriptSignals: TranscriptSignalSegment[]
  duration: number
}) => {
  const output: PacingAdjustment[] = []
  for (const segment of transcriptSignals) {
    if (segment.duration < 0.55) continue
    const fillerSignal = clamp(segment.fillerDensity * 3.4 + segment.hesitationScore * 0.8, 0, 1)
    if (fillerSignal < 0.38) continue
    const trimSpan = clamp(segment.duration * (0.55 + fillerSignal * 0.2), 0.34, 2.4)
    const center = (segment.start + segment.end) / 2
    const start = clamp(center - trimSpan / 2, segment.start + 0.04, Math.max(segment.start + 0.3, duration - 0.3))
    const end = clamp(center + trimSpan / 2, start + 0.3, Math.min(segment.end - 0.04, duration))
    if (end - start < 0.3) continue
    output.push({
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      action: 'trim',
      intensity: Number(clamp(0.34 + fillerSignal * 0.52, 0.32, 0.9).toFixed(3)),
      reason: 'High filler-density phrase detected; trim center while preserving sentence edges.'
    })
    if (output.length >= 5) break
  }
  return output
}

const deriveLowEnergyCompressionAdjustments = ({
  windows,
  duration
}: {
  windows: TimelineWindow[]
  duration: number
}): PacingAdjustment[] => {
  const lowEnergy = windows
    .filter((window) => {
      const lowInfoDensity = window.wordsPerSecond < 1.35
      const lowMotion = window.motionScore < 0.62
      const weakNarrativeEnergy = window.transcriptEnergy < 0.4
      const softConfidence = window.confidenceScore < 0.84 || window.fillerDensity > 0.06
      return window.end - window.start >= 6.5 && lowInfoDensity && (lowMotion || weakNarrativeEnergy || softConfidence)
    })
    .map((window) => ({
      window,
      weakness:
        (1 - window.motionScore) * 0.38 +
        (1 - window.transcriptEnergy) * 0.34 +
        (1 - window.confidenceScore) * 0.2 +
        clamp(0.22 - window.wordsPerSecond / 10, 0, 0.22)
    }))
    .sort((left, right) => right.weakness - left.weakness || left.window.start - right.window.start)
    .slice(0, 5)
    .map((entry) => entry.window)

  const direct = lowEnergy.map((window) => {
    const intensity = clamp(
      0.38 + (1 - window.motionScore) * 0.32 + (1 - window.confidenceScore) * 0.16,
      0.28,
      0.92
    )
    const speedMultiplier = clamp(
      1.24 + (1 - window.motionScore) * 0.28 + (1 - Math.min(1.6, window.wordsPerSecond) / 1.6) * 0.2,
      1.2,
      1.8
    )
    return {
      start: window.start,
      end: window.end,
      action: 'speed_up' as const,
      intensity: Number(intensity.toFixed(3)),
      speedMultiplier: Number(speedMultiplier.toFixed(3)),
      reason: 'Low-energy valley detected; compress section to restore pacing momentum.'
    }
  })

  if (direct.length > 0) return direct

  const fallbackWindow = windows
    .slice()
    .sort(
      (left, right) =>
        (left.transcriptEnergy + left.motionScore + left.confidenceScore) -
        (right.transcriptEnergy + right.motionScore + right.confidenceScore)
    )[0]

  if (!fallbackWindow || duration < 22) return []

  const start = clamp((fallbackWindow.start + fallbackWindow.end) / 2 - 3.4, 0, Math.max(0, duration - 1.2))
  const end = clamp(start + 6.8, start + 0.4, duration)
  return [
    {
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      action: 'speed_up' as const,
      intensity: 0.44,
      speedMultiplier: 1.32,
      reason: 'Fallback compression on weakest timeline window to avoid pacing valley.'
    }
  ]
}

const deriveMotionGapCompressionAdjustments = ({
  motionPeaks,
  duration
}: {
  motionPeaks: number[]
  duration: number
}): PacingAdjustment[] => {
  const sortedPeaks = (Array.isArray(motionPeaks) ? motionPeaks : [])
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value) && value >= 0 && value <= duration)
    .sort((left, right) => left - right)
  if (sortedPeaks.length < 2) return []
  const adjustments: PacingAdjustment[] = []
  for (let index = 1; index < sortedPeaks.length; index += 1) {
    const previous = sortedPeaks[index - 1]
    const current = sortedPeaks[index]
    const gap = current - previous
    if (gap < 8.5) continue
    const start = clamp(previous + gap * 0.18, 0, Math.max(0, duration - 0.8))
    const end = clamp(current - gap * 0.18, start + 0.4, duration)
    if (end - start < 0.8) continue
    adjustments.push({
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      action: 'speed_up' as const,
      intensity: Number(clamp(0.38 + (gap - 8.5) * 0.03, 0.32, 0.78).toFixed(3)),
      speedMultiplier: Number(clamp(1.24 + (gap - 8.5) * 0.025, 1.24, 1.62).toFixed(3)),
      reason: 'Long motion-gap lull detected; compress middle span to maintain momentum.'
    })
    if (adjustments.length >= 4) break
  }
  return adjustments
}

const derivePatternInterruptAdjustments = ({
  windows,
  duration
}: {
  windows: TimelineWindow[]
  duration: number
}) => {
  const spacingTarget = clamp(duration < 75 ? 11.5 : 13.5, 10, 15)
  const timelineInterrupts: PacingAdjustment[] = []
  const maxInterrupts = Math.max(2, Math.min(8, Math.ceil(duration / spacingTarget)))
  let cursor = Math.max(6, spacingTarget * 0.72)
  while (cursor < duration - 1.2 && timelineInterrupts.length < maxInterrupts) {
    const match = windows.find((window) => cursor >= window.start && cursor <= window.end) || null
    const lowPulse = !match || (match.transcriptEnergy < 0.35 && match.motionScore < 0.58)
    const start = clamp(cursor - 0.26, 0, Math.max(0, duration - 0.65))
    const end = clamp(start + 0.58, start + 0.3, duration)
    const intensity = match
      ? (
          lowPulse
            ? clamp(0.34 + (1 - match.motionScore) * 0.27, 0.28, 0.82)
            : clamp(0.24 + (1 - match.transcriptEnergy) * 0.16, 0.18, 0.54)
        )
      : 0.38
    timelineInterrupts.push({
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      action: 'transition_boost',
      intensity: Number(intensity.toFixed(3)),
      reason: lowPulse
        ? 'Pattern interrupt inserted at predicted boredom point (10-15s cadence).'
        : 'Cadence interrupt added to maintain attention rhythm.'
    })
    cursor += spacingTarget
  }
  return timelineInterrupts
}

const preserveMotionContinuity = ({
  adjustments,
  motionPeaks
}: {
  adjustments: PacingAdjustment[]
  motionPeaks: number[]
}): PacingAdjustment[] =>
  adjustments.map((adjustment): PacingAdjustment => {
    if (adjustment.action !== 'trim') return adjustment
    const motionContinuity = scoreMotionContinuity({
      start: adjustment.start,
      end: adjustment.end,
      motionPeaks
    })
    const textReason = adjustment.reason.toLowerCase()
    const isSpeechDrivenTrim =
      textReason.includes('pause') ||
      textReason.includes('hesitation') ||
      textReason.includes('filler') ||
      textReason.includes('confidence')
    if (motionContinuity < 0.62 || isSpeechDrivenTrim) return adjustment
    return {
      ...adjustment,
      action: 'speed_up' as const,
      speedMultiplier: Number(clamp(1.18 + motionContinuity * 0.26, 1.2, 1.5).toFixed(3)),
      intensity: Number(clamp(adjustment.intensity * 0.78, 0.2, 0.82).toFixed(3)),
      reason: 'High motion continuity detected; switched hard trim to compression to avoid visual mismatch.'
    }
  })

const rebalanceForHighMotionShortForm = ({
  adjustments,
  motionPeaks,
  duration
}: {
  adjustments: PacingAdjustment[]
  motionPeaks: number[]
  duration: number
}) => {
  const density = (Array.isArray(motionPeaks) ? motionPeaks.length : 0) / Math.max(1, duration / 7.5)
  if (duration > 90 || density < 0.95) return adjustments
  let protectedTrimCount = 0
  return adjustments.map((adjustment) => {
    if (adjustment.action !== 'trim') return adjustment
    const textReason = adjustment.reason.toLowerCase()
    const keepAsTrim =
      (textReason.includes('filler') || textReason.includes('pause >1s') || textReason.includes('dead air')) &&
      protectedTrimCount < 2
    if (keepAsTrim) {
      protectedTrimCount += 1
      return adjustment
    }
    return {
      ...adjustment,
      action: 'speed_up' as const,
      speedMultiplier: Number(clamp(Number(adjustment.speedMultiplier || 1.34), 1.24, 1.55).toFixed(3)),
      intensity: Number(clamp(adjustment.intensity * 0.82, 0.26, 0.78).toFixed(3)),
      reason: 'High-motion short-form profile: swapped hard trim to compression for continuity.'
    }
  })
}

const ensureMinimumCompressionCoverage = ({
  adjustments,
  windows,
  duration
}: {
  adjustments: PacingAdjustment[]
  windows: TimelineWindow[]
  duration: number
}) => {
  const hasSpeedUp = adjustments.some((adjustment) => adjustment.action === 'speed_up')
  if (hasSpeedUp || duration < 20) return adjustments
  const weakest = windows
    .slice()
    .sort(
      (left, right) =>
        (left.transcriptEnergy + left.motionScore + left.confidenceScore) -
        (right.transcriptEnergy + right.motionScore + right.confidenceScore)
    )
    .slice(0, 2)
  if (!weakest.length) return adjustments
  const supplements = weakest.map((window) => {
    const start = clamp(window.start + 0.2, 0, Math.max(0, duration - 0.7))
    const end = clamp(Math.min(window.end, start + 7.4), start + 0.4, duration)
    return {
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      action: 'speed_up' as const,
      intensity: 0.4,
      speedMultiplier: 1.3,
      reason: 'Guarantee compression coverage in at least one low-energy window.'
    }
  })
  return [...adjustments, ...supplements]
}

const alignAndLimitAdjustments = ({
  adjustments,
  duration,
  anchors,
  fps
}: {
  adjustments: PacingAdjustment[]
  duration: number
  anchors: number[]
  fps: number
}) => {
  const aligned = adjustments
    .map((adjustment) =>
      alignAdjustmentToAnchorsAndFrames({
        adjustment,
        duration,
        anchors,
        fps
      })
    )
    .filter((adjustment): adjustment is PacingAdjustment => Boolean(adjustment))
  return dedupeAdjustmentsByWindow(aligned)
    .slice(0, 16)
    .sort((left, right) => left.start - right.start || right.intensity - left.intensity)
}

const buildHeuristicPacingAdjustments = ({
  duration,
  transcriptSignals,
  windows,
  motionPeaks,
  fps
}: {
  duration: number
  transcriptSignals: TranscriptSignalSegment[]
  windows: TimelineWindow[]
  motionPeaks: number[]
  fps: number
}): PacingAdjustment[] => {
  const anchors = buildBeatAndBoundaryAnchors({
    transcriptSignals,
    motionPeaks,
    duration,
    fps
  })
  const pauseGapAdjustments = derivePauseGapAdjustments({ transcriptSignals, duration })
  const hesitationTrims = deriveHesitationTrimAdjustments({ transcriptSignals, duration })
  const fillerTrims = deriveFillerPhraseTrimAdjustments({ transcriptSignals, duration })
  const lowEnergyCompressions = deriveLowEnergyCompressionAdjustments({ windows, duration })
  const motionGapCompressions = deriveMotionGapCompressionAdjustments({ motionPeaks, duration })
  const interrupts = derivePatternInterruptAdjustments({ windows, duration })

  const preliminary = dedupeAdjustmentsByWindow([
    ...pauseGapAdjustments,
    ...hesitationTrims,
    ...fillerTrims,
    ...lowEnergyCompressions,
    ...motionGapCompressions,
    ...interrupts
  ])
  const motionSafe = preserveMotionContinuity({
    adjustments: preliminary,
    motionPeaks
  })
  const motionProfileBalanced = rebalanceForHighMotionShortForm({
    adjustments: motionSafe,
    motionPeaks,
    duration
  })
  const withCompressionGuarantee = ensureMinimumCompressionCoverage({
    adjustments: motionProfileBalanced,
    windows,
    duration
  })
  const finalList = alignAndLimitAdjustments({
    adjustments: withCompressionGuarantee,
    duration,
    anchors,
    fps
  })

  if (finalList.length > 0) return finalList

  const fallbackStartRaw = Number(clamp(duration * 0.42, 0, Math.max(0, duration - 1.2)).toFixed(3))
  const fallbackStart = snapToFrame({
    time: snapTimeToAnchors({ time: fallbackStartRaw, anchors, tolerance: 0.18 }),
    fps,
    mode: 'floor'
  })
  const fallbackEnd = snapToFrame({
    time: snapTimeToAnchors(
      {
        time: clamp(fallbackStart + 0.9, fallbackStart + 0.3, duration),
        anchors,
        tolerance: 0.18
      }
    ),
    fps,
    mode: 'ceil'
  })
  return [
    {
      start: fallbackStart,
      end: Number(clamp(fallbackEnd, fallbackStart + 0.3, duration).toFixed(3)),
      action: 'transition_boost' as const,
      intensity: 0.42,
      reason: 'Fallback pacing lift at mid-video plateau.'
    }
  ]
}

const parseHookComparisons = (payload: any, candidates: HookCandidate[]) => {
  const map = new Map(candidates.map((candidate) => [candidate.id, candidate]))
  const rows = Array.isArray(payload?.runner_ups)
    ? payload.runner_ups
    : Array.isArray(payload?.comparisons)
      ? payload.comparisons
      : []

  const parsed = rows
    .map((row: any, index: number): HookComparison | null => {
      const id = String(row?.id || row?.candidate_id || '').trim()
      const candidate = map.get(id)
      const start = Number.isFinite(Number(row?.start))
        ? clamp(Number(row?.start), 0, 1_000_000)
        : candidate?.start
      const end = Number.isFinite(Number(row?.end))
        ? Math.max(Number(row?.end), Number(start || 0) + 0.4)
        : candidate?.end
      if (!Number.isFinite(Number(start)) || !Number.isFinite(Number(end))) return null
      const retentionLiftRaw = Number(
        row?.predicted_retention_lift_percent ??
          row?.predictedRetentionLiftPercent ??
          row?.predicted_lift_percent ??
          row?.predictedLiftPercent ??
          row?.predicted_retention_percent ??
          row?.predictedRetentionPercent
      )
      const retentionLift = Number(
        clamp(
          Number.isFinite(retentionLiftRaw)
            ? retentionLiftRaw
            : (candidate?.scores.combined || 0.5) * 100,
          18,
          99
        ).toFixed(1)
      )
      const reason = clipText(
        String(row?.reason || row?.why || row?.explanation || 'Lower opener energy than selected winner.'),
        200
      )
      return {
        id: id || `runner_up_${index + 1}`,
        start: Number(Number(start).toFixed(3)),
        end: Number(Number(end).toFixed(3)),
        predictedRetentionLift: retentionLift,
        reason
      }
    })
    .filter((row): row is HookComparison => Boolean(row))
    .slice(0, 3)

  if (parsed.length >= 2) return parsed

  const fallback = candidates
    .slice(1, 4)
    .map((candidate, index) => ({
      id: candidate.id || `runner_up_${index + 1}`,
      start: candidate.start,
      end: candidate.end,
      predictedRetentionLift: Number(clamp(candidate.scores.combined * 100, 18, 99).toFixed(1)),
      reason: 'Strong candidate but weaker curiosity + pacing fit than selected opener.'
    }))

  return [...parsed, ...fallback].slice(0, 3)
}

const parseSegmentInsights = ({
  payload,
  key,
  duration,
  fallbackReason,
  fallbackFix,
  defaultPredictedRetention
}: {
  payload: any
  key: 'weak_segments' | 'strong_segments'
  duration: number
  fallbackReason: string
  fallbackFix?: string
  defaultPredictedRetention: number
}): SegmentRetentionInsight[] => {
  const rows = Array.isArray(payload?.[key])
    ? payload[key]
    : Array.isArray(payload?.[key === 'weak_segments' ? 'weakParts' : 'strongParts'])
      ? payload[key === 'weak_segments' ? 'weakParts' : 'strongParts']
      : []
  return rows
    .map((row: any, index: number): SegmentRetentionInsight | null => {
      const start = clamp(Number(row?.start ?? row?.from ?? 0), 0, Math.max(0, duration - 0.4))
      const end = clamp(Number(row?.end ?? row?.to ?? start + 0.4), start + 0.4, duration)
      if (end - start < 0.35) return null

      const dropRaw = Number(
        row?.predicted_drop_off_percent ??
          row?.predictedDropOffPercent ??
          row?.drop_off_percent ??
          row?.dropPercent
      )
      const holdRaw = Number(
        row?.predicted_hold_percent ??
          row?.predictedHoldPercent ??
          row?.predicted_retention_percent ??
          row?.predictedRetentionPercent
      )
      const predictedRetention = Number(
        clamp(
          Number.isFinite(holdRaw)
            ? holdRaw
            : Number.isFinite(dropRaw)
              ? 100 - dropRaw
              : defaultPredictedRetention,
          8,
          99
        ).toFixed(1)
      )
      const fixRaw = String(row?.fix || row?.suggested_fix || row?.suggestion || fallbackFix || '').trim()
      let reason = clipText(String(row?.reason || row?.why || fallbackReason), 200)
      if (key === 'weak_segments') {
        const predictedDrop = Number((100 - predictedRetention).toFixed(1))
        if (!/^danger zone/i.test(reason)) {
          const because = clipText(reason.replace(/\.$/, ''), 120) || 'low energy and weak progression'
          const fixForReason = clipText(fixRaw || fallbackFix || 'speed up + text tease', 95)
          reason = clipText(
            `Danger zone - predicted ${predictedDrop}% drop-off because ${because}. Fix: ${fixForReason}.`,
            200
          )
        } else if (fixRaw && !/fix:/i.test(reason)) {
          reason = clipText(`${reason} Fix: ${fixRaw}.`, 200)
        }
      } else if (!/^excellent/i.test(reason)) {
        const dueTo = clipText(reason.replace(/\.$/, ''), 120) || 'high motion peak and emotional payoff'
        reason = clipText(
          `Excellent - ${predictedRetention}% retention hold here due to ${dueTo}. This keeps viewers locked in.`,
          200
        )
      }
      return {
        id: `${key === 'weak_segments' ? 'weak' : 'strong'}_${String(index + 1).padStart(2, '0')}`,
        start: Number(start.toFixed(3)),
        end: Number(end.toFixed(3)),
        predictedRetention,
        reason,
        fix: fixRaw ? clipText(fixRaw, 200) : undefined
      }
    })
    .filter((row): row is SegmentRetentionInsight => Boolean(row))
    .slice(0, 6)
}

const buildHeuristicWeakSegments = ({
  duration,
  windows,
  pacingAdjustments
}: {
  duration: number
  windows: TimelineWindow[]
  pacingAdjustments: PacingAdjustment[]
}): SegmentRetentionInsight[] => {
  const fromWindows = windows
    .filter((window) => window.end - window.start >= 8 && window.wordsPerSecond < 1.1 && window.motionScore < 0.45)
    .map((window, index) => {
      const predictedRetention = Number(clamp(46 - (1 - window.motionScore) * 13, 20, 62).toFixed(1))
      const predictedDrop = Number((100 - predictedRetention).toFixed(1))
      return {
        id: `weak_${String(index + 1).padStart(2, '0')}`,
        start: window.start,
        end: window.end,
        predictedRetention,
        reason: `Danger zone - predicted ${predictedDrop}% drop-off because motion and information density are both weak. Fix: speed 1.4x + text tease.`,
        fix: 'Speed 1.4x plus text tease to force forward momentum.'
      }
    })

  if (fromWindows.length > 0) return fromWindows.slice(0, 4)

  return pacingAdjustments
    .slice(0, 3)
    .map((adjustment, index) => {
      const predictedRetention = Number(clamp(48 - adjustment.intensity * 12, 22, 62).toFixed(1))
      const predictedDrop = Number((100 - predictedRetention).toFixed(1))
      return {
        id: `weak_${String(index + 1).padStart(2, '0')}`,
        start: adjustment.start,
        end: adjustment.end,
        predictedRetention,
        reason: `Danger zone - predicted ${predictedDrop}% drop-off because pacing drifts and novelty stalls. Fix: ${adjustment.action === 'speed_up' ? `speed ${Number(adjustment.speedMultiplier || 1.35).toFixed(2)}x + overlay tease` : 'trim dead air + insert micro-hook on re-entry'}.`,
        fix: adjustment.action === 'speed_up'
          ? `Speed ${Number(adjustment.speedMultiplier || 1.35).toFixed(2)}x + overlay tease.`
          : 'Trim dead air and insert a micro-hook at segment re-entry.'
      }
    })
}

const buildHeuristicStrongSegments = ({
  rankedHooks,
  windows
}: {
  rankedHooks: HookCandidate[]
  windows: TimelineWindow[]
}): SegmentRetentionInsight[] => {
  const fromHooks = rankedHooks
    .slice(0, 3)
    .map((candidate, index) => {
      const predictedRetention = Number(clamp(candidate.scores.combined * 100, 55, 99).toFixed(1))
      return {
        id: `strong_${String(index + 1).padStart(2, '0')}`,
        start: candidate.start,
        end: candidate.end,
        predictedRetention,
        reason: `Excellent - ${predictedRetention}% retention hold here due to high motion, emotional spike, and curiosity pressure. This keeps viewers locked in.`,
        fix: undefined
      }
    })

  if (fromHooks.length >= 2) return fromHooks

  const fromWindows = windows
    .slice()
    .sort((left, right) => (right.transcriptEnergy + right.motionScore) - (left.transcriptEnergy + left.motionScore))
    .slice(0, 2)
    .map((window, index) => {
      const predictedRetention = Number(clamp(68 + window.motionScore * 22 + window.transcriptEnergy * 10, 55, 99).toFixed(1))
      return {
        id: `strong_${String(fromHooks.length + index + 1).padStart(2, '0')}`,
        start: window.start,
        end: window.end,
        predictedRetention,
        reason: `Excellent - ${predictedRetention}% retention hold here due to sustained novelty and momentum. This keeps viewers locked in.`,
        fix: undefined
      }
    })

  return [...fromHooks, ...fromWindows].slice(0, 4)
}

const parsePredictedRetention = (payload: any, fallback: number) => {
  const retentionRaw = Number(
    payload?.predicted_average_retention_percent ??
      payload?.predictedAverageRetentionPercent ??
      payload?.predicted_retention_percent ??
      payload?.predictedRetentionPercent ??
      payload?.predicted_average_retention
  )
  const confidenceRaw = Number(
    payload?.confidence_percent ??
      payload?.prediction_confidence_percent ??
      payload?.confidence ??
      payload?.predictionConfidence
  )
  const predictionConfidence = Number(
    clamp(Number.isFinite(confidenceRaw) ? confidenceRaw : 58, 8, 99).toFixed(1)
  )
  const explicitConfidenceLevel = parseConfidenceLevel(
    payload?.confidence_level ??
      payload?.confidenceLevel ??
      payload?.prediction_confidence_level ??
      payload?.predictionConfidenceLevel
  )
  return {
    predictedAverageRetention: Number(
      clamp(Number.isFinite(retentionRaw) ? retentionRaw : fallback, 8, 99).toFixed(1)
    ),
    predictionConfidence,
    predictionConfidenceLevel: explicitConfidenceLevel || deriveConfidenceLevel(predictionConfidence)
  }
}

const parseTitleSuggestions = (payload: any): RetentionTitleSuggestion[] => {
  const rows = Array.isArray(payload?.title_options)
    ? payload.title_options
    : Array.isArray(payload?.titles)
      ? payload.titles
      : []
  return rows
    .map((row: any, index: number): RetentionTitleSuggestion | null => {
      const title = clipText(String(row?.title || row?.name || ''), 120)
      if (!title) return null
      const explanation = clipText(
        String(row?.explanation || row?.reason || 'Optimized for curiosity + 2026 retention trends.'),
        180
      )
      const confidenceRaw = Number(row?.confidence_percent ?? row?.confidence ?? row?.score)
      const confidence = Number(clamp(Number.isFinite(confidenceRaw) ? confidenceRaw : 66, 12, 99).toFixed(1))
      return {
        id: `title_${String(index + 1).padStart(2, '0')}`,
        title,
        explanation,
        confidence
      }
    })
    .filter((row): row is RetentionTitleSuggestion => Boolean(row))
    .slice(0, 5)
}

const parseRetentionProtectionChanges = ({
  payload,
  selectedHook,
  pacingAdjustments,
  weakSegments
}: {
  payload: any
  selectedHook: HookCandidate | null
  pacingAdjustments: PacingAdjustment[]
  weakSegments: SegmentRetentionInsight[]
}) => {
  const rawRows = Array.isArray(payload?.retention_protection_changes)
    ? payload.retention_protection_changes
    : Array.isArray(payload?.retentionProtectionChanges)
      ? payload.retentionProtectionChanges
      : Array.isArray(payload?.retention_protection_actions)
        ? payload.retention_protection_actions
        : []

  const parsedRows = rawRows
    .map((row: any) => clipText(typeof row === 'string' ? row : String(row?.change || row?.action || ''), 220))
    .filter(Boolean)
    .slice(0, 8)

  if (parsedRows.length > 0) return parsedRows

  const fallback: string[] = []
  if (selectedHook) {
    fallback.push(
      `Locked opener to ${selectedHook.start.toFixed(1)}s-${selectedHook.end.toFixed(1)}s as the primary 8-second continuation trigger.`
    )
  }
  for (const segment of weakSegments.slice(0, 3)) {
    const predictedDrop = Number((100 - segment.predictedRetention).toFixed(1))
    const fix = clipText(segment.fix || 'Trim and inject novelty.', 100)
    fallback.push(
      `Cut/accelerated ${segment.start.toFixed(1)}s-${segment.end.toFixed(1)}s (predicted ${predictedDrop}% drop risk). Fix applied: ${fix}`
    )
  }
  for (const adjustment of pacingAdjustments.slice(0, 3)) {
    if (fallback.length >= 6) break
    if (adjustment.action === 'speed_up') {
      fallback.push(
        `Compressed ${adjustment.start.toFixed(1)}s-${adjustment.end.toFixed(1)}s to ${Number(adjustment.speedMultiplier || 1.35).toFixed(2)}x to remove pacing valley.`
      )
    } else if (adjustment.action === 'trim') {
      fallback.push(`Trimmed ${adjustment.start.toFixed(1)}s-${adjustment.end.toFixed(1)}s to remove dead air and protect consistency.`)
    } else {
      fallback.push(`Injected transition spike at ${adjustment.start.toFixed(1)}s-${adjustment.end.toFixed(1)}s to restore attention.`)
    }
  }
  if (fallback.length === 0) {
    fallback.push('Maintained a shorter, high-momentum cut profile to avoid deep retention valleys.')
  }
  return fallback.slice(0, 8)
}

const parseFinalSummary = (payload: any) => {
  const summary = clipText(
    String(payload?.final_summary ?? payload?.finalSummary ?? payload?.summary_sentence ?? payload?.one_sentence_summary ?? ''),
    240
  )
  return summary || buildDefaultFinalSummary()
}

const buildFallbackTitleSuggestions = ({
  transcriptExcerpt,
  mode
}: {
  transcriptExcerpt: string
  mode: PlannerMode
}): RetentionTitleSuggestion[] => {
  const seedPhrase = clipText(
    transcriptExcerpt
      .replace(/[?!]/g, '')
      .split(/\s+/)
      .slice(0, 5)
      .join(' '),
    48
  ) || 'This Edit'
  const modeToken = mode === 'vertical' ? 'Short-Form' : 'Watch-Time'
  return [
    {
      id: 'title_01',
      title: `${seedPhrase}: The 2026 Retention Blueprint`,
      explanation: 'Strong value promise + 2026 framing.',
      confidence: 74
    },
    {
      id: 'title_02',
      title: `I Recut This for ${modeToken} and This Happened`,
      explanation: 'Curiosity + transformation format.',
      confidence: 71
    },
    {
      id: 'title_03',
      title: `Most People Click Off Here... I Fixed It`,
      explanation: 'Drop-off pain point + resolution promise.',
      confidence: 69
    },
    {
      id: 'title_04',
      title: `The 8-Second Hook That Locked Viewers In`,
      explanation: 'Specific tactic with immediate payoff.',
      confidence: 72
    },
    {
      id: 'title_05',
      title: `Watch Till the End: The Turn Nobody Expected`,
      explanation: 'Open-loop language for completion lift.',
      confidence: 67
    }
  ]
}

const toAIDurationHook = (candidate: HookCandidate, duration: number, targetSeconds = 8) => {
  const safeDuration = Math.max(0.6, Number(duration || 0))
  const start = clamp(candidate.start, 0, Math.max(0, safeDuration - 0.4))
  const end = clamp(start + targetSeconds, start + 0.4, safeDuration)
  const resolved = {
    ...candidate,
    start: Number(start.toFixed(3)),
    end: Number(end.toFixed(3)),
    duration: Number((end - start).toFixed(3))
  }
  return resolved
}

const buildPrompts = ({
  mode,
  duration,
  transcriptExcerpt,
  candidates,
  windows
}: {
  mode: PlannerMode
  duration: number
  transcriptExcerpt: string
  candidates: HookCandidate[]
  windows: TimelineWindow[]
}) => {
  const candidateContext = JSON.stringify(buildCandidateContext(candidates))
  const timelineContext = JSON.stringify(
    windows.map((window) => ({
      id: window.id,
      start: window.start,
      end: window.end,
      transcript: window.transcript,
      energy_score: window.transcriptEnergy,
      words_per_second: window.wordsPerSecond,
      transcript_energy: window.transcriptEnergy,
      motion_score: window.motionScore,
      confidence_score: window.confidenceScore,
      filler_density: window.fillerDensity
    }))
  )
  const context = `Context: mode=${mode}, duration_seconds=${duration.toFixed(2)}, transcript_excerpt="${clipText(transcriptExcerpt, 320)}".
HookCandidates=${candidateContext}
TimelineWindows=${timelineContext}`

  const eligibilityPrompt = `${STRICT_RETENTION_PREFIX}
${RUTHLESS_RETENTION_PLAYBOOK}
Identify which candidate IDs are viable dopamine-trap openers (first 3-15s in final timeline). Return JSON only:
{"eligible_ids":["id1","id2"],"notes":["short reason"]}.
${context}`

  const rankingPrompt = `${STRICT_RETENTION_PREFIX}
${RUTHLESS_RETENTION_PLAYBOOK}
Rank these hook candidates by predicted retention as opener. Select exactly one 8-second opener and compare it against 2-3 runner-ups.
Explain why the selected opener beats alternatives in retention terms.
Return JSON only:
{
  "ranked_ids":["id1","id2","id3"],
  "selected_id":"id1",
  "hook_cut":{"start":0.0,"end":8.0},
  "selected_reason":"Selected this 8-second opener over alternatives because [highest early retention prediction / strongest surprise element / continuation pressure].",
  "runner_ups":[
    {"id":"id2","reason":"why weaker than winner","predicted_retention_lift_percent":84},
    {"id":"id3","reason":"why weaker than winner","predicted_retention_lift_percent":80}
  ]
}
${context}`

  const pacingPrompt = `${STRICT_RETENTION_PREFIX}
${RUTHLESS_RETENTION_PLAYBOOK}
Run full-video ruthless retention analysis and output cut/pacing/insight/title plan.
If any segment predicts >15-20% drop-off, cut or compress it.
Prefer shorter final runtime when it increases average retention.
For weak segments, explain why retention drops and suggest concrete fixes.
For strong segments, explain why retention holds.
Return JSON only:
{
  "cuts":[{"start":12.4,"end":15.2,"action":"trim","intensity":0.7,"reason":"dead air"},
          {"start":48.0,"end":62.0,"action":"speed_up","intensity":0.62,"speed_multiplier":1.5,"reason":"low novelty zone"}],
  "weak_segments":[{"start":48.0,"end":62.0,"predicted_drop_off_percent":40,"reason":"Danger zone - predicted 40% drop-off because low energy and no progress. Fix: speed up + text tease or cut.","fix":"speed 1.5x + text tease"}],
  "strong_segments":[{"start":8.0,"end":16.0,"predicted_hold_percent":95,"reason":"Excellent - 95% retention hold here due to payoff + curiosity spike. This keeps viewers locked in."}],
  "predicted_average_retention_percent":82,
  "confidence_percent":74,
  "confidence_level":"medium",
  "retention_protection_changes":[
    "Removed dead-air valley at 48.0s-62.0s with 1.5x compression and text tease.",
    "Moved strongest 8-second hook to opener to maximize first-15s hold."
  ],
  "final_summary":"This edit prioritizes 80%+ average retention over length - viewers are far more likely to finish this than the original.",
  "title_options":[
    {"title":"...", "explanation":"...", "confidence_percent":83},
    {"title":"...", "explanation":"...", "confidence_percent":80},
    {"title":"...", "explanation":"...", "confidence_percent":79},
    {"title":"...", "explanation":"...", "confidence_percent":77},
    {"title":"...", "explanation":"...", "confidence_percent":75}
  ]
}
Predict average % watched for this edited structure and generate 5 title variations optimized for retention + curiosity in 2026.
${context}`

  return {
    eligibilityPrompt,
    rankingPrompt,
    pacingPrompt
  }
}

const estimateFallbackRetention = ({
  rankedHooks,
  weakSegments,
  strongSegments
}: {
  rankedHooks: HookCandidate[]
  weakSegments: SegmentRetentionInsight[]
  strongSegments: SegmentRetentionInsight[]
}) => {
  const hookScore = avg(rankedHooks.slice(0, 3).map((candidate) => candidate.scores.combined), 0.5)
  const weakPenalty = avg(weakSegments.map((segment) => 1 - segment.predictedRetention / 100), 0.28)
  const strongLift = avg(strongSegments.map((segment) => segment.predictedRetention / 100), 0.62)
  const predictedAverageRetention = Number(clamp(42 + hookScore * 36 + strongLift * 14 - weakPenalty * 18, 18, 96).toFixed(1))
  const predictionConfidence = Number(clamp(48 + rankedHooks.length * 3 + strongSegments.length * 4 - weakSegments.length * 2, 20, 93).toFixed(1))
  return { predictedAverageRetention, predictionConfidence }
}

export const planRetentionEditsWithFreeAi = async (input: PlannerInput): Promise<FreeAiHookPlan> => {
  const mode = input.mode
  const duration = Math.max(1, Number(input.metadata.duration || 0))
  const safeFps = clamp(
    Number.isFinite(Number(input.metadata.fps)) ? Number(input.metadata.fps) : FRAME_ALIGNMENT_FALLBACK_FPS,
    12,
    120
  )
  const transcriptSegments = Array.isArray(input.transcriptSegments) ? input.transcriptSegments : []
  const transcriptSignals = normalizeTranscriptSignals({
    segments: transcriptSegments,
    duration
  })
  const candidatesSeed = buildInitialCandidates({
    mode,
    duration,
    frameScan: input.frameScan,
    transcriptSegments: transcriptSignals
  })
  let candidates = buildScoredCandidates({
    candidates: candidatesSeed,
    frameScan: input.frameScan,
    transcriptSignals
  })
  const windows = buildTimelineWindows({
    duration,
    transcriptSignals,
    motionPeaks: Array.isArray(input.frameScan.motionPeaks) ? input.frameScan.motionPeaks : []
  })

  const prompts = buildPrompts({
    mode,
    duration,
    transcriptExcerpt: input.transcriptExcerpt,
    candidates,
    windows
  })

  const notes: string[] = ['ruthless_retention_prompt:deterministic_local_planner']
  const provider: FreeAiHookPlan['provider'] = 'ruthless_retention_prompt'
  const model: string | null = null

  candidates = candidates
    .sort((left, right) => right.scores.combined - left.scores.combined)
    .map((candidate, index) => ({
      ...candidate,
      scores: {
        ...candidate.scores,
        llm: Number(clamp(1 - index * 0.08, 0.2, 1).toFixed(4))
      }
    }))
  const strongest = candidates[0] || null
  const strongestEarlyCandidate =
    candidates
      .filter((candidate) => candidate.start <= 3.2)
      .sort((left, right) => right.scores.combined - left.scores.combined)[0] || null
  const introAnchorCandidate =
    candidates
      .filter((candidate) => candidate.start <= 1.2)
      .sort((left, right) => right.scores.combined - left.scores.combined)[0] || null
  const selectedCandidate = (
    strongest &&
    introAnchorCandidate &&
    introAnchorCandidate.scores.combined >= strongest.scores.combined * 0.84
  )
    ? introAnchorCandidate
    : (
        strongest &&
        strongestEarlyCandidate &&
        strongest.start > 3.5 &&
        strongestEarlyCandidate.scores.combined >= strongest.scores.combined * 0.9
      )
      ? strongestEarlyCandidate
      : strongest
  const selected = selectedCandidate ? toAIDurationHook(selectedCandidate, duration, 8) : null
  const pacingAdjustments = buildHeuristicPacingAdjustments({
    duration,
    transcriptSignals,
    windows,
    motionPeaks: Array.isArray(input.frameScan.motionPeaks) ? input.frameScan.motionPeaks : [],
    fps: safeFps
  })
  const weakSegments = buildHeuristicWeakSegments({ duration, windows, pacingAdjustments })
  const strongSegments = buildHeuristicStrongSegments({ rankedHooks: candidates, windows })
  const fallbackRetention = estimateFallbackRetention({
    rankedHooks: candidates,
    weakSegments,
    strongSegments
  })
  const predictionConfidenceLevel = deriveConfidenceLevel(fallbackRetention.predictionConfidence)
  const hookComparison = candidates
    .filter((candidate) => candidate.id !== selectedCandidate?.id)
    .slice(0, 3)
    .map((candidate) => ({
      id: candidate.id,
      start: candidate.start,
      end: candidate.end,
      predictedRetentionLift: Number(clamp(candidate.scores.combined * 100, 18, 99).toFixed(1)),
      reason: 'Runner-up had lower surprise/curiosity signal than selected opener.'
    }))
  const retentionProtectionChanges = parseRetentionProtectionChanges({
    payload: null,
    selectedHook: selected,
    pacingAdjustments,
    weakSegments
  })
  const finalSummary = parseFinalSummary(null)

  return {
    provider,
    model,
    selectedHook: selected
      ? {
          ...selected,
          reason:
            selectedCandidate?.id === introAnchorCandidate?.id && introAnchorCandidate?.id !== strongest?.id
              ? `Selected this 8-second opener over alternatives because it anchors at timeline start while preserving comparable retention strength (${selected.start.toFixed(1)}s-${selected.end.toFixed(1)}s).`
              : selectedCandidate?.id === strongestEarlyCandidate?.id && strongestEarlyCandidate?.id !== strongest?.id
              ? `Selected this 8-second opener over alternatives because its early-timeline placement preserves first-3s hold while matching top retention score (${selected.start.toFixed(1)}s-${selected.end.toFixed(1)}s).`
              : `Selected this 8-second opener over alternatives because it delivers the strongest early retention pressure (${selected.start.toFixed(1)}s-${selected.end.toFixed(1)}s) under the ruthless retention prompt rules.`
        }
      : null,
    rankedHooks: candidates.slice(0, 8),
    pacingAdjustments,
    hookComparison,
    weakSegments,
    strongSegments,
    predictedAverageRetention: fallbackRetention.predictedAverageRetention,
    predictionConfidence: fallbackRetention.predictionConfidence,
    predictionConfidenceLevel,
    retentionProtectionChanges,
    finalSummary,
    titleSuggestions: buildFallbackTitleSuggestions({ transcriptExcerpt: input.transcriptExcerpt, mode }),
    notes,
    prompts: {
      eligibility: prompts.eligibilityPrompt,
      ranking: prompts.rankingPrompt,
      pacing: prompts.pacingPrompt
    }
  }
}
