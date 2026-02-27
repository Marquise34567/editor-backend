import fetch from 'node-fetch'

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
  provider: 'huggingface' | 'heuristic' | 'huggingface_with_heuristic'
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

const extractJson = (text: string) => {
  const raw = String(text || '').trim()
  if (!raw) return null
  const candidates = [raw]
  const objStart = raw.indexOf('{')
  const objEnd = raw.lastIndexOf('}')
  if (objStart >= 0 && objEnd > objStart) candidates.push(raw.slice(objStart, objEnd + 1))
  const arrStart = raw.indexOf('[')
  const arrEnd = raw.lastIndexOf(']')
  if (arrStart >= 0 && arrEnd > arrStart) candidates.push(raw.slice(arrStart, arrEnd + 1))
  for (const candidate of candidates) {
    try {
      return JSON.parse(candidate)
    } catch {
      // noop
    }
  }
  return null
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
  transcriptSegments,
  motionPeaks
}: {
  duration: number
  transcriptSegments: PlannerTranscriptSegment[]
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
    const transcript = mergeTranscriptForWindow(transcriptSegments, start, end)
    const transcriptEnergy = computeTranscriptEnergy(transcript)
    const words = transcript ? transcript.split(/\s+/).filter(Boolean).length : 0
    const wordsPerSecond = words / Math.max(0.6, end - start)
    const motionScore = computeMotionScoreForWindow(start, end, motionPeaks)
    windows.push({
      id: `window_${String(index + 1).padStart(2, '0')}`,
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      transcript: clipText(transcript, 140),
      wordsPerSecond: Number(wordsPerSecond.toFixed(3)),
      transcriptEnergy: Number(transcriptEnergy.toFixed(3)),
      motionScore: Number(motionScore.toFixed(3))
    })
    cursor += span * 0.88
    index += 1
  }

  return windows
}

const buildScoredCandidates = ({
  candidates,
  frameScan,
  transcriptSegments
}: {
  candidates: HookCandidate[]
  frameScan: PlannerFrameScan
  transcriptSegments: PlannerTranscriptSegment[]
}) => {
  const peaks = Array.isArray(frameScan.motionPeaks) ? frameScan.motionPeaks : []
  return candidates.map((candidate) => {
    const motionScore = computeMotionScoreForWindow(candidate.start, candidate.end, peaks)
    const audioScore = Math.max(candidate.scores.audio, computeAudioDensityForWindow(candidate.start, candidate.end, transcriptSegments))
    const sentimentScore = candidate.scores.sentiment || computeLexiconSentiment(candidate.transcript)
    const faceCenterBoost = clamp(frameScan.centeredFaceVerticalSignal, 0, 1) * 0.08
    const openerBias = candidate.start <= 2.5 ? 0.08 : 0
    const heuristicScore = clamp(
      motionScore * 0.42 + audioScore * 0.3 + sentimentScore * 0.2 + faceCenterBoost + openerBias,
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

const coerceGeneratedText = (payload: any): string => {
  if (!payload) return ''
  if (typeof payload === 'string') return payload
  if (Array.isArray(payload)) {
    return payload
      .map((item) => {
        if (typeof item === 'string') return item
        if (typeof item?.generated_text === 'string') return item.generated_text
        if (typeof item?.summary_text === 'string') return item.summary_text
        return ''
      })
      .join('\n')
      .trim()
  }
  if (typeof payload.generated_text === 'string') return payload.generated_text
  if (typeof payload.summary_text === 'string') return payload.summary_text
  if (typeof payload.error === 'string') return payload.error
  return ''
}

const requestHuggingFaceText = async ({
  token,
  model,
  prompt,
  maxNewTokens
}: {
  token: string
  model: string
  prompt: string
  maxNewTokens: number
}): Promise<{ text: string; ok: boolean; reason?: string }> => {
  const endpoint = String(process.env.HF_TEXT_ENDPOINT || '').trim() || `https://api-inference.huggingface.co/models/${model}`
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${token}`
      },
      body: JSON.stringify({
        inputs: prompt,
        parameters: {
          max_new_tokens: clamp(Math.round(maxNewTokens), 64, 650),
          temperature: 0.2,
          top_p: 0.9,
          return_full_text: false
        },
        options: {
          wait_for_model: true,
          use_cache: false
        }
      })
    })

    const payload: any = await response.json().catch(() => null)
    if (!response.ok) {
      return {
        text: '',
        ok: false,
        reason: typeof payload?.error === 'string' ? payload.error : `hf_status_${response.status}`
      }
    }

    const text = coerceGeneratedText(payload)
    if (!text) {
      return { text: '', ok: false, reason: 'hf_empty_response' }
    }
    return { text, ok: true }
  } catch (error: any) {
    return { text: '', ok: false, reason: error?.message || 'hf_exception' }
  }
}

const requestHuggingFaceSentiment = async ({
  token,
  model,
  text
}: {
  token: string
  model: string
  text: string
}): Promise<number | null> => {
  const endpoint = String(process.env.HF_SENTIMENT_ENDPOINT || '').trim() || `https://api-inference.huggingface.co/models/${model}`
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${token}`
      },
      body: JSON.stringify({ inputs: text })
    })
    if (!response.ok) return null
    const payload = await response.json().catch(() => null)
    const rows = Array.isArray(payload) ? payload : []
    const labels = Array.isArray(rows[0]) ? rows[0] : rows
    if (!Array.isArray(labels)) return null
    let positive = 0.5
    for (const row of labels) {
      const label = String((row as any)?.label || '').toLowerCase()
      const score = Number((row as any)?.score || 0)
      if (!Number.isFinite(score)) continue
      if (label.includes('positive')) positive = Math.max(positive, score)
      if (label.includes('negative')) positive = Math.min(positive, 1 - score)
    }
    return clamp(positive, 0.05, 0.95)
  } catch {
    return null
  }
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

const parseEligibleIds = (payload: any): string[] => {
  if (!payload) return []
  if (Array.isArray(payload)) return payload.map((value) => String(value || '').trim()).filter(Boolean)
  if (Array.isArray(payload.eligible_ids)) return payload.eligible_ids.map((value: any) => String(value || '').trim()).filter(Boolean)
  if (Array.isArray(payload.eligibleIds)) return payload.eligibleIds.map((value: any) => String(value || '').trim()).filter(Boolean)
  return []
}

const parseRankedIds = (payload: any): string[] => {
  if (!payload) return []
  if (Array.isArray(payload.ranked_ids)) return payload.ranked_ids.map((value: any) => String(value || '').trim()).filter(Boolean)
  if (Array.isArray(payload.rankedIds)) return payload.rankedIds.map((value: any) => String(value || '').trim()).filter(Boolean)
  if (Array.isArray(payload.top_ids)) return payload.top_ids.map((value: any) => String(value || '').trim()).filter(Boolean)
  return []
}

const parseSelectedId = (payload: any): string | null => {
  if (!payload || typeof payload !== 'object') return null
  if (typeof payload.selected_id === 'string') return payload.selected_id.trim()
  if (typeof payload.selectedId === 'string') return payload.selectedId.trim()
  if (typeof payload.best_id === 'string') return payload.best_id.trim()
  if (payload.selected && typeof payload.selected.id === 'string') return payload.selected.id.trim()
  return null
}

const parsePacingAdjustments = (payload: any, duration: number): PacingAdjustment[] => {
  const rows = Array.isArray(payload?.cuts)
    ? payload.cuts
    : Array.isArray(payload?.pacing_adjustments)
      ? payload.pacing_adjustments
      : []
  return rows
    .map((row: any) => {
      const start = clamp(Number(row?.start || 0), 0, Math.max(0, duration - 0.4))
      const end = clamp(Number(row?.end || start + 0.4), start + 0.4, duration)
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

const buildHeuristicPacingAdjustments = ({
  duration,
  transcriptSegments,
  windows
}: {
  duration: number
  transcriptSegments: PlannerTranscriptSegment[]
  windows: TimelineWindow[]
}) => {
  const adjustments: PacingAdjustment[] = []
  const sorted = transcriptSegments
    .slice()
    .sort((left, right) => left.start - right.start)
    .slice(0, 120)

  for (let index = 1; index < sorted.length; index += 1) {
    const previous = sorted[index - 1]
    const current = sorted[index]
    const gap = Number(current.start) - Number(previous.end)
    if (gap < 1.8) continue
    const start = clamp(previous.end + 0.12, 0, Math.max(0, duration - 0.4))
    const end = clamp(current.start - 0.08, start + 0.4, duration)
    if (end - start < 0.4) continue
    adjustments.push({
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      action: 'trim',
      intensity: clamp(gap / 4.5, 0.2, 0.85),
      reason: 'Speech dead-air gap detected by Whisper timestamps.'
    })
    if (adjustments.length >= 6) break
  }

  const lowEnergy = windows
    .filter((window) => window.end - window.start >= 8 && window.wordsPerSecond < 1.05 && window.motionScore < 0.42)
    .slice(0, 3)

  for (const item of lowEnergy) {
    adjustments.push({
      start: item.start,
      end: item.end,
      action: 'speed_up',
      intensity: Number(clamp(0.38 + (1 - item.motionScore) * 0.45, 0.2, 0.9).toFixed(3)),
      speedMultiplier: Number(clamp(1.28 + (1 - item.wordsPerSecond / 2.2) * 0.42, 1.2, 1.8).toFixed(3)),
      reason: 'Low novelty zone: apply compression to avoid retention dip.'
    })
  }

  if (adjustments.length === 0) {
    adjustments.push({
      start: Number(clamp(duration * 0.42, 0, Math.max(0, duration - 1.2)).toFixed(3)),
      end: Number(clamp(duration * 0.42 + 0.9, 0.4, duration).toFixed(3)),
      action: 'transition_boost',
      intensity: 0.42,
      reason: 'Heuristic pacing lift at mid-video plateau.'
    })
  }

  return adjustments
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
      words_per_second: window.wordsPerSecond,
      transcript_energy: window.transcriptEnergy,
      motion_score: window.motionScore
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
Select exactly one 8-second opener and compare it against 2-3 runner-ups.
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
At the end, generate 5 title variations optimized for retention + curiosity in 2026.
${context}`

  return {
    eligibilityPrompt,
    rankingPrompt,
    pacingPrompt
  }
}

const rankByIds = (candidates: HookCandidate[], rankedIds: string[]) => {
  const map = new Map(candidates.map((candidate) => [candidate.id, candidate]))
  const ranked: HookCandidate[] = []
  const used = new Set<string>()
  for (const id of rankedIds) {
    const candidate = map.get(id)
    if (!candidate) continue
    ranked.push(candidate)
    used.add(id)
  }
  const leftovers = candidates
    .filter((candidate) => !used.has(candidate.id))
    .sort((left, right) => right.scores.combined - left.scores.combined)
  return [...ranked, ...leftovers]
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
  const transcriptSegments = Array.isArray(input.transcriptSegments) ? input.transcriptSegments : []
  const candidatesSeed = buildInitialCandidates({
    mode,
    duration,
    frameScan: input.frameScan,
    transcriptSegments
  })
  let candidates = buildScoredCandidates({
    candidates: candidatesSeed,
    frameScan: input.frameScan,
    transcriptSegments
  })
  const windows = buildTimelineWindows({
    duration,
    transcriptSegments,
    motionPeaks: Array.isArray(input.frameScan.motionPeaks) ? input.frameScan.motionPeaks : []
  })

  const hfToken = String(process.env.HUGGINGFACE_API_KEY || process.env.HF_API_TOKEN || '').trim()
  const hfModel = String(process.env.HF_RETENTION_MODEL || 'meta-llama/Meta-Llama-3-8B-Instruct').trim()
  const hfSentimentModel = String(process.env.HF_SENTIMENT_MODEL || 'distilbert-base-uncased-finetuned-sst-2-english').trim()

  const prompts = buildPrompts({
    mode,
    duration,
    transcriptExcerpt: input.transcriptExcerpt,
    candidates,
    windows
  })

  const notes: string[] = []
  let provider: FreeAiHookPlan['provider'] = 'heuristic'
  let model: string | null = null

  if (hfToken && candidates.length > 0) {
    model = hfModel
    const sampleForSentiment = candidates
      .filter((candidate) => candidate.transcript.length > 0)
      .slice(0, 10)

    for (const candidate of sampleForSentiment) {
      const sentiment = await requestHuggingFaceSentiment({
        token: hfToken,
        model: hfSentimentModel,
        text: candidate.transcript
      })
      if (sentiment === null) continue
      candidate.scores.sentiment = Number(sentiment.toFixed(4))
      candidate.scores.combined = Number(
        clamp(candidate.scores.motion * 0.42 + candidate.scores.audio * 0.3 + sentiment * 0.28, 0, 1).toFixed(4)
      )
    }

    const eligibility = await requestHuggingFaceText({
      token: hfToken,
      model: hfModel,
      prompt: prompts.eligibilityPrompt,
      maxNewTokens: 360
    })
    const ranking = await requestHuggingFaceText({
      token: hfToken,
      model: hfModel,
      prompt: prompts.rankingPrompt,
      maxNewTokens: 640
    })
    const pacing = await requestHuggingFaceText({
      token: hfToken,
      model: hfModel,
      prompt: prompts.pacingPrompt,
      maxNewTokens: 900
    })

    if (eligibility.ok || ranking.ok || pacing.ok) {
      provider = (ranking.ok && pacing.ok) ? 'huggingface' : 'huggingface_with_heuristic'
    } else {
      notes.push(`huggingface_unavailable:${eligibility.reason || ranking.reason || pacing.reason || 'unknown'}`)
    }

    const eligibilityJson = extractJson(eligibility.text)
    const rankingJson = extractJson(ranking.text)
    const pacingJson = extractJson(pacing.text)

    const eligibleIds = parseEligibleIds(eligibilityJson)
    if (eligibleIds.length > 0) {
      candidates = candidates.map((candidate) => ({
        ...candidate,
        scores: {
          ...candidate.scores,
          llm: eligibleIds.includes(candidate.id) ? 0.85 : 0.2,
          combined: Number(
            clamp(candidate.scores.combined * 0.74 + (eligibleIds.includes(candidate.id) ? 0.26 : 0.06), 0, 1).toFixed(4)
          )
        }
      }))
    }

    const rankedIds = parseRankedIds(rankingJson)
    if (rankedIds.length > 0) {
      candidates = rankByIds(candidates, rankedIds).map((candidate, index) => ({
        ...candidate,
        scores: {
          ...candidate.scores,
          llm: Number(clamp(1 - index * 0.08, 0.2, 1).toFixed(4))
        }
      }))
    } else {
      candidates = candidates.sort((left, right) => right.scores.combined - left.scores.combined)
    }

    const selectedId = parseSelectedId(rankingJson)
    if (selectedId) {
      candidates = rankByIds(candidates, [selectedId, ...candidates.map((candidate) => candidate.id)])
    }

    const selectedCandidate = candidates[0] ? toAIDurationHook(candidates[0], duration, 8) : null
    const selectedReason = clipText(
      String(
        (rankingJson as any)?.selected_reason ||
        (rankingJson as any)?.why ||
        (selectedCandidate
          ? `Selected this 8-second opener over alternatives because it has the strongest early-retention prediction (${selectedCandidate.start.toFixed(1)}s-${selectedCandidate.end.toFixed(1)}s).`
          : '')
      ),
      220
    )
    const selectedHook = selectedCandidate
      ? { ...selectedCandidate, reason: selectedReason || selectedCandidate.reason }
      : null
    const hookComparison = parseHookComparisons(rankingJson, candidates)
    const parsedPacingAdjustments = parsePacingAdjustments(pacingJson, duration)
    const parsedWeakSegments = parseSegmentInsights({
      payload: pacingJson,
      key: 'weak_segments',
      duration,
      fallbackReason: 'Predicted drop-off from low novelty and weak progression.',
      fallbackFix: 'Trim setup and add speed/text contrast.',
      defaultPredictedRetention: 42
    })
    const parsedStrongSegments = parseSegmentInsights({
      payload: pacingJson,
      key: 'strong_segments',
      duration,
      fallbackReason: 'Strong payoff and curiosity hold.',
      defaultPredictedRetention: 88
    })
    const parsedTitles = parseTitleSuggestions(pacingJson)
    const fallbackWeak = buildHeuristicWeakSegments({
      duration,
      windows,
      pacingAdjustments: parsedPacingAdjustments
    })
    const weakSegments = parsedWeakSegments.length ? parsedWeakSegments : fallbackWeak
    const strongSegments = parsedStrongSegments.length
      ? parsedStrongSegments
      : buildHeuristicStrongSegments({ rankedHooks: candidates, windows })
    const retentionFromModel = parsePredictedRetention(
      pacingJson,
      estimateFallbackRetention({
        rankedHooks: candidates,
        weakSegments,
        strongSegments
      }).predictedAverageRetention
    )
    const retentionProtectionChanges = parseRetentionProtectionChanges({
      payload: pacingJson,
      selectedHook,
      pacingAdjustments: parsedPacingAdjustments.length
        ? parsedPacingAdjustments
        : buildHeuristicPacingAdjustments({ duration, transcriptSegments, windows }),
      weakSegments
    })
    const finalSummary = parseFinalSummary(pacingJson)
    const titleSuggestions = parsedTitles.length
      ? parsedTitles
      : buildFallbackTitleSuggestions({ transcriptExcerpt: input.transcriptExcerpt, mode })

    if (eligibility.ok || ranking.ok || pacing.ok) {
      return {
        provider,
        model,
        selectedHook,
        rankedHooks: candidates.slice(0, 8),
        pacingAdjustments: parsedPacingAdjustments.length
          ? parsedPacingAdjustments
          : buildHeuristicPacingAdjustments({ duration, transcriptSegments, windows }),
        hookComparison,
        weakSegments,
        strongSegments,
        predictedAverageRetention: retentionFromModel.predictedAverageRetention,
        predictionConfidence: retentionFromModel.predictionConfidence,
        predictionConfidenceLevel: retentionFromModel.predictionConfidenceLevel,
        retentionProtectionChanges,
        finalSummary,
        titleSuggestions: titleSuggestions.slice(0, 5),
        notes,
        prompts: {
          eligibility: prompts.eligibilityPrompt,
          ranking: prompts.rankingPrompt,
          pacing: prompts.pacingPrompt
        }
      }
    }
  }

  candidates = candidates.sort((left, right) => right.scores.combined - left.scores.combined)
  const selected = candidates[0] ? toAIDurationHook(candidates[0], duration, 8) : null
  const pacingAdjustments = buildHeuristicPacingAdjustments({ duration, transcriptSegments, windows })
  const weakSegments = buildHeuristicWeakSegments({ duration, windows, pacingAdjustments })
  const strongSegments = buildHeuristicStrongSegments({ rankedHooks: candidates, windows })
  const fallbackRetention = estimateFallbackRetention({
    rankedHooks: candidates,
    weakSegments,
    strongSegments
  })
  const predictionConfidenceLevel = deriveConfidenceLevel(fallbackRetention.predictionConfidence)
  const hookComparison = candidates
    .slice(1, 4)
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
            `Selected this 8-second opener over alternatives because it delivers the strongest early retention pressure (${selected.start.toFixed(1)}s-${selected.end.toFixed(1)}s).`
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
