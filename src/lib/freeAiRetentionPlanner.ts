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
  reason: string
}

export type FreeAiHookPlan = {
  provider: 'huggingface' | 'heuristic' | 'huggingface_with_heuristic'
  model: string | null
  selectedHook: HookCandidate | null
  rankedHooks: HookCandidate[]
  pacingAdjustments: PacingAdjustment[]
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
      const reason = clipText(String(row?.reason || row?.why || 'Retention pacing optimization.'), 120)
      return { start: Number(start.toFixed(3)), end: Number(end.toFixed(3)), action, intensity, reason }
    })
    .filter((row) => row.end - row.start >= 0.35)
    .slice(0, 8)
}

const buildHeuristicPacingAdjustments = ({
  duration,
  transcriptSegments
}: {
  duration: number
  transcriptSegments: PlannerTranscriptSegment[]
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

const buildPrompts = ({
  mode,
  duration,
  transcriptExcerpt,
  candidates
}: {
  mode: PlannerMode
  duration: number
  transcriptExcerpt: string
  candidates: HookCandidate[]
}) => {
  const candidateContext = JSON.stringify(buildCandidateContext(candidates))

  const eligibilityPrompt = `From these video segment descriptions [transcripts + energy scores], determine which are eligible for hooks (high engagement potential, surprising, question-posing, or energetic starts).
Prioritize edits that keep viewers watching longer.
Return JSON only: {"eligible_ids":["id1","id2"],"notes":["short reason"]}.
Context: mode=${mode}, duration_seconds=${duration.toFixed(2)}, transcript_excerpt="${clipText(transcriptExcerpt, 260)}".
Segments=${candidateContext}`

  const rankingPrompt = `Rank these segments by retention-max potential as a video opener (aim for 8 seconds: hook viewer immediately). Select top one and suggest exact timestamps to cut (e.g., trim to 8s with cliffhanger end).
Prioritize edits that keep viewers watching longer.
Return JSON only: {"ranked_ids":["id1","id2"],"selected_id":"id1","hook_cut":{"start":0.0,"end":8.0},"why":"short reason"}.
Segments=${candidateContext}`

  const pacingPrompt = `Optimize cutting/pacing for retention: Suggest cuts to remove boring segments, speed up slow parts, add transitions at emotional beats. Output: list of cut timestamps, pacing adjustments (e.g., slow-mo at hooks).
Prioritize edits that keep viewers watching longer.
Return JSON only: {"cuts":[{"start":12.4,"end":14.2,"action":"trim","intensity":0.7,"reason":"dead air"}]}.
Context: mode=${mode}, duration_seconds=${duration.toFixed(2)}.
Segments=${candidateContext}`

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

  const hfToken = String(process.env.HUGGINGFACE_API_KEY || process.env.HF_API_TOKEN || '').trim()
  const hfModel = String(process.env.HF_RETENTION_MODEL || 'meta-llama/Meta-Llama-3-8B-Instruct').trim()
  const hfSentimentModel = String(process.env.HF_SENTIMENT_MODEL || 'distilbert-base-uncased-finetuned-sst-2-english').trim()

  const prompts = buildPrompts({
    mode,
    duration,
    transcriptExcerpt: input.transcriptExcerpt,
    candidates
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
      maxNewTokens: 260
    })
    const ranking = await requestHuggingFaceText({
      token: hfToken,
      model: hfModel,
      prompt: prompts.rankingPrompt,
      maxNewTokens: 320
    })
    const pacing = await requestHuggingFaceText({
      token: hfToken,
      model: hfModel,
      prompt: prompts.pacingPrompt,
      maxNewTokens: 320
    })

    if (eligibility.ok || ranking.ok || pacing.ok) {
      provider = (eligibility.ok && ranking.ok && pacing.ok) ? 'huggingface' : 'huggingface_with_heuristic'
    } else {
      notes.push(`huggingface_unavailable:${eligibility.reason || ranking.reason || pacing.reason || 'unknown'}`)
    }

    const eligibleJson = extractJson(eligibility.text)
    const rankingJson = extractJson(ranking.text)
    const pacingJson = extractJson(pacing.text)

    const eligibleIds = parseEligibleIds(eligibleJson)
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

    const parsedPacingAdjustments = parsePacingAdjustments(pacingJson, duration)
    if (parsedPacingAdjustments.length > 0) {
      return {
        provider,
        model,
        selectedHook: candidates[0] || null,
        rankedHooks: candidates.slice(0, 8),
        pacingAdjustments: parsedPacingAdjustments,
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
  const pacingAdjustments = buildHeuristicPacingAdjustments({ duration, transcriptSegments })

  return {
    provider,
    model,
    selectedHook: candidates[0] || null,
    rankedHooks: candidates.slice(0, 8),
    pacingAdjustments,
    notes,
    prompts: {
      eligibility: prompts.eligibilityPrompt,
      ranking: prompts.rankingPrompt,
      pacing: prompts.pacingPrompt
    }
  }
}
