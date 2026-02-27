import { gemmaQuery } from '../lib/aiService'

const RUTHLESS_RETENTION_PREFIX = `You are AutoEditor's ruthless retention-maximizing AI brain.
Goal: maximize average retention and completion rate over raw runtime.
Always choose edits that increase completion and reduce drop-off valleys.
Return machine-parseable JSON only.`

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const extractJson = (text: string) => {
  const raw = String(text || '').trim()
  if (!raw) return null
  const starts = [raw.indexOf('{'), raw.indexOf('[')].filter((index) => index >= 0)
  for (const start of starts) {
    const end = raw.lastIndexOf(start === raw.indexOf('[') ? ']' : '}')
    if (end <= start) continue
    const candidate = raw.slice(start, end + 1)
    try {
      return JSON.parse(candidate)
    } catch {
      // noop
    }
  }
  try {
    return JSON.parse(raw)
  } catch {
    return null
  }
}

export const rankSegmentsByRetentionPotential = async (segments: Array<Record<string, any>>) => {
  const prompt = `${RUTHLESS_RETENTION_PREFIX}
Task: Rank segments by retention potential for opener selection.
Return JSON only: {"ranked":[{"id":"seg_01","score":0.91,"reason":"..."},{"id":"seg_02","score":0.82,"reason":"..."}],"selected_id":"seg_01"}.
InputSegments=${JSON.stringify(segments)}`
  const response = await gemmaQuery(prompt, 700)
  if (!response.ok) return { ok: false, reason: response.reason || 'query_failed', ranked: [] as any[], selectedId: null as string | null }
  const payload = extractJson(response.text) as any
  const ranked = Array.isArray(payload?.ranked)
    ? payload.ranked
        .map((row: any, index: number) => ({
          id: String(row?.id || `seg_${String(index + 1).padStart(2, '0')}`),
          score: clamp(Number(row?.score ?? row?.retention_score ?? 0.5), 0, 1),
          reason: String(row?.reason || row?.why || 'High opener curiosity signal.')
        }))
        .slice(0, 8)
    : []
  return {
    ok: ranked.length > 0,
    provider: response.provider,
    model: response.model,
    ranked,
    selectedId: String(payload?.selected_id || ranked[0]?.id || '')
  }
}

export const explainDullSegment = async (segment: {
  start: number
  end: number
  transcript: string
  energyScore?: number
  motionScore?: number
}) => {
  const prompt = `${RUTHLESS_RETENTION_PREFIX}
Task: Explain why a segment is dull and propose fixes.
Return JSON only: {"reason":"Danger zone - ...","fixes":["...","..."],"predicted_drop_off_percent":34}.
Segment=${JSON.stringify(segment)}`
  const response = await gemmaQuery(prompt, 520)
  if (!response.ok) return { ok: false, reason: response.reason || 'query_failed' }
  const payload = extractJson(response.text) as any
  return {
    ok: true,
    provider: response.provider,
    model: response.model,
    reason: String(payload?.reason || 'Danger zone - low novelty and slow progression.'),
    fixes: Array.isArray(payload?.fixes) ? payload.fixes.map((item: any) => String(item)).slice(0, 5) : [],
    predictedDropOffPercent: clamp(Number(payload?.predicted_drop_off_percent ?? payload?.drop_off_percent ?? 30), 5, 95)
  }
}

export const proposePacingFixes = async (segments: Array<Record<string, any>>) => {
  const prompt = `${RUTHLESS_RETENTION_PREFIX}
Task: Optimize cuts for minimal drop-offs. Aim for 80%+ average retention.
Return JSON only: {"cuts":[{"start":0.0,"end":3.2,"action":"trim","intensity":0.7,"reason":"..."}],"predicted_average_retention_percent":81}.
Segments=${JSON.stringify(segments)}`
  const response = await gemmaQuery(prompt, 760)
  if (!response.ok) return { ok: false, reason: response.reason || 'query_failed' }
  const payload = extractJson(response.text) as any
  const cuts = Array.isArray(payload?.cuts)
    ? payload.cuts
        .map((row: any) => ({
          start: Number(row?.start ?? 0),
          end: Number(row?.end ?? 0),
          action: String(row?.action || 'trim'),
          intensity: clamp(Number(row?.intensity ?? 0.4), 0.05, 1),
          reason: String(row?.reason || 'Retention pacing optimization.')
        }))
        .slice(0, 12)
    : []
  return {
    ok: true,
    provider: response.provider,
    model: response.model,
    cuts,
    predictedAverageRetention: clamp(Number(payload?.predicted_average_retention_percent ?? 70), 10, 99)
  }
}

export const generateRetentionTitles = async ({
  transcriptExcerpt,
  keyHooks
}: {
  transcriptExcerpt: string
  keyHooks: string[]
}) => {
  const prompt = `${RUTHLESS_RETENTION_PREFIX}
Task: Generate 5 retention-optimized titles.
Return JSON only: {"titles":[{"title":"...","explanation":"...","confidence_percent":82}]}.
TranscriptExcerpt=${JSON.stringify(transcriptExcerpt)}
KeyHooks=${JSON.stringify(keyHooks)}`
  const response = await gemmaQuery(prompt, 420)
  if (!response.ok) return { ok: false, reason: response.reason || 'query_failed', titles: [] as any[] }
  const payload = extractJson(response.text) as any
  const rows = Array.isArray(payload?.titles) ? payload.titles : []
  return {
    ok: true,
    provider: response.provider,
    model: response.model,
    titles: rows.map((row: any, index: number) => ({
      id: `title_${String(index + 1).padStart(2, '0')}`,
      title: String(row?.title || '').trim(),
      explanation: String(row?.explanation || row?.reason || '').trim(),
      confidencePercent: clamp(Number(row?.confidence_percent ?? row?.confidence ?? 70), 20, 99)
    })).filter((row: any) => row.title).slice(0, 5)
  }
}

export const predictAverageRetention = async (timelineSummary: Record<string, any>) => {
  const prompt = `${RUTHLESS_RETENTION_PREFIX}
Task: Predict average % watched for this edited structure.
Return JSON only: {"predicted_average_retention_percent":78,"confidence_percent":71,"confidence_level":"medium","reason":"..."}.
Timeline=${JSON.stringify(timelineSummary)}`
  const response = await gemmaQuery(prompt, 360)
  if (!response.ok) return { ok: false, reason: response.reason || 'query_failed' }
  const payload = extractJson(response.text) as any
  return {
    ok: true,
    provider: response.provider,
    model: response.model,
    predictedAverageRetention: clamp(
      Number(payload?.predicted_average_retention_percent ?? payload?.predicted_retention_percent ?? 70),
      5,
      99
    ),
    confidencePercent: clamp(Number(payload?.confidence_percent ?? payload?.confidence ?? 60), 10, 99),
    confidenceLevel: String(payload?.confidence_level || 'medium').toLowerCase(),
    reason: String(payload?.reason || 'Estimated from opener strength, pacing cadence, and novelty continuity.')
  }
}
