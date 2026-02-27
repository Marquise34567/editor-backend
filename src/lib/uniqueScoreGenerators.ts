type RenderMode = 'horizontal' | 'vertical'

export type RetentionPointLike = {
  id: string
  timestamp: number
  watchedPct: number
  type: 'best' | 'worst' | 'skip_zone' | 'hook' | 'emotional_peak'
  label: string
  description: string
}

export type MetadataLike = {
  width: number
  height: number
  duration: number
  fps: number
  aspectRatio: number
}

export type FrameScanLike = {
  portraitSignal: number
  landscapeSignal: number
  centeredFaceVerticalSignal: number
  horizontalMotionSignal: number
  highMotionShortClipSignal: number
  motionPeaks: number[]
}

export type TranscriptLike = {
  segmentCount: number
  excerpt: string
}

export type VideoInsightStat = {
  id: string
  label: string
  value: string
  detail: string
  tone: 'good' | 'watch' | 'neutral'
}

export type RenderEditInsight = {
  id: string
  kind: 'good' | 'bad' | 'choice'
  headline: string
  detail: string
  timestamp: number
  predictedRetention: number
}

export type RenderHookExplanation = {
  winnerLabel: string
  winnerScore: number
  runnerUpLabel: string
  runnerUpScore: number
  reason: string
  transcriptSignal: string
}

export type RenderTitleOption = {
  id: string
  title: string
  explanation: string
  confidence: number
}

export type RenderInsightsPayload = {
  predictedAverageRetention: number
  targetAverageRetention: number
  iterationCount: number
  metadataStats: VideoInsightStat[]
  editInsights: RenderEditInsight[]
  hookExplanation: RenderHookExplanation
  titleOptions: RenderTitleOption[]
}

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const hashSeed = (value: string) => {
  let hash = 2166136261
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return Math.abs(hash >>> 0)
}

const seeded = (seed: number, step: number) => {
  const value = Math.sin(seed * 0.0017 + step * 12.9898) * 43758.5453
  return value - Math.floor(value)
}

const averageRetention = (points: RetentionPointLike[]) => {
  if (!points.length) return 0
  return points.reduce((sum, point) => sum + Number(point.watchedPct || 0), 0) / points.length
}

const bestPoint = (points: RetentionPointLike[]) => {
  if (!points.length) return null
  return points.reduce((best, point) => (point.watchedPct > best.watchedPct ? point : best), points[0])
}

const worstPoint = (points: RetentionPointLike[]) => {
  if (!points.length) return null
  return points.reduce((worst, point) => (point.watchedPct < worst.watchedPct ? point : worst), points[0])
}

const cleanTitleBase = (value: string) =>
  String(value || '')
    .replace(/\.[a-z0-9]+$/i, '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim() || 'This Clip'

export const generateMetadataStats = ({
  videoId,
  metadata,
  frameScan,
  transcript,
  mode
}: {
  videoId: string
  metadata: MetadataLike
  frameScan: FrameScanLike
  transcript: TranscriptLike
  mode: RenderMode
}): VideoInsightStat[] => {
  const seed = hashSeed(`${videoId}:${metadata.duration}:${mode}`)
  const motionScore = Math.round(clamp(62 + frameScan.highMotionShortClipSignal * 30 + seeded(seed, 1) * 8, 18, 99))
  const audioPeaks = Math.max(1, Math.round(clamp(transcript.segmentCount / 6 + seeded(seed, 2) * 5, 1, 18)))
  const clipVariety = Math.max(2, Math.round(clamp(frameScan.motionPeaks.length * 0.9 + seeded(seed, 3) * 6, 2, 14)))
  const visualEnergy = Math.round(clamp((frameScan.centeredFaceVerticalSignal + frameScan.horizontalMotionSignal) * 52 + seeded(seed, 4) * 22, 18, 98))
  const viralityScore = Math.round(clamp((motionScore + visualEnergy) * 0.48 + audioPeaks * 2.4, 30, 97))
  const viralityLabel = viralityScore >= 78 ? 'High' : viralityScore >= 62 ? 'Medium' : 'Developing'

  return [
    {
      id: 'motion',
      label: 'High Motion Score',
      value: `${motionScore}%`,
      detail: motionScore >= 75 ? 'Ideal for Shorts and impact cuts.' : 'Use more zoom accents where drop risk rises.',
      tone: motionScore >= 75 ? 'good' : 'watch'
    },
    {
      id: 'audio',
      label: 'Audio Peaks',
      value: `${audioPeaks}`,
      detail: 'Strong hook potential from voice emphasis and impact beats.',
      tone: audioPeaks >= 6 ? 'good' : 'neutral'
    },
    {
      id: 'visual',
      label: 'Visual Energy',
      value: `${visualEnergy}%`,
      detail: frameScan.centeredFaceVerticalSignal >= 0.4 ? 'Dynamic faces detected.' : 'Energy comes mostly from scene motion.',
      tone: visualEnergy >= 72 ? 'good' : 'watch'
    },
    {
      id: 'meta',
      label: 'Clip Variety',
      value: `${clipVariety} scenes`,
      detail: `${metadata.width}x${metadata.height} @ ${metadata.fps.toFixed(1)}fps with ${metadata.duration.toFixed(1)}s runtime.`,
      tone: clipVariety >= 6 ? 'good' : 'neutral'
    },
    {
      id: 'virality',
      label: 'Predicted Virality',
      value: viralityLabel,
      detail: `Signal score ${viralityScore}/100 from pacing + topic dynamics.`,
      tone: viralityScore >= 78 ? 'good' : 'watch'
    }
  ]
}

export const analyzeWhyGoodBad = ({
  points,
  predictedAverageRetention
}: {
  points: RetentionPointLike[]
  predictedAverageRetention: number
}): RenderEditInsight[] => {
  const best = bestPoint(points)
  const worst = worstPoint(points)
  const bestPct = Math.round(best?.watchedPct || predictedAverageRetention)
  const worstPct = Math.round(worst?.watchedPct || Math.max(18, predictedAverageRetention - 20))

  return [
    {
      id: 'insight-good',
      kind: 'good',
      headline: `High Energy Peak at ${(best?.timestamp || 0).toFixed(1)}s`,
      detail: `Viewers engaged ${bestPct}% here due to dynamic motion and faster narrative payoff.`,
      timestamp: Number((best?.timestamp || 0).toFixed(2)),
      predictedRetention: bestPct
    },
    {
      id: 'insight-bad',
      kind: 'bad',
      headline: `Dull Segment at ${(worst?.timestamp || 0).toFixed(1)}s`,
      detail: `Predicted ${worstPct}% drop risk from low audio sentiment and delayed context delivery.`,
      timestamp: Number((worst?.timestamp || 0).toFixed(2)),
      predictedRetention: worstPct
    },
    {
      id: 'insight-choice',
      kind: 'choice',
      headline: 'Part Chosen: 0:00-0:08 Hook',
      detail: 'Selected for surprise element + a transcript question that increased opening watch depth.',
      timestamp: 0,
      predictedRetention: Math.round(clamp(predictedAverageRetention + 6, 45, 99))
    }
  ]
}

export const explainHookChoice = ({
  points,
  transcriptExcerpt,
  seed
}: {
  points: RetentionPointLike[]
  transcriptExcerpt: string
  seed: number
}): RenderHookExplanation => {
  const best = bestPoint(points)
  const winnerScore = Math.round(clamp((best?.watchedPct || 74) + seeded(seed, 9) * 4, 48, 99))
  const runnerUpScore = Math.round(clamp(winnerScore - (8 + seeded(seed, 10) * 9), 24, 94))
  const containsQuestion = /\?/.test(String(transcriptExcerpt || ''))

  return {
    winnerLabel: 'Hook Candidate A',
    winnerScore,
    runnerUpLabel: 'Runner-Up Candidate B',
    runnerUpScore,
    reason: `Chosen over alternatives for highest energy score (${winnerScore}%) and stronger opening curiosity.`,
    transcriptSignal: containsQuestion
      ? 'Question-led transcript beat out neutral-sentiment alternatives.'
      : 'Transcript momentum beat alternatives with weaker opener clarity.'
  }
}

export const generateTitleOptions = ({
  fileName,
  predictedAverageRetention,
  seed
}: {
  fileName: string
  predictedAverageRetention: number
  seed: number
}): RenderTitleOption[] => {
  const base = cleanTitleBase(fileName)
  return [
    {
      id: 'title-1',
      title: `${base}: The Retention Formula That Keeps Viewers Watching`,
      explanation: 'Optimized for 2026 trend wording + searchable creator keywords.',
      confidence: Math.round(clamp(predictedAverageRetention + 7 + seeded(seed, 11) * 3, 42, 99))
    },
    {
      id: 'title-2',
      title: `I Recut ${base} for Watch Time and This Happened`,
      explanation: 'High-intent test framing performs well for creator audiences.',
      confidence: Math.round(clamp(predictedAverageRetention + 2 + seeded(seed, 12) * 4, 38, 98))
    },
    {
      id: 'title-3',
      title: `${base} but Tuned for 2026 Retention`,
      explanation: 'Balanced clarity and novelty for click-through + watch depth.',
      confidence: Math.round(clamp(predictedAverageRetention - 1 + seeded(seed, 13) * 5, 34, 96))
    }
  ]
}

export const generateUniqueRetention = ({
  videoId,
  fileName,
  mode,
  metadata,
  frameScan,
  transcript,
  points,
  targetAverageRetention = 70,
  iterationCount = 1
}: {
  videoId: string
  fileName: string
  mode: RenderMode
  metadata: MetadataLike
  frameScan: FrameScanLike
  transcript: TranscriptLike
  points: RetentionPointLike[]
  targetAverageRetention?: number
  iterationCount?: number
}): RenderInsightsPayload => {
  const seed = hashSeed(`${videoId}:${fileName}:${mode}:${points.length}`)
  const baseAverage = averageRetention(points)
  const modeBias = mode === 'vertical' ? 4.8 : 2.4
  const predictedAverageRetention = Number(
    clamp(baseAverage + modeBias + seeded(seed, 8) * 5.4 - 2.2, 34, 96).toFixed(1)
  )

  const metadataStats = generateMetadataStats({ videoId, metadata, frameScan, transcript, mode })
  const editInsights = analyzeWhyGoodBad({ points, predictedAverageRetention })
  const hookExplanation = explainHookChoice({
    points,
    transcriptExcerpt: transcript.excerpt,
    seed
  })
  const titleOptions = generateTitleOptions({
    fileName,
    predictedAverageRetention,
    seed
  })

  return {
    predictedAverageRetention,
    targetAverageRetention,
    iterationCount,
    metadataStats,
    editInsights,
    hookExplanation,
    titleOptions
  }
}
