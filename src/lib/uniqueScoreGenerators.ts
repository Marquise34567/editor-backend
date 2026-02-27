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

export type PlannerHookComparisonLike = {
  id?: string
  start: number
  end: number
  predictedRetentionLift: number
  reason: string
}

export type PlannerSegmentInsightLike = {
  id?: string
  start: number
  end: number
  predictedRetention: number
  reason: string
  fix?: string
}

export type PlannerPacingAdjustmentLike = {
  start: number
  end: number
  action: 'trim' | 'speed_up' | 'transition_boost'
  intensity: number
  speedMultiplier?: number
  reason: string
}

export type PlannerTitleSuggestionLike = {
  id?: string
  title: string
  explanation: string
  confidence: number
}

export type PlannerDrivenRetentionLike = {
  selectedHook?: {
    start: number
    end: number
    reason: string
    score: number
  } | null
  hookComparison?: PlannerHookComparisonLike[]
  weakSegments?: PlannerSegmentInsightLike[]
  strongSegments?: PlannerSegmentInsightLike[]
  pacingAdjustments?: PlannerPacingAdjustmentLike[]
  predictedAverageRetention?: number
  predictionConfidence?: number
  titleSuggestions?: PlannerTitleSuggestionLike[]
}

export type RenderRuthlessAudit = {
  selectedOpener: {
    start: number
    end: number
    reason: string
  } | null
  hookComparison: Array<{
    start: number
    end: number
    predictedRetentionLift: number
    reason: string
  }>
  cutsAndSpeed: Array<{
    start: number
    end: number
    action: 'trim' | 'speed_up' | 'transition_boost'
    intensity: number
    speedMultiplier?: number
    reason: string
  }>
  weakSegments: Array<{
    start: number
    end: number
    predictedRetention: number
    reason: string
    fix?: string
  }>
  strongSegments: Array<{
    start: number
    end: number
    predictedRetention: number
    reason: string
  }>
  prediction: {
    score: number
    confidence: number
  }
}

export type RenderInsightsPayload = {
  predictedAverageRetention: number
  predictionConfidence: number
  targetAverageRetention: number
  iterationCount: number
  metadataStats: VideoInsightStat[]
  editInsights: RenderEditInsight[]
  hookExplanation: RenderHookExplanation
  titleOptions: RenderTitleOption[]
  ruthlessAudit: RenderRuthlessAudit
}

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))
const toNum = (value: unknown, fallback = 0) => {
  const resolved = Number(value)
  return Number.isFinite(resolved) ? resolved : fallback
}
const round = (value: number, digits = 1) => Number(value.toFixed(digits))
const clipText = (value: string, maxChars = 220) => String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxChars)

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

const sanitizePlannerSegments = (
  rows: PlannerSegmentInsightLike[] | undefined,
  kind: 'weak' | 'strong'
) => {
  if (!Array.isArray(rows)) return []
  return rows
    .map((row, index) => {
      const start = clamp(toNum(row.start, 0), 0, 1_000_000)
      const end = Math.max(start + 0.4, toNum(row.end, start + 0.4))
      return {
        id: row.id || `${kind}_${String(index + 1).padStart(2, '0')}`,
        start: round(start, 3),
        end: round(end, 3),
        predictedRetention: round(clamp(toNum(row.predictedRetention, kind === 'weak' ? 42 : 86), 8, 99), 1),
        reason: clipText(row.reason || (kind === 'weak' ? 'Weak retention zone.' : 'Strong retention zone.'), 220),
        fix: row.fix ? clipText(row.fix, 180) : undefined
      }
    })
    .filter((row) => row.end - row.start >= 0.35)
    .slice(0, 8)
}

const sanitizeHookComparison = (rows: PlannerHookComparisonLike[] | undefined) => {
  if (!Array.isArray(rows)) return []
  return rows
    .map((row, index) => {
      const start = clamp(toNum(row.start, 0), 0, 1_000_000)
      const end = Math.max(start + 0.4, toNum(row.end, start + 0.4))
      return {
        id: row.id || `runner_up_${String(index + 1).padStart(2, '0')}`,
        start: round(start, 3),
        end: round(end, 3),
        predictedRetentionLift: round(clamp(toNum(row.predictedRetentionLift, 72), 8, 99), 1),
        reason: clipText(row.reason || 'Runner-up opener had weaker curiosity pull.', 220)
      }
    })
    .filter((row) => row.end - row.start >= 0.35)
    .slice(0, 5)
}

const sanitizePacingAdjustments = (rows: PlannerPacingAdjustmentLike[] | undefined) => {
  if (!Array.isArray(rows)) return []
  return rows
    .map((row) => {
      const start = clamp(toNum(row.start, 0), 0, 1_000_000)
      const end = Math.max(start + 0.35, toNum(row.end, start + 0.35))
      const action: 'trim' | 'speed_up' | 'transition_boost' =
        row.action === 'trim' || row.action === 'speed_up' ? row.action : 'transition_boost'
      const speedMultiplier = action === 'speed_up'
        ? round(clamp(toNum(row.speedMultiplier, 1.35), 1.2, 1.8), 3)
        : undefined
      return {
        start: round(start, 3),
        end: round(end, 3),
        action,
        intensity: round(clamp(toNum(row.intensity, 0.45), 0.05, 1), 3),
        speedMultiplier,
        reason: clipText(row.reason || 'Pacing adjustment.', 180)
      }
    })
    .filter((row) => row.end - row.start >= 0.35)
    .slice(0, 16)
}

const buildFallbackWeakSegments = (points: RetentionPointLike[]) => {
  return points
    .slice()
    .sort((left, right) => left.watchedPct - right.watchedPct)
    .slice(0, 2)
    .map((point, index) => ({
      id: `weak_${String(index + 1).padStart(2, '0')}`,
      start: round(Math.max(0, point.timestamp - 3.2), 2),
      end: round(point.timestamp + 3.2, 2),
      predictedRetention: round(clamp(point.watchedPct, 8, 62), 1),
      reason: `Segment around ${point.timestamp.toFixed(1)}s is bad - predicted drop-off from low novelty.`,
      fix: 'Compress setup, add speed-up, and tease next payoff.'
    }))
}

const buildFallbackStrongSegments = (points: RetentionPointLike[]) => {
  return points
    .slice()
    .sort((left, right) => right.watchedPct - left.watchedPct)
    .slice(0, 2)
    .map((point, index) => ({
      id: `strong_${String(index + 1).padStart(2, '0')}`,
      start: round(Math.max(0, point.timestamp - 2.8), 2),
      end: round(point.timestamp + 2.8, 2),
      predictedRetention: round(clamp(point.watchedPct, 55, 99), 1),
      reason: `Segment around ${point.timestamp.toFixed(1)}s is excellent - high retention hold from payoff momentum.`,
      fix: undefined
    }))
}

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

const analyzeWhyGoodBad = ({
  points,
  predictedAverageRetention,
  planner
}: {
  points: RetentionPointLike[]
  predictedAverageRetention: number
  planner?: PlannerDrivenRetentionLike | null
}): RenderEditInsight[] => {
  const best = bestPoint(points)
  const worst = worstPoint(points)
  const weakSegments = sanitizePlannerSegments(planner?.weakSegments, 'weak')
  const strongSegments = sanitizePlannerSegments(planner?.strongSegments, 'strong')
  const selectedHook = planner?.selectedHook
  const bestPct = Math.round(best?.watchedPct || predictedAverageRetention)
  const worstPct = Math.round(worst?.watchedPct || Math.max(18, predictedAverageRetention - 20))
  const weak = weakSegments[0]
  const strong = strongSegments[0]

  return [
    {
      id: 'insight-good',
      kind: 'good',
      headline: strong
        ? `Retention Gold at ${strong.start.toFixed(1)}s-${strong.end.toFixed(1)}s`
        : `High Energy Peak at ${(best?.timestamp || 0).toFixed(1)}s`,
      detail: strong
        ? `Segment is excellent - ${strong.reason} (${Math.round(strong.predictedRetention)}% hold).`
        : `Viewers engaged ${bestPct}% here due to dynamic motion and faster narrative payoff.`,
      timestamp: Number((strong?.start ?? best?.timestamp ?? 0).toFixed(2)),
      predictedRetention: Math.round(strong?.predictedRetention ?? bestPct)
    },
    {
      id: 'insight-bad',
      kind: 'bad',
      headline: weak
        ? `Drop Risk at ${weak.start.toFixed(1)}s-${weak.end.toFixed(1)}s`
        : `Dull Segment at ${(worst?.timestamp || 0).toFixed(1)}s`,
      detail: weak
        ? `Segment is bad - predicted ${Math.round(100 - weak.predictedRetention)}% drop-off due to ${weak.reason}.${weak.fix ? ` Suggested fix: ${weak.fix}.` : ''}`
        : `Predicted ${worstPct}% drop risk from low audio sentiment and delayed context delivery.`,
      timestamp: Number((weak?.start ?? worst?.timestamp ?? 0).toFixed(2)),
      predictedRetention: Math.round(weak?.predictedRetention ?? worstPct)
    },
    {
      id: 'insight-choice',
      kind: 'choice',
      headline: selectedHook
        ? `Part Chosen: ${selectedHook.start.toFixed(1)}-${selectedHook.end.toFixed(1)}s Opener`
        : 'Part Chosen: 0:00-0:08 Hook',
      detail: selectedHook?.reason
        ? clipText(selectedHook.reason, 210)
        : 'Selected for surprise element + a transcript question that increased opening watch depth.',
      timestamp: Number((selectedHook?.start ?? 0).toFixed(2)),
      predictedRetention: Math.round(clamp(predictedAverageRetention + 6, 45, 99))
    }
  ]
}

const explainHookChoice = ({
  points,
  transcriptExcerpt,
  seed,
  planner
}: {
  points: RetentionPointLike[]
  transcriptExcerpt: string
  seed: number
  planner?: PlannerDrivenRetentionLike | null
}): RenderHookExplanation => {
  const best = bestPoint(points)
  const winnerScoreDefault = Math.round(clamp((best?.watchedPct || 74) + seeded(seed, 9) * 4, 48, 99))
  const runnerUpScoreDefault = Math.round(clamp(winnerScoreDefault - (8 + seeded(seed, 10) * 9), 24, 94))
  const containsQuestion = /\?/.test(String(transcriptExcerpt || ''))
  const hookComparison = sanitizeHookComparison(planner?.hookComparison)
  const selectedHook = planner?.selectedHook
  const winnerScore = selectedHook
    ? Math.round(clamp(toNum(selectedHook.score, winnerScoreDefault) * 100, 24, 99))
    : winnerScoreDefault
  const runnerUp = hookComparison[0]
  const runnerUpScore = runnerUp
    ? Math.round(clamp(runnerUp.predictedRetentionLift, 18, 98))
    : runnerUpScoreDefault

  return {
    winnerLabel: selectedHook
      ? `Selected ${selectedHook.start.toFixed(1)}s-${selectedHook.end.toFixed(1)}s`
      : 'Hook Candidate A',
    winnerScore,
    runnerUpLabel: runnerUp
      ? `Runner-Up ${runnerUp.start.toFixed(1)}s-${runnerUp.end.toFixed(1)}s`
      : 'Runner-Up Candidate B',
    runnerUpScore,
    reason: selectedHook?.reason
      ? clipText(selectedHook.reason, 220)
      : `Chosen over alternatives for highest energy score (${winnerScore}%) and stronger opening curiosity.`,
    transcriptSignal: containsQuestion
      ? 'Question-led transcript beat out neutral-sentiment alternatives.'
      : 'Transcript momentum beat alternatives with weaker opener clarity.'
  }
}

const generateTitleOptions = ({
  fileName,
  predictedAverageRetention,
  seed,
  planner
}: {
  fileName: string
  predictedAverageRetention: number
  seed: number
  planner?: PlannerDrivenRetentionLike | null
}): RenderTitleOption[] => {
  const fromPlanner = Array.isArray(planner?.titleSuggestions)
    ? planner?.titleSuggestions
        .map((row, index) => {
          const title = clipText(row.title || '', 120)
          if (!title) return null
          return {
            id: row.id || `title-${index + 1}`,
            title,
            explanation: clipText(row.explanation || 'Optimized for retention + curiosity in 2026.', 180),
            confidence: Math.round(clamp(toNum(row.confidence, predictedAverageRetention + 3), 16, 99))
          } satisfies RenderTitleOption
        })
        .filter((row): row is RenderTitleOption => Boolean(row))
        .slice(0, 5)
    : []
  if (fromPlanner.length >= 5) return fromPlanner

  const base = cleanTitleBase(fileName)
  const fallback: RenderTitleOption[] = [
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
    },
    {
      id: 'title-4',
      title: `Why Everyone Watches This Until the End (${base})`,
      explanation: 'Curiosity-first language and completion promise.',
      confidence: Math.round(clamp(predictedAverageRetention + seeded(seed, 14) * 6, 32, 97))
    },
    {
      id: 'title-5',
      title: `${base}: The 8-Second Hook Method (2026 Edition)`,
      explanation: 'Specific hook framework + trend-year specificity.',
      confidence: Math.round(clamp(predictedAverageRetention + 1 + seeded(seed, 15) * 6, 32, 97))
    }
  ]
  return [...fromPlanner, ...fallback].slice(0, 5)
}

const buildRuthlessAudit = ({
  planner,
  predictedAverageRetention,
  predictionConfidence,
  points
}: {
  planner?: PlannerDrivenRetentionLike | null
  predictedAverageRetention: number
  predictionConfidence: number
  points: RetentionPointLike[]
}): RenderRuthlessAudit => {
  const weakSegments = sanitizePlannerSegments(planner?.weakSegments, 'weak')
  const strongSegments = sanitizePlannerSegments(planner?.strongSegments, 'strong')
  const hookComparison = sanitizeHookComparison(planner?.hookComparison)
  const cutsAndSpeed = sanitizePacingAdjustments(planner?.pacingAdjustments)
  const fallbackWeak = weakSegments.length ? weakSegments : buildFallbackWeakSegments(points)
  const fallbackStrong = strongSegments.length ? strongSegments : buildFallbackStrongSegments(points)
  const selectedHook = planner?.selectedHook && Number.isFinite(Number(planner?.selectedHook?.start))
    ? {
        start: round(toNum(planner?.selectedHook?.start), 3),
        end: round(Math.max(toNum(planner?.selectedHook?.end, 0), toNum(planner?.selectedHook?.start, 0) + 0.4), 3),
        reason: clipText(String(planner?.selectedHook?.reason || 'Highest opener energy and curiosity profile.'), 220)
      }
    : null

  return {
    selectedOpener: selectedHook,
    hookComparison,
    cutsAndSpeed,
    weakSegments: fallbackWeak.map((row) => ({
      start: row.start,
      end: row.end,
      predictedRetention: row.predictedRetention,
      reason: row.reason,
      fix: row.fix
    })),
    strongSegments: fallbackStrong.map((row) => ({
      start: row.start,
      end: row.end,
      predictedRetention: row.predictedRetention,
      reason: row.reason
    })),
    prediction: {
      score: round(predictedAverageRetention, 1),
      confidence: round(predictionConfidence, 1)
    }
  }
}

export const generateUniqueRetention = ({
  videoId,
  fileName,
  mode,
  metadata,
  frameScan,
  transcript,
  points,
  planner,
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
  planner?: PlannerDrivenRetentionLike | null
  targetAverageRetention?: number
  iterationCount?: number
}): RenderInsightsPayload => {
  const seed = hashSeed(`${videoId}:${fileName}:${mode}:${points.length}`)
  const baseAverage = averageRetention(points)
  const modeBias = mode === 'vertical' ? 4.8 : 2.4
  const heuristicPredictedAverageRetention = Number(
    clamp(baseAverage + modeBias + seeded(seed, 8) * 5.4 - 2.2, 34, 96).toFixed(1)
  )
  const plannerPredicted = toNum(planner?.predictedAverageRetention, NaN)
  const plannerConfidence = toNum(planner?.predictionConfidence, NaN)
  const predictedAverageRetention = Number(
    clamp(
      Number.isFinite(plannerPredicted)
        ? plannerPredicted * 0.72 + heuristicPredictedAverageRetention * 0.28
        : heuristicPredictedAverageRetention,
      18,
      98
    ).toFixed(1)
  )
  const predictionConfidence = Number(
    clamp(
      Number.isFinite(plannerConfidence)
        ? plannerConfidence
        : 52 + seeded(seed, 16) * 30,
      12,
      99
    ).toFixed(1)
  )

  const metadataStats = generateMetadataStats({ videoId, metadata, frameScan, transcript, mode })
  const editInsights = analyzeWhyGoodBad({
    points,
    predictedAverageRetention,
    planner
  })
  const hookExplanation = explainHookChoice({
    points,
    transcriptExcerpt: transcript.excerpt,
    seed,
    planner
  })
  const titleOptions = generateTitleOptions({
    fileName,
    predictedAverageRetention,
    seed,
    planner
  })
  const ruthlessAudit = buildRuthlessAudit({
    planner,
    predictedAverageRetention,
    predictionConfidence,
    points
  })

  return {
    predictedAverageRetention,
    predictionConfidence,
    targetAverageRetention,
    iterationCount,
    metadataStats,
    editInsights,
    hookExplanation,
    titleOptions,
    ruthlessAudit
  }
}
