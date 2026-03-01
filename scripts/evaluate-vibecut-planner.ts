import fs from 'fs'
import path from 'path'
import {
  planRetentionEditsWithFreeAi,
  PlannerMode,
  PlannerTranscriptSegment,
  PacingAdjustment
} from '../src/lib/freeAiRetentionPlanner'

type Range = { start: number; end: number }

type Fixture = {
  id: string
  mode: PlannerMode
  metadata: {
    width: number
    height: number
    duration: number
    fps: number
  }
  frameScan: {
    portraitSignal: number
    landscapeSignal: number
    centeredFaceVerticalSignal: number
    horizontalMotionSignal: number
    highMotionShortClipSignal: number
    motionPeaks: number[]
  }
  transcriptSegments: PlannerTranscriptSegment[]
  transcriptExcerpt: string
  expected: {
    deadAirRanges: Range[]
    fillerRanges: Range[]
    protectedSpeechRanges: Range[]
    lowEnergyRanges: Range[]
    highMotionRanges: Range[]
    expectedHookRange: Range
    cadenceExpected: boolean
  }
}

type MetricScores = {
  deadAirRecall: number
  fillerRecall: number
  targetPrecision: number
  speechSafety: number
  lowEnergyCoverage: number
  motionContinuity: number
  cadenceScore: number
  frameAlignment: number
  hookScore: number
  overall: number
}

type ScenarioResult = {
  id: string
  current: MetricScores
  legacy: MetricScores
  adjustmentCounts: {
    current: { trim: number; speedUp: number; transitionBoost: number }
    legacy: { trim: number; speedUp: number; transitionBoost: number }
  }
}

type EvaluationOutput = {
  generatedAt: string
  fixtures: ScenarioResult[]
  summary: {
    averageCurrentScore: number
    averageLegacyScore: number
    averageImprovement: number
    editorRatingOutOf10: number
  }
}

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const toRange = (start: number, end: number): Range => ({
  start: Number(start.toFixed(3)),
  end: Number(end.toFixed(3))
})

const seg = (start: number, end: number, text: string, confidence = 0.88): PlannerTranscriptSegment => ({
  start: Number(start.toFixed(3)),
  end: Number(end.toFixed(3)),
  text,
  confidence
})

const rangeDuration = (range: Range) => Math.max(0, range.end - range.start)

const overlapDuration = (left: Range, right: Range) =>
  Math.max(0, Math.min(left.end, right.end) - Math.max(left.start, right.start))

const mergeRanges = (ranges: Range[]) => {
  if (!ranges.length) return []
  const sorted = ranges
    .map((range) => ({ start: Number(range.start), end: Number(range.end) }))
    .filter((range) => range.end > range.start)
    .sort((left, right) => left.start - right.start || left.end - right.end)
  if (!sorted.length) return []
  const merged: Range[] = [toRange(sorted[0].start, sorted[0].end)]
  for (let index = 1; index < sorted.length; index += 1) {
    const current = sorted[index]
    const last = merged[merged.length - 1]
    if (current.start <= last.end + 0.001) {
      last.end = Number(Math.max(last.end, current.end).toFixed(3))
      continue
    }
    merged.push(toRange(current.start, current.end))
  }
  return merged
}

const totalDuration = (ranges: Range[]) =>
  mergeRanges(ranges).reduce((sum, range) => sum + rangeDuration(range), 0)

const coveredDuration = (source: Range[], targets: Range[]) => {
  const normalizedSource = mergeRanges(source)
  const normalizedTargets = mergeRanges(targets)
  let covered = 0
  for (const target of normalizedTargets) {
    const intersections: Range[] = []
    for (const item of normalizedSource) {
      const start = Math.max(target.start, item.start)
      const end = Math.min(target.end, item.end)
      if (end > start) intersections.push({ start, end })
    }
    covered += totalDuration(intersections)
  }
  return covered
}

const boundaryDistance = (value: number, transcript: PlannerTranscriptSegment[]) => {
  if (!transcript.length) return Number.POSITIVE_INFINITY
  let minDistance = Number.POSITIVE_INFINITY
  for (const cue of transcript) {
    minDistance = Math.min(minDistance, Math.abs(value - cue.start), Math.abs(value - cue.end))
  }
  return minDistance
}

const adjustmentToRange = (adjustment: PacingAdjustment): Range => ({
  start: adjustment.start,
  end: adjustment.end
})

const computeHookIoU = (hook: Range | null, expected: Range) => {
  if (!hook) return 0
  const intersection = overlapDuration(hook, expected)
  const union = rangeDuration(hook) + rangeDuration(expected) - intersection
  if (union <= 0) return 0
  return clamp(intersection / union, 0, 1)
}

const computeCadenceScore = ({
  transitionStarts,
  duration,
  expected
}: {
  transitionStarts: number[]
  duration: number
  expected: boolean
}) => {
  if (!expected || duration < 20) return 1
  const sorted = transitionStarts
    .slice()
    .filter((value) => Number.isFinite(value) && value >= 0 && value <= duration)
    .sort((left, right) => left - right)
  const targetCount = Math.max(1, Math.round(duration / 12))
  const countScore = clamp(sorted.length / targetCount, 0, 1)
  if (sorted.length <= 1) return Number((countScore * 0.62).toFixed(4))
  const intervals = sorted.slice(1).map((value, index) => value - sorted[index])
  const avgInterval = intervals.reduce((sum, value) => sum + value, 0) / intervals.length
  const variance = intervals.reduce((sum, value) => sum + (value - avgInterval) ** 2, 0) / intervals.length
  const std = Math.sqrt(variance)
  const averageFit = clamp(1 - Math.abs(avgInterval - 12.5) / 8, 0, 1)
  const consistencyFit = clamp(1 - std / 7.5, 0, 1)
  return Number((0.55 * countScore + 0.3 * averageFit + 0.15 * consistencyFit).toFixed(4))
}

const computeFrameAlignmentScore = ({
  adjustments,
  fps
}: {
  adjustments: PacingAdjustment[]
  fps: number
}) => {
  if (!adjustments.length) return 0.8
  let good = 0
  let total = 0
  for (const adjustment of adjustments) {
    for (const boundary of [adjustment.start, adjustment.end]) {
      const frame = boundary * fps
      const rounded = Math.round(frame)
      const integerAligned = Math.abs(frame - rounded) <= 0.015
      const even = rounded % 2 === 0
      if (integerAligned && even) good += 1
      total += 1
    }
  }
  if (total === 0) return 1
  return Number(clamp(good / total, 0, 1).toFixed(4))
}

const scoreAdjustments = ({
  fixture,
  adjustments,
  hookRange
}: {
  fixture: Fixture
  adjustments: PacingAdjustment[]
  hookRange: Range | null
}): MetricScores => {
  const trimRanges = adjustments.filter((row) => row.action === 'trim').map(adjustmentToRange)
  const speedRanges = adjustments.filter((row) => row.action === 'speed_up').map(adjustmentToRange)
  const transitionRanges = adjustments.filter((row) => row.action === 'transition_boost').map(adjustmentToRange)
  const transitionStarts = adjustments
    .filter((row) => row.action === 'transition_boost')
    .map((row) => row.start)

  const deadAirTargetDuration = totalDuration(fixture.expected.deadAirRanges)
  const fillerTargetDuration = totalDuration(fixture.expected.fillerRanges)
  const lowEnergyDuration = totalDuration(fixture.expected.lowEnergyRanges)
  const trimDuration = totalDuration(trimRanges)
  const protectedDuration = totalDuration(fixture.expected.protectedSpeechRanges)
  const highMotionDuration = totalDuration(fixture.expected.highMotionRanges)

  const deadAirRecall = deadAirTargetDuration > 0
    ? coveredDuration(trimRanges, fixture.expected.deadAirRanges) / deadAirTargetDuration
    : 1
  const fillerRecall = fillerTargetDuration > 0
    ? coveredDuration(trimRanges, fixture.expected.fillerRanges) / fillerTargetDuration
    : 1

  const trimTargets = mergeRanges([...fixture.expected.deadAirRanges, ...fixture.expected.fillerRanges])
  const targetPrecision = trimDuration > 0
    ? coveredDuration(trimRanges, trimTargets) / trimDuration
    : (trimTargets.length > 0 ? 0 : 1)

  const protectedTrimRatio = trimDuration > 0
    ? coveredDuration(trimRanges, fixture.expected.protectedSpeechRanges) / trimDuration
    : 0
  const trimMidSentenceRisks = trimRanges.filter((range) => {
    const startDist = boundaryDistance(range.start, fixture.transcriptSegments)
    const endDist = boundaryDistance(range.end, fixture.transcriptSegments)
    const touchesProtectedSpeech = coveredDuration([range], fixture.expected.protectedSpeechRanges) > 0.12
    return touchesProtectedSpeech && startDist > 0.14 && endDist > 0.14
  }).length
  const midSentenceRiskRate = trimRanges.length > 0 ? trimMidSentenceRisks / trimRanges.length : 0
  const speechSafety = clamp(1 - (protectedTrimRatio * 0.7 + midSentenceRiskRate * 0.3), 0, 1)

  const lowEnergyCoverage = lowEnergyDuration > 0
    ? coveredDuration([...speedRanges, ...transitionRanges], fixture.expected.lowEnergyRanges) / lowEnergyDuration
    : 1

  const highMotionTrimRatio = highMotionDuration > 0
    ? coveredDuration(trimRanges, fixture.expected.highMotionRanges) / highMotionDuration
    : 0
  const highMotionSpeedCoverage = highMotionDuration > 0
    ? coveredDuration(speedRanges, fixture.expected.highMotionRanges) / highMotionDuration
    : 0
  const motionContinuity = clamp(1 - highMotionTrimRatio + highMotionSpeedCoverage * 0.32, 0, 1)

  const cadenceScore = computeCadenceScore({
    transitionStarts,
    duration: fixture.metadata.duration,
    expected: fixture.expected.cadenceExpected
  })

  const frameAlignment = computeFrameAlignmentScore({
    adjustments,
    fps: fixture.metadata.fps
  })

  const hookScore = computeHookIoU(hookRange, fixture.expected.expectedHookRange)

  const weighted =
    deadAirRecall * 0.16 +
    fillerRecall * 0.08 +
    targetPrecision * 0.1 +
    speechSafety * 0.16 +
    lowEnergyCoverage * 0.14 +
    motionContinuity * 0.12 +
    cadenceScore * 0.08 +
    frameAlignment * 0.08 +
    hookScore * 0.08

  return {
    deadAirRecall: Number(clamp(deadAirRecall, 0, 1).toFixed(4)),
    fillerRecall: Number(clamp(fillerRecall, 0, 1).toFixed(4)),
    targetPrecision: Number(clamp(targetPrecision, 0, 1).toFixed(4)),
    speechSafety: Number(speechSafety.toFixed(4)),
    lowEnergyCoverage: Number(clamp(lowEnergyCoverage, 0, 1).toFixed(4)),
    motionContinuity: Number(clamp(motionContinuity, 0, 1).toFixed(4)),
    cadenceScore: Number(clamp(cadenceScore, 0, 1).toFixed(4)),
    frameAlignment: Number(frameAlignment.toFixed(4)),
    hookScore: Number(clamp(hookScore, 0, 1).toFixed(4)),
    overall: Number((clamp(weighted, 0, 1) * 100).toFixed(2))
  }
}

const countWords = (value: string) => {
  const matches = String(value || '').toLowerCase().match(/[a-z0-9']+/g)
  return matches ? matches.length : 0
}

const computeMotionScoreForWindow = (start: number, end: number, peaks: number[]) => {
  if (!peaks.length) return 0.25
  const center = (start + end) / 2
  const distances = peaks.map((peak) => Math.abs(peak - center))
  const minDistance = Math.min(...distances)
  return clamp(1 - minDistance / 9, 0, 1)
}

const buildLegacyWindows = (fixture: Fixture) => {
  const safeDuration = Math.max(1, fixture.metadata.duration)
  const span = clamp(safeDuration / 18, 8, 20)
  const out: Array<{ start: number; end: number; wordsPerSecond: number; motionScore: number }> = []
  let cursor = 0
  while (cursor < safeDuration && out.length < 36) {
    const start = clamp(cursor, 0, Math.max(0, safeDuration - 0.4))
    const end = clamp(start + span, start + 0.4, safeDuration)
    const overlapping = fixture.transcriptSegments.filter((segment) => segment.end > start && segment.start < end)
    const totalWords = overlapping.reduce((sum, segment) => sum + countWords(segment.text), 0)
    const wordsPerSecond = totalWords / Math.max(0.6, end - start)
    const motionScore = computeMotionScoreForWindow(start, end, fixture.frameScan.motionPeaks)
    out.push({
      start: Number(start.toFixed(3)),
      end: Number(end.toFixed(3)),
      wordsPerSecond: Number(wordsPerSecond.toFixed(3)),
      motionScore: Number(motionScore.toFixed(3))
    })
    cursor += span * 0.88
  }
  return out
}

const buildLegacyAdjustments = (fixture: Fixture): PacingAdjustment[] => {
  const adjustments: PacingAdjustment[] = []
  const duration = fixture.metadata.duration
  const sorted = fixture.transcriptSegments
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
      intensity: Number(clamp(gap / 4.5, 0.2, 0.85).toFixed(3)),
      reason: 'Legacy pause trim.'
    })
    if (adjustments.length >= 6) break
  }
  const windows = buildLegacyWindows(fixture)
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
      reason: 'Legacy low-energy speed-up.'
    })
  }
  if (adjustments.length === 0) {
    adjustments.push({
      start: Number(clamp(duration * 0.42, 0, Math.max(0, duration - 1.2)).toFixed(3)),
      end: Number(clamp(duration * 0.42 + 0.9, 0.4, duration).toFixed(3)),
      action: 'transition_boost',
      intensity: 0.42,
      reason: 'Legacy fallback transition.'
    })
  }
  return adjustments
}

const formatCountBreakdown = (adjustments: PacingAdjustment[]) => ({
  trim: adjustments.filter((row) => row.action === 'trim').length,
  speedUp: adjustments.filter((row) => row.action === 'speed_up').length,
  transitionBoost: adjustments.filter((row) => row.action === 'transition_boost').length
})

const buildFixtures = (): Fixture[] => [
  {
    id: 'talking_head_fillers',
    mode: 'horizontal',
    metadata: { width: 1920, height: 1080, duration: 48, fps: 30 },
    frameScan: {
      portraitSignal: 0.22,
      landscapeSignal: 0.78,
      centeredFaceVerticalSignal: 0.76,
      horizontalMotionSignal: 0.38,
      highMotionShortClipSignal: 0.46,
      motionPeaks: [1.8, 6.9, 12.6, 21.4, 34.8, 41.2]
    },
    transcriptSegments: [
      seg(0, 2.5, 'Wait, this one change doubled watch time.', 0.94),
      seg(2.7, 4.8, 'So um today we are testing hook pacing.', 0.74),
      seg(5, 8.2, 'Uh I mean basically this part drags a lot.', 0.64),
      seg(8.4, 12.9, 'Here is the first actionable step and why it works.', 0.93),
      seg(13.1, 14.2, 'Keep this sentence clean and complete.', 0.91),
      seg(16.1, 20.3, 'Now compare the old timeline against the new one.', 0.9),
      seg(20.6, 24.8, 'The audience stays because each beat has progression.', 0.9),
      seg(25.2, 27.2, 'Um okay so we kind of repeat this point.', 0.68),
      seg(30.1, 33.9, 'Here is the re-entry hook right before the dip.', 0.89),
      seg(34.2, 38.1, 'Momentum rises again with a stronger visual beat.', 0.9),
      seg(38.5, 42.3, 'Then we land the payoff and tease the next step.', 0.92),
      seg(42.6, 46.9, 'If you want the preset, check the pinned comment.', 0.91)
    ],
    transcriptExcerpt: 'Wait this one change doubled watch time. We are testing hook pacing and trimming filler.',
    expected: {
      deadAirRanges: [toRange(14.3, 16.0), toRange(27.3, 30.0)],
      fillerRanges: [toRange(2.8, 8.2), toRange(25.2, 27.2)],
      protectedSpeechRanges: [toRange(8.4, 14.2), toRange(16.1, 24.8), toRange(34.2, 46.9)],
      lowEnergyRanges: [toRange(24.8, 33.2)],
      highMotionRanges: [toRange(0.8, 2.8), toRange(34.0, 42.0)],
      expectedHookRange: toRange(0, 8.2),
      cadenceExpected: true
    }
  },
  {
    id: 'gaming_high_motion',
    mode: 'vertical',
    metadata: { width: 1080, height: 1920, duration: 62, fps: 60 },
    frameScan: {
      portraitSignal: 0.84,
      landscapeSignal: 0.16,
      centeredFaceVerticalSignal: 0.71,
      horizontalMotionSignal: 0.81,
      highMotionShortClipSignal: 0.85,
      motionPeaks: [0.9, 4.1, 8.6, 12.2, 16.8, 23.4, 30.9, 38.2, 46.3, 55.1]
    },
    transcriptSegments: [
      seg(0, 2.4, 'No way chat this clip is insane.', 0.92),
      seg(2.5, 5.7, 'Watch the double fake into the wipe.', 0.91),
      seg(6, 9.2, 'Now we speed up and bait the second fight.', 0.89),
      seg(9.5, 13.6, 'This push is where most viewers stay.', 0.9),
      seg(14.1, 17.6, 'We cut dead space and keep only impact.', 0.9),
      seg(18.1, 19.2, 'uh', 0.46),
      seg(20.3, 24.9, 'Mid-round setup is slower but still relevant.', 0.82),
      seg(25.4, 28.1, 'Then the pace rises again immediately.', 0.88),
      seg(29.4, 33.9, 'Huge reaction moment plus score swing.', 0.92),
      seg(34.1, 39.6, 'Second clutch keeps momentum high.', 0.93),
      seg(40, 44.8, 'Short explanation before final rush.', 0.85),
      seg(45.3, 58.6, 'Final sequence delivers payoff and replay.', 0.92),
      seg(58.9, 61.4, 'Follow for the next clip.', 0.9)
    ],
    transcriptExcerpt: 'No way chat this clip is insane. Watch the double fake into the wipe.',
    expected: {
      deadAirRanges: [toRange(19.2, 20.2)],
      fillerRanges: [toRange(18.0, 19.2)],
      protectedSpeechRanges: [toRange(0, 17.6), toRange(29.4, 61.4)],
      lowEnergyRanges: [toRange(20.3, 28.1), toRange(40.0, 44.8)],
      highMotionRanges: [toRange(0.7, 18.5), toRange(29.2, 39.8), toRange(45.1, 58.8)],
      expectedHookRange: toRange(0, 10),
      cadenceExpected: true
    }
  },
  {
    id: 'education_natural_pauses',
    mode: 'horizontal',
    metadata: { width: 1920, height: 1080, duration: 95, fps: 30 },
    frameScan: {
      portraitSignal: 0.28,
      landscapeSignal: 0.72,
      centeredFaceVerticalSignal: 0.69,
      horizontalMotionSignal: 0.3,
      highMotionShortClipSignal: 0.35,
      motionPeaks: [2.1, 18.7, 35.5, 48.8, 63.2, 79.1, 90.2]
    },
    transcriptSegments: [
      seg(0, 4.2, 'Here is the one framework that makes edits predictable.', 0.93),
      seg(4.6, 9.1, 'Step one, map the message into clear beats.', 0.92),
      seg(9.5, 13.2, 'Step two, trim hesitation and repeated clauses.', 0.91),
      seg(13.8, 18.4, 'Step three, pace by energy not by timeline habit.', 0.91),
      seg(19.2, 24.7, 'This example keeps context while removing drag.', 0.9),
      seg(25.4, 32.1, 'We preserve meaning at sentence boundaries only.', 0.9),
      seg(34.3, 39.2, 'Now look at the before and after pacing map.', 0.89),
      seg(39.6, 45.8, 'The middle section often causes retention dips.', 0.88),
      seg(46.1, 52.8, 'Compress that valley but do not kill comprehension.', 0.89),
      seg(53.4, 60.3, 'Then add pattern interrupts every few beats.', 0.9),
      seg(60.9, 70.7, 'This keeps attention without making the edit chaotic.', 0.9),
      seg(73.2, 82.6, 'Finally, close with payoff and a next-step tease.', 0.92),
      seg(83.1, 93.8, 'Use this checklist and your watch time goes up.', 0.93)
    ],
    transcriptExcerpt: 'Framework for trimming hesitation and pacing by energy while keeping meaning intact.',
    expected: {
      deadAirRanges: [toRange(32.2, 34.2), toRange(70.8, 73.1)],
      fillerRanges: [],
      protectedSpeechRanges: [toRange(0, 32.1), toRange(34.3, 70.7), toRange(73.2, 93.8)],
      lowEnergyRanges: [toRange(39.2, 52.8)],
      highMotionRanges: [toRange(0, 5), toRange(78.5, 92.0)],
      expectedHookRange: toRange(0, 9.5),
      cadenceExpected: true
    }
  },
  {
    id: 'podcast_low_confidence',
    mode: 'horizontal',
    metadata: { width: 1920, height: 1080, duration: 78, fps: 24 },
    frameScan: {
      portraitSignal: 0.24,
      landscapeSignal: 0.76,
      centeredFaceVerticalSignal: 0.74,
      horizontalMotionSignal: 0.22,
      highMotionShortClipSignal: 0.28,
      motionPeaks: [1.5, 10.2, 21.8, 36.4, 51.3, 66.7]
    },
    transcriptSegments: [
      seg(0, 3.3, 'This story starts with a simple mistake.', 0.9),
      seg(3.6, 7.4, 'Um we thought speed alone would fix retention.', 0.66),
      seg(7.8, 11.2, 'It did not, because structure was weak.', 0.88),
      seg(11.6, 16.7, 'So we rebuilt the first fifteen seconds.', 0.89),
      seg(17.1, 21.4, 'Uh that instantly raised hold rate.', 0.62),
      seg(21.9, 28.3, 'Next we cut repeated lines and tangent loops.', 0.84),
      seg(28.9, 33.1, 'Like basically kind of the same point again.', 0.58),
      seg(34.7, 40.5, 'Then we stacked curiosity every twelve seconds.', 0.87),
      seg(41.1, 47.2, 'Mid-video compression removed the dead zone.', 0.88),
      seg(47.8, 54.6, 'I mean um that section had almost no progress.', 0.55),
      seg(55.4, 63.8, 'After cleanup the narrative became obvious.', 0.9),
      seg(64.3, 70.7, 'So viewers reached the payoff more often.', 0.9),
      seg(71.3, 77.2, 'That is the full retention playbook.', 0.92)
    ],
    transcriptExcerpt: 'We rebuilt first fifteen seconds, cut tangent loops, and compressed dead zones.',
    expected: {
      deadAirRanges: [toRange(33.2, 34.6), toRange(54.7, 55.3), toRange(70.8, 71.2)],
      fillerRanges: [toRange(3.6, 7.4), toRange(17.1, 21.4), toRange(28.9, 33.1), toRange(47.8, 54.6)],
      protectedSpeechRanges: [toRange(0, 3.3), toRange(7.8, 16.7), toRange(34.7, 47.2), toRange(55.4, 77.2)],
      lowEnergyRanges: [toRange(28.9, 40.4), toRange(47.8, 55.3)],
      highMotionRanges: [toRange(0.8, 3.6), toRange(63.8, 77.2)],
      expectedHookRange: toRange(0, 11),
      cadenceExpected: true
    }
  },
  {
    id: 'non_verbal_motion_montage',
    mode: 'vertical',
    metadata: { width: 1080, height: 1920, duration: 36, fps: 30 },
    frameScan: {
      portraitSignal: 0.88,
      landscapeSignal: 0.12,
      centeredFaceVerticalSignal: 0.62,
      horizontalMotionSignal: 0.86,
      highMotionShortClipSignal: 0.9,
      motionPeaks: [0.8, 3.6, 6.1, 9.4, 13.7, 17.8, 22.1, 27.5, 31.3, 34.6]
    },
    transcriptSegments: [],
    transcriptExcerpt: '',
    expected: {
      deadAirRanges: [],
      fillerRanges: [],
      protectedSpeechRanges: [],
      lowEnergyRanges: [toRange(10.5, 12.4), toRange(24.1, 26.2)],
      highMotionRanges: [toRange(0.5, 9.8), toRange(13.2, 23.0), toRange(27.2, 35.5)],
      expectedHookRange: toRange(0, 8.5),
      cadenceExpected: true
    }
  },
  {
    id: 'storytelling_payoff_end',
    mode: 'horizontal',
    metadata: { width: 1920, height: 1080, duration: 120, fps: 30 },
    frameScan: {
      portraitSignal: 0.31,
      landscapeSignal: 0.69,
      centeredFaceVerticalSignal: 0.7,
      horizontalMotionSignal: 0.47,
      highMotionShortClipSignal: 0.44,
      motionPeaks: [1.9, 9.7, 21.2, 37.6, 54.2, 68.9, 84.1, 100.7, 111.8]
    },
    transcriptSegments: [
      seg(0, 4.5, 'I almost quit after this edit failed hard.', 0.9),
      seg(4.9, 10.4, 'Then one retention rule changed everything.', 0.91),
      seg(10.8, 16.2, 'You open with tension, not context.', 0.91),
      seg(16.7, 23.4, 'The audience must feel unfinished business.', 0.9),
      seg(23.9, 31.7, 'So we delayed explanation and teased payoff.', 0.89),
      seg(32.3, 38.8, 'The middle is where people usually bounce.', 0.88),
      seg(39.1, 44.2, 'Um this part was too slow in version one.', 0.64),
      seg(46.4, 53.5, 'We compressed it and moved the reveal up.', 0.9),
      seg(54.1, 62.7, 'Now every twelve seconds has a new beat.', 0.9),
      seg(63.1, 71.8, 'Curiosity resolves then restarts immediately.', 0.91),
      seg(72.2, 81.9, 'This keeps completion high without feeling rushed.', 0.92),
      seg(85.2, 93.6, 'Near the end, stakes escalate before payoff.', 0.92),
      seg(94.1, 103.4, 'Then the proof lands in one clear sequence.', 0.93),
      seg(103.9, 111.7, 'That is where retention spikes hardest.', 0.93),
      seg(112.2, 118.8, 'Close by teasing the next experiment.', 0.92)
    ],
    transcriptExcerpt: 'Open with tension, compress slow middle, and restart curiosity every twelve seconds.',
    expected: {
      deadAirRanges: [toRange(44.3, 46.3), toRange(82.0, 85.1)],
      fillerRanges: [toRange(39.1, 44.2)],
      protectedSpeechRanges: [toRange(0, 38.8), toRange(46.4, 81.9), toRange(85.2, 118.8)],
      lowEnergyRanges: [toRange(38.8, 54.0), toRange(72.2, 82.0)],
      highMotionRanges: [toRange(0.8, 10.8), toRange(94.0, 112.0)],
      expectedHookRange: toRange(0, 10.8),
      cadenceExpected: true
    }
  }
]

const average = (values: number[]) => (values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0)

const printScenarioLine = (result: ScenarioResult) => {
  const delta = result.current.overall - result.legacy.overall
  const currentCuts = result.adjustmentCounts.current
  const legacyCuts = result.adjustmentCounts.legacy
  console.log(
    [
      result.id.padEnd(26),
      `current=${result.current.overall.toFixed(1).padStart(5)}`,
      `legacy=${result.legacy.overall.toFixed(1).padStart(5)}`,
      `delta=${delta >= 0 ? '+' : ''}${delta.toFixed(1).padStart(5)}`,
      `adj(cur t/s/x=${currentCuts.trim}/${currentCuts.speedUp}/${currentCuts.transitionBoost})`,
      `adj(old t/s/x=${legacyCuts.trim}/${legacyCuts.speedUp}/${legacyCuts.transitionBoost})`
    ].join(' | ')
  )
}

const main = async () => {
  const fixtures = buildFixtures()
  const results: ScenarioResult[] = []

  for (const fixture of fixtures) {
    const plan = await planRetentionEditsWithFreeAi({
      mode: fixture.mode,
      metadata: fixture.metadata,
      frameScan: fixture.frameScan,
      transcriptSegments: fixture.transcriptSegments,
      transcriptExcerpt: fixture.transcriptExcerpt
    })
    const currentAdjustments = Array.isArray(plan.pacingAdjustments) ? plan.pacingAdjustments : []
    const legacyAdjustments = buildLegacyAdjustments(fixture)
    const hookRange = plan.selectedHook
      ? toRange(plan.selectedHook.start, plan.selectedHook.end)
      : null
    const current = scoreAdjustments({
      fixture,
      adjustments: currentAdjustments,
      hookRange
    })
    const legacy = scoreAdjustments({
      fixture,
      adjustments: legacyAdjustments,
      hookRange
    })
    results.push({
      id: fixture.id,
      current,
      legacy,
      adjustmentCounts: {
        current: formatCountBreakdown(currentAdjustments),
        legacy: formatCountBreakdown(legacyAdjustments)
      }
    })
  }

  console.log('\nVibeCut planner metrics (0-100 composite)\n')
  for (const row of results) printScenarioLine(row)

  const avgCurrent = average(results.map((row) => row.current.overall))
  const avgLegacy = average(results.map((row) => row.legacy.overall))
  const avgImprovement = avgCurrent - avgLegacy
  const rating = avgCurrent / 10

  console.log('\nSummary')
  console.log(`- Average current score: ${avgCurrent.toFixed(2)}`)
  console.log(`- Average legacy score:  ${avgLegacy.toFixed(2)}`)
  console.log(`- Average improvement:   ${avgImprovement >= 0 ? '+' : ''}${avgImprovement.toFixed(2)}`)
  console.log(`- Editor rating:         ${rating.toFixed(2)}/10`)

  const payload: EvaluationOutput = {
    generatedAt: new Date().toISOString(),
    fixtures: results,
    summary: {
      averageCurrentScore: Number(avgCurrent.toFixed(2)),
      averageLegacyScore: Number(avgLegacy.toFixed(2)),
      averageImprovement: Number(avgImprovement.toFixed(2)),
      editorRatingOutOf10: Number(rating.toFixed(2))
    }
  }
  const outPath = path.resolve(process.cwd(), '..', 'output', 'vibecut-planner-metrics.json')
  fs.writeFileSync(outPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8')
  console.log(`- Report written to:     ${outPath}`)
}

main().catch((error) => {
  console.error('evaluate-vibecut-planner failed')
  console.error(error)
  process.exitCode = 1
})

