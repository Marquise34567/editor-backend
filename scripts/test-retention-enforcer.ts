import assert from 'assert'
import { __retentionTestUtils } from '../src/routes/jobs'

const {
  pickTopHookCandidates,
  buildRetentionJudgeReport,
  executeQualityGateRetriesForTest,
  buildTimelineWithHookAtStartForTest,
  buildPersistedRenderAnalysis
} = __retentionTestUtils

const makeWindow = (time: number, overrides: Record<string, any> = {}) => ({
  time,
  audioEnergy: 0.28,
  speechIntensity: 0.38,
  motionScore: 0.22,
  facePresence: 0.35,
  textDensity: 0.12,
  sceneChangeRate: 0.16,
  emotionalSpike: 0,
  vocalExcitement: 0.24,
  emotionIntensity: 0.26,
  audioVariance: 0.2,
  keywordIntensity: 0.1,
  curiosityTrigger: 0.08,
  fillerDensity: 0.04,
  boredomScore: 0.2,
  hookScore: 0.25,
  narrativeProgress: 0.3,
  score: 0.3,
  ...overrides
})

const run = () => {
  // 1) Hook selection determinism + length guard (5-8s)
  const windows = new Array(32).fill(null).map((_, idx) => makeWindow(idx))
  for (let second = 12; second <= 19; second += 1) {
    windows[second] = makeWindow(second, {
      score: 0.92,
      hookScore: 0.95,
      emotionIntensity: 0.84,
      vocalExcitement: 0.78,
      speechIntensity: 0.8,
      motionScore: 0.7,
      curiosityTrigger: 0.86,
      keywordIntensity: 0.72
    })
  }
  const cues = [
    { start: 11.6, end: 13.2, text: "Watch this. Here's the trick.", keywordIntensity: 0.8, curiosityTrigger: 0.9, fillerDensity: 0 },
    { start: 13.2, end: 15.1, text: 'This changed everything for us.', keywordIntensity: 0.8, curiosityTrigger: 0.6, fillerDensity: 0 },
    { start: 15.1, end: 18.8, text: 'And then the payoff happened instantly.', keywordIntensity: 0.7, curiosityTrigger: 0.7, fillerDensity: 0 }
  ]
  const segments = [{ start: 0, end: 32, speed: 1 }]
  const pickA = pickTopHookCandidates({ durationSeconds: 32, segments, windows, transcriptCues: cues })
  const pickB = pickTopHookCandidates({ durationSeconds: 32, segments, windows, transcriptCues: cues })
  assert.deepStrictEqual(pickA.selected, pickB.selected, 'hook selection should be deterministic')
  assert.ok(pickA.selected.duration >= 5 && pickA.selected.duration <= 8, 'hook duration must be 5-8s')
  const timelineWithHookFirst = buildTimelineWithHookAtStartForTest([{ start: 0, end: 32, speed: 1 }], pickA.selected)
  assert.ok(timelineWithHookFirst.length >= 1, 'timeline should include hook segment')
  assert.strictEqual(Number(timelineWithHookFirst[0].start.toFixed(3)), Number(pickA.selected.start.toFixed(3)), 'hook must be first in timeline order')
  assert.strictEqual(Number((timelineWithHookFirst[0].end - timelineWithHookFirst[0].start).toFixed(3)), Number(pickA.selected.duration.toFixed(3)), 'first segment must match selected hook length')

  // 2) Quality gate thresholds
  const strongSegments = [
    { start: 0, end: 6, speed: 1 },
    { start: 8, end: 12, speed: 1.1 },
    { start: 14, end: 18, speed: 1.08 }
  ]
  const strongRetention = {
    score: 92,
    notes: [],
    details: {
      hook: 0.9,
      pacingScore: 0.9,
      emotionalSpikeDensity: 0.82,
      boredomRemovalRatio: 0.22,
      interruptDensity: 1,
      interruptDensityRaw: 0.25,
      runtimeSeconds: 16
    }
  }
  const strongWindows = windows.map((window) => ({
    ...window,
    emotionIntensity: Math.max(window.emotionIntensity, 0.82),
    vocalExcitement: Math.max(window.vocalExcitement, 0.78),
    emotionalSpike: 1,
    score: Math.max(window.score, 0.84),
    hookScore: Math.max(window.hookScore ?? 0, 0.88)
  }))
  const strongJudge = buildRetentionJudgeReport({
    retentionScore: strongRetention,
    hook: { ...pickA.selected, score: 0.9, auditScore: 0.92, auditPassed: true, reason: 'strong', text: 'strong hook' },
    windows: strongWindows,
    clarityPenalty: 0.05,
    captionsEnabled: true,
    patternInterruptCount: 5,
    removedRanges: [{ start: 6, end: 8 }],
    segments: strongSegments
  })
  assert.ok(strongJudge.passed, 'strong attempt should pass quality gate')

  const weakRetention = {
    score: 48,
    notes: [],
    details: {
      hook: 0.45,
      pacingScore: 0.4,
      emotionalSpikeDensity: 0.1,
      boredomRemovalRatio: 0.01,
      interruptDensity: 0.1,
      interruptDensityRaw: 0.01,
      runtimeSeconds: 12
    }
  }
  const weakWindows = windows.map((window) => ({
    ...window,
    emotionIntensity: 0.12,
    vocalExcitement: 0.08,
    emotionalSpike: 0,
    score: 0.18,
    hookScore: 0.16
  }))
  const weakJudge = buildRetentionJudgeReport({
    retentionScore: weakRetention,
    hook: { ...pickA.selected, score: 0.45, auditScore: 0.4, auditPassed: false, reason: 'weak', text: '' },
    windows: weakWindows,
    clarityPenalty: 0.4,
    captionsEnabled: false,
    patternInterruptCount: 0,
    removedRanges: [],
    segments: [{ start: 0, end: 12, speed: 1 }]
  })
  assert.ok(!weakJudge.passed, 'weak attempt should fail quality gate')

  // 3) Retry loop max attempts (baseline + up to 3 retries)
  const attemptsAllFail = executeQualityGateRetriesForTest([false, false, false, false, false], 3)
  assert.strictEqual(attemptsAllFail.length, 4, 'must stop after baseline + 3 retries')
  const attemptsSecondPass = executeQualityGateRetriesForTest([false, true, false], 3)
  assert.strictEqual(attemptsSecondPass.length, 2, 'must stop immediately once passing')

  // 4) Metadata persistence through analysis builder
  const persisted = buildPersistedRenderAnalysis({
    existing: {
      hook_start_time: 12.1,
      hook_end_time: 18.1,
      hook_score: 0.91,
      retention_attempts: attemptsAllFail,
      retention_judge: weakJudge,
      selected_strategy: 'PACING_FIRST'
    },
    renderConfig: {
      mode: 'horizontal',
      verticalClipCount: 1,
      horizontalMode: { output: 'quality', fit: 'contain' },
      verticalMode: null
    },
    outputPaths: ['u1/j1/output.mp4']
  })
  assert.strictEqual(persisted.hook_start_time, 12.1, 'hook metadata should persist')
  assert.strictEqual(Array.isArray(persisted.retention_attempts), true, 'attempt metadata should persist')
  assert.strictEqual(persisted.selected_strategy, 'PACING_FIRST', 'selected strategy should persist')

  console.log('PASS retention enforcer tests')
}

try {
  run()
} catch (error) {
  console.error('FAIL retention enforcer tests')
  console.error(error)
  process.exit(1)
}
