import assert from 'assert'
import { __retentionTestUtils } from '../src/routes/jobs'

const {
  pickTopHookCandidates,
  buildRetentionJudgeReport,
  resolveQualityGateThresholds,
  computeContentSignalStrength,
  inferContentStyleProfile,
  getStyleAdjustedAggressionLevel,
  applyStyleToPacingProfile,
  alignSegmentsToRhythm,
  selectRenderableHookCandidate,
  shouldForceRescueRender,
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

  // 1b) Partition-first hooking should pick one strong 8s candidate from each section, then choose best.
  const longWindows = new Array(96).fill(null).map((_, idx) => makeWindow(idx))
  const boostRange = (start: number, end: number, boost: Record<string, number>) => {
    for (let second = start; second <= end; second += 1) {
      longWindows[second] = makeWindow(second, {
        score: 0.8,
        hookScore: 0.82,
        emotionIntensity: 0.74,
        vocalExcitement: 0.7,
        speechIntensity: 0.72,
        motionScore: 0.64,
        curiosityTrigger: 0.62,
        keywordIntensity: 0.58,
        ...boost
      })
    }
  }
  boostRange(8, 16, { score: 0.83, hookScore: 0.85 })
  boostRange(30, 38, { score: 0.86, hookScore: 0.87 })
  boostRange(52, 60, { score: 0.95, hookScore: 0.96, emotionIntensity: 0.9, vocalExcitement: 0.88, curiosityTrigger: 0.86 })
  boostRange(76, 84, { score: 0.84, hookScore: 0.86 })
  const longCues = [
    { start: 8, end: 16, text: 'Watch this first moment.', keywordIntensity: 0.6, curiosityTrigger: 0.7, fillerDensity: 0 },
    { start: 30, end: 38, text: "Here's another strong section.", keywordIntensity: 0.6, curiosityTrigger: 0.65, fillerDensity: 0 },
    { start: 52, end: 60, text: 'This changed everything, biggest payoff.', keywordIntensity: 0.9, curiosityTrigger: 0.88, fillerDensity: 0 },
    { start: 76, end: 84, text: 'Final section with energy.', keywordIntensity: 0.6, curiosityTrigger: 0.62, fillerDensity: 0 }
  ]
  const longPick = pickTopHookCandidates({
    durationSeconds: 96,
    segments: [{ start: 0, end: 96 }],
    windows: longWindows,
    transcriptCues: longCues
  })
  const sectionIndex = (start: number) => Math.min(3, Math.floor((start + 4) / 24))
  const representedSections = new Set(longPick.topCandidates.map((candidate: any) => sectionIndex(candidate.start)))
  assert.ok(representedSections.size >= 3, 'top hook candidates should represent multiple timeline sections')
  assert.strictEqual(sectionIndex(longPick.selected.start), 2, 'selected hook should come from strongest section')
  assert.ok(longPick.selected.duration >= 7 && longPick.selected.duration <= 8, 'partition hook winner should stay near 8 seconds')

  // 1c) Style inference should adapt pacing/aggression for different content archetypes.
  const reactionWindows = new Array(36).fill(null).map((_, idx) =>
    makeWindow(idx, {
      speechIntensity: 0.72,
      sceneChangeRate: 0.58,
      emotionIntensity: 0.82,
      emotionalSpike: idx % 6 === 0 ? 1 : 0,
      vocalExcitement: 0.8,
      audioVariance: 0.7
    })
  )
  const reactionProfile = inferContentStyleProfile({
    windows: reactionWindows,
    transcriptCues: [
      { start: 0, end: 3, text: 'No way chat, this changed everything.', keywordIntensity: 0.7, curiosityTrigger: 0.6, fillerDensity: 0 }
    ],
    durationSeconds: 36
  })
  assert.strictEqual(reactionProfile.style, 'reaction', 'reaction signals should map to reaction style profile')
  const reactionAggression = getStyleAdjustedAggressionLevel('medium', reactionProfile)
  assert.ok(reactionAggression === 'high' || reactionAggression === 'viral', 'reaction profile should elevate aggression level')
  const basePacing = {
    niche: 'story',
    minLen: 4.8,
    maxLen: 8.2,
    earlyTarget: 5.1,
    middleTarget: 6.1,
    lateTarget: 5.2,
    jitter: 0.24,
    speedCap: 1.32
  }
  const reactionPacing = applyStyleToPacingProfile(basePacing as any, reactionProfile, true)
  assert.ok(reactionPacing.minLen < basePacing.minLen, 'reaction style should tighten minimum segment length')
  assert.ok(reactionPacing.speedCap >= basePacing.speedCap, 'reaction style should allow equal or higher speed cap')

  // 1d) Rhythm alignment should snap near-beat boundaries without breaking segment continuity.
  const rhythmAligned = alignSegmentsToRhythm({
    segments: [
      { start: 0, end: 3.12, speed: 1 },
      { start: 3.12, end: 6.14, speed: 1.05 },
      { start: 6.14, end: 9.2, speed: 1.08 }
    ],
    durationSeconds: 9.2,
    anchors: [3, 6, 9],
    styleProfile: {
      style: 'reaction',
      confidence: 0.78,
      rationale: ['test'],
      tempoBias: -0.5,
      interruptBias: 0.2,
      hookBias: 0.08
    }
  })
  assert.strictEqual(Number(rhythmAligned[0].end.toFixed(2)), 3.0, 'first rhythm boundary should snap to nearest anchor')
  assert.strictEqual(Number(rhythmAligned[1].end.toFixed(2)), 6.0, 'second rhythm boundary should snap to nearest anchor')

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

  // 2b) Non-transcript videos should still find a renderable hook candidate.
  const nonVerbalWindows = new Array(28).fill(null).map((_, idx) =>
    makeWindow(idx, {
      score: idx >= 9 && idx <= 17 ? 0.86 : 0.28,
      hookScore: idx >= 9 && idx <= 17 ? 0.9 : 0.24,
      emotionIntensity: idx >= 9 && idx <= 17 ? 0.82 : 0.28,
      vocalExcitement: idx >= 9 && idx <= 17 ? 0.76 : 0.24,
      speechIntensity: idx >= 9 && idx <= 17 ? 0.7 : 0.34,
      motionScore: idx >= 9 && idx <= 17 ? 0.8 : 0.2
    })
  )
  const nonVerbalPick = pickTopHookCandidates({
    durationSeconds: 28,
    segments: [{ start: 0, end: 28 }],
    windows: nonVerbalWindows,
    transcriptCues: []
  })
  const nonVerbalSignalStrength = computeContentSignalStrength(nonVerbalWindows)
  const nonVerbalDecision = selectRenderableHookCandidate({
    candidates: nonVerbalPick.topCandidates.length ? nonVerbalPick.topCandidates : [nonVerbalPick.selected],
    aggressionLevel: 'medium',
    hasTranscript: false,
    signalStrength: nonVerbalSignalStrength
  })
  assert.ok(nonVerbalDecision, 'non-transcript content should still produce a renderable hook decision')
  assert.ok(Boolean(nonVerbalDecision?.candidate), 'hook decision must include a hook candidate')

  // 2c) Adaptive thresholds should relax in no-transcript/low-signal mode.
  const adaptiveThresholds = resolveQualityGateThresholds({
    aggressionLevel: 'medium',
    hasTranscript: false,
    signalStrength: 0.41
  })
  assert.ok(adaptiveThresholds.hook_strength < 80, 'adaptive thresholds should reduce hook requirement when transcript is missing')
  assert.ok(adaptiveThresholds.retention_score < 75, 'adaptive thresholds should reduce retention requirement on low-signal footage')

  // 2d) Rescue mode should allow minimally watchable renders instead of hard fail.
  const rescueEligible = shouldForceRescueRender({
    retention_score: 47,
    hook_strength: 56,
    pacing_score: 52,
    clarity_score: 58,
    emotional_pull: 49,
    why_keep_watching: [],
    what_is_generic: ['low emotional pull'],
    required_fixes: {
      stronger_hook: true,
      raise_emotion: true,
      improve_pacing: false,
      increase_interrupts: false
    },
    applied_thresholds: {
      hook_strength: 70,
      emotional_pull: 62,
      pacing_score: 64,
      retention_score: 66
    },
    gate_mode: 'adaptive',
    passed: false
  })
  assert.ok(rescueEligible, 'rescue override should allow minimally watchable rescue renders')
  const rescueIneligible = shouldForceRescueRender({
    retention_score: 39,
    hook_strength: 44,
    pacing_score: 41,
    clarity_score: 55,
    emotional_pull: 38,
    why_keep_watching: [],
    what_is_generic: ['too generic'],
    required_fixes: {
      stronger_hook: true,
      raise_emotion: true,
      improve_pacing: true,
      increase_interrupts: true
    },
    applied_thresholds: {
      hook_strength: 70,
      emotional_pull: 62,
      pacing_score: 64,
      retention_score: 66
    },
    gate_mode: 'adaptive',
    passed: false
  })
  assert.ok(!rescueIneligible, 'rescue override should still reject unwatchable output')

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
      selected_strategy: 'PACING_FIRST',
      style_profile: {
        style: 'reaction',
        confidence: 0.74,
        rationale: ['test'],
        tempoBias: -0.5,
        interruptBias: 0.2,
        hookBias: 0.08
      },
      beat_anchors: [2.5, 5.2, 8.1],
      output_upload_fallback: {
        used: true,
        mode: 'local',
        failedOutputs: ['u1/j1/output.mp4']
      }
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
  assert.strictEqual(persisted.style_profile?.style, 'reaction', 'style profile should persist')
  assert.strictEqual(Array.isArray(persisted.beat_anchors), true, 'beat anchors should persist')
  assert.strictEqual(Boolean(persisted.output_upload_fallback?.used), true, 'upload fallback metadata should persist')

  console.log('PASS retention enforcer tests')
}

try {
  run()
} catch (error) {
  console.error('FAIL retention enforcer tests')
  console.error(error)
  process.exit(1)
}
