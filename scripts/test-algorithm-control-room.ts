import assert from 'assert'
import { ALGORITHM_PRESET_TEMPLATES, DEFAULT_ALGORITHM_PARAMS } from '../src/dev/algorithm/presets'
import {
  computeFeatures,
  computeRetentionScore,
  computeSubscores,
  evaluateRetentionScoring
} from '../src/dev/algorithm/scoring/retentionScoring'

const syntheticAnalysis = {
  duration: 42,
  engagement_windows: [
    { start: 0, end: 4, score: 0.84, emotionIntensity: 0.78, speechIntensity: 0.7, novelty: 0.82, energy: 0.88 },
    { start: 4, end: 12, score: 0.61, emotionIntensity: 0.55, speechIntensity: 0.62, novelty: 0.53, energy: 0.57 },
    { start: 12, end: 20, score: 0.4, emotionIntensity: 0.35, speechIntensity: 0.3, novelty: 0.42, energy: 0.33 },
    { start: 20, end: 30, score: 0.72, emotionIntensity: 0.68, speechIntensity: 0.7, novelty: 0.58, energy: 0.73 },
    { start: 30, end: 42, score: 0.66, emotionIntensity: 0.61, speechIntensity: 0.64, novelty: 0.52, energy: 0.69 }
  ],
  silence_ratio: 0.13,
  jump_cut_severity: 0.29
}

const syntheticTranscript = [
  { start: 0, end: 4, text: 'Here is the fastest way to fix your edits today.' },
  { start: 4, end: 10, text: 'Um this part shows context and key details before we jump.' },
  { start: 10, end: 18, text: 'Now we compare old flow versus improved pacing in real time.' },
  { start: 18, end: 30, text: 'The payoff comes when the energy spikes and the story turns.' },
  { start: 30, end: 42, text: 'That is why this sequence keeps viewers watching longer.' }
]

const syntheticCuts = [
  { start: 0, end: 3.4, speed: 1 },
  { start: 3.4, end: 8.8, speed: 1.12 },
  { start: 8.8, end: 12.1, speed: 1.2 },
  { start: 12.1, end: 17.5, speed: 1 },
  { start: 17.5, end: 23.8, speed: 1.08 },
  { start: 23.8, end: 32.4, speed: 1 },
  { start: 32.4, end: 42, speed: 1 }
]

const features = computeFeatures(syntheticAnalysis, syntheticTranscript, syntheticCuts)
assert(features.duration_sec > 0, 'duration should be positive')
assert(features.segment_signals.length >= 6, 'segment signals should exist')

const subscores = computeSubscores(features, DEFAULT_ALGORITHM_PARAMS)
assert(subscores.H >= 0 && subscores.H <= 1, 'H subscore out of range')
assert(subscores.J >= 0 && subscores.J <= 1, 'J subscore out of range')

const retention = computeRetentionScore(subscores)
assert(retention >= 0 && retention <= 100, 'retention score out of range')

const full = evaluateRetentionScoring(syntheticAnalysis, syntheticTranscript, syntheticCuts, DEFAULT_ALGORITHM_PARAMS)
assert(full.score_total >= 0 && full.score_total <= 100, 'full scoring should be in range')
assert(typeof full.flags.auto_safety_adjusted === 'boolean', 'flags must include auto_safety_adjusted')

assert(ALGORITHM_PRESET_TEMPLATES.length >= 6, 'missing required presets')
for (const preset of ALGORITHM_PRESET_TEMPLATES) {
  const scored = evaluateRetentionScoring(syntheticAnalysis, syntheticTranscript, syntheticCuts, preset.params)
  assert(scored.score_total >= 0 && scored.score_total <= 100, `preset score out of range for ${preset.key}`)
}

const distinctAggression = new Set(ALGORITHM_PRESET_TEMPLATES.map((preset) => preset.params.cut_aggression))
assert(distinctAggression.size >= 4, 'preset mappings are not meaningfully differentiated')

console.log('algorithm-control-room tests passed')
