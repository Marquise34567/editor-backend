const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value))
const clamp01 = (value: number) => clamp(value, 0, 1)

export type StyleArchetypeId =
  | 'high_stakes_challenge'
  | 'longform_reaction_commentary'
  | 'cinematic_lifestyle_archive'
  | 'energetic_vlog'

export type RetentionModeProfile = 'safe' | 'balanced' | 'viral'
export type ContentStyleSignal = 'reaction' | 'vlog' | 'tutorial' | 'gaming' | 'story'
export type NicheSignal = 'high_energy' | 'education' | 'talking_head' | 'story'

export type StyleArchetypeBlend = Record<StyleArchetypeId, number>

type BaseArchetypeProfile = {
  avgCutInterval: number
  patternInterruptInterval: number
  zoomFrequencyPer10Seconds: number
  captionEmphasisRatePer10Seconds: number
  energyEscalationSlope: number
  silenceTrimTolerance: number
  hookPlacementTargetSec: number
  autoEscalationWindowSec: number
}

const ARCHETYPE_IDS: StyleArchetypeId[] = [
  'high_stakes_challenge',
  'longform_reaction_commentary',
  'cinematic_lifestyle_archive',
  'energetic_vlog'
]

const BASE_ARCHETYPE_PROFILES: Record<StyleArchetypeId, BaseArchetypeProfile> = {
  high_stakes_challenge: {
    avgCutInterval: 1.25,
    patternInterruptInterval: 4.2,
    zoomFrequencyPer10Seconds: 0.62,
    captionEmphasisRatePer10Seconds: 1.08,
    energyEscalationSlope: 0.085,
    silenceTrimTolerance: 0.16,
    hookPlacementTargetSec: 4.2,
    autoEscalationWindowSec: 6
  },
  longform_reaction_commentary: {
    avgCutInterval: 2.7,
    patternInterruptInterval: 8.8,
    zoomFrequencyPer10Seconds: 0.24,
    captionEmphasisRatePer10Seconds: 0.44,
    energyEscalationSlope: 0.028,
    silenceTrimTolerance: 0.42,
    hookPlacementTargetSec: 6.8,
    autoEscalationWindowSec: 7
  },
  cinematic_lifestyle_archive: {
    avgCutInterval: 3.8,
    patternInterruptInterval: 11.6,
    zoomFrequencyPer10Seconds: 0.2,
    captionEmphasisRatePer10Seconds: 0.3,
    energyEscalationSlope: 0.016,
    silenceTrimTolerance: 0.58,
    hookPlacementTargetSec: 7.2,
    autoEscalationWindowSec: 8
  },
  energetic_vlog: {
    avgCutInterval: 1.9,
    patternInterruptInterval: 6.2,
    zoomFrequencyPer10Seconds: 0.38,
    captionEmphasisRatePer10Seconds: 0.72,
    energyEscalationSlope: 0.052,
    silenceTrimTolerance: 0.28,
    hookPlacementTargetSec: 5.6,
    autoEscalationWindowSec: 6
  }
}

const MODE_DEFAULT_BLEND: Record<RetentionModeProfile, StyleArchetypeBlend> = {
  viral: {
    high_stakes_challenge: 0.7,
    longform_reaction_commentary: 0,
    cinematic_lifestyle_archive: 0,
    energetic_vlog: 0.3
  },
  balanced: {
    high_stakes_challenge: 0.08,
    longform_reaction_commentary: 0.44,
    cinematic_lifestyle_archive: 0.24,
    energetic_vlog: 0.24
  },
  safe: {
    high_stakes_challenge: 0.04,
    longform_reaction_commentary: 0.52,
    cinematic_lifestyle_archive: 0.4,
    energetic_vlog: 0.04
  }
}

export type RetentionBehaviorStyleProfile = {
  styleName: string
  avgCutInterval: number
  patternInterruptInterval: number
  zoomFrequencyPer10Seconds: number
  captionEmphasisRatePer10Seconds: number
  energyEscalationCurve: 'calm' | 'steady' | 'aggressive'
  silenceTrimTolerance: number
  hookPlacementTargetSec: number
  autoEscalationWindowSec: number
  archetypeBlend: StyleArchetypeBlend
}

export type ResolveRuntimeStyleProfileArgs = {
  mode: RetentionModeProfile
  contentStyle: ContentStyleSignal
  niche: NicheSignal
  contentStyleConfidence?: number
  nicheConfidence?: number
  explicitBlend?: Partial<StyleArchetypeBlend> | null
}

const createEmptyBlend = (): StyleArchetypeBlend => ({
  high_stakes_challenge: 0,
  longform_reaction_commentary: 0,
  cinematic_lifestyle_archive: 0,
  energetic_vlog: 0
})

export const normalizeStyleArchetypeBlend = (
  blend?: Partial<StyleArchetypeBlend> | null,
  fallbackMode: RetentionModeProfile = 'balanced'
): StyleArchetypeBlend => {
  const fallback = MODE_DEFAULT_BLEND[fallbackMode] || MODE_DEFAULT_BLEND.balanced
  const output = createEmptyBlend()
  let total = 0
  for (const id of ARCHETYPE_IDS) {
    const raw = Number(blend?.[id])
    const value = Number.isFinite(raw) && raw > 0 ? raw : 0
    output[id] = value
    total += value
  }
  if (total <= 0) {
    return { ...fallback }
  }
  for (const id of ARCHETYPE_IDS) {
    output[id] = Number((output[id] / total).toFixed(4))
  }
  return output
}

const weightedMean = (
  blend: StyleArchetypeBlend,
  selector: (profile: BaseArchetypeProfile) => number
) => {
  let sum = 0
  for (const id of ARCHETYPE_IDS) {
    sum += selector(BASE_ARCHETYPE_PROFILES[id]) * blend[id]
  }
  return sum
}

const deriveEscalationCurve = (slope: number): RetentionBehaviorStyleProfile['energyEscalationCurve'] => {
  if (slope >= 0.055) return 'aggressive'
  if (slope >= 0.028) return 'steady'
  return 'calm'
}

export const buildBehaviorStyleProfileFromBlend = (
  blendInput: Partial<StyleArchetypeBlend> | StyleArchetypeBlend,
  styleName = 'custom'
): RetentionBehaviorStyleProfile => {
  const blend = normalizeStyleArchetypeBlend(blendInput)
  const slope = weightedMean(blend, (profile) => profile.energyEscalationSlope)
  return {
    styleName,
    avgCutInterval: Number(clamp(weightedMean(blend, (profile) => profile.avgCutInterval), 0.65, 6).toFixed(3)),
    patternInterruptInterval: Number(clamp(weightedMean(blend, (profile) => profile.patternInterruptInterval), 3, 14).toFixed(3)),
    zoomFrequencyPer10Seconds: Number(clamp(weightedMean(blend, (profile) => profile.zoomFrequencyPer10Seconds), 0.05, 1.2).toFixed(4)),
    captionEmphasisRatePer10Seconds: Number(clamp(weightedMean(blend, (profile) => profile.captionEmphasisRatePer10Seconds), 0.05, 2).toFixed(4)),
    energyEscalationCurve: deriveEscalationCurve(slope),
    silenceTrimTolerance: Number(clamp(weightedMean(blend, (profile) => profile.silenceTrimTolerance), 0.1, 0.9).toFixed(4)),
    hookPlacementTargetSec: Number(clamp(weightedMean(blend, (profile) => profile.hookPlacementTargetSec), 2.8, 8).toFixed(3)),
    autoEscalationWindowSec: Number(clamp(weightedMean(blend, (profile) => profile.autoEscalationWindowSec), 5.2, 9).toFixed(3)),
    archetypeBlend: blend
  }
}

export const resolveRuntimeStyleProfile = (args: ResolveRuntimeStyleProfileArgs) => {
  const mode = args.mode || 'balanced'
  const styleConfidence = clamp01(Number(args.contentStyleConfidence ?? 0.55))
  const nicheConfidence = clamp01(Number(args.nicheConfidence ?? 0.55))
  const baseBlend = args.explicitBlend
    ? normalizeStyleArchetypeBlend(args.explicitBlend, mode)
    : { ...(MODE_DEFAULT_BLEND[mode] || MODE_DEFAULT_BLEND.balanced) }
  const adjusted = { ...baseBlend }

  const styleImpact = 0.12 + styleConfidence * 0.18
  const nicheImpact = 0.14 + nicheConfidence * 0.18

  if (args.contentStyle === 'reaction' || args.contentStyle === 'gaming') {
    adjusted.high_stakes_challenge += styleImpact * 0.55
    adjusted.energetic_vlog += styleImpact * 0.45
  } else if (args.contentStyle === 'tutorial') {
    adjusted.longform_reaction_commentary += styleImpact * 0.62
    adjusted.cinematic_lifestyle_archive += styleImpact * 0.38
  } else if (args.contentStyle === 'vlog') {
    adjusted.energetic_vlog += styleImpact * 0.58
    adjusted.cinematic_lifestyle_archive += styleImpact * 0.42
  } else {
    adjusted.cinematic_lifestyle_archive += styleImpact * 0.45
    adjusted.longform_reaction_commentary += styleImpact * 0.55
  }

  if (args.niche === 'high_energy') {
    adjusted.high_stakes_challenge += nicheImpact * 0.6
    adjusted.energetic_vlog += nicheImpact * 0.4
  } else if (args.niche === 'education') {
    adjusted.longform_reaction_commentary += nicheImpact * 0.67
    adjusted.cinematic_lifestyle_archive += nicheImpact * 0.33
  } else if (args.niche === 'talking_head') {
    adjusted.longform_reaction_commentary += nicheImpact * 0.74
    adjusted.cinematic_lifestyle_archive += nicheImpact * 0.26
  } else {
    adjusted.cinematic_lifestyle_archive += nicheImpact * 0.56
    adjusted.energetic_vlog += nicheImpact * 0.22
    adjusted.longform_reaction_commentary += nicheImpact * 0.22
  }

  const blend = normalizeStyleArchetypeBlend(adjusted, mode)
  const profileName = `${mode}_adaptive_v1`
  return {
    blend,
    profile: buildBehaviorStyleProfileFromBlend(blend, profileName)
  }
}

export type DecisionEventType =
  | 'hook'
  | 'cut'
  | 'zoom'
  | 'caption_emphasis'
  | 'pattern_interrupt'
  | 'broll'
  | 'auto_escalation'

export type DecisionTimelineEvent = {
  t: number
  type: DecisionEventType
  amount?: number
  word?: string
  meta?: Record<string, unknown>
}

export type EditDecisionTimeline = {
  styleName: string
  events: DecisionTimelineEvent[]
}

export type TimelineCue = {
  start: number
  text?: string
  keywordIntensity?: number
  curiosityTrigger?: number
}

export type TimelineHook = {
  start: number
}

export type TimelineSegment = {
  start: number
  end: number
  zoom?: number
  emphasize?: boolean
}

export type AutoEscalationEvent = {
  t: number
  insertedCut: boolean
  reason: string
  actions: string[]
}

export const buildEditDecisionTimeline = ({
  styleName,
  hook,
  segments,
  cues,
  patternInterruptCount,
  autoEscalationEvents,
  includeBrollMarkers = false
}: {
  styleName: string
  hook?: TimelineHook | null
  segments: TimelineSegment[]
  cues?: TimelineCue[]
  patternInterruptCount?: number
  autoEscalationEvents?: AutoEscalationEvent[]
  includeBrollMarkers?: boolean
}): EditDecisionTimeline => {
  const events: DecisionTimelineEvent[] = []
  if (hook && Number.isFinite(hook.start)) {
    events.push({ t: Number(clamp(hook.start, 0, 60 * 60 * 6).toFixed(3)), type: 'hook' })
  }
  const sortedSegments = (Array.isArray(segments) ? segments : [])
    .filter((segment) => Number.isFinite(segment.start) && Number.isFinite(segment.end) && segment.end > segment.start)
    .slice()
    .sort((left, right) => left.start - right.start)

  for (let index = 1; index < sortedSegments.length; index += 1) {
    events.push({ t: Number(sortedSegments[index].start.toFixed(3)), type: 'cut' })
  }
  for (const segment of sortedSegments) {
    if (Number(segment.zoom || 0) >= 0.02) {
      events.push({
        t: Number(segment.start.toFixed(3)),
        type: 'zoom',
        amount: Number((1 + Number(segment.zoom || 0)).toFixed(3))
      })
    }
  }

  const emphasisCandidates = sortedSegments
    .filter((segment) => Boolean(segment.emphasize))
    .map((segment) => Number((segment.start + (segment.end - segment.start) * 0.5).toFixed(3)))
  const explicitInterruptCount = Math.max(0, Number(patternInterruptCount || 0))
  const interruptMarks = explicitInterruptCount > 0
    ? emphasisCandidates.slice(0, explicitInterruptCount)
    : emphasisCandidates
  for (const marker of interruptMarks) {
    events.push({ t: marker, type: 'pattern_interrupt' })
  }

  const captionCues = (Array.isArray(cues) ? cues : [])
    .filter((cue) => Number.isFinite(cue.start))
    .filter((cue) => (
      Number(cue.keywordIntensity ?? 0) >= 0.58 ||
      Number(cue.curiosityTrigger ?? 0) >= 0.55 ||
      /[A-Z]{3,}/.test(String(cue.text || ''))
    ))
    .sort((a, b) => a.start - b.start)
  let lastCaptionAt = -999
  for (const cue of captionCues) {
    if (cue.start - lastCaptionAt < 0.9) continue
    events.push({
      t: Number(cue.start.toFixed(3)),
      type: 'caption_emphasis',
      word: String(cue.text || '').trim().split(/\s+/).slice(0, 3).join(' ')
    })
    lastCaptionAt = cue.start
  }

  if (includeBrollMarkers) {
    for (const segment of sortedSegments) {
      const segmentDuration = segment.end - segment.start
      if (segmentDuration >= 3.6 && !segment.emphasize) {
        events.push({
          t: Number((segment.start + segmentDuration * 0.35).toFixed(3)),
          type: 'broll'
        })
      }
    }
  }

  for (const event of Array.isArray(autoEscalationEvents) ? autoEscalationEvents : []) {
    if (!Number.isFinite(event.t)) continue
    events.push({
      t: Number(event.t.toFixed(3)),
      type: 'auto_escalation',
      meta: {
        insertedCut: Boolean(event.insertedCut),
        reason: event.reason,
        actions: event.actions
      }
    })
  }

  const deduped = events
    .sort((left, right) => left.t - right.t || left.type.localeCompare(right.type))
    .filter((event, index, list) => {
      if (index === 0) return true
      const prev = list[index - 1]
      return !(event.type === prev.type && Math.abs(event.t - prev.t) < 0.04)
    })

  return { styleName, events: deduped }
}

export type TimelineFeatureSnapshot = {
  cutsPer10Seconds: number
  averageShotDuration: number
  zoomFrequencyPerMinute: number
  captionEmphasisRatePer10Seconds: number
  energySpikeDensityPer10Seconds: number
  patternInterruptSpacingSeconds: number
  hookPlacementSeconds: number | null
  emotionalEscalationSlope: number
}

export type EnergySample = {
  t: number
  value: number
}

export const extractTimelineFeatures = ({
  timeline,
  durationSeconds,
  energySamples
}: {
  timeline: EditDecisionTimeline
  durationSeconds: number
  energySamples?: EnergySample[]
}): TimelineFeatureSnapshot => {
  const safeDuration = Math.max(0.1, Number(durationSeconds || 0))
  const events = Array.isArray(timeline?.events) ? timeline.events : []
  const ofType = (type: DecisionEventType) => events.filter((event) => event.type === type)
  const cuts = ofType('cut')
  const zooms = ofType('zoom')
  const captions = ofType('caption_emphasis')
  const interrupts = ofType('pattern_interrupt')
  const hooks = ofType('hook')
  const cutsPer10Seconds = Number((cuts.length / (safeDuration / 10)).toFixed(4))
  const averageShotDuration = Number((safeDuration / Math.max(1, cuts.length + 1)).toFixed(4))
  const zoomFrequencyPerMinute = Number((zooms.length / (safeDuration / 60)).toFixed(4))
  const captionRate = Number((captions.length / (safeDuration / 10)).toFixed(4))
  const patternSpacing = (() => {
    if (interrupts.length <= 1) return 0
    const spacings: number[] = []
    for (let index = 1; index < interrupts.length; index += 1) {
      spacings.push(Math.max(0, interrupts[index].t - interrupts[index - 1].t))
    }
    return Number((spacings.reduce((sum, value) => sum + value, 0) / Math.max(1, spacings.length)).toFixed(4))
  })()
  const hookPlacement = hooks.length ? Number(hooks[0].t.toFixed(4)) : null
  const normalizedEnergySamples = (Array.isArray(energySamples) ? energySamples : [])
    .filter((sample) => Number.isFinite(sample.t) && Number.isFinite(sample.value))
    .map((sample) => ({
      t: Number(sample.t),
      value: clamp01(Number(sample.value))
    }))
    .sort((left, right) => left.t - right.t)
  const energySpikeDensity = normalizedEnergySamples.length
    ? Number((normalizedEnergySamples.filter((sample) => sample.value >= 0.72).length / (safeDuration / 10)).toFixed(4))
    : Number((((zooms.length + interrupts.length) / Math.max(1, safeDuration / 10)) * 0.6).toFixed(4))
  const emotionalEscalationSlope = (() => {
    if (normalizedEnergySamples.length >= 2) {
      const first = normalizedEnergySamples[0]
      const last = normalizedEnergySamples[normalizedEnergySamples.length - 1]
      const dt = Math.max(1, last.t - first.t)
      return Number(((last.value - first.value) / dt).toFixed(5))
    }
    const earlyEvents = events.filter((event) => event.t <= safeDuration * 0.35).length
    const lateEvents = events.filter((event) => event.t >= safeDuration * 0.65).length
    return Number(((lateEvents - earlyEvents) / safeDuration).toFixed(5))
  })()

  return {
    cutsPer10Seconds,
    averageShotDuration,
    zoomFrequencyPerMinute,
    captionEmphasisRatePer10Seconds: captionRate,
    energySpikeDensityPer10Seconds: energySpikeDensity,
    patternInterruptSpacingSeconds: patternSpacing,
    hookPlacementSeconds: hookPlacement,
    emotionalEscalationSlope
  }
}

export const trainStyleProfileFromTimelines = ({
  styleName,
  timelines,
  durations
}: {
  styleName: string
  timelines: EditDecisionTimeline[]
  durations: number[]
}) => {
  const featureRows = timelines.map((timeline, index) => extractTimelineFeatures({
    timeline,
    durationSeconds: Number(durations[index] || 0)
  }))
  if (!featureRows.length) {
    return buildBehaviorStyleProfileFromBlend(MODE_DEFAULT_BLEND.balanced, styleName)
  }
  const mean = <K extends keyof TimelineFeatureSnapshot>(key: K) => (
    featureRows.reduce((sum, row) => sum + Number(row[key] || 0), 0) / featureRows.length
  )
  const cutsPer10 = mean('cutsPer10Seconds')
  const patternSpacing = mean('patternInterruptSpacingSeconds')
  const zoomPerMinute = mean('zoomFrequencyPerMinute')
  const captionPer10 = mean('captionEmphasisRatePer10Seconds')
  const escalationSlope = mean('emotionalEscalationSlope')
  return {
    styleName,
    avgCutInterval: Number(clamp(cutsPer10 > 0 ? 10 / cutsPer10 : 2.4, 0.7, 6).toFixed(3)),
    patternInterruptInterval: Number(clamp(patternSpacing || 8, 3, 14).toFixed(3)),
    zoomFrequencyPer10Seconds: Number(clamp(zoomPerMinute / 6, 0.05, 1.2).toFixed(4)),
    captionEmphasisRatePer10Seconds: Number(clamp(captionPer10, 0.05, 2.2).toFixed(4)),
    energyEscalationCurve: deriveEscalationCurve(escalationSlope),
    silenceTrimTolerance: Number(clamp(0.48 - cutsPer10 * 0.03, 0.14, 0.7).toFixed(4)),
    hookPlacementTargetSec: Number(clamp(mean('hookPlacementSeconds') || 6, 2.8, 8).toFixed(3)),
    autoEscalationWindowSec: Number(clamp(patternSpacing * 1.4 || 7, 5.2, 9.2).toFixed(3)),
    archetypeBlend: normalizeStyleArchetypeBlend(MODE_DEFAULT_BLEND.balanced)
  } as RetentionBehaviorStyleProfile
}

export type EscalationSegment = {
  start: number
  end: number
  speed?: number
  zoom?: number
  brightness?: number
  emphasize?: boolean
}

export const applyAutoEscalationGuarantee = ({
  segments,
  energySamples,
  flatWindowSeconds = 6,
  lowEnergyThreshold = 0.52,
  minZoomDelta = 0.03,
  maxSpeed = 1.24
}: {
  segments: EscalationSegment[]
  energySamples: EnergySample[]
  flatWindowSeconds?: number
  lowEnergyThreshold?: number
  minZoomDelta?: number
  maxSpeed?: number
}) => {
  if (!Array.isArray(segments) || !segments.length) {
    return { segments: [] as EscalationSegment[], events: [] as AutoEscalationEvent[], count: 0 }
  }
  const sortedSegments = segments
    .filter((segment) => Number.isFinite(segment.start) && Number.isFinite(segment.end) && segment.end > segment.start)
    .map((segment) => ({ ...segment }))
    .sort((left, right) => left.start - right.start)
  const normalizedEnergy = (Array.isArray(energySamples) ? energySamples : [])
    .filter((sample) => Number.isFinite(sample.t) && Number.isFinite(sample.value))
    .map((sample) => ({ t: Number(sample.t), value: clamp01(Number(sample.value)) }))
    .sort((left, right) => left.t - right.t)
  if (!normalizedEnergy.length) {
    return { segments: sortedSegments, events: [] as AutoEscalationEvent[], count: 0 }
  }

  const runs: Array<{ start: number; end: number }> = []
  let runStart: number | null = null
  let lastT: number | null = null
  for (const sample of normalizedEnergy) {
    if (sample.value < lowEnergyThreshold) {
      if (runStart === null) runStart = sample.t
      lastT = sample.t
      continue
    }
    if (runStart !== null && lastT !== null) {
      runs.push({ start: runStart, end: lastT })
    }
    runStart = null
    lastT = null
  }
  if (runStart !== null && lastT !== null) runs.push({ start: runStart, end: lastT })
  const longRuns = runs.filter((run) => run.end - run.start >= flatWindowSeconds)
  if (!longRuns.length) {
    return { segments: sortedSegments, events: [] as AutoEscalationEvent[], count: 0 }
  }

  const events: AutoEscalationEvent[] = []
  for (const run of longRuns) {
    const targetTime = Number((run.start + Math.min(2.5, (run.end - run.start) * 0.5)).toFixed(3))
    let index = sortedSegments.findIndex((segment) => segment.start <= targetTime && segment.end >= targetTime)
    if (index < 0) {
      let bestDistance = Number.POSITIVE_INFINITY
      let bestIndex = -1
      for (let candidateIndex = 0; candidateIndex < sortedSegments.length; candidateIndex += 1) {
        const segment = sortedSegments[candidateIndex]
        const center = segment.start + (segment.end - segment.start) * 0.5
        const distance = Math.abs(center - targetTime)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = candidateIndex
        }
      }
      index = bestIndex
    }
    if (index < 0) continue

    const segment = sortedSegments[index]
    const actions: string[] = []
    let insertedCut = false
    const duration = segment.end - segment.start
    if (duration >= 1.6 && targetTime > segment.start + 0.55 && targetTime < segment.end - 0.55) {
      const left = { ...segment, end: Number(targetTime.toFixed(3)) }
      const right = {
        ...segment,
        start: Number(targetTime.toFixed(3)),
        zoom: Number(Math.max(Number(segment.zoom || 0), minZoomDelta).toFixed(3)),
        brightness: Number(Math.max(Number(segment.brightness || 0), 0.02).toFixed(3)),
        speed: Number(clamp((segment.speed && segment.speed > 0 ? segment.speed : 1) + 0.08, 1, maxSpeed).toFixed(3)),
        emphasize: true
      }
      sortedSegments.splice(index, 1, left, right)
      insertedCut = true
      actions.push('insert_cut', 'zoom', 'caption_emphasis')
    } else {
      segment.zoom = Number(Math.max(Number(segment.zoom || 0), minZoomDelta).toFixed(3))
      segment.brightness = Number(Math.max(Number(segment.brightness || 0), 0.02).toFixed(3))
      segment.speed = Number(clamp((segment.speed && segment.speed > 0 ? segment.speed : 1) + 0.06, 1, maxSpeed).toFixed(3))
      segment.emphasize = true
      actions.push('zoom', 'caption_emphasis')
    }
    events.push({
      t: targetTime,
      insertedCut,
      reason: `energy_drop_${flatWindowSeconds}s`,
      actions
    })
  }

  return {
    segments: sortedSegments,
    events,
    count: events.length
  }
}
