const TIME_RANGE_RE = /(\d{1,2}:\d{2}(?::\d{2})?|\d{1,4}(?:\.\d+)?s)\s*(?:-|–|—|to|through|thru|until)\s*(\d{1,2}:\d{2}(?::\d{2})?|\d{1,4}(?:\.\d+)?s)/gi

export const EDITOR_INSTRUCTION_PROMPT_MAX_LENGTH = 600

export type EditorInstructionMarkerType = 'remove' | 'keep' | 'hook'
export type EditorInstructionLongFormPreset = 'balanced' | 'aggressive' | 'ultra'
export type EditorInstructionStrategy = 'safe' | 'balanced' | 'viral'
export type EditorInstructionEditorMode =
  | 'reaction'
  | 'commentary'
  | 'savage-roast'
  | 'vlog'
  | 'gaming'
  | 'sports'
  | 'education'
  | 'podcast'
  | 'ultra'
  | 'retention-king'

export type EditorInstructionMarker = {
  type: EditorInstructionMarkerType
  start: number
  end: number
  rationale: string
}

export type EditorInstructionPlan = {
  prompt: string
  editorMode: EditorInstructionEditorMode | null
  retentionStrategyProfile: EditorInstructionStrategy | null
  onlyCuts: boolean | null
  smartZoom: boolean | null
  transitions: boolean | null
  soundFx: boolean | null
  maxCuts: number | null
  longFormPreset: EditorInstructionLongFormPreset | null
  longFormAggression: number | null
  longFormClarityVsSpeed: number | null
  tangentKiller: boolean | null
  continuityFirstMode: boolean | null
  markers: EditorInstructionMarker[]
  notes: string[]
}

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value))

export const normalizeEditorInstructionPrompt = (value: unknown): string | null => {
  if (typeof value !== 'string') return null
  const normalized = value
    .replace(/\r/g, '')
    .split('\n')
    .map((line) => line.replace(/\s+/g, ' ').trim())
    .filter(Boolean)
    .join('\n')
    .trim()
  if (!normalized) return null
  return normalized.slice(0, EDITOR_INSTRUCTION_PROMPT_MAX_LENGTH)
}

const parseTimeToken = (raw: string): number | null => {
  const token = String(raw || '').trim().toLowerCase()
  if (!token) return null
  if (/^\d+(?:\.\d+)?s$/.test(token)) {
    const seconds = Number(token.slice(0, -1))
    return Number.isFinite(seconds) && seconds >= 0 ? seconds : null
  }
  if (!token.includes(':')) return null
  const parts = token.split(':').map((part) => Number(part))
  if (parts.some((part) => !Number.isFinite(part) || part < 0)) return null
  if (parts.length === 2) {
    return parts[0] * 60 + parts[1]
  }
  if (parts.length === 3) {
    return parts[0] * 3600 + parts[1] * 60 + parts[2]
  }
  return null
}

const buildRangeRationale = (prompt: string, startIdx: number, endIdx: number) => {
  const windowStart = Math.max(0, startIdx - 42)
  const windowEnd = Math.min(prompt.length, endIdx + 42)
  const excerpt = prompt.slice(windowStart, windowEnd).replace(/\s+/g, ' ').trim()
  if (!excerpt) return 'Prompt-directed range'
  return excerpt.slice(0, 160)
}

const resolveMarkerTypeFromContext = ({
  lowerPrompt,
  context
}: {
  lowerPrompt: string
  context: string
}): EditorInstructionMarkerType | null => {
  const keepRe = /\b(?:keep|preserve|leave in|leave this in|do not cut|don't cut)\b/i
  const hookRe = /\b(?:hook|opener|opening|intro)\b/i
  const removeRe = /\b(?:remove|cut|take out|trim|delete|drop|skip)\b/i
  if (hookRe.test(context)) return 'hook'
  if (keepRe.test(context) && !removeRe.test(context)) return 'keep'
  if (removeRe.test(context)) return 'remove'
  if (removeRe.test(lowerPrompt)) return 'remove'
  return null
}

const resolveEditorMode = (lowerPrompt: string): EditorInstructionEditorMode | null => {
  const checks: Array<{ mode: EditorInstructionEditorMode; re: RegExp }> = [
    { mode: 'retention-king', re: /\bretention\s*king\b/i },
    { mode: 'savage-roast', re: /\b(?:roast|savage roast)\b/i },
    { mode: 'reaction', re: /\breaction\b/i },
    { mode: 'commentary', re: /\bcommentary\b/i },
    { mode: 'vlog', re: /\bvlog\b/i },
    { mode: 'gaming', re: /\bgaming\b/i },
    { mode: 'sports', re: /\bsports?\b/i },
    { mode: 'education', re: /\b(?:education|educational|tutorial|teaching)\b/i },
    { mode: 'podcast', re: /\bpodcast\b/i },
    { mode: 'ultra', re: /\bultra\b/i }
  ]
  for (const check of checks) {
    if (check.re.test(lowerPrompt)) return check.mode
  }
  return null
}

const resolveExplicitCutCount = (lowerPrompt: string): number | null => {
  const directCountPatterns = [
    /\bmax(?:imum)?\s*(?:of\s*)?(\d{1,2})\s*(?:cuts?|jump cuts?)\b/i,
    /\b(\d{1,2})\s*(?:cuts?|jump cuts?)\b/i
  ]
  for (const pattern of directCountPatterns) {
    const match = lowerPrompt.match(pattern)
    if (!match?.[1]) continue
    const count = Number(match[1])
    if (Number.isFinite(count)) return Math.round(clamp(count, 1, 15))
  }
  return null
}

export const parseEditorInstructionPrompt = (value: unknown): EditorInstructionPlan | null => {
  const prompt = normalizeEditorInstructionPrompt(value)
  if (!prompt) return null

  const lowerPrompt = prompt.toLowerCase()
  const markers: EditorInstructionMarker[] = []
  const markerKeys = new Set<string>()
  let rangeMatch: RegExpExecArray | null = null
  while ((rangeMatch = TIME_RANGE_RE.exec(prompt)) !== null) {
    const parsedStart = parseTimeToken(rangeMatch[1] || '')
    const parsedEnd = parseTimeToken(rangeMatch[2] || '')
    if (parsedStart === null || parsedEnd === null || !Number.isFinite(parsedStart) || !Number.isFinite(parsedEnd) || parsedEnd <= parsedStart) continue
    const start = Number(parsedStart)
    const end = Number(parsedEnd)
    const context = lowerPrompt.slice(
      Math.max(0, rangeMatch.index - 56),
      Math.min(lowerPrompt.length, rangeMatch.index + rangeMatch[0].length + 56)
    )
    const type = resolveMarkerTypeFromContext({ lowerPrompt, context })
    if (!type) continue
    const clippedStart = Number(start.toFixed(3))
    const clippedEnd = Number(end.toFixed(3))
    const key = `${type}:${clippedStart}:${clippedEnd}`
    if (markerKeys.has(key)) continue
    markerKeys.add(key)
    markers.push({
      type,
      start: clippedStart,
      end: clippedEnd,
      rationale: buildRangeRationale(prompt, rangeMatch.index, rangeMatch.index + rangeMatch[0].length)
    })
  }

  const wantsViral = /\b(?:viral|retention\s*max|maximum retention)\b/i.test(lowerPrompt)
  const wantsBalanced = /\bbalanced\b/i.test(lowerPrompt)
  const wantsFast = /\b(?:faster|fast paced|fast-paced|snapp(?:y|ier)|tighter pacing|tight pacing|pacing tight|pick up the pace|pace it up|quicker|harder cuts?)\b/i.test(lowerPrompt)
  const wantsSlow = /\b(?:slower|slow it down|let it breathe|breathing room|more context|preserve context|preserve the setup|preserve story|deliberate)\b/i.test(lowerPrompt)
  const wantsContinuity = /\b(?:smooth(?:er)?|smooth transitions?|preserve story|preserve context|coheren(?:ce|t)|less jumpy|less choppy|fewer jump cuts?)\b/i.test(lowerPrompt)
  const wantsFillerRemoval = /\b(?:dead air|fillers?|rambling|tangent(?:s)?|waffle|awkward pauses?|ums?\b|uhs?\b|silence)\b/i.test(lowerPrompt)
  const wantsOnlyCuts = /\b(?:hook\s*(?:and|&)\s*cut(?:s)?|only cuts?|just trim|keep it simple|no fancy effects|simple cut)\b/i.test(lowerPrompt)
  const wantsNoTransitions = /\b(?:no|without|skip|avoid|don't add)\s+transitions?\b/i.test(lowerPrompt)
  const wantsTransitions = /\btransitions?\b/i.test(lowerPrompt) && !wantsNoTransitions
  const wantsNoSoundFx = /\b(?:no|without|skip|avoid|don't add)\s+(?:sound\s*effects?|sfx|audio stings?)\b/i.test(lowerPrompt)
  const wantsNoEffects = /\b(?:no|without|skip|avoid|don't add)\s+(?:effects?|fancy editing)\b/i.test(lowerPrompt)
  const wantsMoreCuts = /\b(?:more cuts?|cut faster|aggressive cuts?|quicker cuts?)\b/i.test(lowerPrompt)
  const wantsFewerCuts = /\b(?:fewer cuts?|lighter cuts?)\b/i.test(lowerPrompt)

  const explicitCutCount = resolveExplicitCutCount(lowerPrompt)
  let retentionStrategyProfile: EditorInstructionStrategy | null = null
  let longFormPreset: EditorInstructionLongFormPreset | null = null
  let longFormAggression: number | null = null
  let longFormClarityVsSpeed: number | null = null
  let continuityFirstMode: boolean | null = null

  if (wantsViral || (wantsFast && !wantsSlow && !wantsBalanced)) {
    retentionStrategyProfile = 'viral'
    longFormPreset = wantsViral ? 'ultra' : 'aggressive'
    longFormAggression = wantsViral ? 92 : 86
    longFormClarityVsSpeed = 34
  } else if (wantsSlow && !wantsFast && !wantsBalanced) {
    retentionStrategyProfile = 'safe'
    longFormPreset = 'balanced'
    longFormAggression = 42
    longFormClarityVsSpeed = 84
    continuityFirstMode = true
  } else if ((wantsFast && wantsSlow) || wantsBalanced) {
    retentionStrategyProfile = 'balanced'
    longFormPreset = 'balanced'
    longFormAggression = 72
    longFormClarityVsSpeed = 70
    continuityFirstMode = true
  }

  let maxCuts: number | null = explicitCutCount
  if (maxCuts === null) {
    if (wantsFewerCuts) maxCuts = 4
    else if (wantsMoreCuts) maxCuts = 10
    else if (wantsFillerRemoval) maxCuts = 8
  }

  const editorMode = resolveEditorMode(lowerPrompt)
  const onlyCuts = wantsOnlyCuts ? true : null
  const smartZoom = wantsNoEffects || wantsOnlyCuts ? false : null
  const transitions = wantsNoTransitions || wantsOnlyCuts || wantsNoEffects
    ? false
    : wantsTransitions || wantsContinuity
      ? true
      : null
  const soundFx = wantsNoSoundFx || wantsOnlyCuts || wantsNoEffects ? false : null
  const tangentKiller = wantsFillerRemoval || wantsOnlyCuts ? true : null
  if (wantsContinuity && continuityFirstMode === null) continuityFirstMode = true

  const notes: string[] = []
  if (markers.length > 0) {
    notes.push(`${markers.length} prompt-directed timeline ${markers.length === 1 ? 'range' : 'ranges'}`)
  }
  if (retentionStrategyProfile === 'viral') notes.push('Faster pacing requested')
  else if (retentionStrategyProfile === 'safe') notes.push('Smoother pacing requested')
  else if (retentionStrategyProfile === 'balanced') notes.push('Balanced pacing requested')
  if (tangentKiller) notes.push('Dead air and tangents targeted')
  if (onlyCuts) notes.push('Simple cut-only edit requested')
  if (editorMode) notes.push(`Mode override: ${editorMode}`)

  return {
    prompt,
    editorMode,
    retentionStrategyProfile,
    onlyCuts,
    smartZoom,
    transitions,
    soundFx,
    maxCuts,
    longFormPreset,
    longFormAggression,
    longFormClarityVsSpeed,
    tangentKiller,
    continuityFirstMode,
    markers,
    notes
  }
}
