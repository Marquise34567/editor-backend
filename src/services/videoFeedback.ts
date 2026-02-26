import fetch from 'node-fetch'

type TranscriptCue = {
  start: number
  end: number
  text: string
}

type EngagementWindow = {
  time: number
  score?: number
  audioEnergy?: number
  speechIntensity?: number
  facePresence?: number
  faceCenterX?: number
  faceCenterY?: number
  emotionalSpike?: number
}

type TrendInsight = {
  title: string
  summary: string
  howToApply: string
  source: string
  url: string
  publishedAt: string | null
}

type FeedbackSuggestionPin = {
  second: number
  label: string
}

type FeedbackInput = {
  title?: string | null
  durationSeconds?: number | null
  transcriptCues: TranscriptCue[]
  engagementWindows: EngagementWindow[]
  metadataSummary?: Record<string, any> | null
  nicheHint?: string | null
}

type FeedbackOutput = {
  detectedNiche: string
  detectedTopics: string[]
  trendingTopics: string[]
  voicePerformance: string[]
  positioningAngle: string[]
  contentTips: string[]
  retentionBoosts: string[]
  trendInsights: TrendInsight[]
  retentionBoostEstimatePercent: number
  visuals: {
    retentionCurve: Array<{ second: number; score: number }>
    suggestionPins: FeedbackSuggestionPin[]
  }
}

const STOPWORDS = new Set([
  'about', 'after', 'again', 'also', 'and', 'are', 'because', 'been', 'before', 'being', 'between', 'but', 'came',
  'could', 'does', 'each', 'even', 'from', 'have', 'into', 'just', 'like', 'more', 'most', 'only', 'really', 'that',
  'their', 'them', 'then', 'there', 'these', 'they', 'this', 'those', 'through', 'very', 'want', 'what', 'when',
  'where', 'which', 'while', 'with', 'would', 'your', 'you', 'were', 'here', 'than', 'over', 'under', 'onto', 'http',
  'https', 'www', 'com'
])

const CONTENT_TIPS_BY_NICHE: Record<string, string[]> = {
  'gen z pranks': [
    'Open with the biggest reaction first, then explain context in 3-5 seconds.',
    'Use faster reaction cuts around laughs and public surprise beats.',
    'Add challenge stakes or countdown overlays to maintain tension.'
  ],
  'gaming highlights': [
    'Use short pre-fight setup then slow motion on kill/payoff moments.',
    'Add quick callout captions for strategy pivots and clutch plays.',
    'Stack SFX + zoom only on peak moments so highlights feel earned.'
  ],
  'sports highlights': [
    'Lead with the highest-impact play before full sequence context.',
    'Use stat overlays after key moments, not during action.',
    'Alternate wide-angle context and tight impact replays.'
  ],
  'education / explainer': [
    'Front-load the result viewers will get before the explanation.',
    'Use chapter-like micro-hooks every 45-75 seconds.',
    'Repeat core takeaway visually and verbally near each segment close.'
  ],
  'podcast / commentary': [
    'Trim setup phrases early and keep first sentence high-clarity.',
    'Highlight disagreement, surprise, or insight lines with on-screen text.',
    'Insert fast b-roll or cutaways when dialogue energy drops.'
  ],
  vlog: [
    'Start with a payoff scene then move into chronological context.',
    'Use place/time cards to make progression clear.',
    'Compress travel or dead-air sections into quick montage beats.'
  ]
}

const TRENDING_TOPICS_BY_NICHE: Record<string, string[]> = {
  'gen z pranks': ['AI filters', 'street roast formats', 'public challenge escalations', 'duet reaction chains'],
  'gaming highlights': ['vertical long-form hybrids', 'instant replay loops', 'POV challenge runs', 'fandom meme edits'],
  'sports highlights': ['emotion-first storytelling', 'micd up reactions', 'community prediction hooks', 'split-screen analysis'],
  'education / explainer': ['AI-assisted breakdowns', 'interactive quiz hooks', 'myth-vs-fact shorts', 'micro-case studies'],
  'podcast / commentary': ['hot take cold open', 'fandom deep dives', 'clip-to-longform funnels', 'multi-camera jump pacing'],
  vlog: ['UGC authenticity', 'micro-story arcs', 'emotion-driven cuts', 'community challenge prompts']
}

const RETENTION_TIPS_BY_NICHE: Record<string, string[]> = {
  'gen z pranks': ['Add micro-hooks every 45-60s.', 'Tease next reaction before current payoff lands.'],
  'gaming highlights': ['Use one clear setup line before each high-skill moment.', 'Keep dead-air between plays under 1.5s.'],
  'sports highlights': ['Use score context every 60-90s.', 'Cut to reaction cam at emotional peaks.'],
  'education / explainer': ['Preview the next answer before each section transition.', 'Recap key point every 60-90s.'],
  'podcast / commentary': ['Add topic pivots every 60-90s.', 'Trim filler at the start of each speaker turn.'],
  vlog: ['Use scene-change captions at least every 60s.', 'Anchor each segment with a goal or question.']
}

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const stripHtml = (value: string) =>
  String(value || '')
    .replace(/<!\[CDATA\[(.*?)\]\]>/g, '$1')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()

const decodeXml = (value: string) =>
  String(value || '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")

const cleanNewsTitle = (title: string) => {
  const decoded = decodeXml(stripHtml(title))
  return decoded.replace(/\s*-\s*[^-]+$/, '').trim() || decoded
}

const asNumber = (value: any, fallback = 0) => {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

const normalizeTranscript = (cues: TranscriptCue[]) =>
  (Array.isArray(cues) ? cues : [])
    .map((cue) => ({
      start: asNumber(cue?.start, 0),
      end: asNumber(cue?.end, asNumber(cue?.start, 0) + 0.8),
      text: String(cue?.text || '').trim()
    }))
    .filter((cue) => cue.text.length > 0)

const normalizeWindows = (windows: EngagementWindow[]) =>
  (Array.isArray(windows) ? windows : [])
    .map((window) => ({
      time: asNumber(window?.time, 0),
      score: clamp(asNumber(window?.score, 0.5), 0, 1),
      audioEnergy: clamp(asNumber(window?.audioEnergy, 0.45), 0, 1),
      speechIntensity: clamp(asNumber(window?.speechIntensity, 0.5), 0, 1),
      facePresence: clamp(asNumber(window?.facePresence, 0.5), 0, 1),
      faceCenterX: clamp(asNumber(window?.faceCenterX, 0.5), 0, 1),
      faceCenterY: clamp(asNumber(window?.faceCenterY, 0.5), 0, 1),
      emotionalSpike: clamp(asNumber(window?.emotionalSpike, 0), 0, 1)
    }))
    .sort((a, b) => a.time - b.time)

const formatTimestamp = (seconds: number) => {
  const safe = Math.max(0, Math.floor(seconds))
  const mins = Math.floor(safe / 60)
  const secs = safe % 60
  return `${mins}:${String(secs).padStart(2, '0')}`
}

const keywordScore = (text: string, words: string[]) => {
  let score = 0
  for (const word of words) {
    if (text.includes(word)) score += 1
  }
  return score
}

const detectNiche = (text: string, hint?: string | null) => {
  const normalizedHint = String(hint || '').toLowerCase()
  if (normalizedHint.includes('prank') || normalizedHint.includes('roast')) return 'gen z pranks'
  if (normalizedHint.includes('game')) return 'gaming highlights'
  if (normalizedHint.includes('sport')) return 'sports highlights'
  if (normalizedHint.includes('educat') || normalizedHint.includes('tutorial')) return 'education / explainer'
  if (normalizedHint.includes('podcast') || normalizedHint.includes('commentary')) return 'podcast / commentary'
  if (normalizedHint.includes('vlog')) return 'vlog'

  const lower = text.toLowerCase()
  const scores = [
    { niche: 'gen z pranks', score: keywordScore(lower, ['prank', 'reaction', 'street', 'roast', 'surprise', 'laugh']) },
    { niche: 'gaming highlights', score: keywordScore(lower, ['game', 'kill', 'clutch', 'ranked', 'stream', 'boss']) },
    { niche: 'sports highlights', score: keywordScore(lower, ['goal', 'match', 'score', 'playoff', 'highlight', 'coach']) },
    { niche: 'education / explainer', score: keywordScore(lower, ['how to', 'guide', 'explain', 'lesson', 'tutorial', 'framework']) },
    { niche: 'podcast / commentary', score: keywordScore(lower, ['opinion', 'debate', 'podcast', 'commentary', 'discussion']) },
    { niche: 'vlog', score: keywordScore(lower, ['vlog', 'day in the life', 'travel', 'today', 'behind the scenes']) }
  ]
  scores.sort((a, b) => b.score - a.score)
  return scores[0]?.score > 0 ? scores[0].niche : 'podcast / commentary'
}

const extractTopTopics = (text: string, limit = 6) => {
  const tokens = text
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter((token) => token.length >= 4 && !STOPWORDS.has(token))
  const counts = new Map<string, number>()
  for (const token of tokens) {
    counts.set(token, (counts.get(token) || 0) + 1)
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([topic]) => topic)
}

const buildVoicePerformanceTips = (windows: ReturnType<typeof normalizeWindows>) => {
  const tips: string[] = []
  const lowAudioWindow = windows.find((window) => window.audioEnergy < 0.28)
  if (lowAudioWindow) {
    tips.push(`Talk louder for emphasis (detected low volume near ${formatTimestamp(lowAudioWindow.time)}).`)
  }
  const lowSpeechWindow = windows.find((window) => window.speechIntensity < 0.22)
  if (lowSpeechWindow) {
    tips.push(`Punch key lines with stronger pacing around ${formatTimestamp(lowSpeechWindow.time)}.`)
  }
  if (!tips.length) {
    tips.push('Voice energy is stable. Add 1-2 intentional emphasis spikes per minute for stronger hook recall.')
  }
  return tips
}

const buildPositioningTips = (windows: ReturnType<typeof normalizeWindows>) => {
  if (!windows.length) {
    return [
      'Current angle is flat. Try a 45 degree low angle for more dynamic energy.',
      'Center your face in frame to strengthen audience focus.'
    ]
  }
  const avgCenterX = windows.reduce((sum, window) => sum + window.faceCenterX, 0) / windows.length
  const avgFacePresence = windows.reduce((sum, window) => sum + window.facePresence, 0) / windows.length
  const tips: string[] = []
  if (Math.abs(avgCenterX - 0.5) > 0.14) {
    tips.push('Face framing drifts off-center. Keep your face centered to improve watch focus.')
  }
  if (avgFacePresence < 0.45) {
    tips.push('Sit closer to camera for stronger on-screen intimacy and expression detail.')
  }
  tips.push('Try a 45 degree low angle during key moments to increase perceived energy.')
  return tips
}

const buildSuggestionPins = (windows: ReturnType<typeof normalizeWindows>) => {
  const pins: FeedbackSuggestionPin[] = []
  for (let index = 1; index < windows.length; index += 1) {
    const prev = windows[index - 1]
    const current = windows[index]
    const drop = prev.score - current.score
    if (drop >= 0.18) {
      pins.push({
        second: Math.max(0, Math.round(current.time)),
        label: `Retention dip: add micro-hook around ${formatTimestamp(current.time)}`
      })
    }
    if (pins.length >= 6) break
  }
  return pins
}

const buildRetentionCurve = (windows: ReturnType<typeof normalizeWindows>, durationSeconds: number) => {
  if (windows.length) {
    return windows.map((window) => ({
      second: Math.max(0, Math.round(window.time)),
      score: Math.round(clamp(window.score * 100, 1, 100))
    }))
  }
  const synthetic: Array<{ second: number; score: number }> = []
  const safeDuration = Math.max(30, Math.round(durationSeconds || 120))
  for (let second = 0; second <= safeDuration; second += Math.max(10, Math.round(safeDuration / 12))) {
    synthetic.push({
      second,
      score: Math.round(clamp(92 - second * 0.18, 42, 92))
    })
  }
  return synthetic
}

const getHowToApplyTrend = (niche: string, title: string, summary: string) => {
  const blob = `${title} ${summary}`.toLowerCase()
  if (blob.includes('ai')) return `For your ${niche} video, test AI-driven visual overlays in the first 10 seconds.`
  if (blob.includes('ugc') || blob.includes('authentic')) return 'Keep one raw, less-polished moment in the opener to increase trust.'
  if (blob.includes('interactive')) return 'Add a direct viewer prompt before 0:20 to trigger comments and rewatches.'
  if (blob.includes('story') || blob.includes('emotion')) return 'Add emotion-driven cut-ins on reaction lines instead of constant hard cuts.'
  if (blob.includes('short') || blob.includes('vertical')) return 'Build loopable endings that connect back to the opening beat.'
  return `Adapt this trend to your ${niche} structure using a stronger 0:00-0:08 hook and faster payoff timing.`
}

const parseRssItems = (xml: string) => {
  const items: Array<{ title: string; link: string; description: string; pubDate: string | null }> = []
  const matches = xml.match(/<item>([\s\S]*?)<\/item>/gi) || []
  for (const rawItem of matches.slice(0, 12)) {
    const title = rawItem.match(/<title>([\s\S]*?)<\/title>/i)?.[1] || ''
    const link = rawItem.match(/<link>([\s\S]*?)<\/link>/i)?.[1] || ''
    const description = rawItem.match(/<description>([\s\S]*?)<\/description>/i)?.[1] || ''
    const pubDate = rawItem.match(/<pubDate>([\s\S]*?)<\/pubDate>/i)?.[1] || null
    const cleanTitle = cleanNewsTitle(title)
    const cleanLink = decodeXml(stripHtml(link))
    if (!cleanTitle || !cleanLink) continue
    items.push({
      title: cleanTitle,
      link: cleanLink,
      description: decodeXml(stripHtml(description)),
      pubDate: pubDate ? decodeXml(stripHtml(pubDate)) : null
    })
  }
  return items
}

const fetchTrendInsights = async (niche: string, detectedTopics: string[]) => {
  const topicFragment = detectedTopics.slice(0, 2).join(' ')
  const queries = [
    `current trends in ${niche} video content 2026`,
    `TikTok Shorts trends ${niche} ${topicFragment} 2026`
  ]
  const collected: TrendInsight[] = []
  const seen = new Set<string>()
  for (const query of queries) {
    const endpoint = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=en-US&gl=US&ceid=US:en`
    try {
      const response = await fetch(endpoint, {
        method: 'GET',
        headers: { 'User-Agent': 'AutoEditorTrendAnalyzer/2026' }
      })
      if (!response.ok) continue
      const xml = await response.text()
      const items = parseRssItems(xml)
      for (const item of items) {
        const key = `${item.title.toLowerCase()}::${item.link.toLowerCase()}`
        if (seen.has(key)) continue
        seen.add(key)
        collected.push({
          title: item.title,
          summary: item.description || 'Fresh trend signal detected in creator ecosystem coverage.',
          howToApply: getHowToApplyTrend(niche, item.title, item.description),
          source: 'Google News',
          url: item.link,
          publishedAt: item.pubDate
        })
        if (collected.length >= 6) return collected
      }
    } catch (error) {
      // Ignore external trend fetch errors and fall back to static trend map.
    }
  }
  return collected
}

const buildFallbackTrends = (niche: string, topics: string[]) => {
  const base = TRENDING_TOPICS_BY_NICHE[niche] || TRENDING_TOPICS_BY_NICHE['podcast / commentary']
  return base.slice(0, 6).map((trend, index) => ({
    title: `${trend} momentum signal`,
    summary: `Rising creator usage across short-form uploads in ${new Date().getFullYear()}.`,
    howToApply: `Apply "${trend}" to your next cut with a tighter opening beat and niche-specific captioning.`,
    source: 'AutoEditor trend fallback',
    url: '',
    publishedAt: null
  }))
}

export const buildVideoFeedbackAnalysis = async (input: FeedbackInput): Promise<FeedbackOutput> => {
  const transcript = normalizeTranscript(input.transcriptCues)
  const windows = normalizeWindows(input.engagementWindows)
  const transcriptText = transcript.map((cue) => cue.text).join(' ').trim()
  const title = String(input.title || '').trim()
  const metadataBlob = JSON.stringify(input.metadataSummary || {})
  const textCorpus = `${title} ${transcriptText} ${metadataBlob}`.trim()
  const detectedNiche = detectNiche(textCorpus, input.nicheHint)
  const detectedTopics = extractTopTopics(textCorpus, 6)
  const trendingTopics = (TRENDING_TOPICS_BY_NICHE[detectedNiche] || TRENDING_TOPICS_BY_NICHE['podcast / commentary']).slice(0, 5)
  const voicePerformance = buildVoicePerformanceTips(windows)
  const positioningAngle = buildPositioningTips(windows)
  const contentTips = (CONTENT_TIPS_BY_NICHE[detectedNiche] || CONTENT_TIPS_BY_NICHE['podcast / commentary']).slice(0, 5)
  const retentionBoosts = (RETENTION_TIPS_BY_NICHE[detectedNiche] || RETENTION_TIPS_BY_NICHE['podcast / commentary']).slice(0, 5)
  const retentionCurve = buildRetentionCurve(windows, asNumber(input.durationSeconds, 120))
  const suggestionPins = buildSuggestionPins(windows)
  const dynamicTrends = await fetchTrendInsights(detectedNiche, detectedTopics)
  const trendInsights = dynamicTrends.length ? dynamicTrends : buildFallbackTrends(detectedNiche, detectedTopics)
  const baseEstimate = 12 + suggestionPins.length * 2 + Math.min(8, trendInsights.length)
  const retentionBoostEstimatePercent = clamp(Math.round(baseEstimate), 10, 34)

  return {
    detectedNiche,
    detectedTopics,
    trendingTopics,
    voicePerformance,
    positioningAngle,
    contentTips,
    retentionBoosts,
    trendInsights,
    retentionBoostEstimatePercent,
    visuals: {
      retentionCurve,
      suggestionPins
    }
  }
}

