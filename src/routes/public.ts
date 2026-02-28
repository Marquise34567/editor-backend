import express from 'express'
import fetch from 'node-fetch'
import { getFounderAvailability } from '../services/founder'
import { getRequestIpAddress } from '../services/ipBan'
import { canCreateSignupFromIp, claimSignupIp } from '../services/signupIpGuard'

const router = express.Router()

type TitleTrendTopic = {
  title: string
  traffic: string | null
  publishedAt: string | null
  link: string | null
}

type TitleTrendResponse = {
  ok: boolean
  year: number
  source: string
  generatedAt: string
  topics: TitleTrendTopic[]
}

const GOOGLE_TRENDS_RSS_URL = 'https://trends.google.com/trending/rss?geo=US'
const TITLE_TRENDS_CACHE_TTL_MS = 15 * 60 * 1000
let titleTrendCache: { expiresAt: number; payload: TitleTrendResponse } | null = null

const FALLBACK_TRENDS = [
  'AI workflow',
  'Short-form storytelling',
  'Creator monetization',
  'UGC ad formats',
  'Productivity setup',
  'Performance marketing',
  'Creator economy',
  'Community growth',
]

const decodeXmlEntities = (value: string) =>
  value
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")

const extractTagValue = (item: string, tagName: string) => {
  const escapedTag = tagName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const match = item.match(new RegExp(`<${escapedTag}>([\\s\\S]*?)<\\/${escapedTag}>`, 'i'))
  if (!match) return null
  const raw = match[1].replace(/^<!\[CDATA\[/, '').replace(/\]\]>$/, '').trim()
  return decodeXmlEntities(raw)
}

const parseGoogleTrendsRss = (xml: string): TitleTrendTopic[] => {
  const items = xml.match(/<item\b[\s\S]*?<\/item>/gi) || []
  return items
    .map((item) => {
      const title = extractTagValue(item, 'title')
      if (!title) return null
      return {
        title,
        traffic: extractTagValue(item, 'ht:approx_traffic') || null,
        publishedAt: extractTagValue(item, 'pubDate') || null,
        link: extractTagValue(item, 'link') || null
      } as TitleTrendTopic
    })
    .filter((item): item is TitleTrendTopic => Boolean(item))
}

const buildFallbackTrendPayload = (): TitleTrendResponse => ({
  ok: true,
  year: new Date().getFullYear(),
  source: 'fallback_creator_trends',
  generatedAt: new Date().toISOString(),
  topics: FALLBACK_TRENDS.map((title) => ({
    title,
    traffic: null,
    publishedAt: null,
    link: null
  }))
})

const getTitleTrends = async (): Promise<TitleTrendResponse> => {
  const now = Date.now()
  if (titleTrendCache && titleTrendCache.expiresAt > now) {
    return titleTrendCache.payload
  }

  try {
    const response = await fetch(GOOGLE_TRENDS_RSS_URL, {
      headers: { 'user-agent': 'auto-editor-pro/1.0 (+https://autoeditor.app)' }
    })
    if (!response.ok) {
      throw new Error(`trend_fetch_failed_${response.status}`)
    }
    const xml = await response.text()
    const topics = parseGoogleTrendsRss(xml).slice(0, 12)
    if (!topics.length) {
      throw new Error('trend_parse_empty')
    }
    const payload: TitleTrendResponse = {
      ok: true,
      year: new Date().getFullYear(),
      source: 'google_trends_us_rss',
      generatedAt: new Date().toISOString(),
      topics
    }
    titleTrendCache = {
      payload,
      expiresAt: now + TITLE_TRENDS_CACHE_TTL_MS
    }
    return payload
  } catch (error) {
    const fallback = buildFallbackTrendPayload()
    titleTrendCache = {
      payload: fallback,
      expiresAt: now + Math.min(TITLE_TRENDS_CACHE_TTL_MS, 3 * 60 * 1000)
    }
    return fallback
  }
}

router.get('/founder', async (_req, res) => {
  try {
    const availability = await getFounderAvailability()
    res.json(availability)
  } catch (err) {
    res.status(500).json({ error: 'server_error' })
  }
})

router.get('/title-trends', async (_req, res) => {
  try {
    const payload = await getTitleTrends()
    res.json(payload)
  } catch {
    res.status(500).json({ error: 'server_error' })
  }
})

router.post('/signup/ip-check', async (req, res) => {
  try {
    const ip = getRequestIpAddress(req)
    const result = await canCreateSignupFromIp(ip)
    if (!result.allowed) {
      return res.status(429).json({
        allowed: false,
        error: result.code,
        message: 'Only one account can be created from this IP address.'
      })
    }
    return res.json({ allowed: true })
  } catch (error) {
    console.warn('public signup ip-check failed', error)
    return res.status(500).json({ allowed: false, error: 'server_error' })
  }
})

router.post('/signup/ip-claim', async (req, res) => {
  try {
    const ip = getRequestIpAddress(req)
    const email = typeof req.body?.email === 'string' ? req.body.email : null
    const result = await claimSignupIp({ ip, email })
    if (!result.allowed) {
      return res.status(429).json({
        allowed: false,
        error: result.code,
        message: 'Only one account can be created from this IP address.'
      })
    }
    return res.json({ allowed: true })
  } catch (error) {
    console.warn('public signup ip-claim failed', error)
    return res.status(500).json({ allowed: false, error: 'server_error' })
  }
})

export default router
