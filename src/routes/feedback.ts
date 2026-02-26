import express from 'express'
import path from 'path'
import { prisma } from '../db/prisma'
import { getUserPlan } from '../services/plans'
import { isPaidTier } from '../shared/planConfig'
import { resolveDevAdminAccess } from '../lib/devAccounts'
import { buildVideoFeedbackAnalysis } from '../services/videoFeedback'

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

const router = express.Router()

const asNumber = (value: any, fallback = 0) => {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

const coerceTranscriptCue = (value: any): TranscriptCue | null => {
  const text = String(value?.text || '').trim()
  if (!text) return null
  const start = asNumber(value?.start, 0)
  const end = Math.max(start + 0.4, asNumber(value?.end, start + 1))
  return { start, end, text }
}

const coerceEngagementWindow = (value: any): EngagementWindow | null => {
  const time = asNumber(value?.time, Number.NaN)
  if (!Number.isFinite(time)) return null
  return {
    time,
    score: asNumber(value?.score, 0.5),
    audioEnergy: asNumber(value?.audioEnergy, 0.45),
    speechIntensity: asNumber(value?.speechIntensity, 0.5),
    facePresence: asNumber(value?.facePresence, 0.5),
    faceCenterX: asNumber(value?.faceCenterX, 0.5),
    faceCenterY: asNumber(value?.faceCenterY, 0.5),
    emotionalSpike: asNumber(value?.emotionalSpike, 0)
  }
}

const extractTranscriptCues = (analysis: Record<string, any>): TranscriptCue[] => {
  const candidates: any[] = []
  if (Array.isArray(analysis?.transcript_cues)) candidates.push(...analysis.transcript_cues)
  if (Array.isArray(analysis?.transcriptCues)) candidates.push(...analysis.transcriptCues)
  if (Array.isArray(analysis?.editPlan?.transcriptCues)) candidates.push(...analysis.editPlan.transcriptCues)
  if (Array.isArray(analysis?.editPlan?.transcript_cues)) candidates.push(...analysis.editPlan.transcript_cues)
  const normalized = candidates
    .map((item) => coerceTranscriptCue(item))
    .filter((item): item is TranscriptCue => Boolean(item))
  if (normalized.length) return normalized

  const transcriptText = String(analysis?.transcript_text || '').trim()
  if (!transcriptText) return []
  const sentences = transcriptText.split(/[.!?]+/).map((line) => line.trim()).filter(Boolean).slice(0, 20)
  let cursor = 0
  return sentences.map((sentence) => {
    const duration = Math.min(6, Math.max(2, sentence.split(/\s+/).length * 0.45))
    const cue = { start: cursor, end: cursor + duration, text: sentence }
    cursor += duration
    return cue
  })
}

const extractEngagementWindows = (analysis: Record<string, any>): EngagementWindow[] => {
  const candidates: any[] = []
  if (Array.isArray(analysis?.engagementWindows)) candidates.push(...analysis.engagementWindows)
  if (Array.isArray(analysis?.engagement_windows)) candidates.push(...analysis.engagement_windows)
  if (Array.isArray(analysis?.editPlan?.engagementWindows)) candidates.push(...analysis.editPlan.engagementWindows)
  if (Array.isArray(analysis?.editPlan?.engagement_windows)) candidates.push(...analysis.editPlan.engagement_windows)
  return candidates
    .map((item) => coerceEngagementWindow(item))
    .filter((item): item is EngagementWindow => Boolean(item))
}

const buildTranscriptFromText = (text: string): TranscriptCue[] => {
  const lines = String(text || '')
    .split(/[.!?\n]+/)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(0, 24)
  let cursor = 0
  return lines.map((line) => {
    const duration = Math.min(7, Math.max(2, line.split(/\s+/).length * 0.42))
    const cue = { start: cursor, end: cursor + duration, text: line }
    cursor += duration
    return cue
  })
}

const ensurePremiumFeedbackAccess = async (userId: string, email?: string | null) => {
  const { tier } = await getUserPlan(userId)
  const devAccess = await resolveDevAdminAccess(userId, email)
  const isDev = devAccess.emailAuthorized
  const isPremium = isDev || isPaidTier(tier)
  return { tier, isDev, isPremium }
}

router.get('/jobs', async (req: any, res) => {
  try {
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const access = await ensurePremiumFeedbackAccess(userId, req.user?.email)
    if (!access.isPremium) {
      return res.status(403).json({
        error: 'PREMIUM_REQUIRED',
        message: 'Upgrade to unlock AI video feedback.',
        redirectTo: '/pricing'
      })
    }
    const jobs = await prisma.job.findMany({
      where: { userId },
      orderBy: { createdAt: 'desc' },
      take: 24,
      select: {
        id: true,
        inputPath: true,
        createdAt: true,
        status: true,
        inputDurationSeconds: true
      }
    })
    const completedJobs = jobs
      .filter((job) => String(job.status || '').toLowerCase() === 'completed')
      .map((job) => ({
        id: job.id,
        title: path.basename(String(job.inputPath || `job-${job.id}`)),
        createdAt: job.createdAt,
        durationSeconds: job.inputDurationSeconds ?? null
      }))
    return res.json({ jobs: completedJobs })
  } catch (error) {
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/analyze', async (req: any, res) => {
  try {
    const userId = req.user?.id
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const access = await ensurePremiumFeedbackAccess(userId, req.user?.email)
    if (!access.isPremium) {
      return res.status(403).json({
        error: 'PREMIUM_REQUIRED',
        message: 'Upgrade to unlock AI trends and feedback.',
        redirectTo: '/pricing',
        teaser: {
          title: 'AI Feedback on Your Video',
          hint: 'Upgrade to unlock transcript + trend intelligence.'
        }
      })
    }

    const jobId = String(req.body?.jobId || '').trim()
    const uploadSummary = req.body?.uploadSummary && typeof req.body.uploadSummary === 'object'
      ? req.body.uploadSummary
      : null

    if (!jobId && !uploadSummary) {
      return res.status(400).json({ error: 'missing_source', message: 'Provide jobId or uploadSummary.' })
    }

    if (jobId) {
      const job = await prisma.job.findUnique({ where: { id: jobId } })
      if (!job || job.userId !== userId) return res.status(404).json({ error: 'not_found' })
      const analysis = ((job.analysis && typeof job.analysis === 'object') ? (job.analysis as any) : {}) as Record<string, any>
      const transcriptCues = extractTranscriptCues(analysis)
      const engagementWindows = extractEngagementWindows(analysis)
      const metadataSummary = (
        analysis?.metadata_summary && typeof analysis.metadata_summary === 'object'
          ? analysis.metadata_summary
          : {}
      ) as Record<string, any>
      const nicheHint = typeof analysis?.niche_profile?.niche === 'string'
        ? analysis.niche_profile.niche
        : (typeof metadataSummary?.niche === 'string' ? metadataSummary.niche : null)
      const feedback = await buildVideoFeedbackAnalysis({
        title: path.basename(String(job.inputPath || `job-${job.id}`)),
        durationSeconds: Number(job.inputDurationSeconds || analysis?.duration || 0),
        transcriptCues,
        engagementWindows,
        metadataSummary,
        nicheHint
      })
      return res.json({
        source: { type: 'job', jobId: job.id },
        tier: access.tier,
        isDev: access.isDev,
        generatedAt: new Date().toISOString(),
        feedback
      })
    }

    const transcript = String(uploadSummary?.transcript || uploadSummary?.text || '').trim()
    const feedback = await buildVideoFeedbackAnalysis({
      title: String(uploadSummary?.fileName || 'Uploaded video'),
      durationSeconds: Number(uploadSummary?.durationSeconds || 0),
      transcriptCues: buildTranscriptFromText(transcript),
      engagementWindows: [],
      metadataSummary: (
        uploadSummary?.metadata && typeof uploadSummary.metadata === 'object'
          ? uploadSummary.metadata
          : {}
      ) as Record<string, any>,
      nicheHint: String(uploadSummary?.nicheHint || '')
    })

    return res.json({
      source: { type: 'upload' },
      tier: access.tier,
      isDev: access.isDev,
      generatedAt: new Date().toISOString(),
      feedback
    })
  } catch (error) {
    return res.status(500).json({ error: 'server_error' })
  }
})

export default router

