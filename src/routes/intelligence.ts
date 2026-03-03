import express from 'express'
import { prisma } from '../db/prisma'
import {
  applyBoundaryCriticHardGate,
  bootstrapBoundaryLabelsFromCompletedJobs,
  collectHumanBaselineSample,
  derivePerSecondRewardSignal,
  ensureEditorIntelligenceInfra,
  getActiveBoundaryCriticModel,
  getCreatorStyleProfile,
  getHumanBaselineDatasetStats,
  ingestPlatformRewardSignal,
  labelHumanBaselineSampleBoundaries,
  listHumanBaselineSamples,
  listPolicyPromotionCandidates,
  registerPolicyAssignment,
  registerPolicyOutcomeForJob,
  runMultiPassRefinement,
  scoreBoundarySet,
  selectPolicyWinnerWithLearning,
  trainBoundaryCriticModelFromBaseline,
  upsertCreatorStyleProfileFromFeedback
} from '../services/editorIntelligence'

const router = express.Router()

const asNumber = (value: unknown, fallback = 0) => {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const asObject = (value: unknown) =>
  value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, any>)
    : {}

const deriveSegmentsFromClassicJob = (job: any) => {
  const analysis = asObject(job?.analysis)
  const metadataSummary = asObject(analysis.metadata_summary)
  const timeline = asObject(metadataSummary.timeline)
  const directSegments = Array.isArray(timeline.segments) ? timeline.segments : []
  if (directSegments.length) return directSegments
  const fallback = Array.isArray(analysis.segments) ? analysis.segments : []
  return fallback
}

router.get('/status', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    await ensureEditorIntelligenceInfra()
    const model = await getActiveBoundaryCriticModel()
    const baseline = await getHumanBaselineDatasetStats(userId)
    const style = await getCreatorStyleProfile(userId)
    return res.json({
      ok: true,
      model,
      baseline,
      style
    })
  } catch {
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/baseline/collect', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const jobId = String(req.body?.jobId || req.body?.job_id || '').trim()
    let payload = {
      sourceType: String(req.body?.sourceType || req.body?.source_type || 'manual'),
      sourceJobId: jobId || null,
      videoUrl: req.body?.videoUrl || req.body?.video_url || null,
      durationSeconds: asNumber(req.body?.durationSeconds ?? req.body?.duration_seconds, 0) || null,
      edl: Array.isArray(req.body?.edl) ? req.body.edl : [],
      boundaryLabels: Array.isArray(req.body?.boundaryLabels) ? req.body.boundaryLabels : [],
      metadata: asObject(req.body?.metadata)
    }

    if (jobId) {
      const classic = await prisma.job.findUnique({ where: { id: jobId } })
      if (classic && String(classic.userId) === userId) {
        payload = {
          sourceType: 'classic_job',
          sourceJobId: jobId,
          videoUrl: String(classic.outputPath || classic.inputPath || ''),
          durationSeconds: asNumber(classic.inputDurationSeconds, 0) || null,
          edl: deriveSegmentsFromClassicJob(classic),
          boundaryLabels: Array.isArray(req.body?.boundaryLabels) ? req.body.boundaryLabels : [],
          metadata: {
            status: classic.status,
            retentionScore: classic.retentionScore,
            from: 'jobs'
          }
        }
      } else {
        try {
          const vibe = await (prisma as any).vibeCutJob.findUnique({
            where: { id: jobId }
          })
          if (vibe && String(vibe.userId) === userId) {
            const retention = asObject(vibe.retention)
            payload = {
              sourceType: 'vibecut_job',
              sourceJobId: jobId,
              videoUrl: String(vibe.outputVideoUrl || vibe.outputVideoPath || ''),
              durationSeconds: asNumber(retention?.duration || 0, 0) || null,
              edl: Array.isArray(retention?.segments) ? retention.segments : [],
              boundaryLabels: Array.isArray(req.body?.boundaryLabels) ? req.body.boundaryLabels : [],
              metadata: {
                status: vibe.status,
                from: 'vibecut'
              }
            }
          }
        } catch {
          // vibecut may not exist in environment
        }
      }
    }

    const sample = await collectHumanBaselineSample({
      userId,
      sourceType: payload.sourceType,
      sourceJobId: payload.sourceJobId,
      videoUrl: payload.videoUrl,
      durationSeconds: payload.durationSeconds,
      edl: payload.edl,
      boundaryLabels: payload.boundaryLabels,
      metadata: payload.metadata
    })
    return res.json({
      ok: true,
      sample
    })
  } catch (error: any) {
    return res.status(500).json({
      error: 'baseline_collect_failed',
      reason: String(error?.message || 'unknown_error')
    })
  }
})

router.post('/baseline/bootstrap-from-jobs', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const maxJobs = clamp(Math.round(asNumber(req.body?.maxJobs ?? req.body?.max_jobs, 120)), 8, 400)
    const maxLabelsPerJob = clamp(Math.round(asNumber(req.body?.maxLabelsPerJob ?? req.body?.max_labels_per_job, 22)), 4, 60)
    const focusJobId = String(req.body?.focusJobId || req.body?.focus_job_id || '').trim() || null
    const summary = await bootstrapBoundaryLabelsFromCompletedJobs({
      userId,
      focusJobId,
      maxJobs,
      maxLabelsPerJob
    })
    const stats = await getHumanBaselineDatasetStats(userId)
    return res.json({
      ok: true,
      summary,
      stats
    })
  } catch (error: any) {
    return res.status(500).json({
      error: 'baseline_bootstrap_failed',
      reason: String(error?.message || 'unknown_error')
    })
  }
})

router.post('/baseline/label', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const sampleId = String(req.body?.sampleId || req.body?.sample_id || '').trim()
    const labels = Array.isArray(req.body?.boundaryLabels)
      ? req.body.boundaryLabels
      : Array.isArray(req.body?.labels)
        ? req.body.labels
        : []
    if (!sampleId || !labels.length) {
      return res.status(400).json({
        error: 'invalid_payload',
        message: 'Provide sampleId and boundaryLabels.'
      })
    }
    const sample = await labelHumanBaselineSampleBoundaries({
      userId,
      sampleId,
      boundaryLabels: labels,
      replace: Boolean(req.body?.replace)
    })
    return res.json({
      ok: true,
      sample
    })
  } catch (error: any) {
    const reason = String(error?.message || '')
    if (reason === 'sample_not_found') {
      return res.status(404).json({ error: reason })
    }
    if (reason === 'no_boundary_labels') {
      return res.status(400).json({ error: reason })
    }
    return res.status(500).json({
      error: 'baseline_label_failed',
      reason
    })
  }
})

router.get('/baseline/stats', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const stats = await getHumanBaselineDatasetStats(userId)
    return res.json({
      ok: true,
      stats
    })
  } catch {
    return res.status(500).json({ error: 'server_error' })
  }
})

router.get('/baseline/samples', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const limit = clamp(Math.round(asNumber(req.query?.limit, 40)), 1, 200)
    const samples = await listHumanBaselineSamples({ userId, limit })
    return res.json({
      ok: true,
      samples
    })
  } catch {
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/boundary-critic/train', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const minSamples = clamp(Math.round(asNumber(req.body?.minSamples ?? req.body?.min_samples, 60)), 20, 5000)
    const model = await trainBoundaryCriticModelFromBaseline({
      userId,
      minSamples
    })
    return res.json({
      ok: true,
      model
    })
  } catch (error: any) {
    const reason = String(error?.code || error?.message || '')
    if (reason.startsWith('not_enough_samples')) {
      return res.status(400).json({
        error: 'not_enough_samples',
        detail: reason
      })
    }
    return res.status(500).json({
      error: 'boundary_critic_training_failed',
      reason
    })
  }
})

router.get('/boundary-critic/model', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const model = await getActiveBoundaryCriticModel()
    return res.json({
      ok: true,
      model
    })
  } catch {
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/boundary-critic/score', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const segments = Array.isArray(req.body?.segments) ? req.body.segments : []
    const durationSeconds = asNumber(req.body?.durationSeconds ?? req.body?.duration_seconds, 0)
    if (!segments.length || durationSeconds <= 0) {
      return res.status(400).json({
        error: 'invalid_payload',
        message: 'Provide segments and durationSeconds.'
      })
    }
    const windows = Array.isArray(req.body?.windows) ? req.body.windows : []
    const score = await scoreBoundarySet({
      segments,
      durationSeconds,
      windows
    })
    const gate = await applyBoundaryCriticHardGate({
      segments,
      durationSeconds,
      windows
    })
    return res.json({
      ok: true,
      score,
      gate
    })
  } catch (error: any) {
    return res.status(500).json({
      error: 'boundary_critic_scoring_failed',
      reason: String(error?.message || 'unknown_error')
    })
  }
})

router.post('/multi-pass/refine', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const segments = Array.isArray(req.body?.segments) ? req.body.segments : []
    const durationSeconds = asNumber(req.body?.durationSeconds ?? req.body?.duration_seconds, 0)
    if (!segments.length || durationSeconds <= 0) {
      return res.status(400).json({
        error: 'invalid_payload',
        message: 'Provide segments and durationSeconds.'
      })
    }
    const windows = Array.isArray(req.body?.windows) ? req.body.windows : []
    const creatorProfile = await getCreatorStyleProfile(userId)
    const result = await runMultiPassRefinement({
      segments,
      durationSeconds,
      windows,
      creatorProfile
    })
    return res.json({
      ok: true,
      result
    })
  } catch (error: any) {
    return res.status(500).json({
      error: 'multi_pass_refine_failed',
      reason: String(error?.message || 'unknown_error')
    })
  }
})

router.post('/rewards/ingest', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const jobId = String(req.body?.jobId || req.body?.job_id || '').trim()
    if (!jobId) return res.status(400).json({ error: 'missing_job_id' })
    const source = String(req.body?.source || 'platform').trim() || 'platform'
    const retentionPoints = Array.isArray(req.body?.retentionPoints) ? req.body.retentionPoints : []
    const skipHotspots = Array.isArray(req.body?.skipHotspots) ? req.body.skipHotspots : []
    const rewatchHotspots = Array.isArray(req.body?.rewatchHotspots) ? req.body.rewatchHotspots : []
    const durationSeconds = asNumber(req.body?.durationSeconds ?? req.body?.duration_seconds, 0)
    if (durationSeconds <= 0) {
      return res.status(400).json({
        error: 'invalid_duration',
        message: 'durationSeconds is required for reward signal construction.'
      })
    }
    const reward = derivePerSecondRewardSignal({
      durationSeconds,
      retentionPoints,
      skipHotspots,
      rewatchHotspots
    })
    const stored = await ingestPlatformRewardSignal({
      userId,
      jobId,
      source,
      videoId: req.body?.videoId || req.body?.video_id || null,
      perSecondRewards: reward.perSecondRewards,
      summary: {
        ...reward.summary,
        ...(asObject(req.body?.summary))
      }
    })
    return res.json({
      ok: true,
      reward,
      stored
    })
  } catch (error: any) {
    return res.status(500).json({
      error: 'reward_ingest_failed',
      reason: String(error?.message || 'unknown_error')
    })
  }
})

router.get('/creator-style/profile', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const profile = await getCreatorStyleProfile(userId)
    return res.json({
      ok: true,
      profile
    })
  } catch {
    return res.status(500).json({ error: 'server_error' })
  }
})

router.post('/creator-style/update', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const feedback = asObject(req.body?.feedback || req.body)
    const profile = await upsertCreatorStyleProfileFromFeedback({
      userId,
      feedback
    })
    return res.json({
      ok: true,
      profile
    })
  } catch (error: any) {
    return res.status(500).json({
      error: 'creator_style_update_failed',
      reason: String(error?.message || 'unknown_error')
    })
  }
})

router.post('/ab/select', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const candidates = Array.isArray(req.body?.candidates) ? req.body.candidates : []
    const decision = await selectPolicyWinnerWithLearning({
      userId,
      candidates,
      explorationRate: clamp(asNumber(req.body?.explorationRate ?? 0.14, 0.14), 0, 1)
    })
    if (!decision) return res.status(400).json({ error: 'no_candidates' })
    return res.json({
      ok: true,
      decision
    })
  } catch (error: any) {
    return res.status(500).json({
      error: 'ab_select_failed',
      reason: String(error?.message || 'unknown_error')
    })
  }
})

router.post('/ab/assign', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const jobId = String(req.body?.jobId || req.body?.job_id || '').trim()
    const policyId = String(req.body?.policyId || req.body?.policy_id || '').trim()
    const variantId = String(req.body?.variantId || req.body?.variant_id || '').trim() || null
    if (!jobId || !policyId) return res.status(400).json({ error: 'invalid_payload' })
    const result = await registerPolicyAssignment({
      userId,
      jobId,
      policyId,
      variantId
    })
    return res.json({
      ok: true,
      result
    })
  } catch (error: any) {
    return res.status(500).json({
      error: 'ab_assign_failed',
      reason: String(error?.message || 'unknown_error')
    })
  }
})

router.post('/ab/outcome', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const jobId = String(req.body?.jobId || req.body?.job_id || '').trim()
    if (!jobId) return res.status(400).json({ error: 'missing_job_id' })
    const feedback = asObject(req.body?.feedback || {})
    const result = await registerPolicyOutcomeForJob({
      userId,
      jobId,
      feedback,
      source: req.body?.source || null,
      isPlatform: Boolean(req.body?.isPlatform),
      metadata: asObject(req.body?.metadata)
    })
    return res.json({
      ok: true,
      result
    })
  } catch (error: any) {
    return res.status(500).json({
      error: 'ab_outcome_failed',
      reason: String(error?.message || 'unknown_error')
    })
  }
})

router.get('/ab/promotions', async (req: any, res) => {
  try {
    const userId = String(req.user?.id || '').trim()
    if (!userId) return res.status(401).json({ error: 'unauthorized' })
    const minSamples = clamp(Math.round(asNumber(req.query?.minSamples, 12)), 3, 500)
    const minLift = clamp(asNumber(req.query?.minLift, 2.5), 0.1, 50)
    const zThreshold = clamp(asNumber(req.query?.zThreshold, 1.96), 0.5, 6)
    const candidates = await listPolicyPromotionCandidates({
      userId,
      minSamples,
      minLift,
      zThreshold
    })
    return res.json({
      ok: true,
      candidates
    })
  } catch (error: any) {
    return res.status(500).json({
      error: 'ab_promotions_failed',
      reason: String(error?.message || 'unknown_error')
    })
  }
})

export default router
