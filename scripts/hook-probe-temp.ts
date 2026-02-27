import fs from 'fs'
import path from 'path'
import { __retentionTestUtils } from '../src/routes/jobs'

const inputArg = process.argv[2]
if (!inputArg) {
  console.error('ERR missing_input_arg')
  process.exit(1)
}

const run = async () => {
  const filePath = path.resolve(process.cwd(), '..', inputArg)
  const plan: any = await (__retentionTestUtils as any).buildEditPlanForTest({
    filePath,
    aggressionLevel: 'medium'
  })

  const payload = {
    filePath,
    generatedAt: new Date().toISOString(),
    durationSeconds: plan.durationSeconds,
    styleProfile: plan.styleProfile,
    nicheProfile: plan.nicheProfile,
    topHookCandidates: plan.topHookCandidates,
    hook: plan.hook,
    selectedHookStrategy: plan.selectedHookStrategy,
    emotionalTuning: plan.emotionalTuning,
    emotionalBeatAnchors: plan.emotionalBeatAnchors,
    emotionalBeatCutCount: plan.emotionalBeatCutCount,
    emotionalLeadTrimmedSeconds: plan.emotionalLeadTrimmedSeconds,
    retentionScore: plan.retentionScore,
    windows: plan.windows,
    transcriptCues: plan.transcriptCues,
    analysis: plan.analysis,
    timeline: plan.timeline,
    segments: plan.segments
  }

  const safeName = path.basename(filePath).replace(/[^a-zA-Z0-9_.-]+/g, '_')
  const outPath = path.resolve(process.cwd(), '..', 'output', `hook-analysis-${safeName}.json`)
  fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), 'utf8')

  console.log('WROTE', outPath)
  console.log('INPUT', filePath)
  console.log('DURATION', plan.durationSeconds)
  console.log('HOOK', JSON.stringify(plan.hook || null))
  console.log('TOP_CANDIDATES', JSON.stringify((plan.topHookCandidates || []).slice(0, 8)))
  console.log('STYLE', JSON.stringify(plan.styleProfile || null))
  console.log('NICHE', JSON.stringify(plan.nicheProfile || null))
  console.log('TRANSCRIPT_COUNT', Array.isArray(plan.transcriptCues) ? plan.transcriptCues.length : 0)
  console.log('WINDOW_COUNT', Array.isArray(plan.windows) ? plan.windows.length : 0)
}

run().catch((err) => {
  console.error('ERR', err?.message || err)
  process.exit(1)
})
