import fs from 'fs'
import path from 'path'
import { __retentionTestUtils } from '../src/routes/jobs'

const { buildEditPlanForTest } = __retentionTestUtils

type ClipResult = {
  filePath: string
  style: string
  niche: string
  beatCuts: number
  leadTrimSeconds: number
  thresholdOffset: number
  spacingMultiplier: number
  leadTrimMultiplier: number
}

const walk = (root: string): string[] => {
  if (!fs.existsSync(root)) return []
  const out: string[] = []
  const stack = [root]
  while (stack.length) {
    const dir = stack.pop() as string
    const entries = fs.readdirSync(dir, { withFileTypes: true })
    for (const entry of entries) {
      const full = path.join(dir, entry.name)
      if (entry.isDirectory()) {
        stack.push(full)
        continue
      }
      out.push(full)
    }
  }
  return out
}

const summarizeByNiche = (rows: ClipResult[]) => {
  const groups = new Map<string, ClipResult[]>()
  for (const row of rows) {
    const key = row.niche || 'unknown'
    if (!groups.has(key)) groups.set(key, [])
    groups.get(key)!.push(row)
  }
  const summary = Array.from(groups.entries()).map(([niche, items]) => {
    const avg = (pick: (item: ClipResult) => number) =>
      items.reduce((sum, item) => sum + pick(item), 0) / Math.max(1, items.length)
    return {
      niche,
      clips: items.length,
      avgBeatCuts: Number(avg((item) => item.beatCuts).toFixed(2)),
      avgLeadTrim: Number(avg((item) => item.leadTrimSeconds).toFixed(2)),
      avgThresholdOffset: Number(avg((item) => item.thresholdOffset).toFixed(3)),
      avgSpacingMultiplier: Number(avg((item) => item.spacingMultiplier).toFixed(3)),
      avgLeadTrimMultiplier: Number(avg((item) => item.leadTrimMultiplier).toFixed(3))
    }
  })
  return summary.sort((a, b) => b.clips - a.clips || a.niche.localeCompare(b.niche))
}

const main = async () => {
  const repoRoot = path.resolve(__dirname, '..', '..')
  const outputRoot = path.join(repoRoot, 'output')
  const allFiles = walk(outputRoot)
  const candidates = allFiles
    .filter((filePath) => filePath.toLowerCase().endsWith('.mp4'))
    .filter((filePath) => {
      const stat = fs.statSync(filePath)
      return stat.size > 10_000
    })
    .sort((a, b) => fs.statSync(b).size - fs.statSync(a).size)
    .slice(0, 8)

  if (!candidates.length) {
    throw new Error(`No usable clips found under ${outputRoot}`)
  }

  console.log(`Running emotional tuning analysis on ${candidates.length} real clip(s)...`)
  const results: ClipResult[] = []
  for (const clipPath of candidates) {
    try {
      const plan = await buildEditPlanForTest({
        filePath: clipPath,
        aggressionLevel: 'medium'
      })
      const tuning = plan.emotionalTuning || {
        thresholdOffset: 0,
        spacingMultiplier: 1,
        leadTrimMultiplier: 1
      }
      const row: ClipResult = {
        filePath: clipPath,
        style: plan.styleProfile?.style || 'unknown',
        niche: plan.nicheProfile?.niche || 'unknown',
        beatCuts: Number(plan.emotionalBeatCutCount || 0),
        leadTrimSeconds: Number((plan.emotionalLeadTrimmedSeconds || 0).toFixed(3)),
        thresholdOffset: Number((tuning.thresholdOffset || 0).toFixed(3)),
        spacingMultiplier: Number((tuning.spacingMultiplier || 1).toFixed(3)),
        leadTrimMultiplier: Number((tuning.leadTrimMultiplier || 1).toFixed(3))
      }
      results.push(row)
      console.log(
        `- ${path.basename(clipPath)} | style=${row.style} niche=${row.niche} beatCuts=${row.beatCuts} trim=${row.leadTrimSeconds}s tuning(th=${row.thresholdOffset}, sp=${row.spacingMultiplier}, lt=${row.leadTrimMultiplier})`
      )
    } catch (error: any) {
      console.warn(`- ${path.basename(clipPath)} | skipped (${error?.message || error})`)
    }
  }

  if (!results.length) {
    throw new Error('No clips were successfully analyzed')
  }

  const summary = summarizeByNiche(results)
  console.log('\nNiche summary from real clips:')
  for (const item of summary) {
    console.log(
      `* ${item.niche}: clips=${item.clips}, avgBeatCuts=${item.avgBeatCuts}, avgLeadTrim=${item.avgLeadTrim}s, threshold=${item.avgThresholdOffset}, spacing=${item.avgSpacingMultiplier}, leadTrimMult=${item.avgLeadTrimMultiplier}`
    )
  }
}

main().catch((error) => {
  console.error('FAIL emotional tuning from real clips')
  console.error(error?.message || error)
  process.exit(1)
})
