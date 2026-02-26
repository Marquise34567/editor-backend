import assert from 'assert'
import { __retentionTestUtils } from '../src/routes/jobs'

const buildUniquenessSignatureForTest = (__retentionTestUtils as any).buildUniquenessSignatureForTest as (
  args: {
    strategyProfile: string
    targetPlatform: string
    editorMode: string
    maxCuts: number
    longFormAggression: number
    longFormClarityVsSpeed: number
    tangentKiller: boolean
    durationSeconds?: number
  }
) => string

if (typeof buildUniquenessSignatureForTest !== 'function') {
  throw new Error('uniqueness_signature_utility_unavailable')
}

const run = () => {
  const modes = ['reaction', 'commentary', 'savage-roast', 'vlog', 'gaming', 'sports', 'podcast'] as const
  const vibes = [
    {
      key: 'grounded',
      strategyProfile: 'safe',
      targetPlatform: 'youtube',
      maxCuts: 4,
      longFormAggression: 34,
      longFormClarityVsSpeed: 76,
      tangentKiller: true
    },
    {
      key: 'story',
      strategyProfile: 'balanced',
      targetPlatform: 'youtube',
      maxCuts: 6,
      longFormAggression: 52,
      longFormClarityVsSpeed: 62,
      tangentKiller: true
    },
    {
      key: 'punchy',
      strategyProfile: 'balanced',
      targetPlatform: 'instagram_reels',
      maxCuts: 9,
      longFormAggression: 68,
      longFormClarityVsSpeed: 46,
      tangentKiller: false
    },
    {
      key: 'chaos',
      strategyProfile: 'viral',
      targetPlatform: 'tiktok',
      maxCuts: 12,
      longFormAggression: 86,
      longFormClarityVsSpeed: 28,
      tangentKiller: false
    }
  ] as const

  const factorScenarios = vibes.flatMap((vibe) => modes.map((mode) => ({ ...vibe, mode })))
  const factorSignatures = new Set<string>()
  for (const scenario of factorScenarios) {
    const signature = buildUniquenessSignatureForTest({
      strategyProfile: scenario.strategyProfile,
      targetPlatform: scenario.targetPlatform,
      editorMode: scenario.mode,
      maxCuts: scenario.maxCuts,
      longFormAggression: scenario.longFormAggression,
      longFormClarityVsSpeed: scenario.longFormClarityVsSpeed,
      tangentKiller: scenario.tangentKiller,
      durationSeconds: 420
    })
    factorSignatures.add(signature)
  }
  const factorUniqueCount = factorSignatures.size
  const factorTotal = factorScenarios.length
  const factorRatio = factorUniqueCount / factorTotal
  const factorMinimumUnique = Math.ceil(factorTotal * 0.9)
  assert.ok(
    factorUniqueCount >= factorMinimumUnique,
    `factor-sensitivity uniqueness too low: ${factorUniqueCount}/${factorTotal} (< ${factorMinimumUnique})`
  )
  console.log(
    `[uniqueness] factor-sensitivity run: ${factorTotal} scenarios, ${factorUniqueCount} unique signatures (${(factorRatio * 100).toFixed(1)}%).`
  )

  const propagationModes = ['reaction', 'commentary', 'savage-roast', 'vlog', 'gaming', 'sports', 'education', 'podcast'] as const
  const strategies = ['safe', 'balanced', 'viral'] as const
  const platforms = ['youtube', 'tiktok', 'instagram_reels'] as const
  const payloadProfiles = [
    {
      key: 'tight',
      maxCuts: 5,
      longFormAggression: 44,
      longFormClarityVsSpeed: 72,
      tangentKiller: true
    },
    {
      key: 'loose',
      maxCuts: 12,
      longFormAggression: 79,
      longFormClarityVsSpeed: 34,
      tangentKiller: false
    }
  ] as const

  const propagationScenarios = strategies.flatMap((strategyProfile) => (
    platforms.flatMap((targetPlatform) => (
      propagationModes.flatMap((editorMode) => (
        payloadProfiles.map((profile) => ({
          strategyProfile,
          targetPlatform,
          editorMode,
          profile
        }))
      ))
    ))
  ))
  assert.strictEqual(propagationScenarios.length, 144, 'propagation benchmark should evaluate 144 scenarios')

  const persistedSignatureSet = new Set<string>()
  for (const scenario of propagationScenarios) {
    const signature = buildUniquenessSignatureForTest({
      strategyProfile: scenario.strategyProfile,
      targetPlatform: scenario.targetPlatform,
      editorMode: scenario.editorMode,
      maxCuts: scenario.profile.maxCuts,
      longFormAggression: scenario.profile.longFormAggression,
      longFormClarityVsSpeed: scenario.profile.longFormClarityVsSpeed,
      tangentKiller: scenario.profile.tangentKiller,
      durationSeconds: 560
    })
    persistedSignatureSet.add(signature)
  }
  const propagationUniqueCount = persistedSignatureSet.size
  assert.strictEqual(
    propagationUniqueCount,
    propagationScenarios.length,
    `payload propagation collision detected: ${propagationUniqueCount}/${propagationScenarios.length} unique signatures`
  )
  console.log(
    `[uniqueness] payload propagation run: ${propagationUniqueCount}/${propagationScenarios.length} unique persisted signatures.`
  )

  console.log('PASS uniqueness benchmark')
}

try {
  run()
} catch (error) {
  console.error('FAIL uniqueness benchmark', error)
  process.exit(1)
}
