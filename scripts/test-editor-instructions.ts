import assert from 'assert'
import { __retentionTestUtils } from '../src/routes/jobs'

const {
  parseEditorInstructionPromptForTest,
  mergeEditorInstructionMarkersIntoManualTimestampConfigForTest,
  parsePersistedRenderConfigForTest,
  buildPersistedRenderAnalysis
} = __retentionTestUtils

const run = () => {
  const parsed = parseEditorInstructionPromptForTest(
    'Remove 00:52-01:08. Keep pacing tight, remove dead air, and keep transitions smooth.'
  )
  assert.ok(parsed, 'prompt parser should return a plan')
  assert.strictEqual(parsed?.retentionStrategyProfile, 'viral', 'fast pacing should map to viral strategy')
  assert.strictEqual(parsed?.markers.length, 1, 'timestamp removal should become one manual marker')
  assert.strictEqual(parsed?.markers[0]?.type, 'remove', 'range should be marked for removal')
  assert.strictEqual(parsed?.markers[0]?.start, 52, 'start timestamp should parse to seconds')
  assert.strictEqual(parsed?.markers[0]?.end, 68, 'end timestamp should parse to seconds')
  assert.strictEqual(parsed?.tangentKiller, true, 'dead air directive should enable tangent killer')
  assert.strictEqual(parsed?.transitions, true, 'smooth directive should keep transitions on')

  const mergedManual = mergeEditorInstructionMarkersIntoManualTimestampConfigForTest(
    {
      enabled: true,
      autoAssist: false,
      markers: [
        {
          id: 'existing-keep',
          type: 'keep',
          start: 0,
          end: 45,
          source: 'user'
        }
      ],
      suggestions: [],
      requested: false,
      retentionDeltaEstimate: null
    },
    parsed
  )
  assert.ok(mergedManual?.enabled, 'manual timeline should stay enabled after prompt merge')
  assert.strictEqual(mergedManual?.markers.length, 2, 'prompt markers should append to existing manual markers')
  assert.ok(
    mergedManual?.markers.some((marker) => marker.type === 'remove' && marker.start === 52 && marker.end === 68),
    'prompt removal should be present in merged manual config'
  )

  const renderConfig = parsePersistedRenderConfigForTest({
    analysis: {},
    renderSettings: {},
    jobId: 'editor-instructions-test',
    context: 'test'
  })
  const persistedAnalysis: any = buildPersistedRenderAnalysis({
    existing: {},
    renderConfig,
    retentionTargetPlatform: 'youtube',
    platformProfile: 'youtube',
    editorInstructionPrompt: parsed?.prompt,
    manualTimestampConfig: mergedManual
  })
  assert.strictEqual(
    persistedAnalysis.editorInstructionPrompt,
    parsed?.prompt,
    'persisted analysis should keep the prompt text'
  )
  assert.strictEqual(
    persistedAnalysis.editor_instruction_prompt,
    parsed?.prompt,
    'persisted analysis should keep the snake_case prompt text'
  )
  assert.ok(
    Array.isArray(persistedAnalysis.manualTimestamp?.markers) && persistedAnalysis.manualTimestamp.markers.length === 2,
    'persisted analysis should keep merged manual markers'
  )
}

run()
console.log('editor instruction tests passed')
