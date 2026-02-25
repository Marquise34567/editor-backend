import { prisma } from '../../../db/prisma'
import { parseConfigParams } from '../config/configService'
import { listLastMetrics, listMetricsByRange } from '../integration/pipelineIntegration'
import { AlgorithmConfigParams, RenderQualityMetric } from '../types'

export type ImprovementSuggestion = {
  title: string
  why: string
  change: Record<string, number>
  predicted_delta_score: number
  confidence: number
  risk: string
}

type MetricWithConfig = RenderQualityMetric & {
  config_params: AlgorithmConfigParams
  preset_name: string | null
  config_created_at: string | null
}

const canRunRawSql = () =>
  typeof (prisma as any)?.$executeRawUnsafe === 'function' &&
  typeof (prisma as any)?.$queryRawUnsafe === 'function'

const NUMERIC_PARAM_KEYS: Array<keyof AlgorithmConfigParams> = [
  'cut_aggression',
  'min_clip_len_ms',
  'max_clip_len_ms',
  'silence_db_threshold',
  'silence_min_ms',
  'filler_word_weight',
  'redundancy_weight',
  'energy_floor',
  'spike_boost',
  'pattern_interrupt_every_sec',
  'hook_priority_weight',
  'story_coherence_guard',
  'jank_guard',
  'pacing_multiplier'
]

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const average = (values: number[]) => {
  if (!values.length) return 0
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

const stdDev = (values: number[]) => {
  if (values.length <= 1) return 0
  const mean = average(values)
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length
  return Math.sqrt(variance)
}

const pearson = (x: number[], y: number[]) => {
  if (x.length !== y.length || x.length < 3) return 0
  const xMean = average(x)
  const yMean = average(y)
  let numerator = 0
  let xVar = 0
  let yVar = 0
  for (let i = 0; i < x.length; i += 1) {
    const xDiff = x[i] - xMean
    const yDiff = y[i] - yMean
    numerator += xDiff * yDiff
    xVar += xDiff * xDiff
    yVar += yDiff * yDiff
  }
  if (xVar <= 0 || yVar <= 0) return 0
  return numerator / Math.sqrt(xVar * yVar)
}

const parseRangeToMs = (range: string) => {
  const normalized = String(range || '7d').trim().toLowerCase()
  if (/^\d+h$/.test(normalized)) return Number(normalized.replace('h', '')) * 60 * 60 * 1000
  if (/^\d+d$/.test(normalized)) return Number(normalized.replace('d', '')) * 24 * 60 * 60 * 1000
  if (/^\d+w$/.test(normalized)) return Number(normalized.replace('w', '')) * 7 * 24 * 60 * 60 * 1000
  return 7 * 24 * 60 * 60 * 1000
}

export const fetchMetricsWithConfig = async ({
  limit,
  range
}: {
  limit: number
  range?: string
}): Promise<MetricWithConfig[]> => {
  const safeLimit = Math.max(1, Math.min(5_000, Math.round(Number(limit || 1_000))))

  if (!canRunRawSql()) {
    const fallbackMetrics = range
      ? await listMetricsByRange({ range, limit: safeLimit })
      : await listLastMetrics(safeLimit)
    return fallbackMetrics.map((metric) => ({
      ...metric,
      config_params: parseConfigParams({}),
      preset_name: null,
      config_created_at: null
    }))
  }

  const hasRange = Boolean(range)
  const rangeStartIso = hasRange ? new Date(Date.now() - parseRangeToMs(range || '7d')).toISOString() : null
  const rows = hasRange
    ? await (prisma as any).$queryRawUnsafe(
        `
          SELECT
            m.id,
            m.job_id,
            m.user_id,
            m.created_at,
            m.config_version_id,
            m.score_total,
            m.score_hook,
            m.score_pacing,
            m.score_emotion,
            m.score_visual,
            m.score_story,
            m.score_jank,
            m.features,
            m.flags,
            c.params AS config_params,
            c.preset_name,
            c.created_at AS config_created_at
          FROM render_quality_metrics m
          INNER JOIN editor_config_versions c ON c.id = m.config_version_id
          WHERE m.created_at >= $1
          ORDER BY m.created_at DESC
          LIMIT $2
        `,
        rangeStartIso,
        safeLimit
      )
    : await (prisma as any).$queryRawUnsafe(
        `
          SELECT
            m.id,
            m.job_id,
            m.user_id,
            m.created_at,
            m.config_version_id,
            m.score_total,
            m.score_hook,
            m.score_pacing,
            m.score_emotion,
            m.score_visual,
            m.score_story,
            m.score_jank,
            m.features,
            m.flags,
            c.params AS config_params,
            c.preset_name,
            c.created_at AS config_created_at
          FROM render_quality_metrics m
          INNER JOIN editor_config_versions c ON c.id = m.config_version_id
          ORDER BY m.created_at DESC
          LIMIT $1
        `,
        safeLimit
      )

  if (!Array.isArray(rows)) return []

  return rows.map((row) => ({
    id: String((row as any)?.id || ''),
    job_id: String((row as any)?.job_id || ''),
    user_id: (row as any)?.user_id ? String((row as any).user_id) : null,
    created_at: (row as any)?.created_at ? new Date((row as any).created_at).toISOString() : new Date().toISOString(),
    config_version_id: String((row as any)?.config_version_id || ''),
    score_total: Number((row as any)?.score_total || 0),
    score_hook: Number((row as any)?.score_hook || 0),
    score_pacing: Number((row as any)?.score_pacing || 0),
    score_emotion: Number((row as any)?.score_emotion || 0),
    score_visual: Number((row as any)?.score_visual || 0),
    score_story: Number((row as any)?.score_story || 0),
    score_jank: Number((row as any)?.score_jank || 0),
    features: ((row as any)?.features || {}) as any,
    flags: ((row as any)?.flags || {}) as Record<string, unknown>,
    config_params: parseConfigParams((row as any)?.config_params || {}),
    preset_name: (row as any)?.preset_name ? String((row as any).preset_name) : null,
    config_created_at: (row as any)?.config_created_at ? new Date((row as any).config_created_at).toISOString() : null
  }))
}

const buildCorrelationMap = (rows: MetricWithConfig[]) => {
  const y = rows.map((row) => row.score_total)
  const scoreStd = stdDev(y)
  const map = NUMERIC_PARAM_KEYS.reduce((acc, key) => {
    const x = rows.map((row) => Number((row.config_params as any)[key] || 0))
    const corr = pearson(x, y)
    const xStd = stdDev(x)
    acc[key] = {
      corr,
      xStd,
      mean: average(x)
    }
    return acc
  }, {} as Record<string, { corr: number; xStd: number; mean: number }>)
  return {
    params: map,
    scoreStd
  }
}

const buildBaseSummary = (rows: MetricWithConfig[]) => {
  const scoreTotal = rows.map((row) => row.score_total)
  const hook = rows.map((row) => row.score_hook)
  const pacing = rows.map((row) => row.score_pacing)
  const emotion = rows.map((row) => row.score_emotion)
  const visual = rows.map((row) => row.score_visual)
  const story = rows.map((row) => row.score_story)
  const jank = rows.map((row) => row.score_jank)

  const failureCounts = {
    low_hook: rows.filter((row) => row.score_hook < 0.5).length,
    low_pacing: rows.filter((row) => row.score_pacing < 0.5).length,
    high_jank: rows.filter((row) => row.score_jank > 0.58).length,
    low_story: rows.filter((row) => row.score_story < 0.52).length
  }

  return {
    avg_score_total: average(scoreTotal),
    avg_hook: average(hook),
    avg_pacing: average(pacing),
    avg_emotion: average(emotion),
    avg_visual: average(visual),
    avg_story: average(story),
    avg_jank: average(jank),
    score_std: stdDev(scoreTotal),
    sample_size: rows.length,
    failure_counts: failureCounts
  }
}

const confidenceFromCorrelations = ({
  sampleSize,
  correlations
}: {
  sampleSize: number
  correlations: number[]
}) => {
  const corrStrength = average(correlations.map((value) => Math.abs(value)))
  const sampleStrength = clamp(Math.log10(sampleSize + 1) / 2.4, 0, 1)
  return Number(clamp(0.35 + 0.65 * corrStrength * sampleStrength, 0.2, 0.95).toFixed(4))
}

const predictDeltaFromChange = ({
  changes,
  correlations,
  scoreStd
}: {
  changes: Record<string, number>
  correlations: Record<string, { corr: number; xStd: number }>
  scoreStd: number
}) => {
  let delta = 0
  for (const [key, change] of Object.entries(changes)) {
    const metric = correlations[key]
    if (!metric || !Number.isFinite(change) || Math.abs(change) < 0.0001) continue
    const normalizedStep = metric.xStd > 0.0001 ? Math.abs(change) / metric.xStd : Math.abs(change)
    const direction = Math.sign(change)
    delta += metric.corr * direction * normalizedStep
  }
  const scaled = delta * (scoreStd > 0.001 ? scoreStd : 4.2) * 0.72
  return Number(clamp(scaled, -18, 18).toFixed(3))
}

const rankSuggestions = (suggestions: ImprovementSuggestion[]) =>
  suggestions
    .slice()
    .sort(
      (a, b) =>
        b.predicted_delta_score - a.predicted_delta_score ||
        b.confidence - a.confidence ||
        a.risk.length - b.risk.length
    )

const maybeRollbackSuggestion = (rows: MetricWithConfig[]): ImprovementSuggestion | null => {
  const byConfig = new Map<string, MetricWithConfig[]>()
  for (const row of rows) {
    const list = byConfig.get(row.config_version_id) || []
    list.push(row)
    byConfig.set(row.config_version_id, list)
  }

  if (byConfig.size < 2) return null

  const ranked = Array.from(byConfig.entries())
    .map(([configId, items]) => ({
      configId,
      count: items.length,
      latest: items
        .map((item) => new Date(item.created_at).getTime())
        .sort((a, b) => b - a)[0],
      avg: average(items.map((item) => item.score_total))
    }))
    .sort((a, b) => b.latest - a.latest)

  const newest = ranked[0]
  const previous = ranked[1]
  if (!newest || !previous || newest.count < 5 || previous.count < 5) return null
  const drop = previous.avg - newest.avg
  if (drop < 2.5) return null

  return {
    title: 'Rollback to previous config',
    why: `Latest config underperforms previous by ${drop.toFixed(2)} points over recent renders.`,
    change: {
      rollback_to_config_version: 0
    },
    predicted_delta_score: Number(clamp(drop * 0.82, 0.8, 14).toFixed(3)),
    confidence: 0.8,
    risk: 'May reduce wins on niche content where the newer config outperformed.'
  }
}

export const analyzeRenderImprovements = async ({
  limit,
  range
}: {
  limit: number
  range?: string
}) => {
  const rows = await fetchMetricsWithConfig({ limit, range })
  if (!rows.length) {
    return {
      summary: {
        avg_score_total: 0,
        avg_hook: 0,
        avg_pacing: 0,
        avg_emotion: 0,
        avg_visual: 0,
        avg_story: 0,
        avg_jank: 0,
        score_std: 0,
        sample_size: 0,
        failure_counts: {
          low_hook: 0,
          low_pacing: 0,
          high_jank: 0,
          low_story: 0
        }
      },
      correlations: {},
      groups: [],
      suggestions: [] as ImprovementSuggestion[]
    }
  }

  const summary = buildBaseSummary(rows)
  const correlation = buildCorrelationMap(rows)

  const groupMap = new Map<string, MetricWithConfig[]>()
  for (const row of rows) {
    const list = groupMap.get(row.config_version_id) || []
    list.push(row)
    groupMap.set(row.config_version_id, list)
  }

  const groups = Array.from(groupMap.entries()).map(([configVersionId, items]) => ({
    config_version_id: configVersionId,
    preset_name: items[0]?.preset_name || null,
    sample_size: items.length,
    avg_score_total: Number(average(items.map((item) => item.score_total)).toFixed(4)),
    avg_hook: Number(average(items.map((item) => item.score_hook)).toFixed(4)),
    avg_pacing: Number(average(items.map((item) => item.score_pacing)).toFixed(4)),
    avg_emotion: Number(average(items.map((item) => item.score_emotion)).toFixed(4)),
    avg_visual: Number(average(items.map((item) => item.score_visual)).toFixed(4)),
    avg_story: Number(average(items.map((item) => item.score_story)).toFixed(4)),
    avg_jank: Number(average(items.map((item) => item.score_jank)).toFixed(4))
  }))

  const correlationsByParam = Object.entries(correlation.params).reduce((acc, [key, value]) => {
    acc[key] = Number(value.corr.toFixed(4))
    return acc
  }, {} as Record<string, number>)

  const suggestions: ImprovementSuggestion[] = []

  if (summary.avg_hook < 0.57) {
    const change = {
      hook_priority_weight: 0.15,
      pattern_interrupt_every_sec: -2
    }
    const predicted = predictDeltaFromChange({
      changes: change,
      correlations: correlation.params as any,
      scoreStd: summary.score_std
    })
    suggestions.push({
      title: 'Increase hook priority',
      why: 'Hook subscore is below target and payoff tends to arrive late.',
      change,
      predicted_delta_score: Number(clamp(predicted || 2.1, 0.6, 15).toFixed(3)),
      confidence: confidenceFromCorrelations({
        sampleSize: summary.sample_size,
        correlations: [correlation.params.hook_priority_weight.corr, correlation.params.pattern_interrupt_every_sec.corr]
      }),
      risk: 'May increase jank if cut aggression is already high.'
    })
  }

  if (summary.avg_pacing < 0.56) {
    const change = {
      pacing_multiplier: 0.12,
      cut_aggression: 6
    }
    const predicted = predictDeltaFromChange({
      changes: change,
      correlations: correlation.params as any,
      scoreStd: summary.score_std
    })
    suggestions.push({
      title: 'Tighten pacing rhythm',
      why: 'Pacing subscore is soft and flat segments are accumulating.',
      change,
      predicted_delta_score: Number(clamp(predicted || 1.8, 0.5, 12).toFixed(3)),
      confidence: confidenceFromCorrelations({
        sampleSize: summary.sample_size,
        correlations: [correlation.params.pacing_multiplier.corr, correlation.params.cut_aggression.corr]
      }),
      risk: 'Can reduce story continuity if coherence guard is too low.'
    })
  }

  if (summary.avg_jank > 0.52) {
    const change = {
      jank_guard: 8,
      cut_aggression: -9
    }
    const predicted = predictDeltaFromChange({
      changes: change,
      correlations: correlation.params as any,
      scoreStd: summary.score_std
    })
    suggestions.push({
      title: 'Reduce jank risk',
      why: 'Jank subscore is elevated with frequent discontinuity signals.',
      change,
      predicted_delta_score: Number(clamp(predicted || 2.4, 0.7, 14).toFixed(3)),
      confidence: confidenceFromCorrelations({
        sampleSize: summary.sample_size,
        correlations: [correlation.params.jank_guard.corr, correlation.params.cut_aggression.corr]
      }),
      risk: 'Retention spikes may soften on very fast-cut clips.'
    })
  }

  if (summary.avg_story < 0.58) {
    const change = {
      story_coherence_guard: 10,
      max_clip_len_ms: 1_200
    }
    const predicted = predictDeltaFromChange({
      changes: change,
      correlations: correlation.params as any,
      scoreStd: summary.score_std
    })
    suggestions.push({
      title: 'Strengthen narrative coherence',
      why: 'Story subscore is low and context segments are likely being removed.',
      change,
      predicted_delta_score: Number(clamp(predicted || 1.6, 0.4, 10).toFixed(3)),
      confidence: confidenceFromCorrelations({
        sampleSize: summary.sample_size,
        correlations: [correlation.params.story_coherence_guard.corr, correlation.params.max_clip_len_ms.corr]
      }),
      risk: 'Longer clips can reduce peak pacing on short-form outputs.'
    })
  }

  const avgFillerSignals = average(
    rows.map((row) => {
      const features = (row.features || {}) as any
      return Number(features?.filler_words_per_min || 0)
    })
  )
  if (avgFillerSignals > 5.5) {
    const change = {
      filler_word_weight: 0.2,
      redundancy_weight: 0.12
    }
    const predicted = predictDeltaFromChange({
      changes: change,
      correlations: correlation.params as any,
      scoreStd: summary.score_std
    })
    suggestions.push({
      title: 'Increase filler suppression',
      why: 'Filler and redundancy indicators remain elevated across recent renders.',
      change,
      predicted_delta_score: Number(clamp(predicted || 1.5, 0.3, 9).toFixed(3)),
      confidence: confidenceFromCorrelations({
        sampleSize: summary.sample_size,
        correlations: [correlation.params.filler_word_weight.corr, correlation.params.redundancy_weight.corr]
      }),
      risk: 'Aggressive filler trimming can create abrupt transitions without strong jank guards.'
    })
  }

  const rollback = maybeRollbackSuggestion(rows)
  if (rollback) suggestions.push(rollback)

  const rankedSuggestions = rankSuggestions(suggestions)
  return {
    summary,
    correlations: correlationsByParam,
    groups: groups.sort((a, b) => b.avg_score_total - a.avg_score_total),
    suggestions: rankedSuggestions.slice(0, 5)
  }
}
