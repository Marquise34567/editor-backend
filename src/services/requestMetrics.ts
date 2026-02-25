export type RequestMetricEntry = {
  t: string
  path: string
  method: string
  statusCode: number
  latencyMs: number
  userId: string | null
  ip: string | null
  userAgent: string | null
}

const REQUEST_HISTORY_LIMIT = 6000
const requestHistory: RequestMetricEntry[] = []

const asMs = (value: unknown) => {
  const ms = new Date(value as any).getTime()
  return Number.isFinite(ms) ? ms : 0
}

export const recordRequestMetric = (entry: RequestMetricEntry) => {
  requestHistory.push(entry)
  if (requestHistory.length > REQUEST_HISTORY_LIMIT) {
    requestHistory.splice(0, requestHistory.length - REQUEST_HISTORY_LIMIT)
  }
}

export const getRequestMetrics = (rangeMs: number) => {
  const floorMs = Date.now() - Math.max(1_000, rangeMs)
  return requestHistory.filter((entry) => asMs(entry.t) >= floorMs)
}

const percentile = (values: number[], p: number) => {
  if (!values.length) return 0
  const sorted = values.slice().sort((a, b) => a - b)
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1))
  return sorted[index]
}

export const summarizeRequestMetrics = ({
  rangeMs,
  latencySpikeMs = 1_500
}: {
  rangeMs: number
  latencySpikeMs?: number
}) => {
  const rows = getRequestMetrics(rangeMs)
  const authFailures = rows.filter((row) => row.statusCode === 401 || row.statusCode === 403)
  const statusCounts = rows.reduce((map, row) => {
    const key = String(row.statusCode)
    map.set(key, (map.get(key) || 0) + 1)
    return map
  }, new Map<string, number>())

  const endpointDurations = rows.reduce((map, row) => {
    const key = `${row.method} ${row.path.split('?')[0]}`
    const existing = map.get(key) || []
    existing.push(Math.max(0, Number(row.latencyMs || 0)))
    map.set(key, existing)
    return map
  }, new Map<string, number[]>())

  const latencySpikes = Array.from(endpointDurations.entries())
    .map(([endpoint, samples]) => {
      const avg = samples.length ? samples.reduce((sum, value) => sum + value, 0) / samples.length : 0
      const p95 = percentile(samples, 95)
      return {
        endpoint,
        samples: samples.length,
        avgMs: Number(avg.toFixed(1)),
        p95Ms: Number(p95.toFixed(1))
      }
    })
    .filter((row) => row.samples >= 5 && (row.p95Ms >= latencySpikeMs || row.avgMs >= latencySpikeMs * 0.7))
    .sort((a, b) => b.p95Ms - a.p95Ms)
    .slice(0, 12)

  const authByIp = authFailures.reduce((map, row) => {
    const ip = row.ip || 'unknown'
    map.set(ip, (map.get(ip) || 0) + 1)
    return map
  }, new Map<string, number>())

  const tokenAbuseSignals = Array.from(authByIp.entries())
    .map(([ip, count]) => ({ ip, count }))
    .filter((row) => row.count >= 6)
    .sort((a, b) => b.count - a.count)
    .slice(0, 20)

  return {
    rows,
    totals: {
      requests: rows.length,
      authFailures: authFailures.length,
      serverErrors: rows.filter((row) => row.statusCode >= 500).length
    },
    statusCounts: Object.fromEntries(statusCounts.entries()),
    latencySpikes,
    tokenAbuseSignals
  }
}
