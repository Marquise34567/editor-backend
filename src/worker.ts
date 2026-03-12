import { loadEnv } from './lib/loadEnv'

loadEnv()

const start = async () => {
  await import('./routes/jobs')
  const replicaLabel = String(process.env.WORKER_REPLICA || '').trim()
  const suffix = replicaLabel ? ` replica=${replicaLabel}` : ''
  console.log(`[worker] background job processor started (pid=${process.pid}${suffix})`)

  // Keep process alive; jobs module runs queue recovery + pipeline execution.
  setInterval(() => {
    // no-op heartbeat
  }, 60_000)
}

void start()
