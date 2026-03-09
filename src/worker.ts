import { loadEnv } from './lib/loadEnv'

loadEnv()

const start = async () => {
  await import('./routes/jobs')
  console.log('[worker] background job processor started')

  // Keep process alive; jobs module runs queue recovery + pipeline execution.
  setInterval(() => {
    // no-op heartbeat
  }, 60_000)
}

void start()
