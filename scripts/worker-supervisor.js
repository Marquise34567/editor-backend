const { fork } = require('child_process')
const os = require('os')
const path = require('path')

const parseIntEnv = (value, fallback) => {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback
  return Math.round(parsed)
}

const getCpuCount = () => {
  const cpuInfo = os.cpus()
  if (!Array.isArray(cpuInfo) || cpuInfo.length <= 0) return 1
  return cpuInfo.length
}

const resolveDefaultReplicaCount = (cpuCount) => {
  if (cpuCount <= 1) return 1
  return 2
}

const startWorkerSupervisor = ({
  workerEntryPath,
  label = 'worker'
}) => {
  const resolvedEntryPath = path.resolve(workerEntryPath)
  const cpuCount = getCpuCount()
  const defaultReplicas = resolveDefaultReplicaCount(cpuCount)
  const workerReplicas = parseIntEnv(
    process.env.JOB_WORKER_REPLICAS,
    defaultReplicas
  )
  const restartDelayMs = Math.max(100, parseIntEnv(process.env.JOB_WORKER_RESTART_DELAY_MS, 500))
  const shutdownGraceMs = Math.max(500, parseIntEnv(process.env.JOB_WORKER_SHUTDOWN_GRACE_MS, 5_000))
  let shuttingDown = false
  const childrenBySlot = new Map()

  console.log(
    `[startup] ${label} supervisor config: replicas=${workerReplicas} (cpu=${cpuCount}, default=${defaultReplicas})`
  )
  if (workerReplicas < 2) {
    console.warn(
      `[startup] ${label} redundancy is single-replica; set JOB_WORKER_REPLICAS=2+ for crash failover coverage`
    )
  }

  const spawnWorker = (slot, reason) => {
    if (shuttingDown) return
    const child = fork(resolvedEntryPath, [], {
      stdio: 'inherit',
      env: {
        ...process.env,
        WORKER_SUPERVISED: '1',
        WORKER_SLOT: String(slot),
        WORKER_REPLICA: String(slot + 1),
      },
    })
    childrenBySlot.set(slot, child)
    const replica = slot + 1
    console.log(`[startup] ${label} replica ${replica}/${workerReplicas} started (pid=${child.pid}, reason=${reason})`)
    child.once('exit', (code, signal) => {
      childrenBySlot.delete(slot)
      const exitLabel = signal ? `signal=${signal}` : `code=${code}`
      if (shuttingDown) {
        console.log(`[startup] ${label} replica ${replica} exited during shutdown (${exitLabel})`)
        return
      }
      console.warn(`[startup] ${label} replica ${replica} crashed (${exitLabel}); restarting in ${restartDelayMs}ms`)
      const timer = setTimeout(() => {
        spawnWorker(slot, 'restart')
      }, restartDelayMs)
      if (typeof timer.unref === 'function') {
        timer.unref()
      }
    })
  }

  const shutdown = (signal) => {
    if (shuttingDown) return
    shuttingDown = true
    const runningChildren = Array.from(childrenBySlot.values())
    if (!runningChildren.length) {
      process.exit(0)
      return
    }
    console.log(`[startup] Received ${signal}; stopping ${runningChildren.length} ${label} replica(s)`)
    for (const child of runningChildren) {
      try {
        child.kill('SIGTERM')
      } catch {
        // ignore individual child shutdown errors
      }
    }
    const forceTimer = setTimeout(() => {
      for (const child of Array.from(childrenBySlot.values())) {
        try {
          child.kill('SIGKILL')
        } catch {
          // ignore
        }
      }
      process.exit(0)
    }, shutdownGraceMs)
    if (typeof forceTimer.unref === 'function') {
      forceTimer.unref()
    }
  }

  for (let slot = 0; slot < workerReplicas; slot += 1) {
    spawnWorker(slot, 'startup')
  }

  process.on('SIGINT', () => shutdown('SIGINT'))
  process.on('SIGTERM', () => shutdown('SIGTERM'))
}

module.exports = { startWorkerSupervisor }
