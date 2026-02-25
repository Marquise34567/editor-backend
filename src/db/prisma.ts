import { PrismaClient } from '@prisma/client'
import { getEnv } from '../lib/env'

const env = process.env.DATABASE_URL || ''

let useStub = false

// In-memory stub implementation (minimal subset of Prisma methods used by app)
class StubDB {
  users = new Map<string, any>()
  jobs = new Map<string, any>()
  usage = new Map<string, any>()
  usageMonthlyStore = new Map<string, any>()
  subscriptions = new Map<string, any>()
  lessons = new Map<string, any>()
  settings = new Map<string, any>()
  exports = new Map<string, any>()
  founderInventoryStore = new Map<string, any>()
  adminAudits = new Map<string, any>()
  analyticsEvents = new Map<string, any>()
  bannedIps = new Map<string, any>()
  weeklyReportSubscriptions = new Map<string, any>()

  async $queryRaw() { return 1 }

  user = {
    findUnique: async ({ where }: any) => {
      const id = where?.id
      const email = where?.email
      if (id) return this.users.get(id) ?? null
      if (email) return Array.from(this.users.values()).find((u:any)=>u.email===email) ?? null
      return null
    },
    findMany: async ({ where, orderBy, take }: any = {}) => {
      let rows = Array.from(this.users.values())
      if (where?.email) rows = rows.filter((user: any) => user.email === where.email)
      if (where?.id) rows = rows.filter((user: any) => user.id === where.id)
      if (orderBy?.createdAt) {
        const dir = String(orderBy.createdAt).toLowerCase() === 'asc' ? 1 : -1
        rows = rows.sort((a: any, b: any) => {
          const aMs = new Date(a?.createdAt || 0).getTime()
          const bMs = new Date(b?.createdAt || 0).getTime()
          return (aMs - bMs) * dir
        })
      }
      if (Number.isFinite(Number(take)) && Number(take) > 0) {
        rows = rows.slice(0, Number(take))
      }
      return rows
    },
    create: async ({ data }: any) => {
      const id = data.id || `stub-${Math.random().toString(36).slice(2,9)}`
      const rec = { id, email: data.email || '', createdAt: new Date(), ...data }
      this.users.set(id, rec)
      return rec
    },
    update: async ({ where, data }: any) => {
      const id = where.id
      const orig = this.users.get(id) || {}
      const updated = { ...orig, ...data }
      this.users.set(id, updated)
      return updated
    },
    updateMany: async ({ where, data }: any) => {
      let count = 0
      for (const [id,u] of this.users.entries()) {
        const matchId = where?.id ? u.id === where.id : true
        const matchCustomer = where?.stripeCustomerId ? u.stripeCustomerId === where.stripeCustomerId : true
        const matchSub = where?.stripeSubscriptionId ? u.stripeSubscriptionId === where.stripeSubscriptionId : true
        if (matchId && matchCustomer && matchSub) {
          this.users.set(id, { ...u, ...data })
          count++
        }
      }
      return { count }
    }
  }

  adminAudit = {
    create: async ({ data }: any) => {
      const id = data.id || `audit-${Math.random().toString(36).slice(2,9)}`
      const rec = { id, ...data, createdAt: data?.createdAt || new Date() }
      this.adminAudits.set(id, rec)
      return rec
    },
    findMany: async ({ where }: any) => {
      return Array.from(this.adminAudits.values()).filter((a:any) => {
        if (!where) return true
        if (where.targetEmail) return a.targetEmail === where.targetEmail
        return true
      })
    }
  }

  bannedIp = {
    findUnique: async ({ where }: any) => {
      const ip = where?.ip
      if (!ip) return null
      return this.bannedIps.get(ip) ?? null
    },
    findMany: async ({ where, orderBy }: any = {}) => {
      let rows = Array.from(this.bannedIps.values())
      if (typeof where?.active === 'boolean') {
        rows = rows.filter((row: any) => Boolean(row.active) === where.active)
      }
      if (where?.ip) {
        rows = rows.filter((row: any) => row.ip === where.ip)
      }
      if (orderBy?.createdAt) {
        const dir = String(orderBy.createdAt).toLowerCase() === 'asc' ? 1 : -1
        rows = rows.sort((a: any, b: any) => {
          const aMs = new Date(a?.createdAt || 0).getTime()
          const bMs = new Date(b?.createdAt || 0).getTime()
          return (aMs - bMs) * dir
        })
      }
      return rows
    },
    upsert: async ({ where, update, create }: any) => {
      const ip = where?.ip || create?.ip
      if (!ip) return null
      const existing = this.bannedIps.get(ip)
      if (existing) {
        const next = { ...existing, ...update, updatedAt: new Date() }
        this.bannedIps.set(ip, next)
        return next
      }
      const created = {
        ...create,
        ip,
        active: create?.active ?? true,
        createdAt: create?.createdAt || new Date(),
        updatedAt: new Date()
      }
      this.bannedIps.set(ip, created)
      return created
    },
    delete: async ({ where }: any) => {
      const ip = where?.ip
      const existing = ip ? this.bannedIps.get(ip) : null
      if (!ip || !existing) throw new Error('record_not_found')
      this.bannedIps.delete(ip)
      return existing
    },
    update: async ({ where, data }: any) => {
      const ip = where?.ip
      const existing = ip ? this.bannedIps.get(ip) : null
      if (!ip || !existing) throw new Error('record_not_found')
      const next = { ...existing, ...data, updatedAt: new Date() }
      this.bannedIps.set(ip, next)
      return next
    }
  }

  weeklyReportSubscription = {
    findUnique: async ({ where }: any) => {
      if (where?.id) return this.weeklyReportSubscriptions.get(where.id) ?? null
      if (where?.email) {
        return (
          Array.from(this.weeklyReportSubscriptions.values()).find(
            (row: any) => String(row.email || '').toLowerCase() === String(where.email).toLowerCase()
          ) ?? null
        )
      }
      return null
    },
    findMany: async ({ where, orderBy }: any = {}) => {
      let rows = Array.from(this.weeklyReportSubscriptions.values())
      if (where?.email) {
        rows = rows.filter((row: any) => String(row.email || '').toLowerCase() === String(where.email).toLowerCase())
      }
      if (typeof where?.enabled === 'boolean') {
        rows = rows.filter((row: any) => Boolean(row.enabled) === where.enabled)
      }
      if (where?.nextSendAt?.lte) {
        const lte = new Date(where.nextSendAt.lte).getTime()
        rows = rows.filter((row: any) => {
          const value = row?.nextSendAt ? new Date(row.nextSendAt).getTime() : Number.POSITIVE_INFINITY
          return value <= lte
        })
      }
      if (orderBy?.updatedAt) {
        const dir = String(orderBy.updatedAt).toLowerCase() === 'asc' ? 1 : -1
        rows = rows.sort((a: any, b: any) => {
          const aMs = new Date(a?.updatedAt || 0).getTime()
          const bMs = new Date(b?.updatedAt || 0).getTime()
          return (aMs - bMs) * dir
        })
      }
      return rows
    },
    upsert: async ({ where, update, create }: any) => {
      const existing =
        (where?.id && this.weeklyReportSubscriptions.get(where.id)) ||
        (where?.email &&
          Array.from(this.weeklyReportSubscriptions.values()).find(
            (row: any) => String(row.email || '').toLowerCase() === String(where.email).toLowerCase()
          ))
      if (existing) {
        const next = { ...existing, ...update, updatedAt: new Date() }
        this.weeklyReportSubscriptions.set(next.id, next)
        return next
      }
      const id = create?.id || `wrs-${Math.random().toString(36).slice(2, 9)}`
      const created = {
        id,
        ...create,
        enabled: create?.enabled ?? true,
        createdAt: create?.createdAt || new Date(),
        updatedAt: new Date()
      }
      this.weeklyReportSubscriptions.set(id, created)
      return created
    },
    update: async ({ where, data }: any) => {
      const existing =
        (where?.id && this.weeklyReportSubscriptions.get(where.id)) ||
        (where?.email &&
          Array.from(this.weeklyReportSubscriptions.values()).find(
            (row: any) => String(row.email || '').toLowerCase() === String(where.email).toLowerCase()
          ))
      if (!existing) throw new Error('record_not_found')
      const next = { ...existing, ...data, updatedAt: new Date() }
      this.weeklyReportSubscriptions.set(next.id, next)
      return next
    }
  }

  siteAnalyticsEvent = {
    create: async ({ data }: any) => {
      const id = data?.id || `evt-${Math.random().toString(36).slice(2, 10)}`
      const rec = {
        id,
        userId: data?.userId,
        sessionId: data?.sessionId ?? null,
        eventName: data?.eventName ?? 'unknown',
        category: data?.category ?? 'interaction',
        pagePath: data?.pagePath ?? null,
        retentionProfile: data?.retentionProfile ?? null,
        targetPlatform: data?.targetPlatform ?? null,
        captionStyle: data?.captionStyle ?? null,
        jobId: data?.jobId ?? null,
        metadata: data?.metadata ?? null,
        createdAt: data?.createdAt ? new Date(data.createdAt) : new Date()
      }
      this.analyticsEvents.set(id, rec)
      return rec
    },
    findMany: async ({ where, orderBy, take, select }: any = {}) => {
      let rows = Array.from(this.analyticsEvents.values())
      if (where?.createdAt?.gte) {
        const gte = new Date(where.createdAt.gte).getTime()
        rows = rows.filter((row: any) => new Date(row.createdAt).getTime() >= gte)
      }
      if (where?.createdAt?.lte) {
        const lte = new Date(where.createdAt.lte).getTime()
        rows = rows.filter((row: any) => new Date(row.createdAt).getTime() <= lte)
      }
      if (where?.userId) {
        rows = rows.filter((row: any) => row.userId === where.userId)
      }
      if (where?.category) {
        rows = rows.filter((row: any) => row.category === where.category)
      }
      if (where?.eventName) {
        rows = rows.filter((row: any) => row.eventName === where.eventName)
      }
      const byCreatedAt = orderBy?.createdAt
      if (byCreatedAt === 'desc') {
        rows.sort((a: any, b: any) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
      } else if (byCreatedAt === 'asc') {
        rows.sort((a: any, b: any) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime())
      }
      if (Number.isFinite(Number(take)) && Number(take) > 0) {
        rows = rows.slice(0, Number(take))
      }
      if (!select) return rows
      return rows.map((row: any) =>
        Object.fromEntries(
          Object.entries(select)
            .filter(([, enabled]) => Boolean(enabled))
            .map(([key]) => [key, (row as any)[key]])
        )
      )
    },
    count: async ({ where }: any = {}) => {
      let rows = Array.from(this.analyticsEvents.values())
      if (where?.createdAt?.gte) {
        const gte = new Date(where.createdAt.gte).getTime()
        rows = rows.filter((row: any) => new Date(row.createdAt).getTime() >= gte)
      }
      if (where?.userId) {
        rows = rows.filter((row: any) => row.userId === where.userId)
      }
      if (where?.category) {
        rows = rows.filter((row: any) => row.category === where.category)
      }
      if (where?.eventName) {
        rows = rows.filter((row: any) => row.eventName === where.eventName)
      }
      return rows.length
    }
  }

  job = {
    create: async ({ data }: any) => {
      const id = data.id || `job-${Math.random().toString(36).slice(2,9)}`
      const rec = { id, priorityLevel: data.priorityLevel ?? 2, ...data, createdAt: new Date(), updatedAt: new Date() }
      this.jobs.set(id, rec)
      return rec
    },
    findMany: async ({ where, orderBy, take }: any = {}) => {
      const userId = where?.userId
      const createdAtGte = where?.createdAt?.gte ? new Date(where.createdAt.gte).getTime() : null
      const statusIn = Array.isArray(where?.status?.in) ? where.status.in.map((s: any) => String(s).toLowerCase()) : null
      let rows = Array.from(this.jobs.values()).filter((job: any) => {
        if (userId && job.userId !== userId) return false
        const createdMs = new Date(job.createdAt || 0).getTime()
        if (createdAtGte !== null && createdMs < createdAtGte) return false
        if (statusIn && statusIn.length > 0 && !statusIn.includes(String(job.status || '').toLowerCase())) return false
        return true
      })
      if (orderBy?.createdAt) {
        const dir = String(orderBy.createdAt).toLowerCase() === 'asc' ? 1 : -1
        rows = rows.sort((a: any, b: any) => {
          const aMs = new Date(a?.createdAt || 0).getTime()
          const bMs = new Date(b?.createdAt || 0).getTime()
          return (aMs - bMs) * dir
        })
      }
      if (orderBy?.updatedAt) {
        const dir = String(orderBy.updatedAt).toLowerCase() === 'asc' ? 1 : -1
        rows = rows.sort((a: any, b: any) => {
          const aMs = new Date(a?.updatedAt || 0).getTime()
          const bMs = new Date(b?.updatedAt || 0).getTime()
          return (aMs - bMs) * dir
        })
      }
      if (Number.isFinite(Number(take)) && Number(take) > 0) {
        rows = rows.slice(0, Number(take))
      }
      return rows
    },
    count: async ({ where }: any = {}) => {
      const userId = where?.userId
      const createdAtGte = where?.createdAt?.gte ? new Date(where.createdAt.gte).getTime() : null
      const createdAtLt = where?.createdAt?.lt ? new Date(where.createdAt.lt).getTime() : null
      const statusIn = Array.isArray(where?.status?.in) ? where.status.in.map((s: any) => String(s).toLowerCase()) : null
      return Array.from(this.jobs.values()).filter((j: any) => {
        if (userId && j.userId !== userId) return false
        const createdAt = new Date(j.createdAt || 0).getTime()
        if (createdAtGte !== null && createdAt < createdAtGte) return false
        if (createdAtLt !== null && createdAt >= createdAtLt) return false
        if (statusIn && statusIn.length > 0 && !statusIn.includes(String(j.status || '').toLowerCase())) return false
        return true
      }).length
    },
    findUnique: async ({ where }: any) => this.jobs.get(where.id) ?? null,
    update: async ({ where, data }: any) => {
      const id = where.id
      const orig = this.jobs.get(id) || {}
      const updated = { ...orig, ...data, updatedAt: new Date() }
      this.jobs.set(id, updated)
      return updated
    }
  }

  usageDaily = {
    findUnique: async ({ where }: any) => {
      const composite = where?.userId_date
      const userId = composite?.userId ?? where?.userId
      const date = composite?.date ?? where?.date
      const key = `${userId}_${date}`
      return this.usage.get(key) ?? null
    },
    upsert: async ({ where, update, create }: any) => {
      const composite = where?.userId_date
      const userId = composite?.userId ?? where?.userId
      const date = composite?.date ?? where?.date
      const key = `${userId}_${date}`
      const existing = this.usage.get(key)
      if (existing) {
        const updated = { ...existing, ...update }
        this.usage.set(key, updated)
        return updated
      }
      const created = { ...create }
      this.usage.set(key, created)
      return created
    }
  }

  usageMonthly = {
    findUnique: async ({ where }: any) => {
      const composite = where?.userId_month
      const userId = composite?.userId ?? where?.userId
      const month = composite?.month ?? where?.month
      const key = `${userId}_${month}`
      return this.usageMonthlyStore.get(key) ?? null
    },
    upsert: async ({ where, update, create }: any) => {
      const composite = where?.userId_month
      const userId = composite?.userId ?? where?.userId
      const month = composite?.month ?? where?.month
      const key = `${userId}_${month}`
      const existing = this.usageMonthlyStore.get(key)
      if (existing) {
        const updated = { ...existing, ...update, updatedAt: new Date() }
        this.usageMonthlyStore.set(key, updated)
        return updated
      }
      const created = { ...create, updatedAt: new Date() }
      this.usageMonthlyStore.set(key, created)
      return created
    }
  }

  subscription = {
    findUnique: async ({ where }: any) => {
      if (where?.userId) {
        return Array.from(this.subscriptions.values()).find((s:any)=>s.userId===where.userId) ?? null
      }
      if (where?.stripeCustomerId) {
        return Array.from(this.subscriptions.values()).find((s:any)=>s.stripeCustomerId===where.stripeCustomerId) ?? null
      }
      if (where?.stripeSubscriptionId) {
        return Array.from(this.subscriptions.values()).find((s:any)=>s.stripeSubscriptionId===where.stripeSubscriptionId) ?? null
      }
      return null
    },
    findMany: async ({ where, orderBy }: any = {}) => {
      let rows = Array.from(this.subscriptions.values())
      if (where?.userId) rows = rows.filter((sub: any) => sub.userId === where.userId)
      if (where?.status) rows = rows.filter((sub: any) => String(sub.status || '').toLowerCase() === String(where.status).toLowerCase())
      if (orderBy?.updatedAt) {
        const dir = String(orderBy.updatedAt).toLowerCase() === 'asc' ? 1 : -1
        rows = rows.sort((a: any, b: any) => {
          const aMs = new Date(a?.updatedAt || 0).getTime()
          const bMs = new Date(b?.updatedAt || 0).getTime()
          return (aMs - bMs) * dir
        })
      }
      return rows
    },
    upsert: async ({ where, update, create }: any) => {
      const existing =
        (where?.userId && Array.from(this.subscriptions.values()).find((s:any)=>s.userId===where.userId)) ||
        (where?.stripeCustomerId && Array.from(this.subscriptions.values()).find((s:any)=>s.stripeCustomerId===where.stripeCustomerId)) ||
        (where?.stripeSubscriptionId && Array.from(this.subscriptions.values()).find((s:any)=>s.stripeSubscriptionId===where.stripeSubscriptionId))
      if (existing) {
        const updated = { ...existing, ...update, updatedAt: new Date() }
        this.subscriptions.set(updated.id, updated)
        return updated
      }
      const id = create?.id || `sub-${Math.random().toString(36).slice(2,9)}`
      const created = { id, ...create, updatedAt: new Date() }
      this.subscriptions.set(id, created)
      return created
    },
    updateMany: async ({ where, data }: any) => {
      let count = 0
      for (const [id,s] of this.subscriptions.entries()) {
        const matchCustomer = where?.stripeCustomerId ? s.stripeCustomerId === where.stripeCustomerId : true
        const matchSub = where?.stripeSubscriptionId ? s.stripeSubscriptionId === where.stripeSubscriptionId : true
        if (matchCustomer && matchSub) {
          this.subscriptions.set(id, { ...s, ...data, updatedAt: new Date() })
          count++
        }
      }
      return { count }
    }
  }

  lesson = {
    upsert: async ({ where, update, create }: any) => {
      const key = where.title
      if (this.lessons.has(key)) return this.lessons.get(key)
      this.lessons.set(key, create)
      return create
    }
  }

  userSettings = {
    findUnique: async ({ where }: any) => this.settings.get(where.userId) ?? null,
    upsert: async ({ where, create, update }: any) => {
      const exists = this.settings.get(where.userId)
      if (exists) {
        const updated = { ...exists, ...update, updatedAt: new Date() }
        this.settings.set(where.userId, updated)
        return updated
      }
      const created = { ...create, createdAt: new Date(), updatedAt: new Date() }
      this.settings.set(where.userId, created)
      return created
    }
  }

  exportCounter = {
    findUnique: async ({ where }: any) => {
      const userId = where?.userId
      return this.exports.get(userId) ?? null
    },
    upsert: async ({ where, update, create }: any) => {
      const userId = where?.userId
      const existing = this.exports.get(userId)
      if (existing) {
        const next = { ...existing, ...update, updatedAt: new Date() }
        this.exports.set(userId, next)
        return next
      }
      const created = { ...create, createdAt: new Date(), updatedAt: new Date() }
      this.exports.set(userId, created)
      return created
    },
    update: async ({ where, data }: any) => {
      const userId = where?.userId
      const existing = this.exports.get(userId) ?? { userId, exportsUsed: 0 }
      const next = { ...existing, ...data, updatedAt: new Date() }
      this.exports.set(userId, next)
      return next
    }
  }

  founderInventory = {
    findUnique: async ({ where }: any) => {
      const id = where?.id
      if (!id) return null
      return this.founderInventoryStore.get(id) ?? null
    },
    upsert: async ({ where, update, create }: any) => {
      const id = where?.id ?? create?.id
      if (!id) return null
      const existing = this.founderInventoryStore.get(id)
      if (existing) {
        const updated = { ...existing, ...update, updatedAt: new Date() }
        this.founderInventoryStore.set(id, updated)
        return updated
      }
      const created = { ...create, updatedAt: new Date() }
      this.founderInventoryStore.set(id, created)
      return created
    },
    updateMany: async ({ where, data }: any) => {
      const id = where?.id
      const max = where?.purchasedCount?.lt
      let count = 0
      if (id) {
        const existing = this.founderInventoryStore.get(id)
        if (existing) {
          if (typeof max === 'number' && (existing.purchasedCount ?? 0) >= max) {
            return { count: 0 }
          }
          const updated = {
            ...existing,
            ...data,
            purchasedCount: (existing.purchasedCount ?? 0) + (data?.purchasedCount?.increment ?? 0),
            updatedAt: new Date()
          }
          this.founderInventoryStore.set(id, updated)
          count = 1
        }
      }
      return { count }
    }
  }
}

let prismaClient: any = null

if (!env) {
  useStub = true
  prismaClient = new StubDB()
  console.warn('DATABASE_URL not set — running in STUB DB mode')
} else {
  const globalAny: any = global
  if (!globalAny.__prisma) {
    try {
      globalAny.__prisma = new PrismaClient()
    } catch (e) {
      console.error('PrismaClient init failed, switching to stub mode', e)
      useStub = true
      prismaClient = new StubDB()
    }
  }
  if (!useStub) prismaClient = globalAny.__prisma
}

export const prisma: any = prismaClient

export const checkDb = async () => {
  if (useStub) return false
  try {
    await prisma.$queryRaw`SELECT 1`
    return true
  } catch (e) {
    // on connection issues, transition to stub mode
    console.error('DB unreachable, switching to STUB mode', e)
    useStub = true
    // replace exported prisma reference with a fresh stub instance
    ;(exports as any).prisma = new (StubDB as any)()
    return false
  }
}

export const isStubDb = () => useStub

export const ensureConnection = async () => {
  const ok = await checkDb()
  return ok
}
