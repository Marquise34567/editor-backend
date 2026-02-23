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

  async $queryRaw() { return 1 }

  user = {
    findUnique: async ({ where }: any) => {
      const id = where?.id
      const email = where?.email
      if (id) return this.users.get(id) ?? null
      if (email) return Array.from(this.users.values()).find((u:any)=>u.email===email) ?? null
      return null
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
        const matchCustomer = where?.stripeCustomerId ? u.stripeCustomerId === where.stripeCustomerId : true
        const matchSub = where?.stripeSubscriptionId ? u.stripeSubscriptionId === where.stripeSubscriptionId : true
        if (matchCustomer && matchSub) {
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

  job = {
    create: async ({ data }: any) => {
      const id = data.id || `job-${Math.random().toString(36).slice(2,9)}`
      const rec = { id, priorityLevel: data.priorityLevel ?? 2, ...data, createdAt: new Date(), updatedAt: new Date() }
      this.jobs.set(id, rec)
      return rec
    },
    findMany: async ({ where }: any) => {
      const userId = where?.userId
      return Array.from(this.jobs.values()).filter((j:any)=>!userId || j.userId===userId)
    },
    count: async ({ where }: any = {}) => {
      const userId = where?.userId
      const createdAtGte = where?.createdAt?.gte ? new Date(where.createdAt.gte).getTime() : null
      const createdAtLt = where?.createdAt?.lt ? new Date(where.createdAt.lt).getTime() : null
      return Array.from(this.jobs.values()).filter((j: any) => {
        if (userId && j.userId !== userId) return false
        const createdAt = new Date(j.createdAt || 0).getTime()
        if (createdAtGte !== null && createdAt < createdAtGte) return false
        if (createdAtLt !== null && createdAt >= createdAtLt) return false
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
