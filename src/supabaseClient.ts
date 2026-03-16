import { createClient } from '@supabase/supabase-js'
import { loadEnv } from './lib/loadEnv'

loadEnv()

const SUPABASE_URL = process.env.SUPABASE_URL || ''
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || ''
const hasSupabaseConfig = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY)

const buildMissingError = () => ({
  message: 'supabase_not_configured',
  code: 'SUPABASE_NOT_CONFIGURED'
})

const buildStubResponse = (data: any = null) => ({
  data,
  error: buildMissingError()
})

const createStubQuery = () => {
  const chain: any = {}
  const respond = () => Promise.resolve(buildStubResponse())
  chain.select = () => chain
  chain.eq = () => chain
  chain.in = () => chain
  chain.ilike = () => chain
  chain.order = () => chain
  chain.limit = () => chain
  chain.range = () => chain
  chain.maybeSingle = respond
  chain.single = respond
  chain.insert = () => chain
  chain.update = () => chain
  chain.delete = () => chain
  chain.upsert = () => chain
  chain.then = (resolve: any, reject: any) => respond().then(resolve, reject)
  chain.catch = (reject: any) => respond().catch(reject)
  chain.finally = (cb: any) => respond().finally(cb)
  return chain
}

const createStubClient = () => ({
  auth: {
    getUser: async () => buildStubResponse(),
    admin: {
      updateUserById: async () => buildStubResponse()
    }
  },
  storage: {
    getBucket: async () => buildStubResponse(),
    createBucket: async () => buildStubResponse(),
    from: () => ({
      download: async () => buildStubResponse(),
      upload: async () => ({ error: buildMissingError() }),
      createSignedUrl: async () => buildStubResponse(),
      remove: async () => ({ error: buildMissingError() })
    })
  },
  from: () => createStubQuery()
})

let supabaseAdmin: any = null

if (!hasSupabaseConfig) {
  console.error('Missing Supabase envs SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY')
  supabaseAdmin = createStubClient()
} else {
  try {
    supabaseAdmin = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false }
    })
  } catch (error) {
    console.error('Failed to initialize Supabase client, falling back to stub client', error)
    supabaseAdmin = createStubClient()
  }
}

export { supabaseAdmin }
export default supabaseAdmin
