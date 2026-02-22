import { createClient } from '@supabase/supabase-js'
import { getEnv } from '../src/lib/env'
import { supabaseAdmin } from '../src/supabaseClient'
import { loadEnv } from '../src/lib/loadEnv'

loadEnv()
const env = getEnv()
const APP_BASE = (env.APP_BASE_URL && env.APP_BASE_URL.length > 0) ? env.APP_BASE_URL.replace(/\/$/, '') : 'http://localhost:4000'

const INPUT_BUCKET = process.env.SUPABASE_BUCKET_INPUT || process.env.SUPABASE_BUCKET_UPLOADS || 'uploads'

const log = (msg: string, extra?: any) => {
  if (extra) console.log(msg, extra)
  else console.log(msg)
}

const fail = (label: string, err: any) => {
  console.error(`FAIL: ${label}`, err)
  process.exitCode = 1
}

const pass = (label: string, extra?: any) => {
  log(`PASS: ${label}`, extra)
}

const getAccessToken = async () => {
  if (!env.SUPABASE_URL || !env.SUPABASE_ANON_KEY || !env.SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error('Missing SUPABASE_URL / SUPABASE_ANON_KEY / SUPABASE_SERVICE_ROLE_KEY')
  }
  const admin = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } })
  const anon = createClient(env.SUPABASE_URL, env.SUPABASE_ANON_KEY, { auth: { persistSession: false } })

  const email = `verify+${Date.now()}@autoeditor.pro`
  const password = `Test${Math.random().toString(36).slice(2)}!`
  const created = await admin.auth.admin.createUser({ email, password, email_confirm: true })
  if (created.error) throw created.error
  const signedIn = await anon.auth.signInWithPassword({ email, password })
  if (signedIn.error || !signedIn.data.session?.access_token) throw signedIn.error || new Error('No session')
  return signedIn.data.session.access_token
}

const call = async (url: string, method: string, token?: string, body?: any) => {
  const res = await fetch(url, {
    method,
    headers: {
      'content-type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    },
    body: body ? JSON.stringify(body) : undefined
  })
  const json = await res.json().catch(() => ({}))
  return { status: res.status, json }
}

;(async () => {
  try {
    const health = await fetch(`${APP_BASE}/api/health`)
    const healthJson = await health.json()
    if (!healthJson.ok) throw new Error('Health not ok')
    pass('health', healthJson)
  } catch (e) {
    fail('health', e)
    return
  }

  let token = ''
  try {
    token = await getAccessToken()
    pass('auth')
  } catch (e) {
    fail('auth', e)
    return
  }

  try {
    const created = await call(`${APP_BASE}/api/jobs`, 'POST', token, { filename: 'verify.mp4' })
    if (created.status !== 200) throw created.json
    const job = created.json.job
    if (!job?.id || !job?.inputPath) throw new Error('Job missing fields')
    pass('create job', job.id)

    const uploadRes = await supabaseAdmin.storage.from(INPUT_BUCKET).upload(job.inputPath, Buffer.from('verify-file'), { contentType: 'video/mp4', upsert: true })
    if (uploadRes.error) throw uploadRes.error
    pass('upload input', job.inputPath)

    const uploaded = await call(`${APP_BASE}/api/jobs/${job.id}/set-uploaded`, 'POST', token, { inputPath: job.inputPath })
    if (uploaded.status !== 200) throw uploaded.json
    pass('set-uploaded')

    const analyzed = await call(`${APP_BASE}/api/jobs/${job.id}/analyze`, 'POST', token)
    if (analyzed.status !== 200) throw analyzed.json
    pass('analyze')

    const processed = await call(`${APP_BASE}/api/jobs/${job.id}/process`, 'POST', token)
    if (processed.status !== 200) throw processed.json
    pass('process')

    const output = await call(`${APP_BASE}/api/jobs/${job.id}/output-url`, 'GET', token)
    if (output.status !== 200 || !output.json?.url) throw output.json
    pass('output-url', output.json.url)

    const download = await fetch(output.json.url)
    if (!download.ok) throw new Error(`Download failed: ${download.status}`)
    pass('download', download.status)
  } catch (e) {
    fail('pipeline', e)
  }

  process.exit(process.exitCode || 0)
})()
