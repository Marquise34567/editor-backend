import express from 'express'
import fs from 'fs'
import os from 'os'
import path from 'path'
import crypto from 'crypto'
import { spawn } from 'child_process'
import { supabaseAdmin } from '../supabaseClient'
import { r2 } from '../lib/r2'
import { ListObjectsV2Command } from '@aws-sdk/client-s3'
import { FFMPEG_PATH, formatCommand } from '../lib/ffmpeg'

const router = express.Router()
const FFMPEG_LOG_LIMIT = 20000

const logAwsError = (label: string, err: any) => {
  console.error(label, {
    name: err?.name,
    message: err?.message,
    code: err?.code || err?.Code,
    stack: err?.stack,
    metadata: err?.$metadata,
    raw: err
  })
}

const runProcess = (binaryPath: string, args: string[]) => {
  return new Promise<{ exitCode: number | null; stdout: string; stderr: string }>((resolve, reject) => {
    const proc = spawn(binaryPath, args, { stdio: 'pipe' })
    let stdout = ''
    let stderr = ''
    proc.stdout.on('data', (data) => {
      if (stdout.length < FFMPEG_LOG_LIMIT) stdout += data.toString()
    })
    proc.stderr.on('data', (data) => {
      if (stderr.length < FFMPEG_LOG_LIMIT) stderr += data.toString()
    })
    proc.on('error', reject)
    proc.on('close', (exitCode) => resolve({ exitCode, stdout, stderr }))
  })
}

// Temporary debug endpoint to validate an incoming Supabase access token.
// Returns { ok: true, user: { id, email } } when valid, or { error, message } when invalid.
// IMPORTANT: This endpoint does NOT log or return the token itself.
router.post('/validate-token', async (req: any, res) => {
  try {
    const auth = req.headers.authorization
    if (!auth || !auth.startsWith('Bearer ')) return res.status(400).json({ error: 'missing_token', message: 'Authorization header required' })
    const token = auth.split(' ')[1]
    const { data, error } = await supabaseAdmin.auth.getUser(token)
    if (error || !data?.user) {
      return res.status(401).json({ error: 'invalid_token', message: error?.message || 'Invalid token' })
    }
    return res.json({ ok: true, user: { id: data.user.id, email: data.user.email ?? null } })
  } catch (err: any) {
    console.error('debug.validate-token error', err?.stack || err)
    return res.status(500).json({ error: 'server_error', message: err?.message || String(err) })
  }
})

// GET /api/debug/r2-health
router.get('/r2-health', async (req: any, res) => {
  try {
    if (!r2.isConfigured) {
      return res.status(503).json({ ok: false, error: 'R2_NOT_CONFIGURED', missing: r2.missingEnvVars || [] })
    }
    const bucket = process.env.R2_BUCKET || r2.bucket
    if (!bucket) return res.status(500).json({ ok: false, error: 'R2_BUCKET_NOT_CONFIGURED' })
    try {
      await r2.client.send(new ListObjectsV2Command({ Bucket: bucket, MaxKeys: 1 }))
      return res.json({ ok: true })
    } catch (e: any) {
      logAwsError('r2-health list objects failed', e)
      return res.status(500).json({ ok: false, error: String(e?.message || e) })
    }
  } catch (err: any) {
    logAwsError('r2-health error', err)
    res.status(500).json({ ok: false, error: String(err?.message || err) })
  }
})

// GET /api/debug/ffmpeg
router.get('/ffmpeg', async (req: any, res) => {
  const testDir = path.join(os.tmpdir(), 'auto-editor-pro', 'ffmpeg-debug')
  fs.mkdirSync(testDir, { recursive: true })
  const testFile = path.join(testDir, `ffmpeg-test-${Date.now()}-${crypto.randomUUID().slice(0, 8)}.mp4`)
  const args = ['-y', '-f', 'lavfi', '-i', 'testsrc=size=320x240:rate=30', '-t', '1', testFile]
  const command = formatCommand(FFMPEG_PATH, args)
  try {
    const result = await runProcess(FFMPEG_PATH, args)
    if (result.exitCode !== 0) {
      return res.status(500).json({
        ok: false,
        command,
        exitCode: result.exitCode,
        stdout: result.stdout,
        stderr: result.stderr
      })
    }
    if (!fs.existsSync(testFile)) {
      return res.status(500).json({
        ok: false,
        command,
        error: 'test_file_missing',
        stderr: result.stderr
      })
    }
    const stats = fs.statSync(testFile)
    if (stats.size <= 0) {
      return res.status(500).json({
        ok: false,
        command,
        error: 'test_file_empty',
        sizeBytes: stats.size,
        stderr: result.stderr
      })
    }
    return res.json({
      ok: true,
      path: testFile,
      sizeBytes: stats.size
    })
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      command,
      error: err?.message || String(err)
    })
  }
})

export default router
