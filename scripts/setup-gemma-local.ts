import fs from 'fs'
import path from 'path'
import { spawnSync } from 'child_process'
import { loadEnv } from '../src/lib/loadEnv'

loadEnv()

const preferredModel = String(process.env.OLLAMA_GEMMA_MODEL || 'gemma3:4b-it').trim()
const fallbackModel = String(process.env.OLLAMA_FALLBACK_MODEL || (preferredModel === 'gemma3:4b-it' ? 'gemma3:4b' : '')).trim()
const ollamaBaseUrl = String(process.env.OLLAMA_BASE_URL || 'http://127.0.0.1:11434').trim().replace(/\/+$/, '')
const autoPull = String(process.env.GEMMA_AUTO_PULL || '1').trim() !== '0'
const cloudflareConfigPath = path.resolve(__dirname, '../config/cloudflared-gemma.yml')
let activeModel = preferredModel

const hasBinary = (name: string) => {
  const command = process.platform === 'win32' ? 'where' : 'which'
  const probe = spawnSync(command, [name], { stdio: 'ignore' })
  return probe.status === 0
}

const runCommand = (cmd: string, args: string[]) => {
  const result = spawnSync(cmd, args, { stdio: 'inherit' })
  return result.status === 0
}

const writeCloudflareTemplate = () => {
  if (fs.existsSync(cloudflareConfigPath)) return
  fs.mkdirSync(path.dirname(cloudflareConfigPath), { recursive: true })
  fs.writeFileSync(
    cloudflareConfigPath,
    `# Replace <TUNNEL-UUID> and your hostname before use.
tunnel: <TUNNEL-UUID>
credentials-file: ~/.cloudflared/<TUNNEL-UUID>.json
protocol: http2
edge-ip-version: "4"
ha-connections: 1
ingress:
  - hostname: gemma.yourdomain.com
    service: ${ollamaBaseUrl}
    originRequest:
      httpHostHeader: localhost:11434
  - service: http_status:404
`,
    'utf8'
  )
}

const testGemma = async () => {
  const response = await fetch(`${ollamaBaseUrl}/api/chat`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      model: activeModel,
      stream: false,
      messages: [
        {
          role: 'user',
          content: 'Reply with valid JSON only: {"status":"ok","model":"<name>","purpose":"healthcheck"}'
        }
      ],
      options: {
        temperature: 0.1,
        num_predict: 90
      }
    })
  })
  const payload: any = await response.json().catch(() => null)
  if (!response.ok) {
    throw new Error(String(payload?.error || `ollama_status_${response.status}`))
  }
  const content = String(payload?.message?.content || payload?.response || '').trim()
  if (!content) throw new Error('empty_gemma_response')
  return content
}

;(async () => {
  console.log(`Gemma local setup starting (model=${preferredModel}, ollama=${ollamaBaseUrl})`)

  if (!hasBinary('ollama')) {
    console.error('Missing Ollama binary. Install from https://ollama.com/download and re-run this script.')
    process.exit(1)
  }

  if (autoPull) {
    const pullCandidates = [preferredModel, fallbackModel].filter(Boolean)
    let pulled = false
    for (const candidate of pullCandidates) {
      console.log(`Pulling model: ${candidate}`)
      if (runCommand('ollama', ['pull', candidate])) {
        activeModel = candidate
        pulled = true
        break
      }
      console.warn(`Pull failed for ${candidate}`)
    }
    if (!pulled) {
      console.error(`Failed to pull any configured Gemma model: ${pullCandidates.join(', ')}`)
      process.exit(1)
    }
  } else {
    console.log('Skipping ollama pull (GEMMA_AUTO_PULL=0).')
  }

  try {
    const content = await testGemma()
    console.log(`Model online: ${activeModel}`)
    console.log('Gemma health-check response:')
    console.log(content)
  } catch (error: any) {
    console.error('Gemma health-check failed:', error?.message || error)
    process.exit(1)
  }

  writeCloudflareTemplate()
  console.log(`Cloudflare config template: ${cloudflareConfigPath}`)
  if (!hasBinary('cloudflared')) {
    console.log('cloudflared is not installed. Install from https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/')
  } else {
    console.log('Suggested Cloudflare commands:')
    console.log('  cloudflared tunnel login')
    console.log('  cloudflared tunnel create my-gemma-tunnel')
    console.log(`  cloudflared tunnel route dns my-gemma-tunnel gemma.yourdomain.com`)
    console.log(`  cloudflared tunnel --protocol http2 --config "${cloudflareConfigPath}" run <TUNNEL-UUID>`)
  }

  console.log('Gemma setup complete.')
})().catch((error) => {
  console.error('setup-gemma-local failed:', error)
  process.exit(1)
})
