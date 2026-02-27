import fs from 'fs'
import path from 'path'
import { loadEnv } from '../src/lib/loadEnv'

loadEnv()

const parseArgs = () => {
  const values = new Map<string, string>()
  const args = process.argv.slice(2)
  for (const arg of args) {
    if (!arg.startsWith('--')) continue
    const [key, value = ''] = arg.slice(2).split('=')
    if (!key) continue
    values.set(key, value)
  }
  return values
}

const args = parseArgs()
const tunnelName = args.get('name') || String(process.env.CF_TUNNEL_NAME || 'my-gemma-tunnel').trim()
const tunnelId = args.get('id') || String(process.env.CF_TUNNEL_ID || '<TUNNEL-UUID>').trim()
const hostname = args.get('hostname') || String(process.env.CF_GEMMA_HOSTNAME || 'gemma.yourdomain.com').trim()
const localService = args.get('service') || String(process.env.OLLAMA_BASE_URL || 'http://127.0.0.1:11434').trim().replace(/\/+$/, '')
const credentialsPath = args.get('credentials') ||
  String(process.env.CF_TUNNEL_CREDENTIALS || `~/.cloudflared/${tunnelId}.json`).trim()
const configPath = path.resolve(__dirname, '../config/cloudflared-gemma.yml')

const configBody = `tunnel: ${tunnelId}
credentials-file: ${credentialsPath}
protocol: http2
edge-ip-version: "4"
ha-connections: 1

ingress:
  - hostname: ${hostname}
    service: ${localService}
    originRequest:
      httpHostHeader: localhost:11434
  - service: http_status:404
`

fs.mkdirSync(path.dirname(configPath), { recursive: true })
fs.writeFileSync(configPath, configBody, 'utf8')

console.log(`Wrote Cloudflare config: ${configPath}`)
console.log('')
console.log('Next commands:')
console.log('  cloudflared tunnel login')
console.log(`  cloudflared tunnel create ${tunnelName}`)
console.log(`  cloudflared tunnel route dns ${tunnelName} ${hostname}`)
console.log(`  cloudflared tunnel --protocol http2 --config "${configPath}" run ${tunnelId}`)
console.log('')
console.log('Public endpoint test:')
console.log(`  curl https://${hostname}/api/tags`)
console.log('If Zero Trust Access is enabled, send service token headers:')
console.log(`  curl https://${hostname}/api/tags \\`)
console.log('    -H "CF-Access-Client-Id: <id>" \\')
console.log('    -H "CF-Access-Client-Secret: <secret>"')
