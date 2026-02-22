import fs from 'fs'
import path from 'path'

const parseLine = (line: string) => {
  const idx = line.indexOf('=')
  if (idx === -1) return null
  const key = line.slice(0, idx).trim()
  let value = line.slice(idx + 1).trim()
  if (!key) return null
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    value = value.slice(1, -1)
  }
  return { key, value }
}

export const loadEnv = () => {
  try {
    const envPath = path.join(__dirname, '..', '..', '.env')
    if (!fs.existsSync(envPath)) return
    const content = fs.readFileSync(envPath, 'utf8')
    const lines = content.split(/\r?\n/)
    for (const raw of lines) {
      const line = raw.trim()
      if (!line || line.startsWith('#')) continue
      const parsed = parseLine(line)
      if (!parsed) continue
      if (process.env[parsed.key] === undefined || process.env[parsed.key] === '') {
        process.env[parsed.key] = parsed.value
      }
    }
  } catch (e) {
    // ignore env loading errors
  }
}

