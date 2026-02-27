import fetch from 'node-fetch'
import { HfInference } from '@huggingface/inference'

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

const DEFAULT_PRIMARY_MODEL = 'meta-llama/Meta-Llama-3.1-405B-Instruct'
const DEFAULT_FALLBACK_MODELS = ['meta-llama/Meta-Llama-3.1-70B-Instruct']
const DEFAULT_MAX_TOKENS = 512

const RETRYABLE_STATUS_CODES = new Set([408, 409, 425, 429, 500, 502, 503, 504])

export const RUTHLESS_RETENTION_PROMPT = `You are AutoEditor's ruthless retention-maximizing AI brain.
Mission: maximize average retention percent and full-watch completion, not runtime.
In 2026 ranking behavior, shorter videos with much higher retention usually beat longer videos with weak retention.
Rules:
- Remove/compress segments with predicted drop-off above 15-20%.
- Prioritize smooth retention with no deep valleys.
- Keep strongest opener in first 8-15 seconds and sustain micro-progress every 15-30 seconds.`

export type LlamaProvider = 'huggingface' | 'local'

export type LlamaQueryResult = {
  ok: boolean
  text: string
  model: string | null
  provider: LlamaProvider | null
  attempts: number
  reason?: string
  statusCode?: number
}

export type LlamaQueryOptions = {
  maxTokens?: number
  temperature?: number
  topP?: number
  model?: string
  prependRuthlessPrompt?: boolean
  useFallbackModels?: boolean
}

type ProviderRequestResult = {
  ok: boolean
  text: string
  statusCode?: number
  reason?: string
}

const parseList = (value: string) =>
  String(value || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)

const getHfToken = () => String(process.env.HUGGINGFACE_API_KEY || process.env.HF_API_TOKEN || '').trim()
const getLocalEndpoint = () => {
  const explicit = String(process.env.LLAMA_LOCAL_INFERENCE_URL || process.env.LOCAL_LLM_ENDPOINT || '').trim()
  if (explicit) return explicit
  const ollamaBaseUrl = String(process.env.OLLAMA_BASE_URL || '').trim().replace(/\/+$/, '')
  return ollamaBaseUrl ? `${ollamaBaseUrl}/api/chat` : ''
}

const resolvePrimaryModel = () =>
  String(process.env.HF_LLAMA_PRIMARY_MODEL || process.env.HF_RETENTION_MODEL || DEFAULT_PRIMARY_MODEL).trim() ||
  DEFAULT_PRIMARY_MODEL

const resolveFallbackModels = () => {
  const ollamaTagFallback = String(process.env.OLLAMA_FALLBACK_MODEL || '').trim()
  const hfRetentionFallback = String(process.env.HF_RETENTION_MODEL || '').trim()
  const configured = parseList(process.env.HF_LLAMA_FALLBACK_MODELS || process.env.HF_RETENTION_FALLBACK_MODELS || '')
  const combined = [ollamaTagFallback, hfRetentionFallback, ...configured, ...DEFAULT_FALLBACK_MODELS]
  return Array.from(new Set(combined.filter(Boolean)))
}

const buildModelList = (overrideModel?: string, includeFallback = true) => {
  const selected = String(overrideModel || '').trim()
  const primary = selected || resolvePrimaryModel()
  const fallback = includeFallback ? resolveFallbackModels() : []
  return [primary, ...fallback.filter((model) => model !== primary)]
}

const buildPrompt = (prompt: string, prependRuthlessPrompt = true) => {
  const raw = String(prompt || '').trim()
  if (!prependRuthlessPrompt) return raw
  if (raw.includes("You are AutoEditor's ruthless retention-maximizing AI brain")) return raw
  return `${RUTHLESS_RETENTION_PROMPT}\n${raw}`
}

const delay = async (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

const coerceGeneratedText = (payload: any): string => {
  if (!payload) return ''
  if (typeof payload === 'string') return payload
  if (Array.isArray(payload)) {
    return payload
      .map((entry) => {
        if (typeof entry === 'string') return entry
        if (typeof entry?.generated_text === 'string') return entry.generated_text
        if (typeof entry?.text === 'string') return entry.text
        if (typeof entry?.summary_text === 'string') return entry.summary_text
        if (typeof entry?.content === 'string') return entry.content
        if (typeof entry?.message?.content === 'string') return entry.message.content
        return ''
      })
      .filter(Boolean)
      .join('\n')
      .trim()
  }
  if (typeof payload.generated_text === 'string') return payload.generated_text.trim()
  if (typeof payload.text === 'string') return payload.text.trim()
  if (typeof payload.summary_text === 'string') return payload.summary_text.trim()
  if (typeof payload.content === 'string') return payload.content.trim()
  if (Array.isArray(payload?.choices) && payload.choices[0]) {
    const choice = payload.choices[0]
    if (typeof choice?.text === 'string') return choice.text.trim()
    if (typeof choice?.message?.content === 'string') return choice.message.content.trim()
  }
  return ''
}

const parseStatusCode = (message: string): number | null => {
  const match = String(message || '').match(/\b(4\d{2}|5\d{2})\b/)
  if (!match) return null
  const status = Number(match[1])
  return Number.isFinite(status) ? status : null
}

const isRetryable = (result: ProviderRequestResult) => {
  if (result.statusCode && RETRYABLE_STATUS_CODES.has(result.statusCode)) return true
  const reason = String(result.reason || '').toLowerCase()
  return (
    reason.includes('rate') ||
    reason.includes('timeout') ||
    reason.includes('temporar') ||
    reason.includes('overload') ||
    reason.includes('busy') ||
    reason.includes('currently loading')
  )
}

const requestHf = async ({
  token,
  model,
  prompt,
  maxTokens,
  temperature,
  topP
}: {
  token: string
  model: string
  prompt: string
  maxTokens: number
  temperature: number
  topP: number
}): Promise<ProviderRequestResult> => {
  try {
    const client = new HfInference(token)
    const payload: any = await client.textGeneration({
      model,
      inputs: prompt,
      parameters: {
        max_new_tokens: maxTokens,
        temperature,
        top_p: topP,
        return_full_text: false
      }
    })
    const text = coerceGeneratedText(payload)
    if (!text) {
      return { ok: false, text: '', reason: 'hf_empty_response' }
    }
    return { ok: true, text }
  } catch (error: any) {
    const message = String(error?.message || error || '').trim()
    const statusCode = parseStatusCode(message) || undefined
    return {
      ok: false,
      text: '',
      statusCode,
      reason: message || 'hf_exception'
    }
  }
}

const requestLocal = async ({
  endpoint,
  model,
  prompt,
  maxTokens,
  temperature,
  topP
}: {
  endpoint: string
  model: string
  prompt: string
  maxTokens: number
  temperature: number
  topP: number
}): Promise<ProviderRequestResult> => {
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        ...(process.env.OLLAMA_AUTH_HEADER
          ? { authorization: process.env.OLLAMA_AUTH_HEADER }
          : process.env.LLAMA_LOCAL_API_KEY
            ? { authorization: `Bearer ${process.env.LLAMA_LOCAL_API_KEY}` }
            : {}),
        ...(process.env.CF_ACCESS_CLIENT_ID ? { 'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID } : {}),
        ...(process.env.CF_ACCESS_CLIENT_SECRET ? { 'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET } : {})
      },
      body: JSON.stringify({
        model,
        prompt,
        inputs: prompt,
        messages: [{ role: 'user', content: prompt }],
        stream: false,
        max_tokens: maxTokens,
        max_new_tokens: maxTokens,
        temperature,
        top_p: topP
      })
    })

    const payload: any = await response.json().catch(() => null)
    if (!response.ok) {
      return {
        ok: false,
        text: '',
        statusCode: response.status,
        reason: String(payload?.error || payload?.message || `local_status_${response.status}`)
      }
    }

    const text = coerceGeneratedText(payload)
    if (!text) {
      return { ok: false, text: '', reason: 'local_empty_response', statusCode: response.status }
    }

    return { ok: true, text, statusCode: response.status }
  } catch (error: any) {
    return { ok: false, text: '', reason: String(error?.message || 'local_exception') }
  }
}

const buildProviderOrder = (): LlamaProvider[] => {
  const requested = String(process.env.LLAMA_PROVIDER || '').trim().toLowerCase()
  const hasToken = Boolean(getHfToken())
  const hasLocal = Boolean(getLocalEndpoint())

  if (requested === 'local') {
    return hasLocal ? ['local'] : (hasToken ? ['huggingface'] : [])
  }
  if (requested === 'huggingface' || requested === 'hf') {
    const order: LlamaProvider[] = []
    if (hasToken) order.push('huggingface')
    if (hasLocal) order.push('local')
    return order
  }

  const autoOrder: LlamaProvider[] = []
  if (hasLocal) autoOrder.push('local')
  if (hasToken) autoOrder.push('huggingface')
  return autoOrder
}

export const isLlamaConfigured = () => buildProviderOrder().length > 0

export const getLlamaModelConfig = () => ({
  primary: resolvePrimaryModel(),
  fallback: resolveFallbackModels()
})

export const llamaQuery = async (prompt: string, options: LlamaQueryOptions = {}): Promise<LlamaQueryResult> => {
  const providers = buildProviderOrder()
  if (providers.length === 0) {
    return {
      ok: false,
      text: '',
      model: null,
      provider: null,
      attempts: 0,
      reason: 'llama_not_configured'
    }
  }

  const maxTokens = clamp(Math.round(Number(options.maxTokens || DEFAULT_MAX_TOKENS)), 64, 1200)
  const temperature = clamp(Number(options.temperature ?? 0.2), 0, 1.2)
  const topP = clamp(Number(options.topP ?? 0.9), 0.1, 1)
  const promptText = buildPrompt(prompt, options.prependRuthlessPrompt !== false)
  const includeFallback = options.useFallbackModels !== false
  const models = buildModelList(options.model, includeFallback)
  const maxRetries = clamp(Math.round(Number(process.env.LLAMA_MAX_RETRIES || 3)), 1, 8)
  const baseBackoffMs = clamp(Math.round(Number(process.env.LLAMA_BACKOFF_BASE_MS || 1200)), 200, 15_000)

  let attempts = 0
  let lastReason = 'llama_no_response'
  let lastStatusCode: number | undefined

  for (const provider of providers) {
    const token = getHfToken()
    const localEndpoint = getLocalEndpoint()

    for (const model of models) {
      for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
        attempts += 1
        const result =
          provider === 'huggingface'
            ? await requestHf({
                token,
                model,
                prompt: promptText,
                maxTokens,
                temperature,
                topP
              })
            : await requestLocal({
                endpoint: localEndpoint,
                model,
                prompt: promptText,
                maxTokens,
                temperature,
                topP
              })

        if (result.ok) {
          return {
            ok: true,
            text: result.text,
            model,
            provider,
            attempts
          }
        }

        lastReason = result.reason || 'llama_request_failed'
        lastStatusCode = result.statusCode
        if (!isRetryable(result) || attempt >= maxRetries) break
        const jitter = Math.floor(Math.random() * 250)
        const delayMs = baseBackoffMs * 2 ** (attempt - 1) + jitter
        await delay(delayMs)
      }
    }
  }

  return {
    ok: false,
    text: '',
    model: null,
    provider: null,
    attempts,
    reason: lastReason,
    statusCode: lastStatusCode
  }
}

export const extractJsonFromText = (text: string) => {
  const raw = String(text || '').trim()
  if (!raw) return null
  const candidates = [raw]
  const objectStart = raw.indexOf('{')
  const objectEnd = raw.lastIndexOf('}')
  if (objectStart >= 0 && objectEnd > objectStart) {
    candidates.push(raw.slice(objectStart, objectEnd + 1))
  }
  const arrayStart = raw.indexOf('[')
  const arrayEnd = raw.lastIndexOf(']')
  if (arrayStart >= 0 && arrayEnd > arrayStart) {
    candidates.push(raw.slice(arrayStart, arrayEnd + 1))
  }
  for (const candidate of candidates) {
    try {
      return JSON.parse(candidate)
    } catch {
      // noop
    }
  }
  return null
}

export const llamaBatchQuery = async (prompts: string[], options: LlamaQueryOptions = {}) => {
  const list = Array.isArray(prompts) ? prompts.filter((prompt) => String(prompt || '').trim().length > 0) : []
  if (!list.length) return []

  const batchSize = clamp(Math.round(Number(process.env.HF_LLAMA_BATCH_SIZE || 2)), 1, 4)
  const results: LlamaQueryResult[] = []
  for (let index = 0; index < list.length; index += batchSize) {
    const chunk = list.slice(index, index + batchSize)
    const rows = await Promise.all(chunk.map((prompt) => llamaQuery(prompt, options)))
    results.push(...rows)
  }
  return results
}
