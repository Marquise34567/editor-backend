import { llamaQuery, type LlamaProvider } from '../services/llamaService'

export type AiProvider = 'local' | 'huggingface' | 'none'

export type AiQueryParams = {
  prompt: string
  maxNewTokens?: number
  temperature?: number
}

export type AiQueryResult = {
  ok: boolean
  provider: AiProvider
  model: string | null
  text: string
  reason?: string
}

const resolveRetentionModelOverride = () => {
  const configured = String(
    process.env.HF_RETENTION_MODEL ||
    process.env.HF_LLAMA_PRIMARY_MODEL ||
    'meta-llama/Meta-Llama-3.1-405B-Instruct'
  ).trim()
  return configured || 'meta-llama/Meta-Llama-3.1-405B-Instruct'
}

const mapProvider = (provider: LlamaProvider | null): AiProvider => {
  if (provider === 'local') return 'local'
  if (provider === 'huggingface') return 'huggingface'
  return 'none'
}

export const queryRetentionModel = async ({
  prompt,
  maxNewTokens = 640,
  temperature = 0.2
}: AiQueryParams): Promise<AiQueryResult> => {
  const cleanPrompt = String(prompt || '').trim()
  if (!cleanPrompt) {
    return { ok: false, provider: 'none', model: null, text: '', reason: 'empty_prompt' }
  }

  const result = await llamaQuery(cleanPrompt, {
    model: resolveRetentionModelOverride(),
    maxTokens: maxNewTokens,
    temperature
  })

  if (!result.ok) {
    return {
      ok: false,
      provider: 'none',
      model: null,
      text: '',
      reason: result.reason || 'llama_query_failed'
    }
  }

  return {
    ok: true,
    provider: mapProvider(result.provider),
    model: result.model,
    text: result.text
  }
}

export const gemmaQuery = async (prompt: string, maxNewTokens = 700) => {
  return queryRetentionModel({
    prompt,
    maxNewTokens,
    temperature: 0.2
  })
}
