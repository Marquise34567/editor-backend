import path from 'path'
import { spawn } from 'child_process'

const DEFAULT_MODEL = 'meta-llama/Meta-Llama-3.1-405B-Instruct'
const DEFAULT_DIR = path.join(process.cwd(), 'models', 'meta-llama-3.1-405b-instruct')

const args = process.argv.slice(2)

const getArgValue = (flag: string) => {
  const index = args.findIndex((item) => item === flag)
  if (index === -1) return ''
  return String(args[index + 1] || '').trim()
}

const hasFlag = (flag: string) => args.includes(flag)

const model = getArgValue('--model') || process.env.HF_LOCAL_MODEL || DEFAULT_MODEL
const localDir = getArgValue('--dir') || process.env.HF_LOCAL_MODEL_DIR || DEFAULT_DIR
const shouldDownload = hasFlag('--download')

const printHeader = () => {
  console.log('Llama 3.1 405B local setup')
  console.log('--------------------------------------------')
  console.log('WARNING: Meta-Llama-3.1-405B-Instruct is extremely large.')
  console.log('- Disk: plan for 800+ GB')
  console.log('- Inference: minimum practical target is ~8x A100 80GB GPUs')
  console.log('- Production recommendation: use hosted Hugging Face Inference API for this project')
  console.log('')
}

const printHostedRecommendation = () => {
  console.log('Hosted mode (recommended)')
  console.log('1) Set HUGGINGFACE_API_KEY=<your_hf_token>')
  console.log('2) Leave HF_LLAMA_PRIMARY_MODEL unset to use default 405B, or set explicitly')
  console.log('3) Optional fallback model: HF_LLAMA_FALLBACK_MODELS=meta-llama/Meta-Llama-3.1-70B-Instruct')
  console.log('')
}

const printLocalRecommendation = () => {
  console.log('Local mode (optional)')
  console.log(`Model repo: ${model}`)
  console.log(`Local dir: ${localDir}`)
  console.log('Use a local inference server endpoint and set:')
  console.log('- LLAMA_PROVIDER=local')
  console.log('- LLAMA_LOCAL_INFERENCE_URL=http://127.0.0.1:8000/v1/completions (example)')
  console.log('')
  console.log('4-bit quantization recommendation (Python bitsandbytes):')
  console.log('- load_in_4bit=True')
  console.log('- bnb_4bit_quant_type=nf4')
  console.log('- bnb_4bit_use_double_quant=True')
  console.log('')
}

const runDownload = async () => {
  console.log('Starting Hugging Face CLI download...')
  const downloadArgs = [
    'download',
    model,
    '--local-dir',
    localDir,
    '--resume-download'
  ]
  const child = spawn('huggingface-cli', downloadArgs, {
    stdio: 'inherit',
    shell: true
  })

  await new Promise<void>((resolve, reject) => {
    child.on('exit', (code) => {
      if (code === 0) return resolve()
      reject(new Error(`huggingface-cli exited with code ${code || 1}`))
    })
    child.on('error', reject)
  })
}

const main = async () => {
  printHeader()
  printHostedRecommendation()
  printLocalRecommendation()

  if (!shouldDownload) {
    console.log('Dry run complete. To start the local download, run:')
    console.log(`npm run llama:setup-local -- --download --model ${model} --dir "${localDir}"`)
    return
  }

  try {
    await runDownload()
    console.log('Model download command completed.')
  } catch (error: any) {
    console.error('Local model download failed:', error?.message || error)
    process.exitCode = 1
  }
}

void main()
