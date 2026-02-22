import readline from 'readline'
import { writeEnvFile } from '../src/lib/env'
import path from 'path'

const rl = readline.createInterface({ input: process.stdin, output: process.stdout })

const ask = (q: string) => new Promise<string>(resolve => rl.question(q, ans => resolve(ans.trim())))

;(async () => {
  console.log('Environment Wizard - provide values (enter to accept default where shown)')
  const vals: Record<string,string> = {}
  vals.DATABASE_URL = await ask('DATABASE_URL (postgresql://user:pass@host:5432/db): ')
  vals.SUPABASE_URL = await ask('SUPABASE_URL (https://xyz.supabase.co): ')
  vals.SUPABASE_ANON_KEY = await ask('SUPABASE_ANON_KEY: ')
  vals.SUPABASE_SERVICE_ROLE_KEY = await ask('SUPABASE_SERVICE_ROLE_KEY: ')
  vals.SUPABASE_BUCKET_INPUT = await ask('SUPABASE_BUCKET_INPUT (uploads): ') || 'uploads'
  vals.SUPABASE_BUCKET_OUTPUT = await ask('SUPABASE_BUCKET_OUTPUT (outputs): ') || 'outputs'
  vals.FRONTEND_URL = await ask('FRONTEND_URL (http://localhost:3000): ') || 'http://localhost:3000'
  vals.STRIPE_SECRET_KEY = await ask('STRIPE_SECRET_KEY (sk_test_...): ')
  vals.STRIPE_WEBHOOK_SECRET = await ask('STRIPE_WEBHOOK_SECRET (whsec_...): ')
  vals.STRIPE_PRICE_ID_MONTHLY = await ask('STRIPE_PRICE_ID_MONTHLY (price_...): ')
  vals.APP_BASE_URL = await ask('APP_BASE_URL (http://localhost:4000): ') || 'http://localhost:4000'
  const ai = await ask('AI_PROVIDER (optional, e.g. openai): ')
  if (ai) {
    vals.AI_PROVIDER = ai
    vals.AI_API_KEY = await ask('AI_API_KEY: ')
    vals.AI_MODEL = await ask('AI_MODEL: ')
  }

  const target = path.join(__dirname, '..', '.env')
  writeEnvFile(target, vals)
  console.log('Wrote env to', target)
  rl.close()
})()
