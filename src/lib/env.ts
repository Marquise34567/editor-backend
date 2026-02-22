import { z } from 'zod'
import fs from 'fs'
import path from 'path'

const envSchema = z.object({
  DATABASE_URL: z.string().url(),
  SUPABASE_URL: z.string().url(),
  SUPABASE_ANON_KEY: z.string().min(10),
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(10),
  SUPABASE_BUCKET_INPUT: z.string().min(1),
  SUPABASE_BUCKET_OUTPUT: z.string().min(1),
  SUPABASE_BUCKET_UPLOADS: z.string().min(1).optional(),
  SUPABASE_BUCKET_OUTPUTS: z.string().min(1).optional(),
  FRONTEND_URL: z.string().url(),
  STRIPE_SECRET_KEY: z.string().min(10),
  STRIPE_WEBHOOK_SECRET: z.string().min(10),
  STRIPE_PRICE_ID_MONTHLY: z.string().min(1).optional(),
  STRIPE_PRICE_ID_STARTER: z.string().min(1),
  STRIPE_PRICE_ID_CREATOR: z.string().min(1),
  STRIPE_PRICE_ID_STUDIO: z.string().min(1),
  STRIPE_MODE: z.string().optional(),
  STRIPE_PRICE_ID_STARTER_ANNUAL: z.string().min(1).optional(),
  STRIPE_PRICE_ID_CREATOR_ANNUAL: z.string().min(1).optional(),
  STRIPE_PRICE_ID_STUDIO_ANNUAL: z.string().min(1).optional(),
  STRIPE_PRICE_ID_TRIAL: z.string().min(1).optional(),
  STRIPE_TEST_SECRET_KEY: z.string().min(10).optional(),
  STRIPE_TEST_WEBHOOK_SECRET: z.string().min(10).optional(),
  STRIPE_TEST_PRICE_ID_STARTER: z.string().min(1).optional(),
  STRIPE_TEST_PRICE_ID_CREATOR: z.string().min(1).optional(),
  STRIPE_TEST_PRICE_ID_STUDIO: z.string().min(1).optional(),
  STRIPE_TEST_PRICE_ID_STARTER_ANNUAL: z.string().min(1).optional(),
  STRIPE_TEST_PRICE_ID_CREATOR_ANNUAL: z.string().min(1).optional(),
  STRIPE_TEST_PRICE_ID_STUDIO_ANNUAL: z.string().min(1).optional(),
  STRIPE_TEST_PRICE_ID_TRIAL: z.string().min(1).optional(),
  APP_URL: z.string().url(),
  APP_BASE_URL: z.string().url().optional(),
  AI_PROVIDER: z.string().optional(),
  AI_API_KEY: z.string().optional(),
  AI_MODEL: z.string().optional(),
})

export const getEnv = () => {
  const raw = {
    DATABASE_URL: process.env.DATABASE_URL ?? '',
    SUPABASE_URL: process.env.SUPABASE_URL ?? '',
    SUPABASE_ANON_KEY: process.env.SUPABASE_ANON_KEY ?? '',
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY ?? '',
    SUPABASE_BUCKET_INPUT: process.env.SUPABASE_BUCKET_INPUT ?? '',
    SUPABASE_BUCKET_OUTPUT: process.env.SUPABASE_BUCKET_OUTPUT ?? '',
    SUPABASE_BUCKET_UPLOADS: process.env.SUPABASE_BUCKET_UPLOADS ?? '',
    SUPABASE_BUCKET_OUTPUTS: process.env.SUPABASE_BUCKET_OUTPUTS ?? '',
    FRONTEND_URL: process.env.FRONTEND_URL ?? '',
    STRIPE_SECRET_KEY: process.env.STRIPE_SECRET_KEY ?? '',
    STRIPE_WEBHOOK_SECRET: process.env.STRIPE_WEBHOOK_SECRET ?? '',
    STRIPE_PRICE_ID_MONTHLY: process.env.STRIPE_PRICE_ID_MONTHLY ?? '',
    STRIPE_PRICE_ID_STARTER: process.env.STRIPE_PRICE_ID_STARTER ?? '',
    STRIPE_PRICE_ID_CREATOR: process.env.STRIPE_PRICE_ID_CREATOR ?? '',
    STRIPE_PRICE_ID_STUDIO: process.env.STRIPE_PRICE_ID_STUDIO ?? '',
    STRIPE_MODE: process.env.STRIPE_MODE ?? '',
    STRIPE_PRICE_ID_STARTER_ANNUAL: process.env.STRIPE_PRICE_ID_STARTER_ANNUAL ?? '',
    STRIPE_PRICE_ID_CREATOR_ANNUAL: process.env.STRIPE_PRICE_ID_CREATOR_ANNUAL ?? '',
    STRIPE_PRICE_ID_STUDIO_ANNUAL: process.env.STRIPE_PRICE_ID_STUDIO_ANNUAL ?? '',
    STRIPE_PRICE_ID_TRIAL: process.env.STRIPE_PRICE_ID_TRIAL ?? '',
    STRIPE_TEST_SECRET_KEY: process.env.STRIPE_TEST_SECRET_KEY ?? '',
    STRIPE_TEST_WEBHOOK_SECRET: process.env.STRIPE_TEST_WEBHOOK_SECRET ?? '',
    STRIPE_TEST_PRICE_ID_STARTER: process.env.STRIPE_TEST_PRICE_ID_STARTER ?? '',
    STRIPE_TEST_PRICE_ID_CREATOR: process.env.STRIPE_TEST_PRICE_ID_CREATOR ?? '',
    STRIPE_TEST_PRICE_ID_STUDIO: process.env.STRIPE_TEST_PRICE_ID_STUDIO ?? '',
    STRIPE_TEST_PRICE_ID_STARTER_ANNUAL: process.env.STRIPE_TEST_PRICE_ID_STARTER_ANNUAL ?? '',
    STRIPE_TEST_PRICE_ID_CREATOR_ANNUAL: process.env.STRIPE_TEST_PRICE_ID_CREATOR_ANNUAL ?? '',
    STRIPE_TEST_PRICE_ID_STUDIO_ANNUAL: process.env.STRIPE_TEST_PRICE_ID_STUDIO_ANNUAL ?? '',
    STRIPE_TEST_PRICE_ID_TRIAL: process.env.STRIPE_TEST_PRICE_ID_TRIAL ?? '',
    APP_URL: process.env.APP_URL ?? '',
    APP_BASE_URL: process.env.APP_BASE_URL ?? '',
    AI_PROVIDER: process.env.AI_PROVIDER,
    AI_API_KEY: process.env.AI_API_KEY,
    AI_MODEL: process.env.AI_MODEL,
  }
  const parsed = envSchema.safeParse(raw)
  if (parsed.success) return parsed.data
  // return raw (possibly invalid) to allow running in dev / stub modes
  return raw as any
}

export const writeEnvFile = (targetPath: string, values: Record<string,string>) => {
  const content = Object.entries(values)
    .map(([k,v]) => `${k}=${v}`)
    .join('\n')
  fs.mkdirSync(path.dirname(targetPath), { recursive: true })
  fs.writeFileSync(targetPath, content, { encoding: 'utf8' })
}

export type Env = z.infer<typeof envSchema>
