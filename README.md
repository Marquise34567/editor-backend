# AutoEditor Pro - Backend (Express + Prisma)

This folder contains a TypeScript Express backend implementing:
- Prisma schema (Postgres)
- Supabase server client for Storage + Auth verification
- Stripe integration (checkout, portal, webhooks)
- Job pipeline endpoints (create/upload/analyze/process/output)

Prereqs:
- Node 18+ / npm
- Postgres instance
- Supabase project (for storage + auth)
- Stripe account

Local setup:

1. Copy `.env.example` to `.env` and fill values.

2. Install dependencies:

```bash
cd backend
npm install
```

3. Generate Prisma client and run migrations:

```bash
npm run prisma:generate
npm run prisma:migrate
```

4. Start dev server:

```bash
npm run dev
```

Notes:
- The backend expects Supabase `SERVICE_ROLE_KEY` to validate JWTs server-side and to access Storage.
- Stripe webhooks endpoint is `/webhooks/stripe` and expects the raw body to validate signature. Set `STRIPE_WEBHOOK_SECRET` accordingly.

## Llama 3.1 Retention Reasoning

AutoEditor now uses Llama 3.1 as the core retention model path.

Hosted Hugging Face (recommended):

1. Set `HUGGINGFACE_API_KEY` (or `HF_API_TOKEN`) in `.env`.
2. Primary model defaults to `meta-llama/Meta-Llama-3.1-405B-Instruct`.
3. Fallback defaults to `meta-llama/Meta-Llama-3.1-70B-Instruct`.
4. Optional overrides:
   - `HF_LLAMA_PRIMARY_MODEL`
   - `HF_LLAMA_FALLBACK_MODELS`
   - `LLAMA_MAX_RETRIES`
   - `LLAMA_BACKOFF_BASE_MS`

Local setup (optional):

```bash
npm run llama:setup-local
# and when ready:
npm run llama:setup-local -- --download
```

Optional local 4-bit endpoint:

```bash
python scripts/llama_local_4bit_server.py
# then set LLAMA_PROVIDER=local and LLAMA_LOCAL_INFERENCE_URL=http://127.0.0.1:8000/v1/completions
```

Warning for local 405B:
- Expect 800+ GB model footprint.
- Practical inference target is approximately 8x A100 80GB GPUs.
- For this project, hosted HF inference is the default production recommendation.

R2 (Cloudflare) configuration:

- Add the following environment variables to use Cloudflare R2 for storage:

	- `R2_ENDPOINT` - your R2 endpoint (e.g. https://<accountid>.r2.cloudflarestorage.com)
	- `R2_ACCESS_KEY_ID`
	- `R2_SECRET_ACCESS_KEY`
	- `R2_BUCKET`
	- `R2_PUBLIC_BASE_URL` (optional) — public base URL for reads if you use a custom domain

If these variables are not set, the API now falls back to Supabase storage for upload/download paths (R2 multipart endpoints return `R2_NOT_CONFIGURED`).

R2 bucket CORS policy (required for browser multipart uploads):

Apply this JSON in Cloudflare R2 bucket CORS settings:

```json
[
  {
    "AllowedOrigins": [
      "https://www.autoeditor.app",
      "https://autoeditor.app",
      "http://localhost:8080"
    ],
    "AllowedMethods": ["PUT", "GET", "HEAD", "POST"],
    "AllowedHeaders": ["*"],
    "ExposeHeaders": ["ETag"],
    "MaxAgeSeconds": 3600
  }
]
```

Deployment:
- Use Railway for the server. Set environment variables per `.env.example` in Railway.
- Run API and worker as separate services in production:
- API service command: `npm run start:api` (or set `FORCE_API_SERVER=1` and `JOB_PROCESSOR_ENABLED=0`).
- Worker service command: `npm run start:worker` (or set `JOB_PROCESSOR_ENABLED=1`).
- Set up a Stripe webhook in the Stripe dashboard pointing to your deployed `/webhooks/stripe` URL and copy the signing secret to `STRIPE_WEBHOOK_SECRET`.
- If Railway build shows `failed to stat ... /secrets/R2_ACCESS_KEY_ID`, remove file-based secret references and set `R2_ACCESS_KEY_ID` as a normal environment variable value in Railway.

GPU worker offload (optional):
1. Set `GPU_WORKER_URL` to your GPU worker HTTP endpoint (Crow service).
2. Shared filesystem mode (default): set `GPU_WORKER_SHARED_DIR` to a directory mounted on both the API host and GPU worker host.
3. URL-based mode: set `GPU_WORKER_TRANSFER_MODE=urls` and optionally `GPU_WORKER_URL_TMP_DIR=/tmp/ae-worker`.
4. (Optional) Set `GPU_WORKER_SHARED_DIR_REMOTE` if the GPU worker sees the shared directory at a different path (for example, Docker/WSL on Windows).
5. The backend will automatically offload eligible render jobs to the GPU worker and fall back to local FFmpeg when unsupported.
