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
- Set up a Stripe webhook in the Stripe dashboard pointing to your deployed `/webhooks/stripe` URL and copy the signing secret to `STRIPE_WEBHOOK_SECRET`.
- If Railway build shows `failed to stat ... /secrets/R2_ACCESS_KEY_ID`, remove file-based secret references and set `R2_ACCESS_KEY_ID` as a normal environment variable value in Railway.
