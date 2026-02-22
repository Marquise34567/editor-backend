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

Deployment:
- Use Railway for the server. Set environment variables per `.env.example` in Railway.
- Set up a Stripe webhook in the Stripe dashboard pointing to your deployed `/webhooks/stripe` URL and copy the signing secret to `STRIPE_WEBHOOK_SECRET`.
