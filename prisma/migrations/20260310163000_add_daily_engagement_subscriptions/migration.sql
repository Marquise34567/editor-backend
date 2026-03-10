CREATE TABLE IF NOT EXISTS "daily_engagement_subscriptions" (
  "id" TEXT NOT NULL DEFAULT gen_random_uuid()::text,
  "user_id" TEXT NOT NULL,
  "email" TEXT NOT NULL,
  "enabled" BOOLEAN NOT NULL DEFAULT true,
  "email_enabled" BOOLEAN NOT NULL DEFAULT true,
  "push_enabled" BOOLEAN NOT NULL DEFAULT false,
  "push_endpoint" TEXT,
  "push_p256dh" TEXT,
  "push_auth" TEXT,
  "last_sent_at" TIMESTAMPTZ,
  "next_send_at" TIMESTAMPTZ,
  "last_error" TEXT,
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT "daily_engagement_subscriptions_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "daily_engagement_subscriptions_user_id_fkey"
    FOREIGN KEY ("user_id") REFERENCES "profiles"("id")
    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS "daily_engagement_subscriptions_user_id_key" ON "daily_engagement_subscriptions"("user_id");
CREATE UNIQUE INDEX IF NOT EXISTS "daily_engagement_subscriptions_email_key" ON "daily_engagement_subscriptions"("email");
CREATE INDEX IF NOT EXISTS "idx_daily_engagement_subscriptions_due" ON "daily_engagement_subscriptions"("enabled", "next_send_at");
