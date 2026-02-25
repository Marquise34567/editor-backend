CREATE TABLE "banned_ips" (
    "ip" TEXT NOT NULL,
    "reason" TEXT,
    "created_by" TEXT,
    "active" BOOLEAN NOT NULL DEFAULT true,
    "expires_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "banned_ips_pkey" PRIMARY KEY ("ip")
);

CREATE INDEX "idx_banned_ips_active" ON "banned_ips"("active");
CREATE INDEX "idx_banned_ips_expires_at" ON "banned_ips"("expires_at");

CREATE TABLE "weekly_report_subscriptions" (
    "id" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "enabled" BOOLEAN NOT NULL DEFAULT true,
    "created_by" TEXT,
    "last_sent_at" TIMESTAMP(3),
    "next_send_at" TIMESTAMP(3),
    "last_error" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "weekly_report_subscriptions_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "weekly_report_subscriptions_email_key" ON "weekly_report_subscriptions"("email");
CREATE INDEX "idx_weekly_report_subscriptions_enabled_next_send" ON "weekly_report_subscriptions"("enabled", "next_send_at");
