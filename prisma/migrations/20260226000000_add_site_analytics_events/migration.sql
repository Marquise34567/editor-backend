CREATE TABLE "site_analytics_events" (
    "id" TEXT NOT NULL,
    "user_id" TEXT NOT NULL,
    "session_id" TEXT,
    "event_name" TEXT NOT NULL,
    "category" TEXT NOT NULL DEFAULT 'interaction',
    "page_path" TEXT,
    "retention_profile" TEXT,
    "target_platform" TEXT,
    "caption_style" TEXT,
    "job_id" TEXT,
    "metadata" JSONB,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "site_analytics_events_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "idx_site_analytics_events_created_at" ON "site_analytics_events"("created_at");
CREATE INDEX "idx_site_analytics_events_event_name" ON "site_analytics_events"("event_name");
CREATE INDEX "idx_site_analytics_events_category" ON "site_analytics_events"("category");
CREATE INDEX "idx_site_analytics_events_user_id" ON "site_analytics_events"("user_id");

ALTER TABLE "site_analytics_events"
ADD CONSTRAINT "site_analytics_events_user_id_fkey"
FOREIGN KEY ("user_id") REFERENCES "profiles"("id")
ON DELETE CASCADE ON UPDATE CASCADE;
