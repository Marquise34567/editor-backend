-- CreateTable
CREATE TABLE "vibecut_uploads" (
  "id" TEXT NOT NULL,
  "user_id" TEXT NOT NULL,
  "file_name" TEXT NOT NULL,
  "stored_path" TEXT NOT NULL,
  "metadata" JSONB NOT NULL,
  "auto_detection" JSONB NOT NULL,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT "vibecut_uploads_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "vibecut_jobs" (
  "id" TEXT NOT NULL,
  "user_id" TEXT NOT NULL,
  "upload_id" TEXT NOT NULL,
  "status" TEXT NOT NULL,
  "mode" TEXT NOT NULL,
  "progress" INTEGER NOT NULL DEFAULT 0,
  "file_name" TEXT NOT NULL,
  "output_video_url" TEXT,
  "output_video_path" TEXT,
  "clip_urls" JSONB,
  "ffmpeg_commands" JSONB,
  "thumbnails" JSONB,
  "retention" JSONB,
  "error_message" TEXT,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,

  CONSTRAINT "vibecut_jobs_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "idx_vibecut_uploads_user_created_at" ON "vibecut_uploads"("user_id", "created_at");

-- CreateIndex
CREATE INDEX "idx_vibecut_jobs_user_created_at" ON "vibecut_jobs"("user_id", "created_at");

-- CreateIndex
CREATE INDEX "idx_vibecut_jobs_upload_id" ON "vibecut_jobs"("upload_id");

-- AddForeignKey
ALTER TABLE "vibecut_jobs"
  ADD CONSTRAINT "vibecut_jobs_upload_id_fkey"
  FOREIGN KEY ("upload_id") REFERENCES "vibecut_uploads"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;
