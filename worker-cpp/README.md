# C++ GPU Worker Service

This service listens for render jobs and runs a GPU-only FFmpeg pipeline using NVDEC/NVENC with zero-copy CUDA frames.

Build locally:

```
cmake -S backend/worker-cpp -B backend/worker-cpp/build -DCMAKE_BUILD_TYPE=Release
cmake --build backend/worker-cpp/build -j
```

Run locally:

```
./backend/worker-cpp/build/worker_service
```

Docker build and run:

```
docker build -t ae-worker -f backend/worker-cpp/Dockerfile backend/worker-cpp
docker run --gpus all -p 7001:7001 -e WORKER_PORT=7001 ae-worker
```

Self-hosted GPU worker (recommended for production):

1. Ensure the GPU host has NVIDIA drivers installed and the NVIDIA Container Toolkit configured.
2. Build the image on the host:

```
./backend/worker-cpp/deploy/build-image.sh
```

3. Run it with Docker (foreground):

```
WORKER_THREADS=2 DATA_DIR=/var/autoeditor/data ./backend/worker-cpp/deploy/run-worker.sh
```

4. Or run it with Docker Compose:

```
cd backend/worker-cpp/deploy
docker compose -f docker-compose.gpu.yml up -d
```

5. Or install as a systemd service:

```
sudo mkdir -p /etc/autoeditor
sudo cp backend/worker-cpp/deploy/worker.env.example /etc/autoeditor/worker.env
sudo cp backend/worker-cpp/deploy/ae-worker.service /etc/systemd/system/ae-worker.service
sudo systemctl daemon-reload
sudo systemctl enable --now ae-worker.service
```

API:

`POST /render`

```
{
  "input_path": "/app/input.mp4",
  "output_path": "/app/output.mp4",
  "input_url": "https://signed-input-url",
  "output_upload_url": "https://signed-output-upload-url",
  "output_content_type": "video/mp4",
  "codec": "h264",
  "width": 1280,
  "height": 720,
  "bitrate_kbps": 8000,
  "use_cuda_resize": true,
  "crop_x": 0,
  "crop_y": 0,
  "crop_w": 1920,
  "crop_h": 1080,
  "parallel_segments": 2,
  "watermark": {
    "path": "/app/watermark.rgba",
    "width": 256,
    "height": 256,
    "x": 32,
    "y": 32,
    "alpha": 0.6
  },
  "lut": {
    "path": "/app/lut.txt",
    "size": 256
  },
  "segments": [
    {"start_ms": 0, "duration_ms": 10000},
    {"start_ms": 10000, "duration_ms": 10000}
  ]
}
```

`codec` accepts `h264` or `hevc` (NVENC).

The watermark file is expected to be raw RGBA (width * height * 4 bytes). The LUT file is a text file with one float per entry in the range 0..1.

URL-based transfer (no shared filesystem):
- Provide `input_url` to a signed GET URL and `output_upload_url` to a signed PUT URL.
- `input_path` and `output_path` remain required and are used as local temp paths on the worker host.
- The worker downloads `input_url` to `input_path`, renders to `output_path`, uploads to `output_upload_url`, and cleans up the local files.
- `output_content_type` is optional but recommended when using presigned PUT URLs.

`parallel_segments` is treated as an upper bound. The worker will reduce it automatically based on available VRAM to avoid GPU OOM.

`GET /jobs/<id>` returns job status.

Environment variables:
`WORKER_PORT` sets the HTTP port (default 7001).
`WORKER_THREADS` controls concurrent job workers.

Backend auto-dispatch:
- Set `GPU_WORKER_URL` and `GPU_WORKER_SHARED_DIR` on the API service.
- Mount the same shared directory into both the API host and GPU worker host so paths resolve.
