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

API:

`POST /render`

```
{
  "input_path": "/app/input.mp4",
  "output_path": "/app/output.mp4",
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

`parallel_segments` is treated as an upper bound. The worker will reduce it automatically based on available VRAM to avoid GPU OOM.

`GET /jobs/<id>` returns job status.

Environment variables:
`WORKER_PORT` sets the HTTP port (default 7001).
`WORKER_THREADS` controls concurrent job workers.
