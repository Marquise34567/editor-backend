#include "cuda_kernels.h"

#include <cuda_runtime.h>

namespace {

__device__ __forceinline__ int ClampInt(int v, int lo, int hi) {
  return v < lo ? lo : (v > hi ? hi : v);
}

__global__ void ResizeCropYKernel(
    const uint8_t* src,
    int src_w,
    int src_h,
    int src_pitch,
    uint8_t* dst,
    int dst_w,
    int dst_h,
    int dst_pitch,
    int crop_x,
    int crop_y,
    int crop_w,
    int crop_h) {
  int x = blockIdx.x * blockDim.x + threadIdx.x;
  int y = blockIdx.y * blockDim.y + threadIdx.y;
  if (x >= dst_w || y >= dst_h) {
    return;
  }

  float u = dst_w > 1 ? static_cast<float>(x) / static_cast<float>(dst_w - 1) : 0.0f;
  float v = dst_h > 1 ? static_cast<float>(y) / static_cast<float>(dst_h - 1) : 0.0f;

  int sx = crop_x + ClampInt(static_cast<int>(u * crop_w), 0, crop_w - 1);
  int sy = crop_y + ClampInt(static_cast<int>(v * crop_h), 0, crop_h - 1);

  dst[y * dst_pitch + x] = src[sy * src_pitch + sx];
}

__global__ void ResizeCropUVKernel(
    const uint8_t* src,
    int src_w,
    int src_h,
    int src_pitch,
    uint8_t* dst,
    int dst_w,
    int dst_h,
    int dst_pitch,
    int crop_x,
    int crop_y,
    int crop_w,
    int crop_h) {
  int x = blockIdx.x * blockDim.x + threadIdx.x;
  int y = blockIdx.y * blockDim.y + threadIdx.y;
  if (x >= dst_w || y >= dst_h) {
    return;
  }

  float u = dst_w > 1 ? static_cast<float>(x) / static_cast<float>(dst_w - 1) : 0.0f;
  float v = dst_h > 1 ? static_cast<float>(y) / static_cast<float>(dst_h - 1) : 0.0f;

  int sx = crop_x + ClampInt(static_cast<int>(u * crop_w), 0, crop_w - 1);
  int sy = crop_y + ClampInt(static_cast<int>(v * crop_h), 0, crop_h - 1);

  int src_x = sx * 2;
  int src_y = sy * 2;

  const uint8_t* src_uv = src + src_y * src_pitch + src_x;
  uint8_t* dst_uv = dst + y * dst_pitch + x * 2;
  dst_uv[0] = src_uv[0];
  dst_uv[1] = src_uv[1];
}

__global__ void WatermarkBlendKernel(
    uint8_t* y_plane,
    uint8_t* uv_plane,
    int width,
    int height,
    int pitch_y,
    int pitch_uv,
    const uint8_t* watermark_rgba,
    int wm_w,
    int wm_h,
    int pos_x,
    int pos_y,
    float global_alpha) {
  int x = blockIdx.x * blockDim.x + threadIdx.x;
  int y = blockIdx.y * blockDim.y + threadIdx.y;
  if (x >= wm_w || y >= wm_h) {
    return;
  }

  int dst_x = pos_x + x;
  int dst_y = pos_y + y;
  if (dst_x < 0 || dst_y < 0 || dst_x >= width || dst_y >= height) {
    return;
  }

  int wm_idx = (y * wm_w + x) * 4;
  float a = (watermark_rgba[wm_idx + 3] / 255.0f) * global_alpha;
  if (a <= 0.0f) {
    return;
  }

  float r = watermark_rgba[wm_idx + 0] / 255.0f;
  float g = watermark_rgba[wm_idx + 1] / 255.0f;
  float b = watermark_rgba[wm_idx + 2] / 255.0f;

  float y_val = 0.2126f * r + 0.7152f * g + 0.0722f * b;
  uint8_t* y_ptr = y_plane + dst_y * pitch_y + dst_x;
  float src_y = (*y_ptr) / 255.0f;
  float blended_y = src_y * (1.0f - a) + y_val * a;
  *y_ptr = static_cast<uint8_t>(ClampInt(static_cast<int>(blended_y * 255.0f), 0, 255));

  if ((dst_x % 2 == 0) && (dst_y % 2 == 0)) {
    float u = (b - y_val) * 0.5389f + 0.5f;
    float v = (r - y_val) * 0.6350f + 0.5f;
    int uv_x = dst_x / 2;
    int uv_y = dst_y / 2;
    uint8_t* uv_ptr = uv_plane + uv_y * pitch_uv + uv_x * 2;
    float src_u = uv_ptr[0] / 255.0f;
    float src_v = uv_ptr[1] / 255.0f;
    float blended_u = src_u * (1.0f - a) + u * a;
    float blended_v = src_v * (1.0f - a) + v * a;
    uv_ptr[0] = static_cast<uint8_t>(ClampInt(static_cast<int>(blended_u * 255.0f), 0, 255));
    uv_ptr[1] = static_cast<uint8_t>(ClampInt(static_cast<int>(blended_v * 255.0f), 0, 255));
  }
}

__global__ void ApplyLutKernel(
    uint8_t* y_plane,
    int width,
    int height,
    int pitch_y,
    const float* lut,
    int lut_size) {
  int x = blockIdx.x * blockDim.x + threadIdx.x;
  int y = blockIdx.y * blockDim.y + threadIdx.y;
  if (x >= width || y >= height) {
    return;
  }

  uint8_t* y_ptr = y_plane + y * pitch_y + x;
  int idx = ClampInt(static_cast<int>(*y_ptr), 0, lut_size - 1);
  float mapped = lut[idx];
  int out = ClampInt(static_cast<int>(mapped * 255.0f), 0, 255);
  *y_ptr = static_cast<uint8_t>(out);
}

}  // namespace

void LaunchResizeCropNv12(
    const CudaNv12View& src,
    const CudaNv12View& dst,
    int crop_x,
    int crop_y,
    int crop_w,
    int crop_h,
    cudaStream_t stream) {
  dim3 block(16, 16);
  dim3 grid_y((dst.width + block.x - 1) / block.x, (dst.height + block.y - 1) / block.y);
  ResizeCropYKernel<<<grid_y, block, 0, stream>>>(
      src.y_plane,
      src.width,
      src.height,
      src.pitch_y,
      dst.y_plane,
      dst.width,
      dst.height,
      dst.pitch_y,
      crop_x,
      crop_y,
      crop_w,
      crop_h);

  int uv_w = dst.width / 2;
  int uv_h = dst.height / 2;
  dim3 grid_uv((uv_w + block.x - 1) / block.x, (uv_h + block.y - 1) / block.y);
  ResizeCropUVKernel<<<grid_uv, block, 0, stream>>>(
      src.uv_plane,
      src.width / 2,
      src.height / 2,
      src.pitch_uv,
      dst.uv_plane,
      uv_w,
      uv_h,
      dst.pitch_uv,
      crop_x / 2,
      crop_y / 2,
      crop_w / 2,
      crop_h / 2);
}

void LaunchWatermarkBlendNv12(
    const CudaNv12View& frame,
    const uint8_t* watermark_rgba,
    int watermark_width,
    int watermark_height,
    int pos_x,
    int pos_y,
    float alpha,
    cudaStream_t stream) {
  if (!watermark_rgba || watermark_width <= 0 || watermark_height <= 0) {
    return;
  }
  dim3 block(16, 16);
  dim3 grid((watermark_width + block.x - 1) / block.x, (watermark_height + block.y - 1) / block.y);
  WatermarkBlendKernel<<<grid, block, 0, stream>>>(
      frame.y_plane,
      frame.uv_plane,
      frame.width,
      frame.height,
      frame.pitch_y,
      frame.pitch_uv,
      watermark_rgba,
      watermark_width,
      watermark_height,
      pos_x,
      pos_y,
      alpha);
}

void LaunchApplyLutNv12(
    const CudaNv12View& frame,
    const float* lut,
    int lut_size,
    cudaStream_t stream) {
  if (!lut || lut_size <= 0) {
    return;
  }
  dim3 block(16, 16);
  dim3 grid((frame.width + block.x - 1) / block.x, (frame.height + block.y - 1) / block.y);
  ApplyLutKernel<<<grid, block, 0, stream>>>(
      frame.y_plane,
      frame.width,
      frame.height,
      frame.pitch_y,
      lut,
      lut_size);
}
