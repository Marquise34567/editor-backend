#pragma once

#include <cstdint>
#include <cuda_runtime.h>

struct CudaNv12View {
  uint8_t* y_plane;
  uint8_t* uv_plane;
  int width;
  int height;
  int pitch_y;
  int pitch_uv;
};

void LaunchResizeCropNv12(
    const CudaNv12View& src,
    const CudaNv12View& dst,
    int crop_x,
    int crop_y,
    int crop_w,
    int crop_h,
    cudaStream_t stream);

void LaunchWatermarkBlendNv12(
    const CudaNv12View& frame,
    const uint8_t* watermark_rgba,
    int watermark_width,
    int watermark_height,
    int pos_x,
    int pos_y,
    float alpha,
    cudaStream_t stream);

void LaunchApplyLutNv12(
    const CudaNv12View& frame,
    const float* lut,
    int lut_size,
    cudaStream_t stream);
