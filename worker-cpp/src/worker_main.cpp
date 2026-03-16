#include "crow.h"
#include "cuda_kernels.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <cuda_runtime.h>
#include <curl/curl.h>

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavfilter/avfilter.h>
#include <libavfilter/buffersink.h>
#include <libavfilter/buffersrc.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
#include <libavutil/error.h>
#include <libavutil/hwcontext.h>
#include <libavutil/opt.h>
}

namespace fs = std::filesystem;

struct SegmentPlan {
  int64_t start_ms = 0;
  int64_t duration_ms = -1;
  std::string output_path;
};

struct WatermarkConfig {
  std::string path;
  int width = 0;
  int height = 0;
  int x = 0;
  int y = 0;
  float alpha = 1.0f;
};

struct LutConfig {
  std::string path;
  int size = 0;
};

struct RenderJob {
  std::string id;
  std::string input_path;
  std::string input_url;
  std::string output_path;
  std::string output_upload_url;
  std::string output_content_type;
  std::string codec = "h264";
  int width = 0;
  int height = 0;
  int bitrate_kbps = 0;
  int crop_x = 0;
  int crop_y = 0;
  int crop_w = 0;
  int crop_h = 0;
  bool use_cuda_resize = false;
  int parallel_segments = 1;
  WatermarkConfig watermark;
  LutConfig lut;
  std::vector<SegmentPlan> segments;
};

enum class JobState {
  Queued,
  Running,
  Succeeded,
  Failed
};

struct JobStatus {
  JobState state = JobState::Queued;
  std::string error;
};

class JobQueue {
 public:
  std::string Enqueue(RenderJob job) {
    std::lock_guard<std::mutex> lock(mu_);
    job.id = NextId();
    statuses_[job.id] = JobStatus{JobState::Queued, ""};
    queue_.push(std::move(job));
    cv_.notify_one();
    return queue_.back().id;
  }

  bool WaitDequeue(RenderJob& job) {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [&] { return stop_ || !queue_.empty(); });
    if (stop_) {
      return false;
    }
    job = std::move(queue_.front());
    queue_.pop();
    statuses_[job.id].state = JobState::Running;
    return true;
  }

  void Complete(const std::string& id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = statuses_.find(id);
    if (it != statuses_.end()) {
      it->second.state = JobState::Succeeded;
      it->second.error.clear();
    }
  }

  void Fail(const std::string& id, const std::string& error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = statuses_.find(id);
    if (it != statuses_.end()) {
      it->second.state = JobState::Failed;
      it->second.error = error;
    }
  }

  bool GetStatus(const std::string& id, JobStatus& out) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = statuses_.find(id);
    if (it == statuses_.end()) {
      return false;
    }
    out = it->second;
    return true;
  }

  void Stop() {
    std::lock_guard<std::mutex> lock(mu_);
    stop_ = true;
    cv_.notify_all();
  }

 private:
  std::string NextId() {
    using namespace std::chrono;
    auto now_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    uint64_t seq = counter_.fetch_add(1);
    std::ostringstream oss;
    oss << "job_" << now_ms << "_" << seq;
    return oss.str();
  }

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::queue<RenderJob> queue_;
  std::unordered_map<std::string, JobStatus> statuses_;
  std::atomic<uint64_t> counter_{0};
  bool stop_ = false;
};
static enum AVPixelFormat g_hw_pix_fmt = AV_PIX_FMT_CUDA;

static enum AVPixelFormat GetHwFormat(AVCodecContext* ctx, const enum AVPixelFormat* pix_fmts) {
  for (const enum AVPixelFormat* p = pix_fmts; *p != AV_PIX_FMT_NONE; p++) {
    if (*p == g_hw_pix_fmt) {
      return *p;
    }
  }
  return pix_fmts[0];
}

static std::string AvErrorToString(int err) {
  char buf[AV_ERROR_MAX_STRING_SIZE];
  av_strerror(err, buf, sizeof(buf));
  return std::string(buf);
}

static bool EnsureParentDir(const std::string& file_path, std::string& error) {
  try {
    fs::path path(file_path);
    if (path.has_parent_path()) {
      fs::create_directories(path.parent_path());
    }
    return true;
  } catch (const std::exception& e) {
    error = std::string("mkdir_failed: ") + e.what();
    return false;
  }
}

static bool DownloadToFile(const std::string& url, const std::string& dest_path, std::string& error) {
  if (url.empty()) {
    return true;
  }
  if (!EnsureParentDir(dest_path, error)) {
    return false;
  }

  FILE* fp = std::fopen(dest_path.c_str(), "wb");
  if (!fp) {
    error = "download_open_failed";
    return false;
  }

  CURL* curl = curl_easy_init();
  if (!curl) {
    std::fclose(fp);
    error = "download_curl_init_failed";
    return false;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
  curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

  CURLcode res = curl_easy_perform(curl);
  long status = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
  curl_easy_cleanup(curl);
  std::fclose(fp);

  if (res != CURLE_OK) {
    error = std::string("download_failed: ") + curl_easy_strerror(res);
    return false;
  }
  if (status >= 400) {
    error = "download_http_" + std::to_string(status);
    return false;
  }
  return true;
}

static bool UploadFileToUrl(
  const std::string& url,
  const std::string& file_path,
  const std::string& content_type,
  std::string& error
) {
  if (url.empty()) {
    return true;
  }

  std::error_code ec;
  const auto file_size = fs::file_size(file_path, ec);
  if (ec) {
    error = std::string("upload_stat_failed: ") + ec.message();
    return false;
  }

  FILE* fp = std::fopen(file_path.c_str(), "rb");
  if (!fp) {
    error = "upload_open_failed";
    return false;
  }

  CURL* curl = curl_easy_init();
  if (!curl) {
    std::fclose(fp);
    error = "upload_curl_init_failed";
    return false;
  }

  struct curl_slist* headers = nullptr;
  if (!content_type.empty()) {
    headers = curl_slist_append(headers, ("Content-Type: " + content_type).c_str());
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(curl, CURLOPT_READDATA, fp);
  curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(file_size));
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

  CURLcode res = curl_easy_perform(curl);
  long status = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);

  if (headers) {
    curl_slist_free_all(headers);
  }
  curl_easy_cleanup(curl);
  std::fclose(fp);

  if (res != CURLE_OK) {
    error = std::string("upload_failed: ") + curl_easy_strerror(res);
    return false;
  }
  if (status >= 400) {
    error = "upload_http_" + std::to_string(status);
    return false;
  }
  return true;
}

static bool ProbeInputDimensions(const std::string& path, int& width, int& height, std::string& error) {
  AVFormatContext* ctx = nullptr;
  int ret = avformat_open_input(&ctx, path.c_str(), nullptr, nullptr);
  if (ret < 0) {
    error = "probe input: " + AvErrorToString(ret);
    return false;
  }

  ret = avformat_find_stream_info(ctx, nullptr);
  if (ret < 0) {
    error = "probe stream info: " + AvErrorToString(ret);
    avformat_close_input(&ctx);
    return false;
  }

  int video_stream_index = av_find_best_stream(ctx, AVMEDIA_TYPE_VIDEO, -1, -1, nullptr, 0);
  if (video_stream_index < 0) {
    error = "probe video stream: " + AvErrorToString(video_stream_index);
    avformat_close_input(&ctx);
    return false;
  }

  AVStream* stream = ctx->streams[video_stream_index];
  width = stream->codecpar->width;
  height = stream->codecpar->height;
  avformat_close_input(&ctx);
  return width > 0 && height > 0;
}

static size_t EstimateSegmentVramBytes(int width, int height) {
  if (width <= 0 || height <= 0) {
    return 0;
  }
  size_t frame_bytes = static_cast<size_t>(width) * static_cast<size_t>(height) * 3 / 2;
  // Conservative surface pool estimate: decoder + encoder surfaces + in-flight frames.
  const size_t kSurfaces = 12;
  return frame_bytes * kSurfaces;
}

static bool CheckCuda(cudaError_t err, std::string& error, const char* context) {
  if (err == cudaSuccess) {
    return true;
  }
  error = std::string(context) + ": " + cudaGetErrorString(err);
  return false;
}

static AVCodec* FindHardwareDecoder(AVCodecID codec_id) {
  switch (codec_id) {
    case AV_CODEC_ID_H264:
      return avcodec_find_decoder_by_name("h264_cuvid");
    case AV_CODEC_ID_HEVC:
      return avcodec_find_decoder_by_name("hevc_cuvid");
    case AV_CODEC_ID_AV1:
      return avcodec_find_decoder_by_name("av1_cuvid");
    default:
      return nullptr;
  }
}

static AVCodec* FindHardwareEncoder(const std::string& codec) {
  if (codec == "hevc") {
    return avcodec_find_encoder_by_name("hevc_nvenc");
  }
  return avcodec_find_encoder_by_name("h264_nvenc");
}

static std::string MakeSegmentPath(const std::string& base, int index) {
  auto dot = base.find_last_of('.');
  if (dot == std::string::npos) {
    return base + "_seg" + std::to_string(index);
  }
  return base.substr(0, dot) + "_seg" + std::to_string(index) + base.substr(dot);
}

struct GpuAssets {
  uint8_t* watermark_rgba = nullptr;
  int watermark_width = 0;
  int watermark_height = 0;
  float* lut = nullptr;
  int lut_size = 0;

  ~GpuAssets() {
    if (watermark_rgba) {
      cudaFree(watermark_rgba);
    }
    if (lut) {
      cudaFree(lut);
    }
  }
};

static bool ReadFileBytes(const std::string& path, std::vector<uint8_t>& out) {
  std::ifstream file(path, std::ios::binary | std::ios::ate);
  if (!file) {
    return false;
  }
  auto size = file.tellg();
  if (size <= 0) {
    return false;
  }
  out.resize(static_cast<size_t>(size));
  file.seekg(0, std::ios::beg);
  file.read(reinterpret_cast<char*>(out.data()), size);
  return file.good();
}

static bool LoadWatermark(const WatermarkConfig& cfg, GpuAssets& assets, cudaStream_t stream, std::string& error) {
  if (cfg.path.empty()) {
    return true;
  }
  if (cfg.width <= 0 || cfg.height <= 0) {
    error = "watermark width/height required";
    return false;
  }

  std::vector<uint8_t> data;
  if (!ReadFileBytes(cfg.path, data)) {
    error = "failed to read watermark file";
    return false;
  }

  size_t expected = static_cast<size_t>(cfg.width) * static_cast<size_t>(cfg.height) * 4;
  if (data.size() != expected) {
    error = "watermark file size mismatch (expected raw RGBA)";
    return false;
  }

  uint8_t* device_ptr = nullptr;
  if (!CheckCuda(cudaMalloc(&device_ptr, expected), error, "cudaMalloc watermark")) {
    return false;
  }
  if (!CheckCuda(cudaMemcpyAsync(device_ptr, data.data(), expected, cudaMemcpyHostToDevice, stream), error, "cudaMemcpy watermark")) {
    cudaFree(device_ptr);
    return false;
  }

  assets.watermark_rgba = device_ptr;
  assets.watermark_width = cfg.width;
  assets.watermark_height = cfg.height;
  return true;
}

static bool LoadLut(const LutConfig& cfg, GpuAssets& assets, cudaStream_t stream, std::string& error) {
  if (cfg.path.empty()) {
    return true;
  }

  std::ifstream file(cfg.path);
  if (!file) {
    error = "failed to read LUT file";
    return false;
  }

  std::vector<float> values;
  float value = 0.0f;
  while (file >> value) {
    values.push_back(value);
  }

  if (values.empty()) {
    error = "LUT file empty";
    return false;
  }

  int lut_size = cfg.size > 0 ? cfg.size : static_cast<int>(values.size());
  if (static_cast<int>(values.size()) < lut_size) {
    error = "LUT file shorter than lut_size";
    return false;
  }

  float* device_ptr = nullptr;
  size_t bytes = static_cast<size_t>(lut_size) * sizeof(float);
  if (!CheckCuda(cudaMalloc(&device_ptr, bytes), error, "cudaMalloc lut")) {
    return false;
  }
  if (!CheckCuda(cudaMemcpyAsync(device_ptr, values.data(), bytes, cudaMemcpyHostToDevice, stream), error, "cudaMemcpy lut")) {
    cudaFree(device_ptr);
    return false;
  }

  assets.lut = device_ptr;
  assets.lut_size = lut_size;
  return true;
}

static CudaNv12View MakeNv12View(AVFrame* frame) {
  CudaNv12View view;
  view.y_plane = reinterpret_cast<uint8_t*>(frame->data[0]);
  view.uv_plane = reinterpret_cast<uint8_t*>(frame->data[1]);
  view.width = frame->width;
  view.height = frame->height;
  view.pitch_y = frame->linesize[0];
  view.pitch_uv = frame->linesize[1];
  return view;
}

static AVBufferRef* CreateEncoderFramesContext(AVBufferRef* hw_device_ctx, int width, int height, std::string& error) {
  AVBufferRef* frames_ref = av_hwframe_ctx_alloc(hw_device_ctx);
  if (!frames_ref) {
    error = "alloc hwframe context";
    return nullptr;
  }

  auto* frames_ctx = reinterpret_cast<AVHWFramesContext*>(frames_ref->data);
  frames_ctx->format = AV_PIX_FMT_CUDA;
  frames_ctx->sw_format = AV_PIX_FMT_NV12;
  frames_ctx->width = width;
  frames_ctx->height = height;
  frames_ctx->initial_pool_size = 32;

  int ret = av_hwframe_ctx_init(frames_ref);
  if (ret < 0) {
    error = "init hwframe context: " + AvErrorToString(ret);
    av_buffer_unref(&frames_ref);
    return nullptr;
  }
  return frames_ref;
}
class GpuTranscoder {
 public:
  bool Transcode(const RenderJob& job, std::string& error) {
    auto assets = std::make_shared<GpuAssets>();

    cudaStream_t upload_stream = nullptr;
    if (!CheckCuda(cudaStreamCreateWithFlags(&upload_stream, cudaStreamNonBlocking), error, "cudaStreamCreate")) {
      return false;
    }

    bool loaded = LoadWatermark(job.watermark, *assets, upload_stream, error);
    if (loaded) {
      loaded = LoadLut(job.lut, *assets, upload_stream, error);
    }
    if (loaded) {
      loaded = CheckCuda(cudaStreamSynchronize(upload_stream), error, "cudaStreamSynchronize");
    }
    cudaStreamDestroy(upload_stream);

    if (!loaded) {
      return false;
    }

    std::vector<SegmentPlan> segments = job.segments;
    if (segments.empty()) {
      segments.push_back(SegmentPlan{0, -1, job.output_path});
    }

    int parallel = job.parallel_segments > 0 ? job.parallel_segments : 1;
    int out_w = job.width;
    int out_h = job.height;
    if (out_w <= 0 || out_h <= 0) {
      std::string probe_error;
      int probe_w = 0;
      int probe_h = 0;
      if (ProbeInputDimensions(job.input_path, probe_w, probe_h, probe_error)) {
        out_w = probe_w;
        out_h = probe_h;
      }
    }

    if (out_w > 0 && out_h > 0) {
      size_t free_bytes = 0;
      size_t total_bytes = 0;
      if (cudaMemGetInfo(&free_bytes, &total_bytes) == cudaSuccess) {
        size_t per_segment = EstimateSegmentVramBytes(out_w, out_h);
        if (per_segment > 0) {
          const size_t headroom = 256ull * 1024 * 1024;
          size_t usable = free_bytes > headroom ? free_bytes - headroom : free_bytes;
          int max_parallel = static_cast<int>(usable / per_segment);
          if (max_parallel < 1) {
            max_parallel = 1;
          }
          parallel = std::min(parallel, max_parallel);
        }
      }
    }

    if (parallel > static_cast<int>(segments.size())) {
      parallel = static_cast<int>(segments.size());
    }

    std::atomic<size_t> next_index{0};
    std::atomic<bool> ok{true};
    std::mutex err_mu;

    auto worker = [&] {
      cudaStream_t stream = nullptr;
      if (!CheckCuda(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking), error, "cudaStreamCreate")) {
        ok.store(false);
        return;
      }

      while (ok.load()) {
        size_t idx = next_index.fetch_add(1);
        if (idx >= segments.size()) {
          break;
        }
        std::string seg_error;
        if (!TranscodeSegment(job, segments[idx], stream, assets, seg_error)) {
          std::lock_guard<std::mutex> lock(err_mu);
          if (error.empty()) {
            error = seg_error;
          }
          ok.store(false);
          break;
        }
      }

      cudaStreamDestroy(stream);
    };

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(parallel));
    for (int i = 0; i < parallel; ++i) {
      threads.emplace_back(worker);
    }
    for (auto& t : threads) {
      t.join();
    }

    return ok.load();
  }

 private:
  bool ApplyCudaFilters(AVFrame* frame,
                        const RenderJob& job,
                        const GpuAssets& assets,
                        cudaStream_t stream,
                        std::string& error) {
    bool used = false;
    if (assets.watermark_rgba) {
      CudaNv12View view = MakeNv12View(frame);
      LaunchWatermarkBlendNv12(
          view,
          assets.watermark_rgba,
          assets.watermark_width,
          assets.watermark_height,
          job.watermark.x,
          job.watermark.y,
          job.watermark.alpha,
          stream);
      used = true;
    }

    if (assets.lut) {
      CudaNv12View view = MakeNv12View(frame);
      LaunchApplyLutNv12(view, assets.lut, assets.lut_size, stream);
      used = true;
    }

    if (used) {
      if (!CheckCuda(cudaGetLastError(), error, "cuda kernel")) {
        return false;
      }
      if (!CheckCuda(cudaStreamSynchronize(stream), error, "cuda sync")) {
        return false;
      }
    }
    return true;
  }

  bool TranscodeSegment(const RenderJob& job,
                        const SegmentPlan& segment,
                        cudaStream_t stream,
                        const std::shared_ptr<GpuAssets>& assets,
                        std::string& error) {
    AVFormatContext* input_ctx = nullptr;
    AVFormatContext* output_ctx = nullptr;
    AVCodecContext* decoder_ctx = nullptr;
    AVCodecContext* encoder_ctx = nullptr;
    AVFilterGraph* filter_graph = nullptr;
    AVFilterContext* buffersrc_ctx = nullptr;
    AVFilterContext* buffersink_ctx = nullptr;
    AVBufferRef* hw_device_ctx = nullptr;
    AVPacket* packet = nullptr;
    AVPacket* out_packet = nullptr;
    AVFrame* frame = nullptr;
    AVFrame* filt_frame = nullptr;
    AVFrame* resize_frame = nullptr;
    AVBufferRef* encoder_frames_ctx = nullptr;

    int ret = 0;
    int video_stream_index = -1;
    int audio_stream_index = -1;
    AVStream* input_audio_stream = nullptr;
    AVStream* output_audio_stream = nullptr;

    if ((ret = avformat_open_input(&input_ctx, job.input_path.c_str(), nullptr, nullptr)) < 0) {
      error = "open input: " + AvErrorToString(ret);
      goto cleanup;
    }

    if ((ret = avformat_find_stream_info(input_ctx, nullptr)) < 0) {
      error = "find stream info: " + AvErrorToString(ret);
      goto cleanup;
    }

    video_stream_index = av_find_best_stream(input_ctx, AVMEDIA_TYPE_VIDEO, -1, -1, nullptr, 0);
    if (video_stream_index < 0) {
      error = "no video stream";
      ret = video_stream_index;
      goto cleanup;
    }

    AVStream* input_stream = input_ctx->streams[video_stream_index];
    AVCodecParameters* input_params = input_stream->codecpar;
    audio_stream_index = av_find_best_stream(input_ctx, AVMEDIA_TYPE_AUDIO, -1, -1, nullptr, 0);
    if (audio_stream_index >= 0) {
      input_audio_stream = input_ctx->streams[audio_stream_index];
    }

    AVCodec* decoder = FindHardwareDecoder(input_params->codec_id);
    if (!decoder) {
      error = "hardware decoder not found";
      ret = AVERROR_DECODER_NOT_FOUND;
      goto cleanup;
    }

    if ((ret = av_hwdevice_ctx_create(&hw_device_ctx, AV_HWDEVICE_TYPE_CUDA, nullptr, nullptr, 0)) < 0) {
      error = "cuda device: " + AvErrorToString(ret);
      goto cleanup;
    }

    decoder_ctx = avcodec_alloc_context3(decoder);
    if (!decoder_ctx) {
      error = "alloc decoder context";
      ret = AVERROR(ENOMEM);
      goto cleanup;
    }

    if ((ret = avcodec_parameters_to_context(decoder_ctx, input_params)) < 0) {
      error = "decoder parameters: " + AvErrorToString(ret);
      goto cleanup;
    }

    decoder_ctx->get_format = GetHwFormat;
    decoder_ctx->hw_device_ctx = av_buffer_ref(hw_device_ctx);

    if ((ret = avcodec_open2(decoder_ctx, decoder, nullptr)) < 0) {
      error = "open decoder: " + AvErrorToString(ret);
      goto cleanup;
    }

    if (decoder_ctx->pix_fmt != AV_PIX_FMT_CUDA || !decoder_ctx->hw_frames_ctx) {
      error = "decoder did not output CUDA frames";
      ret = AVERROR(EINVAL);
      goto cleanup;
    }

    if ((ret = avformat_alloc_output_context2(&output_ctx, nullptr, nullptr, segment.output_path.c_str())) < 0) {
      error = "alloc output: " + AvErrorToString(ret);
      goto cleanup;
    }

    AVCodec* encoder = FindHardwareEncoder(job.codec);
    if (!encoder) {
      error = "nvenc encoder not found";
      ret = AVERROR_ENCODER_NOT_FOUND;
      goto cleanup;
    }

    encoder_ctx = avcodec_alloc_context3(encoder);
    if (!encoder_ctx) {
      error = "alloc encoder context";
      ret = AVERROR(ENOMEM);
      goto cleanup;
    }

    int out_w = job.width > 0 ? job.width : decoder_ctx->width;
    int out_h = job.height > 0 ? job.height : decoder_ctx->height;

    encoder_ctx->width = out_w;
    encoder_ctx->height = out_h;
    encoder_ctx->pix_fmt = AV_PIX_FMT_CUDA;
    encoder_ctx->time_base = av_inv_q(av_guess_frame_rate(input_ctx, input_stream, nullptr));
    if (encoder_ctx->time_base.num == 0 || encoder_ctx->time_base.den == 0) {
      encoder_ctx->time_base = input_stream->time_base;
    }
    encoder_ctx->framerate = av_guess_frame_rate(input_ctx, input_stream, nullptr);
    encoder_ctx->gop_size = 30;
    encoder_ctx->max_b_frames = 0;
    if (job.bitrate_kbps > 0) {
      encoder_ctx->bit_rate = static_cast<int64_t>(job.bitrate_kbps) * 1000;
    }

    av_opt_set(encoder_ctx->priv_data, "preset", "p1", 0);
    av_opt_set(encoder_ctx->priv_data, "tune", "ll", 0);
    av_opt_set(encoder_ctx->priv_data, "rc", "cbr", 0);

    bool use_cuda_resize = job.use_cuda_resize || job.crop_w > 0 || job.crop_h > 0;

    if (use_cuda_resize) {
      encoder_frames_ctx = CreateEncoderFramesContext(hw_device_ctx, out_w, out_h, error);
      if (!encoder_frames_ctx) {
        ret = AVERROR(EINVAL);
        goto cleanup;
      }
      encoder_ctx->hw_frames_ctx = av_buffer_ref(encoder_frames_ctx);
    } else {
      filter_graph = avfilter_graph_alloc();
      if (!filter_graph) {
        error = "alloc filter graph";
        ret = AVERROR(ENOMEM);
        goto cleanup;
      }
      filter_graph->hw_device_ctx = av_buffer_ref(hw_device_ctx);

      const AVFilter* buffersrc = avfilter_get_by_name("buffer");
      const AVFilter* buffersink = avfilter_get_by_name("buffersink");
      const AVFilter* scale = avfilter_get_by_name("scale_cuda");
      if (!buffersrc || !buffersink || !scale) {
        error = "required filter missing (buffer/buffersink/scale_cuda)";
        ret = AVERROR_FILTER_NOT_FOUND;
        goto cleanup;
      }

      char args[512];
      snprintf(args, sizeof(args),
               "video_size=%dx%d:pix_fmt=%d:time_base=%d/%d:pixel_aspect=%d/%d",
               decoder_ctx->width,
               decoder_ctx->height,
               decoder_ctx->pix_fmt,
               decoder_ctx->time_base.num,
               decoder_ctx->time_base.den,
               decoder_ctx->sample_aspect_ratio.num,
               decoder_ctx->sample_aspect_ratio.den);

      if ((ret = avfilter_graph_create_filter(&buffersrc_ctx, buffersrc, "in", args, nullptr, filter_graph)) < 0) {
        error = "create buffer source: " + AvErrorToString(ret);
        goto cleanup;
      }

      if ((ret = avfilter_graph_create_filter(&buffersink_ctx, buffersink, "out", nullptr, nullptr, filter_graph)) < 0) {
        error = "create buffer sink: " + AvErrorToString(ret);
        goto cleanup;
      }

      const enum AVPixelFormat pix_fmts[] = { AV_PIX_FMT_CUDA, AV_PIX_FMT_NONE };
      if ((ret = av_opt_set_int_list(buffersink_ctx, "pix_fmts", pix_fmts, AV_PIX_FMT_NONE, AV_OPT_SEARCH_CHILDREN)) < 0) {
        error = "buffersink pix fmts: " + AvErrorToString(ret);
        goto cleanup;
      }

      AVBufferSrcParameters* src_params = av_buffersrc_parameters_alloc();
      if (!src_params) {
        error = "alloc buffersrc params";
        ret = AVERROR(ENOMEM);
        goto cleanup;
      }
      src_params->hw_frames_ctx = av_buffer_ref(decoder_ctx->hw_frames_ctx);
      if ((ret = av_buffersrc_parameters_set(buffersrc_ctx, src_params)) < 0) {
        av_freep(&src_params);
        error = "buffersrc params: " + AvErrorToString(ret);
        goto cleanup;
      }
      av_freep(&src_params);

      char scale_args[128];
      snprintf(scale_args, sizeof(scale_args), "w=%d:h=%d:format=nv12", out_w, out_h);
      AVFilterContext* scale_ctx = nullptr;
      if ((ret = avfilter_graph_create_filter(&scale_ctx, scale, "scale", scale_args, nullptr, filter_graph)) < 0) {
        error = "create scale_cuda: " + AvErrorToString(ret);
        goto cleanup;
      }

      if ((ret = avfilter_link(buffersrc_ctx, 0, scale_ctx, 0)) < 0) {
        error = "link buffer -> scale: " + AvErrorToString(ret);
        goto cleanup;
      }
      if ((ret = avfilter_link(scale_ctx, 0, buffersink_ctx, 0)) < 0) {
        error = "link scale -> sink: " + AvErrorToString(ret);
        goto cleanup;
      }

      if ((ret = avfilter_graph_config(filter_graph, nullptr)) < 0) {
        error = "config filter graph: " + AvErrorToString(ret);
        goto cleanup;
      }

      AVBufferRef* sink_hw_frames = av_buffersink_get_hw_frames_ctx(buffersink_ctx);
      if (!sink_hw_frames) {
        error = "buffersink missing hw frames context";
        ret = AVERROR(EINVAL);
        goto cleanup;
      }
      encoder_ctx->hw_frames_ctx = av_buffer_ref(sink_hw_frames);
    }

    if ((ret = avcodec_open2(encoder_ctx, encoder, nullptr)) < 0) {
      error = "open encoder: " + AvErrorToString(ret);
      goto cleanup;
    }

    AVStream* output_stream = avformat_new_stream(output_ctx, nullptr);
    if (!output_stream) {
      error = "alloc output stream";
      ret = AVERROR(ENOMEM);
      goto cleanup;
    }
    output_stream->time_base = encoder_ctx->time_base;
    if ((ret = avcodec_parameters_from_context(output_stream->codecpar, encoder_ctx)) < 0) {
      error = "stream params: " + AvErrorToString(ret);
      goto cleanup;
    }
    output_stream->codecpar->codec_tag = 0;

    if (input_audio_stream) {
      output_audio_stream = avformat_new_stream(output_ctx, nullptr);
      if (!output_audio_stream) {
        error = "alloc audio stream";
        ret = AVERROR(ENOMEM);
        goto cleanup;
      }
      if ((ret = avcodec_parameters_copy(output_audio_stream->codecpar, input_audio_stream->codecpar)) < 0) {
        error = "audio params copy: " + AvErrorToString(ret);
        goto cleanup;
      }
      output_audio_stream->codecpar->codec_tag = 0;
      output_audio_stream->time_base = input_audio_stream->time_base;
    }

    if (!(output_ctx->oformat->flags & AVFMT_NOFILE)) {
      if ((ret = avio_open(&output_ctx->pb, segment.output_path.c_str(), AVIO_FLAG_WRITE)) < 0) {
        error = "open output file: " + AvErrorToString(ret);
        goto cleanup;
      }
    }

    if ((ret = avformat_write_header(output_ctx, nullptr)) < 0) {
      error = "write header: " + AvErrorToString(ret);
      goto cleanup;
    }

    packet = av_packet_alloc();
    out_packet = av_packet_alloc();
    frame = av_frame_alloc();
    filt_frame = av_frame_alloc();
    resize_frame = av_frame_alloc();
    if (!packet || !out_packet || !frame || !filt_frame || !resize_frame) {
      error = "alloc packets/frames";
      ret = AVERROR(ENOMEM);
      goto cleanup;
    }

    int64_t start_ts = 0;
    int64_t end_ts = INT64_MAX;
    int64_t audio_start_ts = 0;
    int64_t audio_end_ts = INT64_MAX;
    if (segment.start_ms > 0) {
      start_ts = av_rescale_q(segment.start_ms, AV_TIME_BASE_Q, input_stream->time_base);
      av_seek_frame(input_ctx, video_stream_index, start_ts, AVSEEK_FLAG_BACKWARD);
      avcodec_flush_buffers(decoder_ctx);
      if (input_audio_stream) {
        audio_start_ts = av_rescale_q(segment.start_ms, AV_TIME_BASE_Q, input_audio_stream->time_base);
      }
    }
    if (segment.duration_ms > 0) {
      int64_t dur_ts = av_rescale_q(segment.duration_ms, AV_TIME_BASE_Q, input_stream->time_base);
      end_ts = start_ts + dur_ts;
      if (input_audio_stream) {
        int64_t audio_dur = av_rescale_q(segment.duration_ms, AV_TIME_BASE_Q, input_audio_stream->time_base);
        audio_end_ts = audio_start_ts + audio_dur;
      }
    }

    bool reached_end = false;
    while (!reached_end && (ret = av_read_frame(input_ctx, packet)) >= 0) {
      if (packet->stream_index == audio_stream_index && output_audio_stream) {
        int64_t pts = packet->pts != AV_NOPTS_VALUE ? packet->pts : packet->dts;
        if (pts != AV_NOPTS_VALUE) {
          if (pts < audio_start_ts) {
            av_packet_unref(packet);
            continue;
          }
          if (pts > audio_end_ts) {
            av_packet_unref(packet);
            continue;
          }
          if (packet->pts != AV_NOPTS_VALUE) {
            packet->pts -= audio_start_ts;
          }
          if (packet->dts != AV_NOPTS_VALUE) {
            packet->dts -= audio_start_ts;
          }
        }
        packet->stream_index = output_audio_stream->index;
        av_packet_rescale_ts(packet, input_audio_stream->time_base, output_audio_stream->time_base);
        if ((ret = av_interleaved_write_frame(output_ctx, packet)) < 0) {
          error = "write audio frame: " + AvErrorToString(ret);
          goto cleanup;
        }
        av_packet_unref(packet);
        continue;
      }

      if (packet->stream_index != video_stream_index) {
        av_packet_unref(packet);
        continue;
      }

      if ((ret = avcodec_send_packet(decoder_ctx, packet)) < 0) {
        error = "send packet: " + AvErrorToString(ret);
        goto cleanup;
      }
      av_packet_unref(packet);

      while ((ret = avcodec_receive_frame(decoder_ctx, frame)) >= 0) {
        int64_t pts = frame->best_effort_timestamp;
        if (pts != AV_NOPTS_VALUE && pts < start_ts) {
          av_frame_unref(frame);
          continue;
        }
        if (pts != AV_NOPTS_VALUE && pts > end_ts) {
          reached_end = true;
          av_frame_unref(frame);
          break;
        }

        if (pts != AV_NOPTS_VALUE) {
          frame->pts = pts - start_ts;
        }

        AVFrame* encode_frame = nullptr;

        if (use_cuda_resize) {
          int crop_x = job.crop_x;
          int crop_y = job.crop_y;
          int crop_w = job.crop_w > 0 ? job.crop_w : frame->width - crop_x;
          int crop_h = job.crop_h > 0 ? job.crop_h : frame->height - crop_y;
          crop_w = crop_w > 0 ? crop_w : frame->width;
          crop_h = crop_h > 0 ? crop_h : frame->height;

          if ((ret = av_hwframe_get_buffer(encoder_ctx->hw_frames_ctx, resize_frame, 0)) < 0) {
            error = "alloc resize frame: " + AvErrorToString(ret);
            goto cleanup;
          }

          CudaNv12View src_view = MakeNv12View(frame);
          CudaNv12View dst_view = MakeNv12View(resize_frame);
          LaunchResizeCropNv12(src_view, dst_view, crop_x, crop_y, crop_w, crop_h, stream);
          if (!CheckCuda(cudaGetLastError(), error, "cuda resize")) {
            goto cleanup;
          }
          if (!CheckCuda(cudaStreamSynchronize(stream), error, "cuda sync")) {
            goto cleanup;
          }
          resize_frame->pts = frame->pts;
          encode_frame = resize_frame;
        } else {
          if ((ret = av_buffersrc_add_frame_flags(buffersrc_ctx, frame, AV_BUFFERSRC_FLAG_KEEP_REF)) < 0) {
            error = "buffersrc add frame: " + AvErrorToString(ret);
            goto cleanup;
          }
          av_frame_unref(frame);

          if ((ret = av_buffersink_get_frame(buffersink_ctx, filt_frame)) < 0) {
            if (ret == AVERROR(EAGAIN) || ret == AVERROR_EOF) {
              ret = 0;
              continue;
            }
            error = "buffersink get frame: " + AvErrorToString(ret);
            goto cleanup;
          }
          encode_frame = filt_frame;
        }

        if (!ApplyCudaFilters(encode_frame, job, *assets, stream, error)) {
          goto cleanup;
        }

        if ((ret = avcodec_send_frame(encoder_ctx, encode_frame)) < 0) {
          error = "send frame: " + AvErrorToString(ret);
          goto cleanup;
        }

        if (encode_frame == resize_frame) {
          av_frame_unref(resize_frame);
        } else {
          av_frame_unref(filt_frame);
        }

        while ((ret = avcodec_receive_packet(encoder_ctx, out_packet)) >= 0) {
          av_packet_rescale_ts(out_packet, encoder_ctx->time_base, output_stream->time_base);
          out_packet->stream_index = output_stream->index;
          if ((ret = av_interleaved_write_frame(output_ctx, out_packet)) < 0) {
            error = "write frame: " + AvErrorToString(ret);
            goto cleanup;
          }
          av_packet_unref(out_packet);
        }
        if (ret == AVERROR(EAGAIN) || ret == AVERROR_EOF) {
          ret = 0;
        }
      }
      if (ret == AVERROR(EAGAIN) || ret == AVERROR_EOF) {
        ret = 0;
      }
    }
    if (ret == AVERROR_EOF) {
      ret = 0;
    }

    if (use_cuda_resize) {
      avcodec_send_packet(decoder_ctx, nullptr);
      while ((ret = avcodec_receive_frame(decoder_ctx, frame)) >= 0) {
        int64_t pts = frame->best_effort_timestamp;
        if (pts != AV_NOPTS_VALUE && pts < start_ts) {
          av_frame_unref(frame);
          continue;
        }
        if (pts != AV_NOPTS_VALUE && pts > end_ts) {
          av_frame_unref(frame);
          break;
        }

        if (pts != AV_NOPTS_VALUE) {
          frame->pts = pts - start_ts;
        }

        int crop_x = job.crop_x;
        int crop_y = job.crop_y;
        int crop_w = job.crop_w > 0 ? job.crop_w : frame->width - crop_x;
        int crop_h = job.crop_h > 0 ? job.crop_h : frame->height - crop_y;
        crop_w = crop_w > 0 ? crop_w : frame->width;
        crop_h = crop_h > 0 ? crop_h : frame->height;

        if ((ret = av_hwframe_get_buffer(encoder_ctx->hw_frames_ctx, resize_frame, 0)) < 0) {
          error = \"alloc resize frame: \" + AvErrorToString(ret);
          goto cleanup;
        }

        CudaNv12View src_view = MakeNv12View(frame);
        CudaNv12View dst_view = MakeNv12View(resize_frame);
        LaunchResizeCropNv12(src_view, dst_view, crop_x, crop_y, crop_w, crop_h, stream);
        if (!CheckCuda(cudaGetLastError(), error, \"cuda resize\")) {
          goto cleanup;
        }
        if (!CheckCuda(cudaStreamSynchronize(stream), error, \"cuda sync\")) {
          goto cleanup;
        }
        resize_frame->pts = frame->pts;

        if (!ApplyCudaFilters(resize_frame, job, *assets, stream, error)) {
          goto cleanup;
        }

        if ((ret = avcodec_send_frame(encoder_ctx, resize_frame)) < 0) {
          error = \"send frame: \" + AvErrorToString(ret);
          goto cleanup;
        }
        av_frame_unref(resize_frame);

        while ((ret = avcodec_receive_packet(encoder_ctx, out_packet)) >= 0) {
          av_packet_rescale_ts(out_packet, encoder_ctx->time_base, output_stream->time_base);
          out_packet->stream_index = output_stream->index;
          if ((ret = av_interleaved_write_frame(output_ctx, out_packet)) < 0) {
            error = \"write frame: \" + AvErrorToString(ret);
            goto cleanup;
          }
          av_packet_unref(out_packet);
        }
        if (ret == AVERROR(EAGAIN) || ret == AVERROR_EOF) {
          ret = 0;
        }
      }
    } else {
      avcodec_send_packet(decoder_ctx, nullptr);
      while ((ret = avcodec_receive_frame(decoder_ctx, frame)) >= 0) {
        int64_t pts = frame->best_effort_timestamp;
        if (pts != AV_NOPTS_VALUE && pts < start_ts) {
          av_frame_unref(frame);
          continue;
        }
        if (pts != AV_NOPTS_VALUE && pts > end_ts) {
          av_frame_unref(frame);
          break;
        }
        if (pts != AV_NOPTS_VALUE) {
          frame->pts = pts - start_ts;
        }
        if ((ret = av_buffersrc_add_frame_flags(buffersrc_ctx, frame, AV_BUFFERSRC_FLAG_KEEP_REF)) < 0) {
          break;
        }
        av_frame_unref(frame);
        while ((ret = av_buffersink_get_frame(buffersink_ctx, filt_frame)) >= 0) {
          if (!ApplyCudaFilters(filt_frame, job, *assets, stream, error)) {
            goto cleanup;
          }
          avcodec_send_frame(encoder_ctx, filt_frame);
          av_frame_unref(filt_frame);
          while ((ret = avcodec_receive_packet(encoder_ctx, out_packet)) >= 0) {
            av_packet_rescale_ts(out_packet, encoder_ctx->time_base, output_stream->time_base);
            out_packet->stream_index = output_stream->index;
            av_interleaved_write_frame(output_ctx, out_packet);
            av_packet_unref(out_packet);
          }
        }
      }
    }

    avcodec_send_frame(encoder_ctx, nullptr);
    while ((ret = avcodec_receive_packet(encoder_ctx, out_packet)) >= 0) {
      av_packet_rescale_ts(out_packet, encoder_ctx->time_base, output_stream->time_base);
      out_packet->stream_index = output_stream->index;
      av_interleaved_write_frame(output_ctx, out_packet);
      av_packet_unref(out_packet);
    }

    av_write_trailer(output_ctx);
    ret = 0;

  cleanup:
    if (packet) {
      av_packet_free(&packet);
    }
    if (out_packet) {
      av_packet_free(&out_packet);
    }
    if (frame) {
      av_frame_free(&frame);
    }
    if (filt_frame) {
      av_frame_free(&filt_frame);
    }
    if (resize_frame) {
      av_frame_free(&resize_frame);
    }
    if (filter_graph) {
      avfilter_graph_free(&filter_graph);
    }
    if (decoder_ctx) {
      avcodec_free_context(&decoder_ctx);
    }
    if (encoder_ctx) {
      avcodec_free_context(&encoder_ctx);
    }
    if (encoder_frames_ctx) {
      av_buffer_unref(&encoder_frames_ctx);
    }
    if (input_ctx) {
      avformat_close_input(&input_ctx);
    }
    if (output_ctx) {
      if (!(output_ctx->oformat->flags & AVFMT_NOFILE) && output_ctx->pb) {
        avio_closep(&output_ctx->pb);
      }
      avformat_free_context(output_ctx);
    }
    if (hw_device_ctx) {
      av_buffer_unref(&hw_device_ctx);
    }

    if (ret < 0 && error.empty()) {
      error = AvErrorToString(ret);
    }
    return ret >= 0;
  }
};

static std::string StateToString(JobState state) {
  switch (state) {
    case JobState::Queued:
      return "queued";
    case JobState::Running:
      return "running";
    case JobState::Succeeded:
      return "succeeded";
    case JobState::Failed:
      return "failed";
    default:
      return "unknown";
  }
}

static bool ParseJob(const crow::json::rvalue& body, RenderJob& job, std::string& error) {
  if (!body.has("input_path") || !body.has("output_path")) {
    error = "input_path and output_path required";
    return false;
  }

  job.input_path = body["input_path"].s();
  job.output_path = body["output_path"].s();
  if (body.has("input_url")) {
    job.input_url = body["input_url"].s();
  }
  if (body.has("output_upload_url")) {
    job.output_upload_url = body["output_upload_url"].s();
  }
  if (body.has("output_content_type")) {
    job.output_content_type = body["output_content_type"].s();
  }
  job.codec = body.has("codec") ? body["codec"].s() : "h264";
  job.width = body.has("width") ? body["width"].i() : 0;
  job.height = body.has("height") ? body["height"].i() : 0;
  job.bitrate_kbps = body.has("bitrate_kbps") ? body["bitrate_kbps"].i() : 0;
  job.crop_x = body.has("crop_x") ? body["crop_x"].i() : 0;
  job.crop_y = body.has("crop_y") ? body["crop_y"].i() : 0;
  job.crop_w = body.has("crop_w") ? body["crop_w"].i() : 0;
  job.crop_h = body.has("crop_h") ? body["crop_h"].i() : 0;
  job.use_cuda_resize = body.has("use_cuda_resize") ? body["use_cuda_resize"].b() : false;
  job.parallel_segments = body.has("parallel_segments") ? body["parallel_segments"].i() : 1;

  if (body.has("watermark")) {
    auto wm = body["watermark"];
    if (wm.has("path")) {
      job.watermark.path = wm["path"].s();
    }
    job.watermark.width = wm.has("width") ? wm["width"].i() : 0;
    job.watermark.height = wm.has("height") ? wm["height"].i() : 0;
    job.watermark.x = wm.has("x") ? wm["x"].i() : 0;
    job.watermark.y = wm.has("y") ? wm["y"].i() : 0;
    job.watermark.alpha = wm.has("alpha") ? static_cast<float>(wm["alpha"].d()) : 1.0f;
  }

  if (body.has("lut")) {
    auto lut = body["lut"];
    if (lut.has("path")) {
      job.lut.path = lut["path"].s();
    }
    job.lut.size = lut.has("size") ? lut["size"].i() : 0;
  }

  if (body.has("segments") && body["segments"].t() == crow::json::type::List) {
    int idx = 0;
    for (const auto& seg : body["segments"]) {
      SegmentPlan plan;
      plan.start_ms = seg.has("start_ms") ? seg["start_ms"].i() : 0;
      plan.duration_ms = seg.has("duration_ms") ? seg["duration_ms"].i() : -1;
      if (seg.has("output_path")) {
        plan.output_path = seg["output_path"].s();
      } else {
        plan.output_path = MakeSegmentPath(job.output_path, idx);
      }
      job.segments.push_back(std::move(plan));
      idx++;
    }
  }

  return true;
}

int main(int argc, char** argv) {
  av_log_set_level(AV_LOG_ERROR);
  avformat_network_init();
  curl_global_init(CURL_GLOBAL_DEFAULT);

  JobQueue queue;
  GpuTranscoder transcoder;
  std::atomic<bool> running{true};

  int worker_threads = 1;
  if (const char* env = std::getenv("WORKER_THREADS")) {
    worker_threads = std::max(1, std::atoi(env));
  }

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(worker_threads));
  for (int i = 0; i < worker_threads; ++i) {
    workers.emplace_back([&] {
      while (running.load()) {
        RenderJob job;
        if (!queue.WaitDequeue(job)) {
          break;
        }
        std::string error;
        std::string cleanup_input;
        std::string cleanup_output;
        bool ok = true;
        if (!job.input_url.empty()) {
          if (!DownloadToFile(job.input_url, job.input_path, error)) {
            ok = false;
          } else {
            cleanup_input = job.input_path;
          }
        }
        if (ok && !EnsureParentDir(job.output_path, error)) {
          ok = false;
        }
        if (ok && transcoder.Transcode(job, error)) {
          if (!job.output_upload_url.empty()) {
            const std::string content_type =
              job.output_content_type.empty() ? "application/octet-stream" : job.output_content_type;
            cleanup_output = job.output_path;
            if (!UploadFileToUrl(job.output_upload_url, job.output_path, content_type, error)) {
              ok = false;
            }
          }
        } else if (ok) {
          ok = false;
        }

        if (!cleanup_input.empty()) {
          std::error_code ec;
          fs::remove(cleanup_input, ec);
        }
        if (!cleanup_output.empty()) {
          std::error_code ec;
          fs::remove(cleanup_output, ec);
        }

        if (ok) {
          queue.Complete(job.id);
        } else {
          queue.Fail(job.id, error);
        }
      }
    });
  }

  crow::SimpleApp app;

  CROW_ROUTE(app, "/health")([] {
    return crow::response(200, "ok");
  });

  CROW_ROUTE(app, "/render").methods("POST"_method)([&](const crow::request& req) {
    auto body = crow::json::load(req.body);
    if (!body) {
      return crow::response(400, "invalid json");
    }

    RenderJob job;
    std::string error;
    if (!ParseJob(body, job, error)) {
      return crow::response(400, error);
    }

    auto id = queue.Enqueue(job);

    crow::json::wvalue res;
    res["job_id"] = id;
    res["status"] = "queued";
    return crow::response{res};
  });

  CROW_ROUTE(app, "/jobs/<string>")([&](const std::string& id) {
    JobStatus status;
    if (!queue.GetStatus(id, status)) {
      return crow::response(404, "job not found");
    }
    crow::json::wvalue res;
    res["job_id"] = id;
    res["status"] = StateToString(status.state);
    if (status.state == JobState::Failed) {
      res["error"] = status.error;
    }
    return crow::response{res};
  });

  int port = 7001;
  if (const char* env = std::getenv("WORKER_PORT")) {
    port = std::atoi(env);
  }

  app.port(port).multithreaded().run();

  running.store(false);
  queue.Stop();
  for (auto& t : workers) {
    t.join();
  }
  curl_global_cleanup();
  return 0;
}
