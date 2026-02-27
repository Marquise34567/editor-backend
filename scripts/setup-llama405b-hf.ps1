param(
  [string]$ModelId = "meta-llama/Meta-Llama-3.1-405B-Instruct",
  [string]$OutDir = ".\models\Meta-Llama-3.1-405B-Instruct",
  [switch]$RunDownload
)

Write-Host "Llama 3.1 405B local setup helper" -ForegroundColor Cyan
Write-Host "Model: $ModelId"
Write-Host "Target directory: $OutDir"
Write-Host ""
Write-Host "WARNING:" -ForegroundColor Yellow
Write-Host "- This model is very large (commonly 800+ GB depending on precision/sharding)." -ForegroundColor Yellow
Write-Host "- Real-time inference commonly requires multi-GPU server-class hardware (often 8x A100 class)." -ForegroundColor Yellow
Write-Host "- For production APIs, hosted inference is usually more practical." -ForegroundColor Yellow
Write-Host ""

if (-not $RunDownload) {
  Write-Host "Dry run only. Re-run with -RunDownload to execute download."
  exit 0
}

if (-not (Get-Command huggingface-cli -ErrorAction SilentlyContinue)) {
  Write-Error "huggingface-cli not found. Install with: pip install -U huggingface_hub"
  exit 1
}

Write-Host "Starting download..."
huggingface-cli download $ModelId --local-dir $OutDir --resume-download

if ($LASTEXITCODE -ne 0) {
  Write-Error "Download failed."
  exit $LASTEXITCODE
}

Write-Host "Download completed." -ForegroundColor Green
