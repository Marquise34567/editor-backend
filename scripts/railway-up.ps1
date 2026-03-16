param(
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$Args
)

$ErrorActionPreference = "Stop"

# Work around buildx progress panics by forcing plain progress output.
$env:BUILDKIT_PROGRESS = "plain"
$env:DOCKER_BUILDKIT = "1"

& railway @Args
