#!/usr/bin/env bash
# azcopy_bulk.sh — Robust AzCopy wrapper with live progress display
# Updated: Adds current folder/file display, progress tracking, and support for overwrite/check-length from env.

set -euo pipefail

### ===== Load environment if available =====
if [[ -f "$HOME/azcopy.env" ]]; then
  echo "🔧 Loading environment from $HOME/azcopy.env..."
  set -a
  . "$HOME/azcopy.env"
  set +a
  echo "SRC_PATH=$SRC_PATH"
  echo "DEST_URL=$DEST_URL"
else
  echo "⚠️ No azcopy.env found — using defaults."
fi

### ===== User defaults =====
SRC_PATH="${SRC_PATH:-$HOME/data}"
DEST_URL="${DEST_URL:-}"
MODE="${MODE:-copy}"
RECURSIVE="${RECURSIVE:-true}"
OVERWRITE="${OVERWRITE:-ifSourceNewer}"     # now read from env
CHECK_LENGTH="${CHECK_LENGTH:-true}"        # now read from env
PUT_MD5="${PUT_MD5:-false}"
CAP_MBPS="${CAP_MBPS:-0}"
CONCURRENCY="${CONCURRENCY:-auto}"
LOG_DIR="${LOG_DIR:-$HOME/.azcopy_logs}"
EXCLUDE_PATTERN="${EXCLUDE_PATTERN:-}"
INCLUDE_PATTERN="${INCLUDE_PATTERN:-}"
DRY_RUN="${DRY_RUN:-false}"
AZCOPY_PATH="${AZCOPY_PATH:-azcopy}"
RETRY_TIMES="${RETRY_TIMES:-1}"
OUTPUT_LEVEL="${OUTPUT_LEVEL:-info}"

if [[ -z "${DEST_URL}" ]]; then
  echo "❌ ERROR: DEST_URL is required."
  exit 1
fi

### ===== Validate paths =====
IS_REMOTE_SRC=false
if [[ "${SRC_PATH}" =~ ^https?:// ]] || [[ "${SRC_PATH}" == *".dfs.core.windows.net"* ]] || [[ "${SRC_PATH}" == *".blob.core.windows.net"* ]]; then
  IS_REMOTE_SRC=true
fi

if [[ "${IS_REMOTE_SRC}" == "false" && ! -e "${SRC_PATH}" ]]; then
  echo "❌ ERROR: SRC_PATH not found: ${SRC_PATH}" >&2
  exit 1
fi

mkdir -p "${LOG_DIR}"
STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_LOG="${LOG_DIR}/run_${STAMP}.log"
MANIFEST_LOG="${LOG_DIR}/manifest_${STAMP}.txt"

if [[ "${CONCURRENCY}" != "auto" ]]; then
  export AZCOPY_CONCURRENCY_VALUE="${CONCURRENCY}"
fi

### ===== Pre-run environment dump =====
echo "==========================================="
echo "🧩 Current AzCopy environment variables"
"${AZCOPY_PATH}" env || echo "⚠️ Unable to query azcopy env"
echo "==========================================="

### ===== Common flags =====
FLAGS=(
  "--recursive=${RECURSIVE}"
  "--cap-mbps=${CAP_MBPS}"
  "--log-level=INFO"
  "--output-level=${OUTPUT_LEVEL}"
  "--check-length=${CHECK_LENGTH}"
)

# Retry compatibility
if "${AZCOPY_PATH}" --help 2>&1 | grep -q -- '--retry-times'; then
  FLAGS+=( "--retry-times=${RETRY_TIMES}" )
else
  export AZCOPY_JOB_RETRY_COUNT="${RETRY_TIMES}"
  echo "ℹ️ Using AZCOPY_JOB_RETRY_COUNT=${RETRY_TIMES} (flag deprecated)"
fi

# Mode and overwrite logic
if [[ "${MODE}" == "copy" ]]; then
  FLAGS+=( "--overwrite=${OVERWRITE}" )
fi

# Additional flags
if [[ "${PUT_MD5}" == "true" && "${IS_REMOTE_SRC}" == "false" ]]; then
  FLAGS+=( "--put-md5" )
fi
if [[ -n "${EXCLUDE_PATTERN}" ]]; then
  FLAGS+=( "--exclude-pattern=${EXCLUDE_PATTERN}" )
fi
if [[ -n "${INCLUDE_PATTERN}" ]]; then
  FLAGS+=( "--include-pattern=${INCLUDE_PATTERN}" )
fi

DIRECTION=$([[ "${IS_REMOTE_SRC}" == "true" ]] && echo "download" || echo "upload")

echo "========== AzCopy Bulk ${MODE^^} (${DIRECTION^^}) =========="
echo "Source       : ${SRC_PATH}"
echo "Destination  : ${DEST_URL}"
echo "Mode         : ${MODE}"
echo "Retries      : ${RETRY_TIMES}"
echo "Overwrite    : ${OVERWRITE}"
echo "Check-Length : ${CHECK_LENGTH}"
echo "Dry Run      : ${DRY_RUN}"
echo "Log File     : ${RUN_LOG}"
echo "Manifest Log : ${MANIFEST_LOG}"
echo "==========================================="

### ===== Build command =====
if [[ "${MODE}" == "sync" ]]; then
  DELETE_DEST="${DELETE_DEST:-false}"
  CMD=( "${AZCOPY_PATH}" sync "${SRC_PATH}" "${DEST_URL}" "--delete-destination=${DELETE_DEST}" )
else
  CMD=( "${AZCOPY_PATH}" copy "${SRC_PATH}" "${DEST_URL}" )
fi
CMD+=( "${FLAGS[@]}" )

echo "Command:"
printf '  %q ' "${CMD[@]}"; echo

### ===== Execute =====
if [[ "${DRY_RUN}" == "true" ]]; then
  echo "💡 DRY_RUN=true — simulation only, no data transfer."
  "${CMD[@]}" --dry-run=true | tee "${RUN_LOG}"
  exit 0
fi

set +e

# Count total files (for progress)
if [[ "${IS_REMOTE_SRC}" == "false" ]]; then
  TOTAL_FILES=$(find "${SRC_PATH}" -type f | wc -l)
else
  TOTAL_FILES=0
fi

PROCESSED=0

# Enhanced progress tracking with validation visibility
"${CMD[@]}" 2>&1 | tee -a "${RUN_LOG}" | awk -v total="${TOTAL_FILES}" -v manifest="${MANIFEST_LOG}" '
BEGIN { processed = 0 }
{
  if ($0 ~ /INFO: Scanning directory/) {
    dir=$0; sub(/.*Scanning directory: /,"",dir)
    printf("[%s] 📂 Scanning: %s\n", strftime("%Y-%m-%d %H:%M:%S"), dir)
  }
  else if ($0 ~ /INFO: / && $0 ~ /(Copying|Validating) file:/) {
    f=$0; sub(/.*(Copying|Validating) file: /,"",f)
    processed++
    percent = (total > 0 ? int((processed/total)*100) : "N/A")
    printf("[%s] 🚚 Processing: %s (%d/%d - %s%%)\n", strftime("%Y-%m-%d %H:%M:%S"), f, processed, total, percent)
    fflush()
  }
  else if ($0 ~ /INFO: / && $0 ~ /Skipped/) {
    f=$0; sub(/.*Skipped file: /,"",f)
    processed++
    percent = (total > 0 ? int((processed/total)*100) : "N/A")
    printf("[%s] ⏭️ Skipped: %s (%d/%d - %s%%)\n", strftime("%Y-%m-%d %H:%M:%S"), f, processed, total, percent)
    fflush()
  }
  else if ($0 ~ /INFO: / && $0 ~ /Copied/) {
    print strftime("[%Y-%m-%d %H:%M:%S]"), $0 >> manifest
  }
  print $0
}
'

EXIT_CODE=${PIPESTATUS[0]}
set -e

### ===== Post-run summary =====
if [[ ${EXIT_CODE} -ne 0 ]]; then
  echo "⚠️  AzCopy exited with ${EXIT_CODE}. Check ${RUN_LOG} for details."
  "${AZCOPY_PATH}" jobs list | tee -a "${RUN_LOG}" || true
  echo "See logs in ~/.azcopy and ${RUN_LOG}"
  exit "${EXIT_CODE}"
fi

echo "✅ Transfer completed successfully."
echo "Manifest saved: ${MANIFEST_LOG}"
echo "Tip: Run 'azcopy list \"${DEST_URL}\"' to validate remote contents."