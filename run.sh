#!/usr/bin/env bash
set -euo pipefail

echo "[run.sh] Starting NISAR subset job..."
echo "[run.sh] Args: $*"

# Always ensure output dir exists (matches CWL defaults)
OUT_DIR="${OUTPUT_DIR:-/tmp/output}"
mkdir -p "$OUT_DIR"

PY="/opt/app/nisar_access_subset.py"

if [[ ! -f "$PY" ]]; then
  echo "[run.sh] ERROR: Missing $PY (did build.sh copy it?)"
  exit 2
fi

# ------------------------------------------------------------
# Option B: allow positional args from UI like:
#   run.sh https HHHH <https_href> <s3_href> <bbox> <bbox_crs> <out_name>
# ------------------------------------------------------------
if [[ $# -ge 2 && "$1" != --* ]]; then
  ACCESS_MODE="${1:-auto}"
  VARS="${2:-HHHH}"
  HTTPS_HREF="${3:-}"
  S3_HREF="${4:-}"
  BBOX="${5:-}"
  BBOX_CRS="${6:-}"
  OUT_NAME="${7:-nisar_subset.zarr}"

  CMD=(python "$PY"
    --access_mode "$ACCESS_MODE"
    --vars "$VARS"
    --out_dir "$OUT_DIR"
    --out_name "$OUT_NAME"
  )

  # Only pass hrefs if non-empty (prevents HySDS from trying to "localize" empty strings)
  [[ -n "$HTTPS_HREF" ]] && CMD+=(--https_href "$HTTPS_HREF")
  [[ -n "$S3_HREF" ]] && CMD+=(--s3_href "$S3_HREF")
  [[ -n "$BBOX" ]] && CMD+=(--bbox "$BBOX")
  [[ -n "$BBOX_CRS" ]] && CMD+=(--bbox_crs "$BBOX_CRS")

  echo "[run.sh] Running (positional-mode): ${CMD[*]}"
  "${CMD[@]}"

else
  # ------------------------------------------------------------
  # Flag-style passthrough:
  #   run.sh --access_mode https --vars HHHH --https_href ... --out_name ...
  # ------------------------------------------------------------
  echo "[run.sh] Running (flag-mode): python $PY $*"
  python "$PY" "$@"
fi

echo "[run.sh] Listing outputs in $OUT_DIR"
ls -lah "$OUT_DIR" || true

echo "[run.sh] Done."
