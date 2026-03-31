#!/usr/bin/env bash
set -euo pipefail

basedir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
PY='conda run --live-stream -p /opt/conda/envs/nisar_access_subset python'

mkdir -p output

ACCESS_MODE="${1:-auto}"
HTTPS_HREF="${2:-}"
S3_HREF="${3:-}"
VARS="${4:-HHHH}"
GROUP="${5:-/science/LSAR/GCOV/grids/frequencyA}"
BBOX="${6:-}"
BBOX_CRS="${7:-}"
OUT_NAME="${8:-nisar_subset.zarr}"

ARGS=(
  "--access_mode" "${ACCESS_MODE}"
  "--vars" "${VARS}"
  "--group" "${GROUP}"
  "--out_dir" "output"
  "--out_name" "${OUT_NAME}"
)

[[ -n "${HTTPS_HREF}" ]] && ARGS+=("--https_href" "${HTTPS_HREF}")
[[ -n "${S3_HREF}" ]] && ARGS+=("--s3_href" "${S3_HREF}")
[[ -n "${BBOX}" ]] && ARGS+=("--bbox" "${BBOX}")
[[ -n "${BBOX_CRS}" ]] && ARGS+=("--bbox_crs" "${BBOX_CRS}")

logfile="_nisar-access-subset.log"

set -x
${PY} "${basedir}/nisar_access_subset.py" "${ARGS[@]}" 2>"${logfile}"
cp -v _stderr.txt _stdout.txt output/ 2>/dev/null || true
mv -v "${logfile}" output/
set +x

find output -maxdepth 3 -print || true
