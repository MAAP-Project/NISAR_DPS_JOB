#!/usr/bin/env bash
set -euo pipefail

# Positional args (Option B)
# 1: access_mode   (https|s3|auto)
# 2: vars          (e.g., HHHH or HHHH,HVHV)
# 3: https_href    (https://...)
# 4: s3_href       (s3://...)   (optional)
# 5: bbox          (minx,miny,maxx,maxy) (optional)
# 6: bbox_crs      (e.g., EPSG:32615)    (optional)
# 7: out_dir       (optional; if blank -> /tmp/output)
# 8: out_name      (e.g., nisar_subset.zarr) (optional)

ACCESS_MODE="${1:-https}"
VARS="${2:-HHHH}"
HTTPS_HREF="${3:-}"
S3_HREF="${4:-}"
BBOX="${5:-}"
BBOX_CRS="${6:-}"
OUT_DIR="${7:-/tmp/output}"
OUT_NAME="${8:-nisar_subset.zarr}"

mkdir -p "${OUT_DIR}"

# Run inside the container image we built
docker run --rm \
  -v "${OUT_DIR}:${OUT_DIR}" \
  nisar_dps_job \
  python /app/nisar_access_subset.py \
    --access_mode "${ACCESS_MODE}" \
    --vars "${VARS}" \
    --https_href "${HTTPS_HREF}" \
    --s3_href "${S3_HREF}" \
    --bbox "${BBOX}" \
    --bbox_crs "${BBOX_CRS}" \
    --out_dir "${OUT_DIR}" \
    --out_name "${OUT_NAME}"
