#!/usr/bin/env bash
set -euo pipefail

basedir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ENV_PREFIX="/opt/conda/envs/nisar_access_subset"

conda env create -f "${basedir}/env.yml" --prefix "${ENV_PREFIX}"
conda clean -afy

conda run -p "${ENV_PREFIX}" python - <<'PY'
import earthaccess, h5py, numpy, s3fs, xarray, zarr
print("Conda env OK")
PY
