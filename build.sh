# !/usr/bin/env bash
set -euo pipefail

basedir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_PREFIX="/opt/conda/envs/nisar_subset_env"

# Create conda env from env.yml (lives next to this script)
conda env create -f "${basedir}/env.yml" --prefix "$ENV_PREFIX"
conda clean -afy

# Sanity check
conda run -p "$ENV_PREFIX" python - <<'PY'
import numpy, xarray, h5py, zarr, fsspec, s3fs, requests
import earthaccess
from maap.maap import MAAP
print("Conda env OK")
PY

echo "Built conda env at $ENV_PREFIX"
