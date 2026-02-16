# !/usr/bin/env bash
set -euo pipefail

# Build the container image used by the algorithm
docker build -t nisar_dps_job .
