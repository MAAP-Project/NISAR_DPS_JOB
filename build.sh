# !/usr/bin/env bash
set -euo pipefail

echo "[build.sh] Installing Python deps..."

# If you have requirements.txt in repo root:
pip install --no-cache-dir -r requirements.txt

echo "[build.sh] Creating /opt/app and staging algorithm files..."

mkdir -p /opt/app

# Copy your algorithm python + CWL into /opt/app
# (update filenames if yours differ)
cp -v nisar_access_subset.py /opt/app/nisar_access_subset.py
cp -v nisar_access_subset.cwl /opt/app/nisar_access_subset.cwl

chmod 755 /opt/app/nisar_access_subset.py

echo "[build.sh] Done."
