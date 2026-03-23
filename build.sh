#!/bin/bash
set -ex

mkdir -p /opt/app

cp -v nisar_access_subset.py /opt/app/nisar_access_subset.py
cp -v nisar_access_subset.cwl /opt/app/nisar_access_subset.cwl
cp -v run.sh /opt/app/run.sh

chmod 755 /opt/app/nisar_access_subset.py
chmod 755 /opt/app/run.sh
