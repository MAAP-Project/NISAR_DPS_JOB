# NISAR_DPS_JOB

A MAAP DPS / OGC Application Package for accessing a NISAR GCOV granule, subsetting selected variables, optionally subsetting by bounding box, and writing the result to an intermediate Zarr store.

## What this job does

This job:

- opens a NISAR GCOV granule from either S3 or HTTPS
- reads one or more variables from `/science/LSAR/GCOV/grids/frequencyA`
- optionally subsets the data using a bounding box
- writes the subset to a Zarr output
- writes a small manifest JSON alongside the Zarr output

The current workflow is designed for MAAP DPS execution and is intended to support cloud-based NISAR access and downstream processing.

---

## Current access behavior

The job supports three access modes:

- `s3`  
  Uses MAAP / ASF temporary S3 credentials and reads the granule from S3.

- `https`  
  Uses HTTPS only when non-interactive Earthaccess credentials are available.

- `auto`  
  Prefers S3 first, then HTTPS only if non-interactive Earthaccess credentials are available.

