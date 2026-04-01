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


`https://nisar.asf.earthdatacloud.nasa.gov/NISAR/NISAR_L2_GCOV_BETA_V1/NISAR_L2_PR_GCOV_003_005_D_077_4005_DHDH_A_20251017T132451_20251017T132526_X05007_N_F_J_001/NISAR_L2_PR_GCOV_003_005_D_077_4005_DHDH_A_20251017T132451_20251017T132526_X05007_N_F_J_001.h5 `



` s3://sds-n-cumulus-prod-nisar-products/NISAR_L2_GCOV_BETA_V1/NISAR_L2_PR_GCOV_002_109_D_063_4005_DHDH_A_20251012T182508_20251012T182531_X05010_N_P_J_001/NISAR_L2_PR_GCOV_002_109_D_063_4005_DHDH_A_20251012T182508_20251012T182531_X05010_N_P_J_001.h5 `
`

`access_mode = s3`
`vars = HHHH`
`group = /science/LSAR/GCOV/grids/frequencyA`
`bbox = 148325,5392805,519115,5759995`
`bbox_crs = EPSG:32633`
`out_name = nisar_subset.zarr`
