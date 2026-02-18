# NISAR_DPS_JOB (GCOV Access + Subset)
    algo_id="nisar_access_subset",
    version="main",
    queue="maap-dps-worker-8gb",
    access_mode="https",
    vars="HHHH",
    https_href="https://nisar.asf.earthdatacloud.nasa.gov/NISAR/NISAR_L2_GCOV_BETA_V1/NISAR_L2_PR_GCOV_003_005_D_077_4005_DHDH_A_20251017T132451_20251017T132526_X05007_N_F_J_001/NISAR_L2_PR_GCOV_003_005_D_077_4005_DHDH_A_20251017T132451_20251017T132526_X05007_N_F_J_001.h5",
    s3_href="s3://sds-n-cumulus-prod-nisar-products/NISAR_L2_GCOV_BETA_V1/NISAR_L2_PR_GCOV_003_005_D_077_4005_DHDH_A_20251017T132451_20251017T132526_X05007_N_F_J_001/NISAR_L2_PR_GCOV_003_005_D_077_4005_DHDH_A_20251017T132451_20251017T132526_X05007_N_F_J_001.h5",
    bbox="",
    bbox_crs="",
    out_name="nisar_subset.zarr")
