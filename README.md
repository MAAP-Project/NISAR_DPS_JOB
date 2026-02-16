# NISAR_DPS_JOB (GCOV Access + Subset)

This repo packages a MAAP DPS algorithm that:
1) Discovers a NISAR L2 GCOV Beta granule using earthaccess,
2) Opens the same granule via either:
   - direct S3 streaming (MAAP ADE/DPS using maap-py temp creds), or
   - authenticated HTTPS streaming (portable earthaccess flow),
3) Reads a small window from a selected covariance variable (e.g., HHHH),
4) Writes a NumPy output to the DPS output directory.

Based on the MAAP "NISAR Access and Exploration" tutorial.
