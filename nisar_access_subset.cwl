cwlVersion: v1.2
class: CommandLineTool
label: nisar_access_subset
baseCommand:
  - python
  - /opt/app/nisar_access_subset.py

inputs:
  access_mode:
    type: string
    default: auto
    inputBinding:
      prefix: --access_mode
  https_href:
    type:
      - "null"
      - string
    default: ""
    inputBinding:
      prefix: --https_href
  s3_href:
    type:
      - "null"
      - string
    default: ""
    inputBinding:
      prefix: --s3_href
  vars:
    type: string
    default: HHHH
    inputBinding:
      prefix: --vars
  group:
    type: string
    default: /science/LSAR/GCOV/grids/frequencyA
    inputBinding:
      prefix: --group
  bbox:
    type:
      - "null"
      - string
    default: ""
    inputBinding:
      prefix: --bbox
  bbox_crs:
    type:
      - "null"
      - string
    default: ""
    inputBinding:
      prefix: --bbox_crs
  out_dir:
    type: string
    default: $(runtime.outdir)
    inputBinding:
      prefix: --out_dir
  out_name:
    type: string
    default: nisar_subset.zarr
    inputBinding:
      prefix: --out_name

outputs:
  zarr_store:
    type: Directory
    outputBinding:
      glob: $(inputs.out_name)
  manifest:
    type: File
    outputBinding:
      glob: manifest.json
