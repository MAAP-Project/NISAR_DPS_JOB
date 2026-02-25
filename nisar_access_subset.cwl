cwlVersion: v1.2
class: CommandLineTool

label: nisar_access_subset
baseCommand: [python, /opt/app/nisar_access_subset.py]

inputs:
  access_mode:
    type: string
    inputBinding: { prefix: --access_mode }
    default: auto

  https_href:
    type: string?
    inputBinding: { prefix: --https_href }

  s3_href:
    type: string?
    inputBinding: { prefix: --s3_href }

  vars:
    type: string
    inputBinding: { prefix: --vars }
    default: HHHH

  group:
    type: string
    inputBinding: { prefix: --group }
    default: /science/LSAR/GCOV/grids/frequencyA

  bbox:
    type: string?
    inputBinding: { prefix: --bbox }

  bbox_crs:
    type: string?
    inputBinding: { prefix: --bbox_crs }

  # Write outputs to the CWL job output directory by default (publishable by HySDS)
  out_dir:
    type: string
    inputBinding: { prefix: --out_dir }
    default: $(runtime.outdir)

  out_name:
    type: string
    inputBinding: { prefix: --out_name }
    default: nisar_subset.zarr

outputs:
  # Outputs are written under out_dir (default: runtime.outdir),
  # so glob can be relative/publish-friendly.
  zarr_store:
    type: Directory
    outputBinding:
      glob: $(inputs.out_name)

  manifest:
    type: File
    outputBinding:
      glob: manifest.json
