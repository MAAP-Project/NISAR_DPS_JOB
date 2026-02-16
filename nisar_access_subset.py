#!/usr/bin/env python3
import argparse
import os
import json
from typing import List, Optional, Tuple

import numpy as np
import h5py
import xarray as xr
import earthaccess


DEFAULT_GROUP = "/science/LSAR/GCOV/grids/frequencyA"
DEFAULT_X = f"{DEFAULT_GROUP}/xCoordinates"
DEFAULT_Y = f"{DEFAULT_GROUP}/yCoordinates"


def _split_csv(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def parse_args():
    p = argparse.ArgumentParser(
        description="NISAR GCOV: stream (S3/HTTPS), subset variables, optional bbox subset, write Zarr intermediate."
    )

    # --- How to identify the granule ---
    p.add_argument("--https_href", default="", help="HTTPS href to the HDF5 granule (preferred if running outside MAAP).")
    p.add_argument("--s3_href", default="", help="Direct S3 href to the HDF5 granule (preferred inside MAAP ADE/DPS).")

    # Optional: fallback discovery if hrefs not supplied
    p.add_argument("--short_name", default="NISAR_L2_GCOV_BETA_V1")
    p.add_argument("--count", type=int, default=10)
    p.add_argument("--granule_index", type=int, default=0)

    # --- Access mode ---
    p.add_argument("--access_mode", choices=["auto", "s3", "https"], default="auto",
                   help="auto prefers s3 if available, else https.")
    p.add_argument("--asf_s3_creds_url", default="https://nisar.asf.earthdatacloud.nasa.gov/s3credentials",
                   help="ASF S3 credentials endpoint for MAAP temp creds (used for S3 streaming).")

    # --- What to extract ---
    p.add_argument("--group", default=DEFAULT_GROUP,
                   help="Base group containing GCOV variables, e.g. /science/LSAR/GCOV/grids/frequencyA")
    p.add_argument("--vars", default="HHHH",
                   help="Comma-separated list of variable dataset names inside --group (e.g., HHHH,HVHV,VVVV).")

    # Coordinate datasets
    p.add_argument("--x_path", default=DEFAULT_X)
    p.add_argument("--y_path", default=DEFAULT_Y)

    # --- Optional spatial bbox subset ---
    p.add_argument("--bbox", default="",
                   help="Optional bbox as 'minx,miny,maxx,maxy' in SAME CRS/units as xCoordinates/yCoordinates.")
    p.add_argument("--bbox_crs", default="",
                   help="Optional CRS label for bbox (e.g., EPSG:32615). Stored as metadata only for now.")

    # --- Output ---
    p.add_argument("--out_dir", default=os.environ.get("OUTPUT_DIR", "/tmp/output"))
    p.add_argument("--out_name", default="nisar_subset.zarr",
                   help="Name of the output Zarr directory (written under --out_dir).")

    return p.parse_args()


def resolve_granule_hrefs(args) -> Tuple[str, str]:
    """
    Returns (https_href, s3_href). If not provided, uses earthaccess search fallback.
    """
    https_href = args.https_href.strip()
    s3_href = args.s3_href.strip()

    if https_href or s3_href:
        return https_href, s3_href

    # Fallback discovery (matches MAAP NISAR tutorial pattern)
    earthaccess.login()
    results = earthaccess.search_data(
        short_name=args.short_name,
        count=args.count,
        cloud_hosted=True,
    )
    if not results:
        raise RuntimeError("No granules found in search fallback. Provide --https_href/--s3_href from driver notebook.")

    g = results[args.granule_index]

    # https external
    https_links = g.data_links()
    https_href = https_links[0] if https_links else ""

    # direct S3
    s3_links = g.data_links(access="direct")
    s3_href = s3_links[0] if s3_links else ""

    if not https_href and not s3_href:
        raise RuntimeError("Could not resolve any hrefs for granule. Provide --https_href/--s3_href explicitly.")
    return https_href, s3_href


def open_file_like(access_mode: str, https_href: str, s3_href: str, asf_s3_creds_url: str):
    """
    Returns (file_obj, h5py_driver_kwds, chosen_mode, chosen_href).
    Uses the same cloud-optimized caching knobs described in the MAAP NISAR access tutorial.
    """
    # fsspec cache and HDF5 driver tuning (cloud-friendly)
    fsspec_params = {"cache_type": "blockcache", "block_size": 8 * 1024 * 1024}
    h5py_driver_kwds = {"page_buf_size": 16 * 1024 * 1024, "rdcc_nbytes": 4 * 1024 * 1024}

    def open_https():
        if not https_href:
            raise RuntimeError("HTTPS href not available. Provide --https_href or allow search fallback to find one.")
        fs = earthaccess.get_fsspec_https_session()
        return fs.open(https_href, mode="rb", **fsspec_params), "https", https_href

    def open_s3():
        if not s3_href:
            raise RuntimeError("S3 href not available. Provide --s3_href or allow search fallback to find one.")
        from maap.maap import MAAP
        import s3fs

        maap = MAAP()
        creds = maap.aws.earthdata_s3_credentials(asf_s3_creds_url)
        fs_s3 = s3fs.S3FileSystem(
            anon=False,
            key=creds["accessKeyId"],
            secret=creds["secretAccessKey"],
            token=creds["sessionToken"],
        )
        return fs_s3.open(s3_href, mode="rb", **fsspec_params), "s3", s3_href

    # Decide mode
    mode = access_mode
    if mode == "auto":
        # Prefer S3 if provided, else HTTPS
        mode = "s3" if s3_href else "https"

    if mode == "s3":
        file_obj, chosen_mode, chosen_href = open_s3()
    else:
        file_obj, chosen_mode, chosen_href = open_https()

    return file_obj, h5py_driver_kwds, chosen_mode, chosen_href


def parse_bbox(bbox_str: str) -> Optional[Tuple[float, float, float, float]]:
    if not bbox_str.strip():
        return None
    parts = _split_csv(bbox_str)
    if len(parts) != 4:
        raise ValueError("bbox must be 'minx,miny,maxx,maxy'")
    return tuple(float(x) for x in parts)  # type: ignore


def bbox_to_slices(x: np.ndarray, y: np.ndarray, bbox: Tuple[float, float, float, float]) -> Tuple[slice, slice]:
    """
    Convert bbox (minx,miny,maxx,maxy) into (yslice, xslice) based on 1D x/y coordinate vectors.

    Assumptions:
    - xCoordinates and yCoordinates are 1D arrays aligned to dataset dims (y,x).
    - bbox is in same CRS/units as x/y. If bbox is lon/lat but x/y are projected meters,
      UWG transform info is needed later (your note).
    """
    minx, miny, maxx, maxy = bbox

    # Ensure 1D
    x = np.asarray(x).ravel()
    y = np.asarray(y).ravel()

    # Handle monotonic (typical grids)
    x_asc = x[0] < x[-1]
    y_asc = y[0] < y[-1]

    # X indices where minx<=x<=maxx
    if x_asc:
        x0 = int(np.searchsorted(x, minx, side="left"))
        x1 = int(np.searchsorted(x, maxx, side="right"))
    else:
        # descending
        x0 = int(np.searchsorted(x[::-1], maxx, side="left"))
        x1 = int(np.searchsorted(x[::-1], minx, side="right"))
        # convert back to original indexing
        n = len(x)
        x0, x1 = n - x1, n - x0

    # Y indices where miny<=y<=maxy
    if y_asc:
        y0 = int(np.searchsorted(y, miny, side="left"))
        y1 = int(np.searchsorted(y, maxy, side="right"))
    else:
        y0 = int(np.searchsorted(y[::-1], maxy, side="left"))
        y1 = int(np.searchsorted(y[::-1], miny, side="right"))
        n = len(y)
        y0, y1 = n - y1, n - y0

    # Clamp to valid range
    x0 = max(0, min(len(x), x0))
    x1 = max(0, min(len(x), x1))
    y0 = max(0, min(len(y), y0))
    y1 = max(0, min(len(y), y1))

    if x1 <= x0 or y1 <= y0:
        raise RuntimeError("BBox resulted in empty slice. Check bbox is in same CRS/units as x/y coordinates.")

    return slice(y0, y1), slice(x0, x1)


def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    # Resolve hrefs (driver notebook can supply these directly)
    https_href, s3_href = resolve_granule_hrefs(args)

    # Open file-like object with streaming + caching knobs
    file_obj, driver_kwds, chosen_mode, chosen_href = open_file_like(
        args.access_mode, https_href, s3_href, args.asf_s3_creds_url
    )

    var_names = _split_csv(args.vars)
    if not var_names:
        raise ValueError("No variables provided in --vars")

    bbox = parse_bbox(args.bbox)

    # Read requested vars + coords (optionally sliced)
    with h5py.File(file_obj, "r", driver_kwds=driver_kwds) as f:
        # Read coords
        x = f[args.x_path][()]
        y = f[args.y_path][()]

        if bbox is not None:
            yslice, xslice = bbox_to_slices(x, y, bbox)
            x_sub = x[xslice]
            y_sub = y[yslice]
        else:
            yslice, xslice = slice(None), slice(None)
            x_sub = x
            y_sub = y

        data_vars = {}
        for vn in var_names:
            dpath = f"{args.group}/{vn}"
            if dpath not in f:
                raise KeyError(f"Dataset not found: {dpath}")
            arr = f[dpath][yslice, xslice]
            data_vars[vn] = (("y", "x"), arr)

    # Build xarray Dataset and write Zarr intermediate
    ds = xr.Dataset(
        data_vars=data_vars,
        coords={
            "x": ("x", np.asarray(x_sub)),
            "y": ("y", np.asarray(y_sub)),
        },
        attrs={
            "source_href": chosen_href,
            "access_mode": chosen_mode,
            "group": args.group,
            "vars": ",".join(var_names),
            "bbox": args.bbox,
            "bbox_crs": args.bbox_crs,
            "note": "bbox assumed in same CRS/units as xCoordinates/yCoordinates; reprojection/resampling to be added later.",
        },
    )

    out_path = os.path.join(args.out_dir, args.out_name)
    # Overwrite if exists (clean DPS reruns)
    if os.path.exists(out_path):
        # Zarr is a directory store
        import shutil
        shutil.rmtree(out_path)

    ds.to_zarr(out_path, mode="w")

    # Also write a small JSON manifest for downstream steps
    manifest = {
        "out_zarr": out_path,
        "source_href": chosen_href,
        "access_mode": chosen_mode,
        "group": args.group,
        "vars": var_names,
        "bbox": bbox,
        "bbox_crs": args.bbox_crs,
        "x_path": args.x_path,
        "y_path": args.y_path,
    }
    manifest_path = os.path.join(args.out_dir, "manifest.json")
    with open(manifest_path, "w") as fp:
        json.dump(manifest, fp, indent=2)

    print("WROTE_ZARR:", out_path)
    print("WROTE_MANIFEST:", manifest_path)
    print("SOURCE_MODE:", chosen_mode)
    print("SOURCE_HREF:", chosen_href)


if __name__ == "__main__":
    main()
