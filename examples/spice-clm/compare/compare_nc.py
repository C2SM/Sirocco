"""Compare the netCDF files of two directory trees variable by variable.

Usage: python compare_nc.py DIR_A DIR_B

Compared are the file format, the global attributes and, for each variable, the data type,
dimensions, attributes, compression and chunking, and all values. Time axes are compared as
absolute dates. Global attributes that record the host, the software history or the ICON
version label (wrong in SPICE rc-2.4.1) are ignored.
"""
import sys
from pathlib import Path

import cftime
import numpy as np
from netCDF4 import Dataset

IGNORED_ATTRS = {"history", "creation_date", "uuidOfHGrid", "NCO", "CDO", "CDI", "history_of_appended_files",
                 "comment", "icon-clm_version"}


def compare_file(fa, fb):
    out = []
    with Dataset(fa) as a, Dataset(fb) as b:
        va, vb = set(a.variables), set(b.variables)
        for v in sorted(va ^ vb):
            out.append(f"  only in {'A' if v in va else 'B'}: variable {v}")
        for k in sorted(set(a.ncattrs()) | set(b.ncattrs())):
            if k in IGNORED_ATTRS:
                continue
            x, y = getattr(a, k, None), getattr(b, k, None)
            if str(x) != str(y):
                out.append(f"  global attr {k}: {x!r} vs {y!r}")
        if a.data_model != b.data_model:
            out.append(f"  format: {a.data_model} vs {b.data_model}")
        for v in sorted(va & vb):
            x, y = a.variables[v], b.variables[v]
            x.set_auto_mask(False)
            y.set_auto_mask(False)
            if x.dtype != y.dtype:
                out.append(f"  {v}: dtype {x.dtype} vs {y.dtype}")
            if x.dimensions != y.dimensions:
                out.append(f"  {v}: dimensions {x.dimensions} vs {y.dimensions}")
            for k in sorted(set(x.ncattrs()) | set(y.ncattrs())):
                # The reference date of the time units may differ, the dates are compared below
                if k == "units" and v in ("time", "time_bnds"):
                    continue
                p, q = getattr(x, k, None), getattr(y, k, None)
                if str(p) != str(q):
                    out.append(f"  {v}: attr {k}: {p!r} vs {q!r}")
            if (x.filters(), x.chunking()) != (y.filters(), y.chunking()):
                out.append(f"  {v}: storage {x.filters()} {x.chunking()} vs {y.filters()} {y.chunking()}")
            if x.shape != y.shape:
                out.append(f"  {v}: shape {x.shape} vs {y.shape}")
                continue
            dx, dy = x[...], y[...]
            if v in ("time", "time_bnds") and "time" in a.variables:
                # Compare as absolute dates, since the reference date of the time axis may differ
                ta, tb = a.variables["time"], b.variables["time"]
                cal = getattr(ta, "calendar", "standard")
                dx = cftime.date2num(cftime.num2date(dx, ta.units, cal), "seconds since 1900-01-01", cal)
                dy = cftime.date2num(cftime.num2date(dy, tb.units, cal), "seconds since 1900-01-01", cal)
            if dx.dtype.kind == "S" and dx.ndim == 0:
                continue  # grid mapping placeholder, value is never written
            if dx.dtype.kind not in "fiu":
                if not np.array_equal(dx, dy):
                    out.append(f"  {v}: non-numeric values differ")
                continue
            if np.array_equal(dx, dy, equal_nan=dx.dtype.kind == "f"):
                continue
            d = np.abs(dx.astype("f8") - dy.astype("f8"))
            scale = np.nanmax(np.abs(dx.astype("f8"))) or 1.0
            ndiff = np.count_nonzero(d > 0)
            out.append(f"  {v}: max|diff|={np.nanmax(d):.3e} rel={np.nanmax(d) / scale:.3e} "
                       f"differing={ndiff}/{d.size}")
    return out


def main(root_a, root_b):
    root_a, root_b = Path(root_a), Path(root_b)
    fa = {p.relative_to(root_a) for p in root_a.rglob("*.nc")}
    fb = {p.relative_to(root_b) for p in root_b.rglob("*.nc")}
    for p in sorted(fa - fb):
        print(f"ONLY IN A: {p}")
    for p in sorted(fb - fa):
        print(f"ONLY IN B: {p}")
    identical, differing = 0, 0
    for p in sorted(fa & fb):
        diffs = compare_file(root_a / p, root_b / p)
        if diffs:
            differing += 1
            print(f"DIFF {p}")
            print("\n".join(diffs))
        else:
            identical += 1
    print(f"SUMMARY common={len(fa & fb)} identical={identical} differing={differing} "
          f"only_a={len(fa - fb)} only_b={len(fb - fa)}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
