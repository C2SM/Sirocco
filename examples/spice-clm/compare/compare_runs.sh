#!/bin/bash
# Compare a SPICE sp001 chain (A) with a Sirocco run of this example (B): archived ICON output,
# monthly time series and constant fields. Archives are extracted one month at a time to WORK_DIR.
#
# Usage: compare_runs.sh SPICE_CHAIN_DIR SIROCCO_RUN_DIR WORK_DIR YYYY-MM [YYYY-MM ...]
#   SPICE_CHAIN_DIR  e.g. /path/to/spice/chain
#   SIROCCO_RUN_DIR  the "run" directory of the Sirocco workflow
# PYTHON must provide netCDF4 and cftime (default: python3), e.g. the venv of clm-tools.
set -eu

A=$(realpath "$1"); B=$(realpath "$2"); WORK=$3; shift 3
PYTHON=${PYTHON:-python3}
CMP=("${PYTHON}" -I "$(dirname "$(realpath "$0")")/compare_nc.py")
FIRST=$1
mkdir -p "${WORK}"

for D in "$@"; do
  YM=${D/-/_}
  SRC_A=${A}/arch/sp001/${YM}
  SRC_B=${B}/arch__date_${D}-01T00-00-00/archive
  echo "===== archive ${YM}"
  diff <(ls "${SRC_A}") <(ls "${SRC_B}") && echo "tar lists identical"
  rm -rf "${WORK:?}/A" "${WORK:?}/B"; mkdir -p "${WORK}/A" "${WORK}/B"
  for t in "${SRC_A}"/*.tar; do tar -xf "$t" -C "${WORK}/A"; done
  for t in "${SRC_B}"/*.tar; do tar -xf "$t" -C "${WORK}/B"; done
  "${CMP[@]}" "${WORK}/A" "${WORK}/B"
  rm -rf "${WORK:?}/A" "${WORK:?}/B"
  echo "===== time series ${YM}"
  "${CMP[@]}" "${A}/work/sp001/post/${YM}" "${B}/post__date_${D}-01T00-00-00/post/${YM}"
done

# SPICE keeps all constant fields in post/, Sirocco keeps the topography in the arch output
echo "===== constants"
rm -rf "${WORK:?}/A" "${WORK:?}/B"; mkdir -p "${WORK}/A" "${WORK}/B"
cp "${A}"/work/sp001/post/*.nc "${WORK}/A/"
cp "${B}/post__date_${FIRST}-01T00-00-00"/post/*.nc "${B}/arch__date_${FIRST}-01T00-00-00/const/z_mc-topography_c.nc" "${WORK}/B/"
"${CMP[@]}" "${WORK}/A" "${WORK}/B"
rm -rf "${WORK:?}/A" "${WORK:?}/B"
