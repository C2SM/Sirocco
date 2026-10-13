#!/bin/bash
# Remap one month of prepared ERA5 data to the ICON grid (SPICE conv2icon parts II and III):
# lower boundary (SST, sea ice) to LOWBC.nc and lateral boundaries to cas<date>_lbc.nc.
# Usage: YDATE_START=<iso> conv2icon.sh <forcing_dir> <inidata_dir> <latbc_dir>
source "$(dirname "$0")/common.sh"
PIN=$1
INIDIR=$2
OUT=$(realpath -m "$3")
TMP=${PWD}/tmp
mkdir -p "${OUT}" "${TMP}"

echo "[CONV2ICON] lower boundary data ${YYYY_MM}"
"${NCRCAT[@]}" -h -v SIC,SST "${PIN}/${GCM_PREFIX}"??????????.nc "${TMP}/lowbc.nc"
"${CDO[@]}" -s setmisstodis -selname,SIC "${TMP}/lowbc.nc" "${TMP}/sic.nc"
"${CDO[@]}" -s div -selname,SST "${TMP}/lowbc.nc" "${INIDIR}/input_ocean_area.nc" "${TMP}/sst.nc"
"${CDO[@]}" -s setmisstodis "${TMP}/sst.nc" "${TMP}/sst2.nc"
"${CDO[@]}" -s merge "${TMP}/sst2.nc" "${TMP}/sic.nc" "${TMP}/sst_sic.nc"
"${CDO[@]}" -s -P "${OMP_THREADS_CONV2ICON}" "${GCM_REMAP},${INIDIR}/triangular-grid.nc" \
  "${TMP}/sst_sic.nc" "${OUT}/LOWBC.nc"
rm -rf "${TMP}"

echo "[CONV2ICON] lateral boundary data ${YYYY_MM}"
COUNTPP=0
for FILE in "${PIN}/${GCM_PREFIX}"??????????.nc; do
  make_lbc "${FILE}" "${OUT}" "${INIDIR}" &
  run_parallel_throttle
done
wait_or_fail "LATERAL_BOUNDARY_DATA"

# ICON reads the boundary file of the initial date at every (re)start
[[ -e "${OUT}/${GCM_PREFIX}${YDATE_START}_lbc.nc" ]] || \
  ln -s "${INIDIR}/${GCM_PREFIX}${YDATE_START}_lbc.nc" "${OUT}/"

check_forcing_files "${OUT}" _lbc.nc
echo "[CONV2ICON] finished ${YYYY_MM}"
