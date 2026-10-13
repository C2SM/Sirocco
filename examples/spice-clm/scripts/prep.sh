#!/bin/bash
# Extract one month of ERA5 caf/cas files and convert them to ICON-compatible input (SPICE prep).
# Usage: YDATE_START=<iso> prep.sh <era5_dir> <era5_soiltype_file> <output_dir>
source "$(dirname "$0")/common.sh"
GCM_DATADIR=$1
GCM_SOILTYPE=$2
OUT=$(realpath -m "$3")
UNTAR=${PWD}/untar
mkdir -p "${UNTAR}" "${OUT}"

echo "[PREP] untar ERA5 ${YYYY_MM} every ${HINCBOUND} hours"
TARFILE=${GCM_DATADIR}/year${YYYY}/ERA5_${YYYY_MM}.tar
COUNTPP=0
for (( HOUR=0; HOUR<=23; HOUR+=HINCBOUND )); do
  tar --wildcards -C "${UNTAR}" -xf "${TARFILE}" "${YYYY_MM}/${GCM_PREFIX}*$(printf %02d "${HOUR}").ncz" &
  run_parallel_throttle
done
wait_or_fail "UNTAR"

echo "[PREP] unzip netCDF files"
COUNTPP=0
for FILE in "${UNTAR}/${YYYY_MM}"/*.ncz; do
  "${NCCOPY[@]}" -k 2 "${FILE}" "${OUT}/$(basename "${FILE}" .ncz).nc" &
  run_parallel_throttle
done
wait_or_fail "UNZIP"
rm -rf "${UNTAR}"

echo "[PREP] convert ccaf to icaf"
cd "${OUT}"
COUNTPP=0
for FILE in *.nc; do
  ( "${CCAF2ICAF[@]}" "${FILE}" 2 "${GCM_SOILTYPE}"
    "${NCKS[@]}" -h -O -x -v W_SO_REL,T_SO,soil1,soil1_bnds "${FILE}" "${FILE}" ) &
  run_parallel_throttle
done
wait_or_fail "CCAF2ICAF"
cd - > /dev/null

check_forcing_files "${OUT}" .nc
echo "[PREP] finished ${YYYY_MM}"
