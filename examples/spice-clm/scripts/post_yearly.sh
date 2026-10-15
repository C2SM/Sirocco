#!/bin/bash
# Combine the monthly time series of one year into yearly ones (SPICE post_yearly_cmor.sh).
# Usage: YDATE_START=<iso> YDATE_STOP=<iso> ITYPE_TS=<2|3> post_yearly.sh <output_dir> [--prev <post_dir>] <post_dir> ...
# <post_dir>: output of post for each month of the year
# --prev (all but the first year): output of post for December of the previous year
# ITYPE_TS=2 deletes the monthly time series afterwards, ITYPE_TS=3 keeps them
source "$(dirname "$0")/common.sh"
OUTDIR=$(realpath -m "$1")
shift
declare -A MONTH_DIR
while [[ $# -gt 0 ]]; do
  case $1 in
    --prev) PREV_DEC=$(echo "$2"/????_12); shift 2 ;;
    *) MONTH_DIR[$(basename "$1"/????_??)]=$(echo "$1"/????_??); shift ;;
  esac
done
MONTHS=($(printf '%s\n' "${!MONTH_DIR[@]}" | sort))
FIRST_DIR=${MONTH_DIR[${MONTHS[0]}]}
LAST_DIR=${MONTH_DIR[${MONTHS[-1]}]}
FINAL_YEAR=0
[[ "${NEXT_DATE}" == "$(to_cfu_date "${YDATE_STOP:?YDATE_STOP must be set}")" ]] && FINAL_YEAR=1
mkdir -p "${OUTDIR}"
# Skip duplicate time steps in cdo mergetime
export SKIP_SAME_TIME=1

echo "[POST_YEARLY] combining ${MONTHS[0]} to ${MONTHS[-1]} into yearly time series of ${YYYY}"
NFILES=$(ls -1 "${LAST_DIR}" | wc -l)
for M in "${MONTHS[@]}"; do
  if [[ $(ls -1 "${MONTH_DIR[$M]}" | wc -l) -ne ${NFILES} ]]; then
    echo "ERROR: not all monthly directories of ${YYYY} contain ${NFILES} files" >&2
    exit 1
  fi
done

# Output file name <var>_<first>-<last>.nc with dates in YYYYMMDDHH (args: file, var)
yearly_name() {
  "${PYTHON[@]}" - "$1" "$2" <<'EOF'
import sys
import cftime
import netCDF4

with netCDF4.Dataset(sys.argv[1]) as nc:
    time = nc["time"]
    calendar = getattr(time, "calendar", "standard")
    if hasattr(time, "bounds"):
        values = [nc[time.bounds][0, 0], nc[time.bounds][-1, -1]]
    else:
        values = [time[0], time[-1]]
    dates = cftime.num2date(values, time.units, calendar)
print(f"{sys.argv[2]}_{dates[0]:%Y%m%d%H}-{dates[1]:%Y%m%d%H}.nc")
EOF
}

# Combine the monthly files of one variable (arg: file name of the monthly time series)
combine() {
  local FILE=$1 PARAM CELL_METHODS VAR DIR TMP FILES N_TIME NAME
  VAR=${FILE%_ts.*}
  DIR=${OUTDIR}/${VAR}
  TMP=${PWD}/tmp/${VAR}
  mkdir -p "${DIR}" "${TMP}"
  PARAM=$("${NCDUMP[@]}" -h "${LAST_DIR}/${FILE}" | grep ", rlat, rlon" | cut -d' ' -f2 | cut -d'(' -f1)
  FILES=()
  for M in "${MONTHS[@]}"; do FILES+=("${MONTH_DIR[$M]}/${FILE}"); done
  CELL_METHODS=$("${CFU[@]}" get_attval "${LAST_DIR}/${FILE}" "${PARAM}" cell_methods)
  if [[ "${CELL_METHODS}" != *"time: point"* && "${CELL_METHODS}" != "--" ]]; then
    # Accumulated, averaged or extreme values: the time bounds of each month do not overlap
    "${CDO[@]}" -s -P "${OMP_THREADS_POST}" mergetime "${FILES[@]}" "${TMP}/merged.nc"
  else
    # Instantaneous values: the last time step of December (1 January, 00 UTC) belongs to the next year
    if [[ -n "${PREV_DEC:-}" ]]; then
      "${NCKS[@]}" -h -d time,-1 "${PREV_DEC}/${FILE}" "${TMP}/prev.nc"
      FILES=("${TMP}/prev.nc" "${FILES[@]}")
    fi
    if [[ ${MONTHS[-1]} == *_12 ]]; then
      N_TIME=$("${CDO[@]}" -s ntime "${LAST_DIR}/${FILE}")
      "${NCKS[@]}" -h -d time,0,$(( N_TIME - 2 )) "${LAST_DIR}/${FILE}" "${TMP}/dec.nc"
      FILES[-1]=${TMP}/dec.nc
      if [[ ${FINAL_YEAR} -eq 1 ]]; then
        "${NCKS[@]}" -h -O -d time,-1,-1 "${LAST_DIR}/${FILE}" "${DIR}/${VAR}_${NEXT_DATE}.nc"
      fi
    fi
    "${CDO[@]}" -s -P "${OMP_THREADS_POST}" mergetime "${FILES[@]}" "${TMP}/merged.nc"
  fi
  NAME=$(yearly_name "${TMP}/merged.nc" "${VAR}")
  mv "${TMP}/merged.nc" "${DIR}/${NAME}"
  rm -rf "${TMP}"
}

COUNTPP=0
for FILE in $(ls -1 "${LAST_DIR}"); do
  combine "${FILE}" &
  run_parallel_throttle
done
wait_or_fail "POST_YEARLY"
rmdir "${PWD}/tmp"

for VAR in HSURF FR_LAND; do
  mkdir -p "${OUTDIR}/${VAR}"
  "${NCKS[@]}" -h -O -v "${VAR}" "${FIRST_DIR}/../${EXPID}_c.nc" "${OUTDIR}/${VAR}/${VAR}.nc"
done

echo "[POST_YEARLY] checks"
FILES=($(compgen -G "${OUTDIR}/*/*_${YYYY}*" || true))
if [[ ${#FILES[@]} -ne ${NFILES} ]]; then
  echo "ERROR: ${#FILES[@]} yearly time series for ${YYYY} in ${OUTDIR}, expected ${NFILES}" >&2
  exit 1
fi
for FILE in "${OUTDIR}"/*/*.nc; do
  "${CDO[@]}" -s showvar "${FILE}" > /dev/null 2>&1 || { echo "ERROR: ${FILE} is not readable" >&2; exit 1; }
done

# ITYPE_TS=2: keep only the December needed by the next year
if [[ ${ITYPE_TS:?ITYPE_TS must be set} -eq 2 ]]; then
  echo "[POST_YEARLY] deleting the monthly time series"
  for M in "${MONTHS[@]}"; do
    [[ ${M} == *_12 && ${FINAL_YEAR} -eq 0 ]] || rm -rf -- "${MONTH_DIR[$M]}"
  done
  if [[ -n "${PREV_DEC:-}" ]]; then rm -rf -- "${PREV_DEC}"; fi
fi
echo "[POST_YEARLY] finished ${YYYY}"
