#!/bin/bash
# Build monthly time series on the rotated lat/lon grid from CF-corrected ICON output (SPICE post).
# Usage: YDATE_START=<iso> post.sh <corrected_dir> <target_grid> <extpar> <output_dir> [<output_dir_first_month>]
source "$(dirname "$0")/common.sh"
INPDIR=$1
TARGET_GRID=$2
EXTPAR=$3
OUTDIR=$(realpath -m "$4")
FIRST=${5:-}
mkdir -p "${OUTDIR}/${YYYY_MM}"

if [[ -z "${FIRST}" ]]; then
  echo "[POST] constant fields and remapping weights"
  ICON_C=${OUTDIR}/icon_c.nc
  cp "${INPDIR}/icon_${YDATE_START:0:8}T${YDATE_START:8:2}0000Zc.nc" "${ICON_C}"
  "${NCKS[@]}" -h -v clon,clat,clon_bnds,clat_bnds "${ICON_C}" "${OUTDIR}/icon_grid.nc"
  "${NCATTED[@]}" -h -a ,global,d,, "${OUTDIR}/icon_grid.nc"
  iconcor 0 00:00:00 "${ICON_C}"
  timeseries FIELDCAP
  "${CDO[@]}" -P "${OMP_THREADS_POST}" gennn,"${TARGET_GRID}" "${ICON_C}" "${OUTDIR}/remapnn_weights.nc"
  "${CDO[@]}" -s -P "${OMP_THREADS_POST}" remap,"${TARGET_GRID}","${OUTDIR}/remapnn_weights.nc" \
    "${ICON_C}" "${OUTDIR}/${EXPID}_c.nc.tmp"
  "${CDO[@]}" -s setmissval,-1.E20 "${OUTDIR}/${EXPID}_c.nc.tmp" "${OUTDIR}/${EXPID}_c.nc"
  rm "${OUTDIR}/${EXPID}_c.nc.tmp"
else
  for FILE in "${FIRST}"/*.nc; do ln -sf "${FILE}" "${OUTDIR}/"; done
fi

run_ts_commands() {
  local TS_COMMAND
  COUNTPP=0
  for TS_COMMAND in "${TS_COMMANDS[@]}"; do
    ${TS_COMMAND} &
    run_parallel_throttle
  done
  wait_or_fail "$1"
}

echo "[POST] time series part 1: selected variables"
TS_COMMANDS=(
  'timeseries RAIN_CON  out03 remapnn'
  'timeseries SNOW_CON  out03 remapnn'
  'timeseries RAIN_GSP  out03 remapnn'
  'timeseries SNOW_GSP  out03 remapnn'
  'timeseries TOT_PREC  out03 remapnn'
  'timeseries AEVAP_S   out08 remapnn'
  'timeseries ALHFL_S   out08 remapnn'
  'timeseries ASHFL_S   out08 remapnn'
  'timeseries ATHU_S    out08 remapnn'
  'timeseries ASOB_S    out08 remapnn'
  'timeseries ASOB_T    out08 remapnn'
  'timeseries ASOD_T    out08 remapnn'
  'timeseries ASODIFD_S out08 remapnn'
  'timeseries ASODIFU_S out08 remapnn'
  'timeseries ATHB_S    out08 remapnn'
  'timeseries ATHB_T    out08 remapnn'
  'timeseries AUMFL_S   out08 remapnn'
  'timeseries AVMFL_S   out08 remapnn'
  'timeseries CLCT      out03 remapnn'
  'timeseries CLCT_MOD  out03 remapnn'
  'timeseries SPGUST_10M  out03 remapnn'
  'timeseries SP_10M    out03 remapnn'
  'timeseries PMSL      out03 remapnn'
  'timeseries PS        out03 remapnn'
  'timeseries QV_2M     out03 remapnn'
  'timeseries T_2M      out03 remapnn'
  'timeseries TD_2M     out03 remapnn'
  'timeseries U_10M     out03 remapnn'
  'timeseries V_10M     out03 remapnn'
  'timeseries RELHUM_2M out03 remapnn'
  'timeseries RUNOFF_S  out03 remapnn'
  'timeseries RUNOFF_G  out03 remapnn'
  'timeseries SNOW_MELT out03 remapnn'
  'timeseries CAPE_ML   out05 remapnn'
  'timeseries CAPE_CON  out05 remapnn'
  'timeseries H_SNOW    out05 remapnn'
  'timeseries TQC       out05 remapnn'
  'timeseries TQI       out05 remapnn'
  'timeseries TQR       out05 remapnn'
  'timeseries TQS       out05 remapnn'
  'timeseries TQV       out05 remapnn'
  'timeseries W_I       out02 remapnn'
  'timeseries T_SO      out02 remapnn'
  'timeseries W_SO      out02 remapnn'
  'timeseries W_SO_ICE  out02 remapnn'
  'timeseries T_S       out02 remapnn'
  'timeseries T_G       out02 remapnn'
  'timeseries W_SNOW    out02 remapnn'
  'timeseries TMAX_2M   out04 remapnn'
  'timeseries TMIN_2M   out04 remapnn'
  'timeseries DURSUN    out04 remapnn'
  'timeseries LAI       out04 remapnn'
  'timeseries PLCOV     out04 remapnn'
  'timeseries ROOTDP    out04 remapnn'
  'timeseries SODIFD_S  out09 remapnn'
  'timeseries SOBS_RAD  out09 remapnn'
  'timeseries SODIFU_S  out09 remapnn'
  'timeseries THBS_RAD  out09 remapnn'
)
NUMFILES=${#TS_COMMANDS[@]}
run_ts_commands "TIMESERIES_PART1"

echo "[POST] time series part 2: pressure and height levels"
TS_COMMANDS=(
  'timeseriesp OMEGA    out06 PLEVS[@] remapnn'
  'timeseriesp T        out06 PLEVS[@] remapnn'
  'timeseriesp U        out06 PLEVS[@] remapnn'
  'timeseriesp V        out06 PLEVS[@] remapnn'
  'timeseriesp FI       out06 PLEVS[@] remapnn'
  'timeseriesp QV       out06 PLEVS[@] remapnn'
  'timeseriesp RELHUM   out06 PLEVS[@] remapnn'
  'timeseriesz P        out07 ZLEVS[@] remapnn'
  'timeseriesz P        out10 HLEVS[@] remapnn'
)
count_level_files "${TS_COMMANDS[@]}"
run_ts_commands "TIMESERIES_PART2"

echo "[POST] time series part 3: derived quantities on pressure levels"
TS_COMMANDS=(
  'timeseriesap SP PLEVS[@]'
  'timeseriesap DD PLEVS[@]'
)
count_level_files "${TS_COMMANDS[@]}"
run_ts_commands "TIMESERIES_PART3"

echo "[POST] time series part 4: derived quantities"
TS_COMMANDS=(
  'timeseries ASODIRD_S'
  'timeseries ASOD_S'
  'timeseries ASOU_T'
  'timeseries ATHD_S'
  'timeseries DD_10M'
  'timeseries DTR_2M'
  'timeseries FR_SNOW'
  'timeseries PREC_CON'
  'timeseries RUNOFF_T'
  'timeseries TOT_SNOW'
  'timeseries TQW'
)
# RUNOFF_T is computed again in part 5 and must be counted only once
NUMFILES=$(( NUMFILES + ${#TS_COMMANDS[@]} - 1 ))
run_ts_commands "TIMESERIES_PART4"

echo "[POST] time series part 5: quantities depending on part 4"
TS_COMMANDS=('timeseries RUNOFF_T')
NUMFILES=$(( NUMFILES + 1 ))
run_ts_commands "TIMESERIES_PART5"

echo "[POST] checks"
FILES=("${OUTDIR}/${YYYY_MM}"/*)
if [[ ${#FILES[@]} -ne ${NUMFILES} ]]; then
  echo "ERROR: ${#FILES[@]} time series files in ${OUTDIR}/${YYYY_MM}, expected ${NUMFILES}" >&2
  exit 1
fi
if compgen -G "${OUTDIR}/${YYYY_MM}/*tmp" > /dev/null; then
  echo "ERROR: temporary files left in ${OUTDIR}/${YYYY_MM}" >&2
  exit 1
fi
for FILE in "${FILES[@]}"; do
  "${NCDUMP[@]}" -h "${FILE}" | grep -q rotated || { echo "ERROR: $(basename "${FILE}") is not on the rotated grid" >&2; exit 1; }
done
if [[ ${ITYPE_SAMOVAR_TS} -eq 1 ]]; then
  SAMOVAR_FILES=$(ls "${OUTDIR}/${YYYY_MM}"/*ts.nc | grep -v uncorr | tr '\n' ' ')
  if ! "${SAMOVAR_SH[@]}" F "${SAMOVAR_LIST_TS}" "${PWD}/samovar_${YYYY_MM}_ts.log" "${SAMOVAR_FILES}" cdo "${SAMOVAR_EXE}"; then
    echo "ERROR: SAMOVAR check of time series failed, see ${PWD}/samovar_${YYYY_MM}_ts.log" >&2
    exit 2
  fi
fi
echo "[POST] finished ${YYYY_MM}"
