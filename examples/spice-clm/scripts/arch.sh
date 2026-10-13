#!/bin/bash
# Check, CF-correct and archive one month of ICON output (SPICE arch).
# Usage: YDATE_START=<iso> arch.sh --corrected <dir> --archive <dir> --const-out <dir>
#          [--icon-const <dir>] [--const-in <dir>] <stream_dir_01> ... <stream_dir_NN>
# --icon-const (first month only): ICON constants stream
# --const-in (later months): const-out of the first month
source "$(dirname "$0")/common.sh"
while [[ $# -gt 0 ]]; do
  case $1 in
    --corrected) CORRECTED=$(realpath -m "$2"); shift 2 ;;
    --archive) ARCHIVE=$(realpath -m "$2"); shift 2 ;;
    --const-out) CONST_OUT=$(realpath -m "$2"); shift 2 ;;
    --icon-const) ICON_CONST=$2; shift 2 ;;
    --const-in) CONST_IN=$2; shift 2 ;;
    *) STREAMS+=("$1"); shift ;;
  esac
done
if [[ ${#STREAMS[@]} -ne ${#HOUT_INC[@]} ]]; then
  echo "ERROR: expected ${#HOUT_INC[@]} output streams, got ${#STREAMS[@]}" >&2
  exit 1
fi
POUT=${CORRECTED}/${YYYY_MM}
mkdir -p "${POUT}" "${ARCHIVE}" "${CONST_OUT}"
stream_name() { printf "out%02d" "$1"; }

echo "[ARCH] check completeness of the output streams"
ZERO2ZERO=F
[[ "${CURRENT_DATE}" == "${YDATE_START}" ]] && ZERO2ZERO=T
for (( N=1; N<=${#STREAMS[@]}; N++ )); do
  DIR=${STREAMS[N-1]}
  INC=${HOUT_INC[N-1]}
  BASENAME=$(ls -1 "${DIR}" | head -n 1 | cut -d. -f1)
  case "${BASENAME: -1}" in
    p) SUFFIX="p.nc" ;;
    z) SUFFIX="z.nc" ;;
    *) SUFFIX=".nc" ;;
  esac
  START_CHECK=${CURRENT_DATE}
  [[ ${ZERO2ZERO} == F ]] && START_CHECK=$("${CFU[@]}" add_hours "${CURRENT_DATE}" "${INC:0:2}" "${ITYPE_CALENDAR}")
  RC=$("${CFU[@]}" check_files_iso "${START_CHECK}0000" "${NEXT_DATE}0000" "${INC}" icon_ icon_ "${SUFFIX}" \
       "${DIR}" "${ZERO2ZERO}" "${ITYPE_CALENDAR}")
  if [[ "${RC}" -ne 0 ]]; then
    cat check_files.log >&2
    echo "ERROR: missing ICON output in $(stream_name "${N}")" >&2
    exit 1
  fi
done

if [[ ${ITYPE_SAMOVAR} -ne 0 ]]; then
  echo "[ARCH] SAMOVAR range check of the last day"
  SAMOVAR_LOG=${PWD}/samovar_${YYYY_MM}.log
  NOD=$(cal "${MM}" "${YYYY}" | grep -v '[A-Za-z]' | wc -w)
  SAMOVAR_ERROR=0
  for (( N=1; N<=${#STREAMS[@]}; N++ )); do
    if ! "${SAMOVAR_SH[@]}" T "${SAMOVAR_LIST}" "${SAMOVAR_LOG}" "${STREAMS[N-1]}/icon_*${NOD}T*.nc" cdo "${SAMOVAR_EXE}"; then
      echo "SAMOVAR check for $(stream_name "${N}") -- FAILED"
      SAMOVAR_ERROR=1
    fi
  done
  if [[ ${SAMOVAR_ERROR} -ne 0 ]]; then
    echo "ERROR: SAMOVAR check failed, see ${SAMOVAR_LOG}" >&2
    exit 2
  fi
fi

echo "[ARCH] constant fields"
TOPO=${CONST_OUT}/z_mc-topography_c.nc
if [[ -n "${ICON_CONST:-}" ]]; then
  ICON_C=${ICON_CONST}/icon_${YDATE_START:0:8}T${YDATE_START:8:2}0000Zc.nc
  "${CDO[@]}" -s sellevidx,${UL}/${LL} -sub -selvar,z_mc "${ICON_C}" -selvar,topography_c "${ICON_C}" "${TOPO}"
  cp "${ICON_C}" "${CONST_OUT}/"
else
  for FILE in "${CONST_IN}"/*; do ln -sf "${FILE}" "${CONST_OUT}/"; done
fi
# post needs the constants file next to the corrected streams
cp "${CONST_OUT}"/icon_*c.nc "${CORRECTED}/"
ln -sf "${TOPO}" "${WORKDIR}/${EXPID}/post/"

echo "[ARCH] CF correction and height interpolation"
for (( N=1; N<=${#STREAMS[@]}; N++ )); do
  [[ ${N} -eq ${NESTING_STREAM} ]] && continue
  OUTDIR_N=${POUT}/$(stream_name "${N}")
  mkdir -p "${OUTDIR_N}"
  COUNTPP=0
  if [[ ${N} -eq ${HLEV_STREAM} ]]; then
    for FILE in "${STREAMS[N-1]}"/*; do
      height_interpolation "${COUNTPP}" HLEVS[@] "${OUTDIR_N}" "${FILE}" &
      run_parallel_throttle
    done
    wait_or_fail "HEIGHT_INTERPOLATION"
  else
    cp "${STREAMS[N-1]}"/* "${OUTDIR_N}/"
  fi
  COUNTPP=0
  for FILE in "${OUTDIR_N}"/*; do
    iconcor "${COUNTPP}" "${HOUT_INC[N-1]}" "${FILE}" "${OPERATION[N-1]}" &
    run_parallel_throttle
  done
  wait_or_fail "CF_CORRECTION"
done

echo "[ARCH] tar output streams"
cd "${POUT}"
mkdir -p nesting
cp "${STREAMS[NESTING_STREAM-1]}"/*.nc nesting/
COUNTPP=0
for (( N=1; N<=${#STREAMS[@]}; N++ )); do
  NAME=$(stream_name "${N}")
  if [[ ${N} -eq ${NESTING_STREAM} ]]; then
    tar -chf "${ARCHIVE}/${EXPID}_${YYYY_MM}_nesting.tar" nesting/*.nc &
  else
    tar -chf "${ARCHIVE}/${EXPID}_${YYYY_MM}_${NAME}.tar" "${NAME}"/*.nc &
  fi
  run_parallel_throttle
done
wait_or_fail "TAR_ARCHIVE"
rm -rf nesting
ls -lh "${ARCHIVE}"
echo "[ARCH] finished ${YYYY_MM}"
