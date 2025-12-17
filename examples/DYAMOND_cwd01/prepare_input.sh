#!/usr/bin/bash

while [ "$#" -gt 0 ]; do
  case "$1" in
    --pool=*) DATA_POOL="${1#*=}"; shift 1;;
    --sst-ice=*) SST_ICE_DIR="${1#*=}"; shift 1;;
    --ozone=*) OZONE_DIR="${1#*=}"; shift 1;;
    --aero-kine=*) AERO_KINE_DIR="${1#*=}"; shift 1;;
    --icon-input=*) ICON_INPUT_DIR="${1#*=}"; shift 1;;
    *) echo "ERROR: unrecognized argument: $1" >&2; exit 1;;
  esac
done

mkdir -p ${DATA_POOL} ${ICON_INPUT_DIR}

for DIR in ${DATA_POOL} ${SST_ICE_DIR} ${OZONE_DIR} ${AERO_KINE_DIR} ${ICON_INPUT_DIR}; do
    if [ ! -d "${DIR}" ]; then
        echo "ERROR: ${DIR} is not a directory"
        exit 1
    fi
done

YYYY_START="${SIROCCO_START_DATE:0:4}"
MM_START="${SIROCCO_START_DATE:5:2}"
YYYY_STOP="${SIROCCO_STOP_DATE:0:4}"
MM_STOP="${SIROCCO_STOP_DATE:5:2}"

shift_YYYY_MM(){
    if [ "${MM}" == "12" ]; then
        ((YYYY ++))
        MM="01"
    else
        M=${MM#0}
        ((M ++))
        MM="$(printf "%02g" "${M}")"
    fi
}

import_and_link(){
    # - add file to workflow data pool on scratch if not found
    # - link to icon chunk input
    DATA_POOL_FILE_PATH="${DATA_POOL}/$2"
    ICON_INPUT_LINKNAME="${3:-$2}"
    if [ ! -e "${DATA_POOL_FILE_PATH}" ]; then
        ORIGIN_FILE_PATH="$1/$2"
        if [ -e "${ORIGIN_FILE_PATH}" ]; then 
            cp "${ORIGIN_FILE_PATH}" "${DATA_POOL_FILE_PATH}"
        else
            echo "ERROR: ${ORIGIN_FILE_PATH} not found"
            exit 1
        fi
    fi
    ln -s "${DATA_POOL_FILE_PATH}" "./${ICON_INPUT_LINKNAME}"
}

# Enter ICON input dir for current chunk
pushd ${ICON_INPUT_DIR} >/dev/null || exit

# SST_ICE
YYYY=${YYYY_START}
MM=${MM_START}
while : ; do
    import_and_link "${SST_ICE_DIR}" "SST_${YYYY}_${MM}_icon_grid_0021_R02B06_G.nc"
    import_and_link "${SST_ICE_DIR}" "CI_${YYYY}_${MM}_icon_grid_0021_R02B06_G.nc"
    if [ ${YYYY} == ${YYYY_STOP} ] && [ ${MM} == ${MM_STOP} ]; then
        shift_YYYY_MM
        break
    else 
        shift_YYYY_MM
    fi
done
# NOTE: Needs a last step outside of the loop
import_and_link "${SST_ICE_DIR}" "SST_${YYYY}_${MM}_icon_grid_0021_R02B06_G.nc"
import_and_link "${SST_ICE_DIR}" "CI_${YYYY}_${MM}_icon_grid_0021_R02B06_G.nc"

# OZONE
# TODO: Not necessary for R02B06?
# YYYY=${YYYY_START}
# while : ; do
#     import_and_link "${OZONE_DIR}" "bc_ozone_${YYYY}.nc"
#     [ ${YYYY} == ${YYYY_STOP} ] && break
#     ((YYYY ++))
# done

# AERO KINE
import_and_link ${AERO_KINE_DIR} "bc_aeropt_kinne_lw_b16_coa.nc"
import_and_link ${AERO_KINE_DIR} "bc_aeropt_kinne_sw_b14_coa.nc"
import_and_link ${AERO_KINE_DIR} "bc_aeropt_kinne_sw_b14_fin_1865.nc" "bc_aeropt_kinne_sw_b14_fin.nc"
    
popd  >/dev/null || exit
