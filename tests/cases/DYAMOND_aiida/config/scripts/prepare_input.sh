#!/usr/bin/bash

while [ "$#" -gt 0 ]; do
  case "$1" in
    --pool=*) DATA_POOL="${1#*=}"; shift 1;;
    *) echo "ERROR: unrecognized argument: $1" >&2; exit 1;;
  esac
done

# Create icon_input directory in current working directory
ICON_INPUT_DIR="./icon_input"
mkdir -p ${ICON_INPUT_DIR}

if [ ! -d "${DATA_POOL}" ]; then
    echo "ERROR: ${DATA_POOL} is not a directory"
    exit 1
fi

# TODO: Sirocco should export this information (dates and parameters)
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

link_from_pool(){
    # Link file from data pool to icon chunk input
    # Assumes all files are already present in DATA_POOL
    FILENAME="$1"
    ICON_INPUT_LINKNAME="${2:-$1}"
    DATA_POOL_FILE_PATH="${DATA_POOL}/${FILENAME}"

    if [ ! -e "${DATA_POOL_FILE_PATH}" ]; then
        echo "ERROR: ${DATA_POOL_FILE_PATH} not found in data pool"
        exit 1
    fi

    ln -s "${DATA_POOL_FILE_PATH}" "./${ICON_INPUT_LINKNAME}"
}

# Enter ICON input dir for current chunk
pushd ${ICON_INPUT_DIR} >/dev/null || exit

# SST_ICE
YYYY=${YYYY_START}
MM=${MM_START}
while : ; do
    link_from_pool "SST_${YYYY}_${MM}_icon_grid_0021_R02B06_G.nc"
    link_from_pool "CI_${YYYY}_${MM}_icon_grid_0021_R02B06_G.nc"
    if [ ${YYYY} == ${YYYY_STOP} ] && [ ${MM} == ${MM_STOP} ]; then
        shift_YYYY_MM
        break
    else
        shift_YYYY_MM
    fi
done
# NOTE: Needs a last step outside of the loop
link_from_pool "SST_${YYYY}_${MM}_icon_grid_0021_R02B06_G.nc"
link_from_pool "CI_${YYYY}_${MM}_icon_grid_0021_R02B06_G.nc"

# OZONE
# TODO: Not necessary for R02B06?
# YYYY=${YYYY_START}
# while : ; do
#     import_and_link "${OZONE_DIR}" "bc_ozone_${YYYY}.nc"
#     [ ${YYYY} == ${YYYY_STOP} ] && break
#     ((YYYY ++))
# done

# AERO KINE
link_from_pool "bc_aeropt_kinne_lw_b16_coa.nc"
link_from_pool "bc_aeropt_kinne_sw_b14_coa.nc"
link_from_pool "bc_aeropt_kinne_sw_b14_fin_1865.nc" "bc_aeropt_kinne_sw_b14_fin.nc"
    
popd  >/dev/null || exit
