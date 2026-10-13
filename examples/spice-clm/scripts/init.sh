#!/bin/bash
# One-off preparation (SPICE conv2icon part I and ICON radiation input links):
#  - remapping helpers and the first lateral boundary file in <inidata_dir>
#  - initial state in <ifs2icon_file>
#  - aerosol, volcanic and ozone files under the names ICON expects in <radiation_dir>
# Usage: YDATE_START=<iso> YDATE_STOP=<iso> init.sh <forcing_dir> <lam_grid> <extpar>
#          <kinne_dir> <volc_dir> <ozone_dir> <inidata_dir> <ifs2icon_file> <radiation_dir>
source "$(dirname "$0")/common.sh"
PIN=$1
LAM_GRID=$2
EXTPAR=$3
KINNE_DIR=$4
VOLC_DIR=$5
OZONE_DIR=$6
INIDIR=$(realpath -m "$7")
INI_FILE=$(realpath -m "$8")
RADDIR=$(realpath -m "$9")
TMP=${PWD}/tmp
mkdir -p "${INIDIR}" "${RADDIR}" "${TMP}"

echo "[INIT] radiation input links"
ln -sf "${KINNE_DIR}/germany011_aeropt_kinne_sw_b14_coa_rast.nc" "${RADDIR}/bc_aeropt_kinne_sw_b14_coa.nc"
ln -sf "${KINNE_DIR}/germany011_aeropt_kinne_lw_b16_coa_rast.nc" "${RADDIR}/bc_aeropt_kinne_lw_b16_coa.nc"
ln -sf "${KINNE_DIR}/germany011_aeropt_kinne_sw_b14_fin_1850_rast.nc" "${RADDIR}/bc_aeropt_kinne_sw_b14_fin.nc"
LAST_YEAR=$(date -u -d "$(date -u -d "${YDATE_STOP}" '+%Y-%m-%d %H:%M') 1 hour ago" +%Y)
for (( Y=${YDATE_START:0:4}-1; Y<=LAST_YEAR+1; Y++ )); do
  ln -sf "${VOLC_DIR}/bc_aeropt_cmip6_volc_lw_b16_sw_b14_${Y}.nc" "${RADDIR}/bc_aeropt_volc_lw_b16_sw_b14_${Y}.nc"
  # From 2015 on, SPICE takes ozone from the SSP2-4.5 scenario
  if (( Y > 2014 )); then
    ln -sf "${OZONE_DIR}/SSP_scenarios/bc_ozone_ssp245_${Y}.nc" "${RADDIR}/bc_ozone_${Y}.nc"
  else
    ln -sf "${OZONE_DIR}/bc_ozone_${SCEN}_${Y}.nc" "${RADDIR}/bc_ozone_${Y}.nc"
  fi
done
for LINK in "${RADDIR}"/*; do
  [[ -e "${LINK}" ]] || echo "WARNING: missing target for $(basename "${LINK}")"
done

echo "[INIT] initial data for ${YDATE_START}"
INPUT_ICON_FILE=${PIN}/${GCM_PREFIX}${YDATE_START}.nc
TRIANGULAR_GRID=${INIDIR}/triangular-grid.nc
HYAI_HYBI=${INIDIR}/hyai_hybi.nc
INPUT_OCEAN_AREA=${INIDIR}/input_ocean_area.nc
INPUT_LAND_AREA=${TMP}/input_land_area.nc
INPUT_FR_LAND=${TMP}/input_FR_LAND.nc
OUTPUT_FR_LAND=${TMP}/output_FR_LAND.nc
OUTPUT_OCEAN_AREA=${TMP}/output_ocean_area.nc
OUTPUT_LAND_AREA=${TMP}/output_land_area.nc
OUTPUT_LSM=${TMP}/output_lsm.nc

# Land/sea masks of the driving data and of the ICON domain
"${CDO[@]}" -s selname,LSM "${INPUT_ICON_FILE}" "${INPUT_FR_LAND}"
"${NCRENAME[@]}" -h -v LSM,FR_LAND "${INPUT_FR_LAND}"
"${CDO[@]}" -s selname,FR_LAND "${EXTPAR}" "${OUTPUT_FR_LAND}"
"${NCKS[@]}" --no_tmp_fl -h -A -v time "${INPUT_FR_LAND}" "${OUTPUT_FR_LAND}"
"${NCECAT[@]}" -O -u time "${OUTPUT_FR_LAND}" "${OUTPUT_FR_LAND}"
"${CDO[@]}" -L -s setctomiss,0. -ltc,0.5 "${INPUT_FR_LAND}" "${INPUT_OCEAN_AREA}"
"${CDO[@]}" -L -s setctomiss,0. -gec,0.5 "${INPUT_FR_LAND}" "${INPUT_LAND_AREA}"
"${CDO[@]}" -L -s setctomiss,0. -ltc,1. "${OUTPUT_FR_LAND}" "${OUTPUT_OCEAN_AREA}"
"${CDO[@]}" -L -s setctomiss,0. -gtc,0. "${OUTPUT_FR_LAND}" "${OUTPUT_LAND_AREA}"
"${CDO[@]}" -s setrtoc2,0.5,1.0,1,0 "${OUTPUT_FR_LAND}" "${OUTPUT_LSM}"
"${CDO[@]}" -s selvar,cell_area "${LAM_GRID}" "${TRIANGULAR_GRID}"

# Land-only fields (soil, snow)
"${CDO[@]}" -s setmisstodis -selname,SMIL1,SMIL2,SMIL3,SMIL4,STL1,STL2,STL3,STL4,W_SNOW,T_SNOW \
  "${INPUT_ICON_FILE}" "${TMP}/l1.nc"
"${CDO[@]}" -s -P "${OMP_THREADS_CONV2ICON}" "${GCM_REMAP},${TRIANGULAR_GRID}" "${TMP}/l1.nc" "${TMP}/l2.nc"
"${CDO[@]}" -s div "${TMP}/l2.nc" "${OUTPUT_LAND_AREA}" "${TMP}/output_l.nc"

# Skin temperature, remapped separately over sea and land
"${CDO[@]}" -s selname,SKT "${INPUT_ICON_FILE}" "${TMP}/skt.nc"
for PART in ocean land; do
  IN_AREA=${INPUT_OCEAN_AREA}; OUT_AREA=${OUTPUT_OCEAN_AREA}
  [[ ${PART} == land ]] && IN_AREA=${INPUT_LAND_AREA} && OUT_AREA=${OUTPUT_LAND_AREA}
  "${CDO[@]}" -s div "${TMP}/skt.nc" "${IN_AREA}" "${TMP}/ls1.nc"
  "${CDO[@]}" -s setmisstodis "${TMP}/ls1.nc" "${TMP}/ls2.nc"
  "${CDO[@]}" -s -P "${OMP_THREADS_CONV2ICON}" "${GCM_REMAP},${TRIANGULAR_GRID}" "${TMP}/ls2.nc" "${TMP}/ls3.nc"
  "${CDO[@]}" -s div "${TMP}/ls3.nc" "${OUT_AREA}" "${TMP}/${PART}_part.nc"
done
"${CDO[@]}" -s ifthenelse "${OUTPUT_LSM}" "${TMP}/land_part.nc" "${TMP}/ocean_part.nc" "${TMP}/output_ls.nc"

# Remaining fields, then merge everything into the initial file
"${NCKS[@]}" -h -O -x -v W_SNOW,T_SNOW,STL1,STL2,STL3,STL4,SMIL1,SMIL2,SMIL3,SMIL4,SKT,LSM \
  "${INPUT_ICON_FILE}" "${TMP}/rest.nc"
"${CDO[@]}" -s -P "${OMP_THREADS_CONV2ICON}" "${GCM_REMAP},${TRIANGULAR_GRID}" "${TMP}/rest.nc" "${INI_FILE}"
"${NCKS[@]}" -h -A "${TMP}/output_l.nc" "${INI_FILE}"
"${NCKS[@]}" -h -A "${TMP}/output_ls.nc" "${INI_FILE}"
"${NCKS[@]}" -h -A "${OUTPUT_LSM}" "${INI_FILE}"
"${NCATTED[@]}" -h -a coordinates,FR_LAND,o,c,"clon clat" "${INI_FILE}"
"${NCRENAME[@]}" -h -v FR_LAND,LSM "${INI_FILE}"
"${NCRENAME[@]}" -h -v SIC,CI "${INI_FILE}"
rename_dims "${INI_FILE}"

# Vertical coordinate coefficients are lost by CDO and have to be added again
"${NCKS[@]}" -h -O -C -v ak,bk "${INPUT_ICON_FILE}" "${HYAI_HYBI}"
"${NCATTED[@]}" -h -a ,global,d,, "${HYAI_HYBI}"
"${NCRENAME[@]}" -d level1,nhyi "${HYAI_HYBI}"
"${NCRENAME[@]}" -v ak,hyai "${HYAI_HYBI}"
"${NCRENAME[@]}" -v bk,hybi "${HYAI_HYBI}"
"${NCKS[@]}" -h -A "${HYAI_HYBI}" "${INI_FILE}"

# ICON reads the boundary file of the initial date at every (re)start
make_lbc "${INPUT_ICON_FILE}" "${INIDIR}" "${INIDIR}"

rm -rf "${TMP}"
echo "[INIT] finished"
