# Shared settings and helpers for the SPICE sp001 (gcm2icon) shell tasks.
# Sourced by every task script; tasks run inside the climtools uenv (sbatch --uenv).
set -e

# -- SPICE installation providing cfu, ccaf2icaf, the Python venv, SAMOVAR and functions.inc
SPICE_DIR=/capstor/scratch/cscs/mjaehn/spice
SRCDIR=${SPICE_DIR}/src
PYTHON_VENVDIR=${SRCDIR}/python_util/venv
EXPID=sp001

# -- Tools (no "uenv run" wrapper needed, the uenv is mounted by the job)
CDO=(cdo)
NCCOPY=(nccopy)
NCDUMP=(ncdump)
NCKS=(ncks)
NCATTED=(ncatted)
NCRENAME=(ncrename)
NCRCAT=(ncrcat)
NCECAT=(ncecat)
NCAP2=(ncap2)
NCWA=(ncwa)
NCPDQ=(ncpdq)
CFU=("${SRCDIR}/cfu/bin/cfu")
CCAF2ICAF=("${SRCDIR}/utils/bin/ccaf2icaf")
PYTHON=("${PYTHON_VENVDIR}/bin/python")
CORRECT_CF=("${PYTHON[@]}" "${SRCDIR}/python_util/correct_cf.py")
export INFOV=infon
export SAMOVAR_EXE="${PYTHON_VENVDIR}/bin/python ${SRCDIR}/python_util/samovar.py"
SAMOVAR_SH=("${SRCDIR}/samovar/samovar.sh")
SAMOVAR_LIST=${SRCDIR}/samovar/samovar.csv
SAMOVAR_LIST_TS=${SRCDIR}/samovar/samovar_ts.csv
OUTPUT_MAPPING_FILE=${SPICE_DIR}/data/csv/mapping_to_cosmo.csv

# -- Experiment settings (SPICE job_settings of sp001)
ITYPE_CALENDAR=0          # proleptic gregorian
GCM_PREFIX=cas
HINCBOUND=3
GCM_REMAP=remaplaf
ICON_INPUT_OPTIONAL=
SCEN=historical
ITYPE_SAMOVAR=1           # check last day of each output stream in arch
ITYPE_SAMOVAR_TS=1        # check monthly time series in post
HOUT_INC=(01:00:00 24:00:00 03:00:00 24:00:00 06:00:00 06:00:00 24:00:00 03:00:00 03:00:00 24:00:00)
OPERATION=("" "" "" "" "" "" "" "mean" "" "")
NESTING_STREAM=1
HLEV_STREAM=10
PLEVS=(200. 300. 500. 700. 850. 925. 950.)
ZLEVS=(10.0 34.5 69.0 116.0 178.5 258.5 357.5 477.0)
HLEVS=(10.0 34.5 69.0 116.0 178.5 258.5 357.5 477.0)
UL=47
LL=59
GA_INSTITUTION=
GA_TITLE="Germany 0.11 SPICE"
GA_PROJECT_ID="-"
GA_REALIZATION=1
GA_CONVENTIONS="CF-1.4"
GA_CONVENTIONSURL="http://www.cfconventions.org/"
GA_CONTACT=
GA_ICON_CLM_VERSION="ICON-2025.10"
GA_REFERENCES="https://www.icon-model.org/, https://spice.clm-community.eu/"

# -- Parallelism within one node
MAXPP=${SLURM_CPUS_ON_NODE:-$(nproc)}
OMP_THREADS_CONV2ICON=1
OMP_THREADS_POST=1

# -- Dates in SPICE format (YYYYMMDDHH); YDATE_START is set on the command line
to_cfu_date() { date -u -d "$1" +%Y%m%d%H; }
YDATE_START=$(to_cfu_date "${YDATE_START:?YDATE_START must be set}")
if [[ -n "${SIROCCO_START_DATE:-}" ]]; then
  CURRENT_DATE=$(to_cfu_date "${SIROCCO_START_DATE}")
  NEXT_DATE=$(to_cfu_date "${SIROCCO_STOP_DATE}")
  YYYY=${CURRENT_DATE:0:4}
  MM=${CURRENT_DATE:4:2}
  YYYY_MM=${YYYY}_${MM}
  ISO_NEXT_DATE="${NEXT_DATE:0:8}T${NEXT_DATE:8:2}0000Z"
fi

# -- The SPICE function library expects ${PFDIR}/${EXPID} and ${WORKDIR}/${EXPID}/post
PFDIR=${PWD}/.spice
WORKDIR=${PFDIR}
mkdir -p "${PFDIR}/${EXPID}/scripts" "${WORKDIR}/${EXPID}/post"
ln -sf "${SPICE_DIR}/configure_scripts/template_gcm2icon/scripts/calc_potevap_s.py" "${PFDIR}/${EXPID}/scripts/"
"${CFU[@]}" shellpp "${SPICE_DIR}/configure_scripts/template_gcm2icon/scripts/functions.inc" \
  "${PFDIR}/functions.inc" cscs-alps > /dev/null
source "${PFDIR}/functions.inc"

# Abort unless all files between CURRENT_DATE and NEXT_DATE exist (args: directory, suffix)
check_forcing_files() {
  local RC
  RC=$("${CFU[@]}" check_files "${CURRENT_DATE}" "${NEXT_DATE}" "$(printf %02d "${HINCBOUND}"):00:00" \
       "${GCM_PREFIX}" "${GCM_PREFIX}" "$2" "$1" T "${ITYPE_CALENDAR}")
  if [[ "${RC}" -ne 0 ]]; then
    cat check_files.log >&2
    echo "ERROR: incomplete input files in $1" >&2
    exit 1
  fi
}

# Remap one prepared GCM file to an ICON lateral boundary file (args: file, out_dir, inidata_dir)
make_lbc() {
  local LBC_FILE
  LBC_FILE="$2/$(basename "$1" .nc)_lbc.nc"
  "${CDO[@]}" -s -P "${OMP_THREADS_CONV2ICON}" "${GCM_REMAP},$3/triangular-grid.nc" \
    -selname,T,U,V,W,LNPS,GEOP_ML,QV,QC,QI${ICON_INPUT_OPTIONAL} "$1" "${LBC_FILE}"
  "${NCKS[@]}" -h -A "$3/hyai_hybi.nc" "${LBC_FILE}"
  rename_dims "${LBC_FILE}"
}

# One rename per call, as in SPICE (combined renames are unreliable for netCDF-4 files)
rename_dims() {
  "${NCRENAME[@]}" -h -d level,lev "$1"
  "${NCRENAME[@]}" -h -d cell,ncells "$1"
  "${NCRENAME[@]}" -h -d nv,vertices "$1"
}
