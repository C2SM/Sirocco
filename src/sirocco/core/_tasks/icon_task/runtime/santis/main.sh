#!/usr/local/bin/bash

set -e

# Dump environment
# ----------------
# Dump SLURM environment variables to stdout
set | grep SLURM
# Dump full environment to file
set > ./env_${SLURM_JOB_ID}

# Generate SLURM hostfile for arbitrary task distribution
# -------------------------------------------------------
SIROCCO_HOSTFILE="./hostfile-sirocco"
export SLURM_HOSTFILE="./hostfile-${SLURM_JOB_ID}"
export SLURM_HOSTFILE_ANNOTATED="./hostfile-${SLURM_JOB_ID}_annotated"
./generate_hostfile.sh ${SIROCCO_HOSTFILE} ${SLURM_HOSTFILE} ${SLURM_HOSTFILE_ANNOTATED}

# Build srun command
# ------------------
srun_cmd="srun -l --kill-on-bad-exit=1 --mpi=cray_shasta --ntasks=${N_PROCS} --hint=nomultithread --distribution=arbitrary"
[ -n "${CORES_PER_PROC}" ] && srun_cmd+=" --cpus-per-task=${CORES_PER_PROC}"
if [ -n "${SIROCCO_UENV}" ]; then
    srun_cmd+=" --uenv=${SIROCCO_UENV}"
    if [ -n "${ICON_SQUASH}" ]; then
        if [ -z "${ICON_MOUNT}" ]; then
            echo "ERROR: ICON_SQUASH can only be used in conjunction with ICON_MOUNT"
            exit 1
        else
            mkdir -p "${ICON_MOUNT}"
            srun_cmd+=",${ICON_SQUASH}:$(realpath ${ICON_MOUNT})"
        fi
    fi
    [ -n "${SIROCCO_VIEW}" ] && srun_cmd+=" --view=${SIROCCO_VIEW}"
else
    echo "ERROR: SIROCCO_UENV must be provided on Santis"
fi
if [ "${SIROCCO_TARGET}" == "cpu" ]; then
    srun_cmd+=" ./santis_icon_wrapper.sh ./icon_cpu"
elif [ "${SIROCCO_TARGET}" == "gpu" ]; then
    srun_cmd+=" ./santis_icon_wrapper.sh ./icon_gpu"
elif [ "${SIROCCO_TARGET}" == "hybrid" ]; then
    srun_cmd+=" --multi-prog multi-prog.conf"
else
    echo "ERROR: unrecognized SIROCCO_TARGET, got ${SIROCCO_TARGET}"
    exit 1
fi

# Launch
# ------
echo "running ICON with ${srun_cmd}"
${srun_cmd}

# Accounting
# ----------
echo " ==> Accounting"
sacct -j "${SLURM_JOB_ID}" --format "JobID, JobName, AllocCPUs, Elapsed, ElapsedRaw, CPUTimeRAW, ConsumedEnergyRaw, MaxRSS, MaxVMSize, AveRSS"
