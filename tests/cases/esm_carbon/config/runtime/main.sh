#!/usr/local/bin/bash

set -e

# Hang watchdog settings: a log that has not grown for the given time is treated as a hang
WATCHDOG_TIMEOUT=${WATCHDOG_TIMEOUT:-1800}            # after the first time step
WATCHDOG_INIT_TIMEOUT=${WATCHDOG_INIT_TIMEOUT:-3600}  # before it, a full GT4Py recompilation can exceed 30 min
export WATCHDOG_GDB_CPU_PER_NODE=${WATCHDOG_GDB_CPU_PER_NODE:-2}
TIMEWARP=${TIMEWARP:-/capstor/scratch/cscs/mjaehn/jcp/jcp/jcp_build/icon-mpim/utils/timewarp}

job_log="${PWD}/run_script.${SLURM_JOB_ID}.o"
export DIAG_DIR="${PWD}/diag.${SLURM_JOB_ID}"
export ICON_GPU_EXE="${PWD}/icon_gpu"
mkdir -p "${DIAG_DIR}/gpu"
scontrol show hostnames "${SLURM_JOB_NODELIST}" > "${DIAG_DIR}/nodelist.txt"

# Per-line timestamps in the job log
# ---------------------------------
if [ -x "${TIMEWARP}" ]; then
    pipe="${PWD}/timewarp_$$.pipe"
    mkfifo "${pipe}"
    trap "rm -f ${pipe}" EXIT
    "${TIMEWARP}" "${pipe}" &
    # without a reader on the pipe, the exec below would block until the time limit
    sleep 2; kill -0 $! 2>/dev/null || { echo "ERROR: timewarp failed to start"; exit 1; }
    exec > "${pipe}" 2>&1
else
    echo "WARNING: timewarp not found at ${TIMEWARP}, log lines are not timestamped"
fi

# Progressive Lustre layout: small files on one OST, large ones over up to 32 OSTs
# --------------------------------------------------------------------------------
# Only real directories, so that linked input directories are never modified
for dir in . $(find . -mindepth 1 -maxdepth 1 -type d); do
    lfs setstripe --component-end 4M --stripe-count 1 \
                  --component-end 64M --stripe-count 4 \
                  --component-end -1 --stripe-count 32 --stripe-size 4M \
                  "${dir}" || echo "WARNING: could not set stripe layout on ${dir}"
done

# Caches of the ICON4Py just-in-time compilation
# ----------------------------------------------
if [ -n "${ICON4PY_VENV}" ]; then
    export GT4PY_BUILD_CACHE_DIR=${GT4PY_BUILD_CACHE_DIR:-".."}
    export CUPY_CACHE_DIR="$(dirname "${GT4PY_BUILD_CACHE_DIR}")/cupy"
    mkdir -p "${GT4PY_BUILD_CACHE_DIR}" "${CUPY_CACHE_DIR}"
fi

# Generate SLURM hostfile for arbitrary task distribution
# -------------------------------------------------------
sirocco_hostfile="./hostfile-sirocco"
export SLURM_HOSTFILE="./hostfile-${SLURM_JOB_ID}"
export SLURM_HOSTFILE_ANNOTATED="./hostfile-${SLURM_JOB_ID}_annotated"
./generate_hostfile.sh ${sirocco_hostfile} ${SLURM_HOSTFILE} ${SLURM_HOSTFILE_ANNOTATED}

if [ -n "$SLURM_NTASKS_PER_NODE" ]; then
    # has to happen before `srun` and bash cannot export arrays, so kept as string
    export VISIBLE_NUMA_NODES_GLOBAL=$(numactl --show | grep nodebind | sed 's/^nodebind://')
fi

# Dump environment
# ----------------
# Dump SLURM environment variables to stdout
set | grep SLURM
# Dump full environment to file
set > ./env_${SLURM_JOB_ID}

# Build srun command
# ------------------
uenv_opts=""
if [ -n "${ICON_UENV}" ]; then
    uenv_opts=" --uenv=${ICON_UENV}"
    [ -n "${ICON_VIEW}" ] && uenv_opts+=" --view=${ICON_VIEW}"
fi
srun_cmd="srun -l --kill-on-bad-exit=1 --mpi=cray_shasta --ntasks=${N_PROCS} --hint=nomultithread --gres-flags=allow-task-sharing --distribution=arbitrary${uenv_opts}"
[ -n "${CORES_PER_PROC}" ] && srun_cmd+=" --cpus-per-task=${CORES_PER_PROC}"
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

# Hang watchdog
# -------------
# On a hang: collect per-node diagnostics, then abort the model step with SIGABRT
# to obtain Python and NVHPC tracebacks
hang_watchdog(){
    local timeout=${WATCHDOG_INIT_TIMEOUT} started=0 idle main_step
    while kill -0 ${srun_pid} 2>/dev/null; do
        sleep 60
        if (( ! started )) && grep -qE -m1 '^(\S+ +)?0: +Time step: ' "${job_log}"; then
            started=1; timeout=${WATCHDOG_TIMEOUT}
        fi
        idle=$(( $(date +%s) - $(stat -c %Y "${job_log}") ))
        (( idle < timeout )) && continue

        echo "WATCHDOG: no log output for ${idle} s, assuming a hang. Collecting diagnostics in ${DIAG_DIR}"
        main_step=$(squeue -h -s -j ${SLURM_JOB_ID} -o %i | grep -E '\.[0-9]+$' | sort -t. -k2 -n | head -1)
        cp ${SLURM_HOSTFILE_ANNOTATED} multi-prog.conf "${DIAG_DIR}/" 2>/dev/null || true
        scontrol show job ${SLURM_JOB_ID} > "${DIAG_DIR}/scontrol_job.txt"
        squeue -s -j ${SLURM_JOB_ID} > "${DIAG_DIR}/squeue_steps.txt"
        env -u SLURM_HOSTFILE timeout 1200 srun --overlap --jobid=${SLURM_JOB_ID} --nodes=${SLURM_JOB_NUM_NODES} \
            --ntasks-per-node=1 --distribution=block --mem=0 --gpus-per-node=${SLURM_GPUS_ON_NODE}${uenv_opts} \
            ./diag_collect.sh "${DIAG_DIR}" > "${DIAG_DIR}/collect.log" 2>&1
        echo "WATCHDOG: diagnostics done, sending SIGABRT to step ${main_step}"
        scancel --signal=ABRT ${main_step}
        sleep 120
        echo "WATCHDOG: cancelling step ${main_step}"
        scancel ${main_step}
        return
    done
}

# Launch
# ------
echo "running ICON with ${srun_cmd}"
${srun_cmd} &
srun_pid=$!
hang_watchdog &
watchdog_pid=$!
srun_status=0
wait ${srun_pid} || srun_status=$?
kill ${watchdog_pid} 2>/dev/null || true

# Accounting
# ----------
echo " ==> Accounting"
sacct -j "${SLURM_JOB_ID}" --format "JobID, JobName, AllocCPUs, Elapsed, ElapsedRaw, CPUTimeRAW, ConsumedEnergyRaw, MaxRSS, MaxVMSize, AveRSS"

if [ ${srun_status} -ne 0 ]; then
    echo "ERROR: srun exited with status ${srun_status}, diagnostics in ${DIAG_DIR}"
    exit ${srun_status}
fi
