source ./santis_run_environment.sh
srun_cmd="srun --ntasks=$((N_NODES*N_TASKS_PER_NODE)) --ntasks-per-node="${N_TASKS_PER_NODE}" --threads-per-core=1 --distribution=block:block:block"
[ -n "${SIROCCO_UENV}" ] && srun_cmd+=" --uenv=${SIROCCO_UENV}"
[ -n "${SIROCCO_VIEW}" ] && srun_cmd+=" --view=${SIROCCO_VIEW}"
srun_cmd+=" ./santis_cpu.sh ./icon_cpu"
echo "running ICON with ${srun_cmd}"
${srun_cmd}
