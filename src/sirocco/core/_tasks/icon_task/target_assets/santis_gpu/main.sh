source ./santis_run_environment.sh
srun --ntasks=$((N_NODES*N_TASKS_PER_NODE)) --ntasks-per-node="${N_TASKS_PER_NODE}" --threads-per-core=1 --distribution=cyclic ./santis_gpu.sh icon_gpu
