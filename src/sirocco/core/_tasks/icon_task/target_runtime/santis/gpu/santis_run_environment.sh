# OpenMP environment variables
# ----------------------------
export OMP_NUM_THREADS=1
export ICON_THREADS=1
export OMP_SCHEDULE=static,1
export OMP_DYNAMIC="false"
export OMP_STACKSIZE=200M

# Other runtime environment variables
# -----------------------------------
export CUDA_BUFFER_PAGE_IN_THRESHOLD_MS=0.001
export FI_CXI_SAFE_DEVMEM_COPY_THRESHOLD=0
export FI_CXI_RX_MATCH_MODE=software
export FI_MR_CACHE_MONITOR=disabled
export MPICH_GPU_SUPPORT_ENABLED=1
export NVCOMPILER_ACC_DEFER_UPLOADS=1
export NVCOMPILER_TERM=trace
