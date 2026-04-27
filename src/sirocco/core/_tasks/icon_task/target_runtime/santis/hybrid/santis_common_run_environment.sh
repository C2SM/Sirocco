ulimit -s unlimited
ulimit -c 0

# Libfabric / Slingshot
# ---------------------
export FI_CXI_SAFE_DEVMEM_COPY_THRESHOLD=0
export FI_CXI_RX_MATCH_MODE=software
export FI_MR_CACHE_MONITOR=disabled

# MPICH
# -----
export MPICH_OFI_NIC_POLICY=GPU
export MPICH_GPU_SUPPORT_ENABLED=1
export MPICH_GPU_IPC_ENABLED=1

# OpenMP
# ------
export OMP_SCHEDULE=guided,16
export OMP_DYNAMIC="false"
export OMP_STACKSIZE=200M

# NVHPC
# ----
export NVCOMPILER_TERM=trace
