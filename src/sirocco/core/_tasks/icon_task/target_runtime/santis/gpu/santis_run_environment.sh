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

# ICON4PY
# -------
if [ -n "${ICON4PY_VENV}" ]; then
    source "${ICON4PY_VENV}/bin/activate"
    export CUDAARCHS=90
    export PYTHONOPTIMIZE=2
    # Default GT4PY_BUILD_CACHE_DIR one level above rundir, i.e. case run directory
    # so that it's set to a common path for all chunks
    export GT4PY_BUILD_CACHE_DIR=${GT4PY_BUILD_CACHE_DIR:-".."}
    export CUPY_CACHE_IN_MEMORY=1
    export GT4PY_BUILD_CACHE_LIFETIME=persistent
    export GT4PY_UNSTRUCTURED_HORIZONTAL_HAS_UNIT_STRIDE=1
    export DACE_compiler_cuda_block_size_limit=256
    export PY2FGEN_LOG_LEVEL=WARNING
fi
