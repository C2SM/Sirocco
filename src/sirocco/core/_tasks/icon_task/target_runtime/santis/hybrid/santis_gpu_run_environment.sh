 # NVHPC/CUDA
 # ----------
export NVCOMPILER_ACC_DEFER_UPLOADS=1
export NVCOMPILER_ACC_SYNCHRONOUS=0
export NVCOMPILER_ACC_DEFER_UPLOADS=1
export NVCOMPILER_ACC_USE_GRAPH=1  # Harmless if cuda-graphs is disabled
export NVCOMPILER_ACC_NOTIFY=0
export CUDA_BUFFER_PAGE_IN_THRESHOLD_MS=0.001

# ICON4PY
# -------
if [ "${ICON4PY}" == "true" ]; then
    export CUDAARCHS=90
    export PYTHONOPTIMIZE=2
    export GT4PY_BUILD_CACHE_DIR=${GT4PY_BUILD_CACHE_DIR:-".."}
    # export CUPY_CACHE_DIR=${CUPY_CACHE_DIR:-"${SCRATCH}/.cupy-cache"}
    export CUPY_CACHE_IN_MEMORY=1
    export GT4PY_BUILD_CACHE_LIFETIME=persistent
    export GT4PY_UNSTRUCTURED_HORIZONTAL_HAS_UNIT_STRIDE=1
    export DACE_compiler_cuda_block_size_limit=256
    export PY2FGEN_LOG_LEVEL=WARNING
    source "${ICON4PY_VENV}/bin/activate"
fi
