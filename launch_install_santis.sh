#!/usr/bin/bash

BUILD_DIR="/dev/shm/${USER}/sirocco"
mkdir -p "${BUILD_DIR}"
uenv run --view sirocco /capstor/store/cscs/userlab/cwd01/leclairm/uenvs/images/sirocco_v0.0.1.sqfs -- ${PWD}/install_santis.sh ${BUILD_DIR}
# srun --uenv /capstor/store/cscs/userlab/cwd01/leclairm/uenvs/images/sirocco_v0.0.1.sqfs --view sirocco $PWD/install_santis.sh ${BUILD_DIR}


