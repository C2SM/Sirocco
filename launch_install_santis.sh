#!/usr/bin/bash

BUILD_DIR="/dev/shm/${USER}/sirocco"
uenv run --view default /capstor/store/cscs/userlab/cwd01/leclairm/uenvs/images/sirocco_25.9.sqfs -- "${PWD}/install_santis.sh ${BUILD_DIR}"
# srun --uenv /capstor/store/cscs/userlab/cwd01/leclairm/uenvs/images/sirocco_25.9.sqfs --view default $PWD/install_santis.sh ${1}


