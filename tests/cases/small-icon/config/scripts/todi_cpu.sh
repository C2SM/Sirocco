#!/usr/local/bin/bash -l

# ICON
#
# ---------------------------------------------------------------
# Copyright (C) 2004-2024, DWD, MPI-M, DKRZ, KIT, ETH, MeteoSwiss
# Contact information: icon-model.org
#
# See AUTHORS.TXT for a list of authors
# See LICENSES/ for license information
# SPDX-License-Identifier: BSD-3-Clause
# ---------------------------------------------------------------

export LOCAL_RANK=$SLURM_LOCALID
export GLOBAL_RANK=$SLURM_PROCID
export NUMA=(0 1 2 3)
export SOCKET_ID=$(($LOCAL_RANK / 72))
export NUMA_NODE=${NUMA[$SOCKET_ID]}

ulimit -s unlimited
numactl --cpunodebind=$NUMA_NODE --membind=$NUMA_NODE bash -c "$@"
