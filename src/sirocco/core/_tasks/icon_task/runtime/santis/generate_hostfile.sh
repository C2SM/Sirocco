#!/usr/local/bin/bash

# Generate final and annotated SLURM hostfiles by replacing node id palceholders in the Sirocco hostfile
# The node id list is obtained by parsing SLURM_NODELIST which is expected to be of the form
# "NID_ROOT[id1,idmin2-idmax2,idmin3-idmax3,id4,...]"

set -e

# HOSTFILE names
if [ $# != 3 ]; then
    echo "ERROR: generate_hostfile.sh takes exactly 3 arguments, got $#"
    exit 1
fi
SIROCCO_HOSTFILE="${1}"
SLURM_HOSTFILE="${2}"
SLURM_HOSTFILE_ANNOTATED="${3}"

if [ ! -f ${SIROCCO_HOSTFILE} ]; then
    echo "ERROR ${SIROCCO_HOSTFILE} not found"
fi
cp ${SIROCCO_HOSTFILE} ${SLURM_HOSTFILE}
cp ${SIROCCO_HOSTFILE} ${SLURM_HOSTFILE_ANNOTATED}

# Extract node list (ranges or single values) from SLURM_NODELIST
NODE_LIST="${SLURM_NODELIST##*[}"
NODE_LIST="${NODE_LIST%%]*}"

# final node root name and 0-padded width
NID_ROOT="${SLURM_NODELIST%%[*}"
nid_width=-1  # initialize with wrong value, infer at first encounter

# infer sirocco node root name and 0-padded width from first line of SIROCCO_HOSTFILE
SIROCCO_NID_ROOT="sirocco_nid"
sirocco_first_line=($(head -n 1 ${SIROCCO_HOSTFILE}))
sirocco_first_nid=${sirocco_first_line[0]}
sirocco_first_idx_pattern=${sirocco_first_nid##*${SIROCCO_NID_ROOT}}
sirocco_nid_width=${#sirocco_first_idx_pattern}

# Replace sirocco nids with actual nids in SLURM_HOSTFILE and SLURM_HOSTFILE_ANNOTATED
sirocco_node_idx=0
# loop over comma separated list
for node_range in ${NODE_LIST//,/ }; do
    # Load node range or single node in array node_range
    IFS='-' read -ra node_min_max <<< "${node_range}"
    # Set min node for current range
    node_min="${node_min_max[0]}"
    # If not set yet, set nid_width
    [ ${nid_width} == -1 ] && nid_width=${#node_min}
    # Set max node for current range (min = max if curent range is single value)
    n_bounds=${#node_min_max[@]}
    [ ${n_bounds} == 1 ] && node_max="${node_min_max[0]}" || node_max="${node_min_max[1]}"
    # Loop over current node range and replace corresponding sirocco node
    for nid_int in $(seq "${node_min}" "${node_max}"); do
        # rebuild sirocco and final node ids
        nid="${NID_ROOT}$(printf "%0${nid_width}d" "${nid_int}")"
        sirocco_nid="${SIROCCO_NID_ROOT}$(printf "%0${sirocco_nid_width}d" "${sirocco_node_idx}")"
        # Replace in final hostfile
        sed -i "s/^${sirocco_nid}.*/${nid}/g" ${SLURM_HOSTFILE}  # => remove annotations for SLURM_HOSTFILE
        # NOTE: Maybe commentting with "#" is possible diretly in the real SLURM_HOSTFILE
        sed -i "s/^${sirocco_nid}/${nid}/g" ${SLURM_HOSTFILE_ANNOTATED}
        # switch to next sirocco node id 
        ((sirocco_node_idx = sirocco_node_idx + 1))
    done
done
