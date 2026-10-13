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
sirocco_hostfile="${1}"
slurm_hostfile="${2}"
slurm_hostfile_annotated="${3}"

if [ ! -f ${sirocco_hostfile} ]; then
    echo "ERROR ${sirocco_hostfile} not found"
fi
cp ${sirocco_hostfile} ${slurm_hostfile}
cp ${sirocco_hostfile} ${slurm_hostfile_annotated}

# Extract node list (ranges or single values) from SLURM_NODELIST
node_list="${SLURM_NODELIST##*[}"
node_list="${node_list%%]*}"

# final node root name and 0-padded width
NID_ROOT="${SLURM_NODELIST%%[*}"
nid_width=-1  # initialize with wrong value, infer at first encounter

# infer sirocco node root name and 0-padded width from first line of sirocco_hostfile
sirocco_nid_root="sirocco_nid"
sirocco_first_line=($(head -n 1 ${sirocco_hostfile}))
sirocco_first_nid=${sirocco_first_line[0]}
sirocco_first_idx_pattern=${sirocco_first_nid##*${sirocco_nid_root}}
sirocco_nid_width=${#sirocco_first_idx_pattern}

# Replace sirocco nids with actual nids in slurm_hostfile and slurm_hostfile_annotated
sirocco_node_idx=0
# loop over comma separated list
for node_range in ${node_list//,/ }; do
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
        sirocco_nid="${sirocco_nid_root}$(printf "%0${sirocco_nid_width}d" "${sirocco_node_idx}")"
        # Replace in final hostfile
        sed -i "s/^${sirocco_nid}.*/${nid}/g" ${slurm_hostfile}  # => remove annotations for slurm_hostfile
        # NOTE: Maybe commentting with "#" is possible diretly in the real slurm_hostfile
        sed -i "s/^${sirocco_nid}/${nid}/g" ${slurm_hostfile_annotated}
        # switch to next sirocco node id 
        ((sirocco_node_idx = sirocco_node_idx + 1))
    done
done
