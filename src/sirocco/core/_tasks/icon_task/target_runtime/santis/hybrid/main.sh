source ./santis_run_environment.sh

gen_slurm_host_file() {
    # Generate final SLURM hostfile by replacing node id palceholders in the Sirocco hostfile
    # The node id list is obtained by parsing SLURM_NODELIST which is expected to be of the form
    # "NID_ROOT[id1,idmin2-idmax2,idmin3-idmax3,id4,...]"

    # HOSTFILE names
    SIROCCO_HOSTFILE="./hostfile-sirocco"
    if [ ! -f ${SIROCCO_HOSTFILE} ]; then
        echo "ERROR ${SIROCCO_HOSTFILE} not found"
    fi
    cp ${SIROCCO_HOSTFILE} ${SLURM_HOSTFILE} 

    # Extract node list (ranges or single values) from SLURM_NODELIST
    NODE_LIST="${SLURM_NODELIST##*[}"
    NODE_LIST="${NODE_LIST%%]*}"

    # final node root name and 0-padded length
    NID_ROOT="${SLURM_NODELIST%%[*}"
    nid_length=0

    # Sirocco node root name and 0-padded length
    SIROCCO_NID_ROOT="sirocco_nid"
    sirocco_first_line="$(head -n 1 ${SIROCCO_HOSTFILE})"
    sirocco_first_id=${sirocco_first_line##*${SIROCCO_NID_ROOT}}
    sirocco_nid_length=${#sirocco_first_id}

    # Initial Sirocco node id
    sirocco_nid_int=0

    # Sub-function to replace sircco node id with actual node id
    repalce_and_iter_sirocco_id() {
        # rebuild sirocco and final node ids
        nid="${NID_ROOT}$(printf "%0${nid_length}d" "${nid_int}")"
        sirocco_nid="${SIROCCO_NID_ROOT}$(printf "%0${sirocco_nid_length}d" "${sirocco_nid_int}")"
        # Replace in final hostfile
        sed -i "s/${sirocco_nid}/${nid}/g" ${SLURM_HOSTFILE}
        # switch to next sirocco node id
        ((sirocco_nid_int ++))
    }
    
    # loop over comma separated list
    for node_range in ${NODE_LIST//,/ }; do
        IFS='-' read -ra node_min_max <<< "${node_range}"
        [ ${nid_length} == 0 ] && nid_length=${#node_min_max[0]}
        n_bounds=${#node_min_max[@]}
        if [ ${n_bounds} == 1 ]; then  # => single node id
            nid_int=$((10#"${node_min_max[0]}"))
            repalce_and_iter_sirocco_id
        elif [ ${n_bounds} == 2 ]; then  # => node range
            for nid_int in $(seq "${node_min_max[0]}" "${node_min_max[1]}"); do
                repalce_and_iter_sirocco_id
            done
        else
            echo "ERROR looking for either value or min-max range: should find 1 or 2 items, got {n_bounds}"
        fi
    done
}

# Generate SLURM hostfile for arbitrary task distribution
export SLURM_HOSTFILE="./hostfile-${SLURM_JOB_ID}"
gen_slurm_host_file

# Dump SLURM environment variables
set | grep SLURM

# build srun command
srun_cmd="srun --ntasks=${N_TASKS} --hint='nomultithread' --distribution='arbitrary' --multi-prog multi-prog.conf"
[ -n "${CPUS_PER_TASK}" ] && srun_cmd+=" --cpus-per-task=${CPUS_PER_TASK}"
[ -n "${SIROCCO_UENV}" ] && srun_cmd+=" --uenv=${SIROCCO_UENV}"
[ -n "${SIROCCO_VIEW}" ] && srun_cmd+=" --view=${SIROCCO_VIEW}"

# launch
echo "running ICON with ${srun_cmd}"
${srun_cmd}
