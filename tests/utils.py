import pathlib

def setup_hpc_data_environment(config_rootdir, computer_label="slurm-ssh"):
    """Setup HPC-like data environment for testing."""
    import subprocess
    import os

    ssh_key = os.path.expanduser("~/.ssh/slurm_rsa")
    hpc_data_path = "/scratch/icon_data"

    # Copy data to container
    subprocess.run([
        "ssh", "-p", "5001", "-i", ssh_key, "-o", "StrictHostKeyChecking=no",
        "xenon@localhost", f"mkdir -p {hpc_data_path}"
    ], check=True)

    local_icon_data = config_rootdir / "ICON"
    subprocess.run([
        "scp", "-r", "-P", "5001", "-i", ssh_key, "-o", "StrictHostKeyChecking=no",
        str(local_icon_data) + "/",
        f"xenon@localhost:{hpc_data_path}/"
    ], check=True)

    return hpc_data_path

def update_workflow_data_paths(workflow, hpc_data_path):
    """Update workflow data paths to point to HPC locations."""
    for data in workflow.data.available:
        if str(data.src).startswith("/TESTS_ROOTDIR"):
            filename = pathlib.Path(data.src).name
            data.src = pathlib.Path(f"{hpc_data_path}/{filename}")
    return workflow