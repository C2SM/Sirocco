# DON

import os
import shutil
from pathlib import Path
from aiida import load_profile
from aiida.orm import load_computer
load_profile()

from sirocco.core import Workflow
from sirocco.workgraph import AiidaWorkGraph


# put the download things to test utils
import requests
class DownloadError(RuntimeError):
    def __init__(self, url: str, response: requests.Response):
        super().__init__(f"Failed downloading file {url} , exited with response {response}")


def download_file(url: str, file_path: Path):
    response = requests.get(url)
    if not response.ok:
        raise DownloadError(url, response)

    file_path.write_bytes(response.content)


def download_icon_grid(dest_dir: Path, filename: str):
    url = "https://github.com/agoscinski/icon-testfiles/raw/refs/heads/main/icon_grid_0013_R02B04_R.nc"
    if not dest_dir.exists():
        raise ValueError(f"Destination directory {dest_dir} does not exist.")
    if not dest_dir.is_dir():
        raise ValueError(f"Destination directory {dest_dir} is not a directory.")

    icon_grid_path = dest_dir / filename
    download_file(url, icon_grid_path)

    return icon_grid_path

# must be run from repo rootdir
test_case = Path("tests/cases/small-icon/config").absolute()
workdir = Path("workdir").absolute()
if workdir.exists():
    workdir.rmdir()
(workdir / "tests/cases/small-icon/").mkdir(parents=True)
shutil.copytree(test_case, workdir / "tests/cases/small-icon/config")
yml_path = workdir / "tests/cases/small-icon/config/config.yml"

REMOTE_TESTSDIR = "/capstor/scratch/cscs/ricoh/sirocco-tests/"
yml_path.write_text(yml_path.read_text().replace("/TESTS_ROOTDIR/", REMOTE_TESTSDIR).replace("/DATA_REMOTEDIR/", REMOTE_TESTSDIR))
print("yml", yml_path, "content:\n", yml_path.read_text())

# TODO @ ali why this file cannot be put?
#download_icon_grid(workdir / "tests/cases/small-icon/config/ICON", "icon_grid_simple.nc")
(workdir / "tests/cases/small-icon/config/ICON/icon_grid_simple.nc").touch()


computer = load_computer('remote')
prepend_text = f"""
#TODO matthieu what to set for santis?

# TODO ali workaround because we cannot put gridfile
wget https://github.com/agoscinski/icon-testfiles/raw/refs/heads/main/icon_grid_0013_R02B04_R.nc
mv icon_grid_0013_R02B04_R.nc {Path(REMOTE_TESTSDIR) / "tests/cases/small-icon/config/ICON/icon_grid_simple.nc"}
"""
computer.set_prepend_text(prepend_text)
transport = computer.get_transport()
with transport:
    transport.rmtree(REMOTE_TESTSDIR)
    for dirpath, dirnames, filenames in os.walk(workdir):
        dirpath = Path(dirpath)
        relative_dirpath = dirpath.relative_to(workdir)
        path_on_remote = Path(REMOTE_TESTSDIR) / relative_dirpath
        print("relative_dirpath", relative_dirpath)
        print("create path", path_on_remote)
        transport.mkdir(path_on_remote, ignore_existing=True)
        for filename in filenames:
            source = dirpath / filename
            dest = Path(REMOTE_TESTSDIR) / relative_dirpath / filename
            print("put file from", source, "to", dest)
            transport.putfile(source, dest)

    print(transport.listdir(REMOTE_TESTSDIR))

core_workflow = Workflow.from_config_file(str(yml_path))
aiida_workflow = AiidaWorkGraph(core_workflow)
output_node = aiida_workflow.run()

remote_bin_path = Path(REMOTE_TESTSDIR) / Path("tests/cases/small-icon/config/ICON/bin/icon")
icon_bin_path = "/capstor/store/cscs/userlab/cwd01/leclairm/archive_icon_build/cpu/icon-nwp/bin/icon"
with transport:
    transport.symlink(icon_bin_path, remote_bin_path)


print("output_node.is_finished_ok", output_node.is_finished_ok)
if not output_node.is_finished_ok:
    print(f"Not successful run. Got exit code {output_node.exit_code} with message {output_node.exit_message}.")
    from aiida.cmdline.utils.common import get_workchain_report, get_calcjob_report
    # overall report but often not enough to really find the bug, one has to go to calcjob
    print(get_workchain_report(output_node, levelname='REPORT'))
    # the calcjobs are typically stored in 'called_descendants'
    for node in output_node.called_descendants:
        print(f"{node.process_label} workdir:", node.get_remote_workdir())
        print(f"{node.process_label} report:\n", get_calcjob_report(node))
