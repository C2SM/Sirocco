# DON

import shutil
from pathlib import Path
from sirocco.core import Workflow
from sirocco.workgraph import AiidaWorkGraph

test_case = Path("../tests/cases/small-icon/").absolute()
workdir = Path("../workdir").absolute()
if workdir.exists():
    workdir.rmdir()
workdir.mkdir()
shutil.copytree(test_case, workdir)
yml_path = workdir / "config/config.yml"
yml_path.read_text().replace("/TESTS_ROOTDIR", str(workdir))
yml_path.read_text().replace("/REMOTE_DATADIR", str(workdir))
# TODO maybe need to copy remote datadir

core_workflow = Workflow.from_config_file(str(workdir / "config.yml"))
aiida_workflow = AiidaWorkGraph(core_workflow)
output_node = aiida_workflow.run()

#for 
#    aiida_workflow.data
#         src: /DATA_REMOTEDIR/tests/cases/small-icon/config/ICON/icon_grid_simple.nc
#         computer: localhost_ssh
#     - ecrad_data:
#         type: file
#         src: /DATA_REMOTEDIR/tests/cases/small-icon/config/ICON/ecrad_data
#         computer: localhost_ssh
#     - ECHAM6_CldOptProps:
#         type: file
#         src: /DATA_REMOTEDIR/tests/cases/small-icon/config/ICON/ECHAM6_CldOptProps.nc
#         computer: localhost_ssh
#     - rrtmg_sw:
#         type: file
#         src: /DATA_REMOTEDIR/tests/cases/small-icon/config/ICON/rrtmg_sw.nc
#         computer: localhost_ssh
#     - dmin_wetgrowth_lookup:
#         type: file
#         src: /DATA_REMOTEDIR/tests/cases/small-icon/config/ICON/dmin_wetgrowth_lookup.nc
#

#config[key].write_text(config[key].read_text().replace("/TESTS_ROOTDIR", str(tmp_path)))
#"/capstor/store/cscs/userlab/cwd01/ricoh"

# real test case
#/capstor/store/cscs/userlab/cwd01/leclairm/Sirocco_test_cases/exclaim_ape_R02B04
