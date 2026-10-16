"""Unit tests for ICON port handlers of coupled atmosphere/ocean/land setups."""

import textwrap
from itertools import chain

import pytest

from sirocco.core import Workflow
from sirocco.core.tasks.icon_task.ports import PortHandler

MASTER_NML = """
&master_nml
 lrestart = .false.
/
&master_model_nml
 model_name = "atmo"
 model_namelist_filename = "atm.nml"
 model_type = 1
/
&master_model_nml
 model_name = "ocean"
 model_namelist_filename = "oce.nml"
 model_type = 2
/
&jsb_control_nml
 restart_jsbach = .false.
/
&jsb_model_nml
 model_name = "JSBACH"
 model_namelist_filename = "lnd.nml"
/
"""

MODEL_NMLS = {
    "atm.nml": "&parallel_nml\n/\n&io_nml\n restart_write_mode = 'joint procs multifile'\n/\n",
    "oce.nml": (
        "&parallel_nml\n/\n&io_nml\n restart_write_mode = 'joint procs multifile'\n/\n"
        "&ocean_initialConditions_nml\n initialize_fromRestart = .false.\n/\n"
    ),
    "lnd.nml": "&jsb_hd_nml\n read_initial_reservoirs = .true.\n/\n",
}

CONFIG = """
name: coupled
scheduler: slurm
cycles:
  - chunks:
      cycling:
        start_date: 2020-01-01T00:00
        stop_date: 2020-03-01T00:00
        period: P1M
      tasks:
        - icon:
            components:
              atmo:
                inputs:
                  aes_ghg_file: [ghg]
                  output_dict: [dict]
                  restart_in:
                    - atmo_restart:
                        when:
                          after: 2020-01-01T00:00
                        target_cycle:
                          lag: -P1M
                outputs:
                  restart_out: [atmo_restart]
              JSBACH:
                inputs:
                  jsb_lctlib: [lctlib]
              ocean:
                inputs:
                  hamocc_dust: [dust]
                  ocean_restart_init:
                    - spinup_restart:
                        when:
                          at: 2020-01-01T00:00
                  restart_in:
                    - ocean_restart:
                        when:
                          after: 2020-01-01T00:00
                        target_cycle:
                          lag: -P1M
                outputs:
                  restart_out: [ocean_restart]
tasks:
  - icon:
      plugin: icon
      computer: localhost
      yac_coupling: ICON/coupling.yaml
      nodes: 1
      procs_per_node: 4
      exe:
        cpu:
          path: {root}/icon
          procs:
            atmo: {{}}
            ocean: {{}}
      namelists:
        - ./ICON/icon_master.namelist
        - ./ICON/atm.nml
        - ./ICON/oce.nml
        - ./ICON/lnd.nml
data:
  available:
{available}
  generated:
    - atmo_restart: {{}}
    - ocean_restart: {{}}
"""

AVAILABLE = ["ghg", "dict", "lctlib", "dust", "spinup_restart"]


@pytest.fixture
def coupled_tasks(tmp_path):
    icon_dir = tmp_path / "ICON"
    icon_dir.mkdir()
    (icon_dir / "icon_master.namelist").write_text(MASTER_NML)
    for name, content in MODEL_NMLS.items():
        (icon_dir / name).write_text(content)
    (tmp_path / "icon").touch()
    for name in AVAILABLE:
        (tmp_path / name).touch()
    available = "\n".join(f"    - {name}:\n        path: {tmp_path / name}" for name in AVAILABLE)
    config = CONFIG.format(root=tmp_path, available=available)
    wf = Workflow.from_config_str(textwrap.dedent(config), rootdir=tmp_path)
    tasks = list(wf.tasks)
    for task in tasks:
        task.resolve_output_data_paths()
        task.run_dir.mkdir(parents=True)
        for model in task.models.values():
            for port in chain(model.inputs, model.outputs):
                PortHandler.handle(port, model)
    return tasks


def test_fixed_name_links(coupled_tasks, tmp_path):
    run_dir = coupled_tasks[0].run_dir
    assert (run_dir / "bc_greenhouse_gases.nc").readlink() == tmp_path / "ghg"
    assert (run_dir / "lctlib_nlct21.def").readlink() == tmp_path / "lctlib"
    assert (run_dir / "dust.nc").readlink() == tmp_path / "dust"


def test_output_dict(coupled_tasks, tmp_path):
    io_nml = coupled_tasks[0].models["atmo"].namelist["io_nml"]
    assert io_nml["output_nml_dict"] == io_nml["netcdf_dict"] == str(tmp_path / "dict")


def test_ocean_restart_init_first_chunk(coupled_tasks, tmp_path):
    first = coupled_tasks[0]
    assert not first.is_restart
    assert first.models["ocean"].namelist["ocean_initialConditions_nml"]["initialize_fromRestart"]
    assert (first.run_dir / "multifile_restart_ocean.mfr").readlink() == tmp_path / "spinup_restart"
    assert first.models["JSBACH"].namelist["jsb_hd_nml"]["read_initial_reservoirs"]


def test_restart_chunk(coupled_tasks):
    first, second = coupled_tasks
    assert second.is_restart
    assert not second.models["ocean"].namelist["ocean_initialConditions_nml"]["initialize_fromRestart"]
    assert (second.run_dir / "multifile_restart_ocean.mfr").readlink() == first.run_dir / "multifile_restart_ocean.mfr"
    assert not second.models["JSBACH"].namelist["jsb_hd_nml"]["read_initial_reservoirs"]
