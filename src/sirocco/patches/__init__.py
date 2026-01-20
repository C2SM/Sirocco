"""Patches for third-party libraries to support Sirocco's workflow patterns."""

from sirocco.patches.firecrest_symlink import patch_firecrest_symlink
from sirocco.patches.slurm_dependencies import patch_slurm_dependency_handling
from sirocco.patches.workgraph_window import patch_workgraph_window

__all__ = ["patch_firecrest_symlink", "patch_slurm_dependency_handling", "patch_workgraph_window"]
