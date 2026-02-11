"""Sanity checks for ICON availability.

This test verifies that the ICON environment is properly set up before running
more expensive integration tests. Useful for HPC runners where ICON needs to be
available.
"""

import pytest


@pytest.mark.requires_icon
@pytest.mark.usefixtures("icon_filepath_executable", "icon_grid_path")
def test_icon():
    """Verify ICON executable and test data are available.

    This test checks:
    - ICON executable can be found (via icon_filepath_executable fixture)
    - ICON grid test data can be downloaded (via icon_grid_path fixture)

    The actual checks happen in the fixtures - if either fails, this test will fail.
    """
    # Test is performed by fixtures
