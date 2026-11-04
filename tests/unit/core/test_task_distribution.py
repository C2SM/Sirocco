"""Unit tests for the distribution of ICON ranks over nodes and NUMA nodes."""

from sirocco.core.tasks.icon_task.task_distribution import (
    RankInfo,
    distribute_procs_load_balanced,
)


def test_load_balanced_numa_nodes_within_sockets():
    """A second rank range starting mid-node must wrap around the sockets."""
    rank_bounds = [(0, 4), (5, 12)]
    ranks_info = {
        rank: RankInfo(
            node_id=-1, numa_node=-1, pe_type="compute", target="cpu", model="m"
        )
        for rank in range(13)
    }
    distribute_procs_load_balanced(ranks_info, rank_bounds, nodes=(0,), n_sockets=4)
    assert {info["numa_node"] for info in ranks_info.values()} == {0, 1, 2, 3}
