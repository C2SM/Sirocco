from collections.abc import Iterable, Iterator
from itertools import chain
from typing import Literal, TypedDict


class RankInfo(TypedDict):
    node_id: int
    numa_node: int
    pe_type: Literal["compute", "io", "hiopy"]
    target: Literal["cpu", "gpu", "hiopy"]
    model: str


def allocate_tasks_from_weights(n_tot_tasks: int, weights: dict[str, int]) -> dict[str, int]:
    """Allocate a total number of tasks to components according to weights"""

    # Compute ideal target float tasks nuber and keep only integer part
    tot_weight = sum(weights.values())
    float_tasks = {comp: n_tot_tasks * w / tot_weight for comp, w in weights.items()}
    int_tasks = {comp: int(f) for comp, f in float_tasks.items()}

    # redistribute missing tasks
    missing_tasks = n_tot_tasks - sum(int_tasks.values())
    sorted_comp_iterator = iter_sorted_comp_by_delta(float_tasks, int_tasks)
    while missing_tasks > 0:
        int_tasks[next(sorted_comp_iterator)] += 1
        missing_tasks -= 1

    # Check
    for comp, n_tasks in int_tasks.items():
        if n_tasks == 0:
            msg = f"Component {comp}: weight {weights[comp]}/{tot_weight} too low resulting in 0/{n_tot_tasks} allocated task"
            raise ValueError(msg)
    if s := sum(int_tasks.values()) != n_tot_tasks:
        msg = f"sum of allocated tasks {s} does not match total taks {n_tot_tasks}"
        raise ValueError(msg)

    return int_tasks


def iter_sorted_comp_by_delta(float_tasks: dict[str, float], int_tasks: dict[str, int]) -> Iterator[str]:
    """Yield component names ordered by the largest discrepancy between ideal float task number and integer task number"""

    delta = [(comp, f - i) for (comp, f), (_, i) in zip(float_tasks.items(), int_tasks.items(), strict=False)]
    delta.sort(key=lambda t: -t[1])
    yield from (t[0] for t in delta)


def iter_procs_by_blocks(n_procs: int, n_blocks: int) -> Iterator[tuple[int, int]]:
    base, mod = divmod(n_procs, n_blocks)
    for k in range(n_blocks):
        if k < mod:
            yield k, base + 1
        else:
            yield k, base


def distribute_procs_load_balanced(
    ranks_info: dict[int, RankInfo], rank_bounds: Iterable[tuple[int, int]], nodes: tuple[int, ...], n_sockets: int = 1
) -> None:
    """distribute procs by blocks both at the node and numa node level with as even block sizes as possible.

    This is close to srun --distribution:plane=n:plane=m with n / m adjusted on each node / numa node to generate
    an as even distribution as possible.

    inputs:
    - ranks_info: dict[int, RankInfo]
      dictionnary of RankInfo to be updated with node_id and numa_node
    - rank_bounds: Iterable[tuple[int, int]]
      Iterable of min and max ranks (e.g. model ranks)
    - nodes: tuple[int, ...]
      nodes over which ranks are distributed
    - n_sockets: int = 1
      number of sockets per node"""

    n_nodes: int = len(nodes)
    start_node_idx: int = 0
    start_numa: dict[int, int] = dict.fromkeys(nodes, 0)
    # Loop over rank ranges
    for min_rank, max_rank in rank_bounds:
        n_procs = max_rank - min_rank + 1
        min_block_rank = min_rank
        # Loop over rank blocks to be distributed on each node
        for node_inc, node_procs_block_size in iter_procs_by_blocks(n_procs=n_procs, n_blocks=n_nodes):
            node = nodes[(start_node_idx + node_inc) % n_nodes]
            # Loop over blocks to be distributed on each socket
            for numa_inc, numa_procs_block_size in iter_procs_by_blocks(
                n_procs=node_procs_block_size, n_blocks=n_sockets
            ):
                numa = start_numa[node] + numa_inc % n_sockets
                for rank in range(min_block_rank, min_block_rank + numa_procs_block_size):
                    ranks_info[rank]["node_id"] = node
                    ranks_info[rank]["numa_node"] = numa
                min_block_rank += numa_procs_block_size
            start_numa[node] = (start_numa[node] + node_procs_block_size % n_sockets) % n_sockets
        # Check that all ranks have been distributed
        if min_block_rank != max_rank + 1:
            msg = f"not all ranks were distributed: min_node_rank={min_block_rank} != max_rank+1={max_rank + 1}"
            raise RuntimeError(msg)
        for rank in range(min_rank, max_rank + 1):
            if ranks_info[rank]["node_id"] == -1 or ranks_info[rank]["numa_node"] == -1:
                msg = f"node_id or numa_node not set for rank {rank}"
                raise RuntimeError(msg)
        start_node_idx = (start_node_idx + n_procs % n_nodes) % n_nodes


def distribute_procs_cyclic(
    ranks_info: dict[int, RankInfo], rank_bounds: Iterable[tuple[int, int]], nodes: tuple[int, ...], n_sockets: int = 1
) -> None:
    """distribute procs in a round-robin way both at the node and numa node level.

    This is equivalent to srun --distribution=cyclic:cyclic.

    inputs:
    - ranks_info: dict[int, RankInfo]
      dictionnary of RankInfo to be updated with node_id and numa_node
    - rank_bounds: Iterable[tuple[int, int]]
      Iterable of min and max ranks (e.g. model ranks)
    - nodes: tuple[int, ...]
      nodes over which ranks are distributed
    - n_sockets: int = 1
      number of sockets per node"""

    n_nodes: int = len(nodes)
    node_idx: int = 0
    numa: dict[int, int] = dict.fromkeys(nodes, 0)
    for rank in chain(*(range(min_rank, max_rank + 1) for min_rank, max_rank in rank_bounds)):
        # TODO: distribute over n_sockets
        node = nodes[node_idx]
        ranks_info[rank]["node_id"] = node
        ranks_info[rank]["numa_node"] = numa[node]
        node_idx = (node_idx + 1) % n_nodes
        numa[node] = (numa[node] + 1) % n_sockets
