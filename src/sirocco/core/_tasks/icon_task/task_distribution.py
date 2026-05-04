from collections.abc import Iterable, Iterator
from itertools import chain


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


def iter_tasks_block_size(n_tasks: int, n_blocks: int) -> Iterator[tuple[int, int]]:
    base, mod = divmod(n_tasks, n_blocks)
    for k in range(n_blocks):
        if k < mod:
            yield k, base + 1
        else:
            yield k, base


def distribute_tasks_by_blocks(rank_bounds: Iterable[tuple[int, int]], nodes: tuple[int, ...]) -> dict[int, int]:
    """distribute tasks by blocks with as even block sizes as possible

    inputs:
    - compute_ranks: Iterable[tuple[int, int]]
      Iterable of min and max ranks
    - nodes: tuple[int, ...]
      nodes over which ranks are distributed"""

    distributed_ranks: dict[int, int] = {}
    n_nodes = len(nodes)
    first_index = 0
    for min_rank, max_rank in rank_bounds:
        n_tasks = max_rank - min_rank + 1
        current_rank = min_rank
        for node_inc, tasks_block_size in iter_tasks_block_size(n_tasks=n_tasks, n_blocks=n_nodes):
            node = nodes[(first_index + node_inc) % n_nodes]
            distributed_ranks.update(dict.fromkeys(range(current_rank, current_rank + tasks_block_size), node))
            current_rank += tasks_block_size
        first_index = (first_index + n_tasks % n_nodes) % n_nodes

    return distributed_ranks


def distribute_tasks_round_robin(rank_bounds: Iterable[tuple[int, int]], nodes: tuple[int, ...]) -> dict[int, int]:
    """distribute tasks in a round-robin way

    inputs:
    - compute_ranks: Iterable[tuple[int, int]]
      Iterable of min and max ranks
    - nodes: tuple[int, ...]
      nodes over which ranks are distributed"""

    distributed_ranks: dict[int, int] = {}
    n_nodes = len(nodes)
    current_index = 0
    for rank in chain(*(range(min_rank, max_rank + 1) for min_rank, max_rank in rank_bounds)):
        distributed_ranks[rank] = nodes[current_index]
        current_index = (current_index + 1) % n_nodes
    return distributed_ranks
