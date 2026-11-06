# Orchestration :lucide-wand-sparkles:

Sirocco workflows can be orchestrated by two engines. A **standalone** one running autonomously on the HPC cluster and an **[AiiDA :lucide-external-link:](https://aiida.net/)** based one, still under development, which runs on a dedicated server.

## The deep propagating submitted task front

Both orchestrators implement the concept of deep propagating task front. Taking advantage of the deterministic nature of climate and weather workflows, it enables ahead of time task submission. Using rather grandiose language, one could qualify this front as a **deep**, **propagating** set of **dynamical topological generations**. To further explain what this concept is, we will consider the [directed acyclic graph (DAG) :lucide-external-link:](https://en.wikipedia.org/wiki/Directed_acyclic_graph) constituted by task nodes only, removing data nodes from the [full workflow graph](configuration/cycles/#cycles). 

- [**topological generations** :lucide-external-link:](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.dag.topological_generations.html) partition a DAG into sequential levels (the "generations") based on their dependency depth. In this structure, nodes in generation 0 have no incoming dependencies. Nodes in generation 1 depend only on nodes from generation 0, and more generally, any node in generation n only has incoming edges from nodes in generations strictly less than n (0 to n-1).
- **deep front**. Topological generations partition the whole graph into generations. Our front has a finite depth corresponding to the number of generations of submitted tasks taken into account. A depth of 1 would correspond to solely currently running tasks without ahead of time submission. A depth of 2 implies one generation submitted ahead of time, etc ...
- **propagating dynamical generations**. A the beginning of the workflow execution, the front will coincide with the first global topological generations. As the workflow progresses, tasks are terminating and new ones are getting submitted. Generations are updated dynamically, so that they do not necessarily coincide anymore with global topological generations. Think about a DAG where a branch advances faster than others. The front will be distorted and dynamical generations will contain tasks that with different global ranks: higher ranks for tasks of the branch, lower for others.


## Standalone engine
