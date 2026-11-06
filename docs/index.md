<style>
  /* Justify all paragraphs on this page */
  .md-content {
    text-align: justify;
  }
  /* Completely hides the header block containing the headline */
  .md-content article h1:first-of-type,
  /* Targets and hides the first H1 header only on section index landing pages */
.md-content h1:first-of-type {
    display: none !important;
}
</style>

![Image title](assets/Sirocco_logo.svg){width="500"}

<br>
<br>

**Sirocco** is a workflow tool dedicated to climate & weather applications running on HPC systems written in Python. At its core is a graph representation of the workflow that borrows concepts from the [AiiDA :lucide-external-link:](https://aiida.net/) workflow library and the [cylc :lucide-external-link:](https://cylc.github.io/) format, expressed by users in a [YAML dialect](configuration/configuration):

- Equally to task nodes, **data nodes** are represented in the graph so that users explicitly specify tasks input and output data. The requirement for making assumptions on upstream and downstream tasks thus disappears, leading to truly composable workflows.
- The graph results from unrolling specified sets of recurring tasks that have a certain periodicity, called **cycles**, so that describing climate and weather workflows feels natural.

Leveraging the deterministic nature of these workflows, Sirocco also introduces the concept of **propagating deep task front**. In short, it can be qualified as a set of dynamic [topological generations :lucide-external-link:](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.dag.topological_generations.html) propagating through the graph. It enables the ahead-of-time submission of tasks to the HPC system using the dependency handling feature of its scheduler. The later hence receives a maximized amount of information so that computational resources can be used optimally.

Sirocco uses a [plugin mechanism](plugins) for defining tasks. Since this project is originally developed in a context where the [**ICON** model :lucide-external-link:](icon-model.org) plays a crucial role, Sirocco includes a [built-in plugin for ICON](plugins/icon) which provides a user-friendly interface, even for complex configurations of the model like coupled runs on hybrid architectures.

Finally, Sirocco workflows can [be orchestrated](orchestration) by two different engines. The **standalone engine** lets the workflow run autonomously on the HPC system, relying on the scheduler as the sole running daemon while the **AiiDA engine** (still under development) runs on a dedicated machine where an AiiDA server is deployed.

<u><b>Aknowledgments</b></u>

We would like to aknowledge the [EXCLAIM :lucide-external-link:](https://exclaim.ethz.ch) and [Swiss Twins :lucide-external-link:](https://www.cscs.ch/about/collaborations/swisstwins) projects for the initial funding of Sirocco.
