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

![Sirocco logo](assets/Sirocco_logo.svg){width="500"}

<br>
<br>

**Sirocco** is a workflow tool written in Python dedicated to climate & weather applications running on HPC systems. At its core is a graph representation of the workflow that borrows concepts from the [AiiDA :lucide-external-link:](https://aiida.net/) workflow library and the [cylc :lucide-external-link:](https://cylc.github.io/) format, expressed by users in a [YAML dialect](configuration/configuration.md):

- **Data nodes** are represented in the graph alongside task nodes, so that users explicitly specify the input and output data of each task. Tasks thus no longer need to make assumptions about upstream and downstream tasks, which makes workflows truly composable.
- The graph results from unrolling specified sets of recurring tasks that have a certain periodicity, called **cycles**, so that describing climate and weather workflows feels natural.

Leveraging the deterministic nature of these workflows, Sirocco also introduces the concept of [**deep propagating task front**](orchestration.md#the-deep-propagating-task-front): a bounded window of task generations submitted ahead of time with scheduler dependencies, and moved forward through the graph as tasks complete. Tasks no longer wait for their parents to finish before entering the queue, and the scheduler gets more information about the upcoming workload to plan resources with.

Sirocco uses a [plugin mechanism](plugins/plugins.md) for defining tasks. Since this project was originally developed in a context where the [**ICON** model :lucide-external-link:](https://www.icon-model.org) plays a crucial role, Sirocco includes a [built-in plugin for ICON](plugins/icon.md) which provides a user-friendly interface, even for complex configurations of the model like coupled runs on hybrid architectures.

Finally, Sirocco workflows can [be orchestrated](orchestration.md) by two different engines. The **standalone engine** lets the workflow run autonomously on the HPC system, relying on the scheduler as the sole running daemon, while the **AiiDA engine** (still under development) runs on a dedicated machine where an AiiDA server is deployed.

<u><b>Where to start</b></u>

- [Configuration](configuration/configuration.md): write your first `sirocco.yaml`
- [Usage :material-arrow-right: Standalone](usage/standalone.md): verify, visualize and run a workflow
- [Glossary](glossary.md): Sirocco terminology at a glance

<u><b>Acknowledgments</b></u>

We would like to acknowledge the [EXCLAIM :lucide-external-link:](https://exclaim.ethz.ch) and [Swiss Twins :lucide-external-link:](https://www.cscs.ch/about/collaborations/swisstwins) projects for the initial funding of Sirocco.
