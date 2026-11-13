---
title: AiiDA
icon: custom_icons/aiida
---

!!! warning

    The AiiDA engine is still under development

The AiiDA engine translates the Sirocco workflow graph into an [AiiDA WorkGraph :lucide-external-link:](https://aiida-workgraph.readthedocs.io), run by an [AiiDA :lucide-external-link:](https://www.aiida.net/) server deployed on a dedicated machine rather than on the HPC system itself. On top of orchestrating the workflow with the same [deep propagating task front](../orchestration.md#the-deep-propagating-task-front) as the standalone engine, it records the full provenance of every task and piece of data in the AiiDA database.

It requires installing Sirocco with the `aiida` extra and a configured AiiDA profile:
```shell
uv pip install "sirocco[aiida]"
```
The workflow can then be either run in a blocking fashion with `sirocco run` or submitted to the AiiDA daemon with `sirocco submit`. Both take the [configuration directory](../glossary.md#configuration-directory) as positional argument, like the [standalone commands](standalone.md#cli).

Its interface and supported features may still change in upcoming versions.
