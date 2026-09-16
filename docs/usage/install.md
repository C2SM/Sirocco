---
title: install
icon: material/download
---



# `pip` installation

Sirocco is a pure python package simply installed through (`uv`) `pip`
```shell
❯ [uv] pip install sirocco
```

# Running requirements

Sirocco can be ran through two [orchestration engines](orchestration), namely standalone and AiiDA.

## Standalone engine

To run Sirocco you will need an HPC system using the [SLURM :lucide-external-link:](https://slurm.schedmd.com/) scheduler, for now the only supported one.

## AiiDA engine


!!! warning

    The AiiDA engine is still in development

To run Sirocco you will need a dedicated server where AiiDA is set up and constantly up and running to manage workflows. It can also be ran from personal computers for testing purposes. Workflows would then be suspended while the AiiDA daemon is not running.
