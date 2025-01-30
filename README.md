# Sirocco

Sirocco is a library for creating weather and climate workflows from a yaml
format inspired by [cylc](https://cylc.github.io/) using
[AiiDA](https://www.aiida.net/) as workflow engine.

## Install

To install it please use

``` bash
pip install -e .
```

## Developer tools

To manage the repo we use [hatch](https://hatch.pypa.io) please install it

``` bash
pip install hatch
hatch test # run tests
hatch fmt # run formatting
hatch run docs:build # build docs
hatch run docs:serve # live preview of doc for development
```

## Resources

-   <https://aiida-workgraph.readthedocs.io/en/latest/>
-   <https://github.com/aiidateam/aiida-workgraph>

## Acknowledgements
Sirocco acknowledges the support by [SwissTwins](https://www.cscs.ch/about/collaborations/swisstwins) and [ExClaim](https://exclaim.ethz.ch/)
