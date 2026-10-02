"""Unit tests for visualization graph generation."""

from sirocco.vizgraph import VizGraph


def test_vizgraph(config_paths):
    """Test generating visualization graph from config file."""
    VizGraph.from_config_path(config_paths["dir"]).draw(file_path=config_paths["svg"])
