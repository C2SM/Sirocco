"""Unit tests for visualization graph generation."""

from sirocco.vizgraph import VizGraph


def test_vizgraph(config_paths):
    """Test generating visualization graph from config file."""
    VizGraph.from_config_file(config_paths["yml"], template_context=config_paths["variables"]).draw(
        file_path=config_paths["svg"]
    )
