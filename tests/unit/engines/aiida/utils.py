"""AiiDA-specific test utilities.

Helper functions for creating AiiDA-related test objects.
"""

from unittest.mock import MagicMock


def create_mock_transport(path_exists=True, isfile=True):  # noqa FBT002: Boolean default positional argument in function definition
    """Create a mock transport configured as a context manager.

    Note: We still mock transports for tests that need to simulate
    remote file checks without actual remote connections.

    Args:
        path_exists: Whether path_exists() should return True
        isfile: Whether isfile() should return True

    Returns:
        MagicMock configured as transport context manager
    """
    mock_transport = MagicMock()
    mock_transport.path_exists.return_value = path_exists
    mock_transport.isfile.return_value = isfile
    mock_transport.__enter__.return_value = mock_transport
    mock_transport.__exit__.return_value = None
    return mock_transport


def create_authinfo(transport=None):
    """Create or mock authinfo with transport.

    For tests simulating remote scenarios, we mock the authinfo.
    For local scenarios, we could use real authinfo.

    Args:
        transport: Optional mock transport (creates mock if None)

    Returns:
        MagicMock configured as authinfo with transport
    """
    if transport is None:
        transport = create_mock_transport()

    mock_authinfo = MagicMock()
    mock_authinfo.get_transport.return_value = transport
    return mock_authinfo
