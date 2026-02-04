"""Unit tests for sirocco.engines.aiida.types module."""

import pytest


class TestAiidaShellTaskSpecValidation:
    """Test Pydantic validation for AiidaShellTaskSpec."""

    def test_code_pk_none_raises_error(self):
        """Test that code_pk validator raises ValueError when None (lines 205-206)."""
        from sirocco.engines.aiida.types import AiidaShellTaskSpec

        # Test the validator directly
        with pytest.raises(ValueError, match="code_pk cannot be None"):
            AiidaShellTaskSpec.validate_code_pk_not_none(None)


class TestAiidaIconTaskSpecValidation:
    """Test Pydantic validation for AiidaIconTaskSpec."""

    def test_code_pk_none_raises_error(self):
        """Test that PK validator raises ValueError when None (lines 231-232)."""
        from sirocco.engines.aiida.types import AiidaIconTaskSpec

        # Test the validator directly
        with pytest.raises(ValueError, match="PKs cannot be None"):
            AiidaIconTaskSpec.validate_pk_not_none(None)

    def test_model_namelist_pks_with_none_raises_error(self):
        """Test that model_namelist_pks validator raises ValueError for None values (lines 240-241)."""
        from sirocco.engines.aiida.types import AiidaIconTaskSpec

        # Test the validator directly with a dict containing None
        with pytest.raises(ValueError, match="Model namelist PK for atm cannot be None"):
            AiidaIconTaskSpec.validate_model_pks_not_none({"atm": None})
