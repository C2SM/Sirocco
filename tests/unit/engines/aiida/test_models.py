"""Unit tests for sirocco.engines.aiida.models module."""

import pytest

from sirocco.engines.aiida.models import AiidaIconTaskSpec, AiidaShellTaskSpec


class TestAiidaShellTaskSpecValidation:
    """Test Pydantic validation for AiidaShellTaskSpec."""

    def test_code_pk_none_raises_error(self):
        """Test that code_pk validator raises ValueError when None."""

        print("\n=== Test AiidaShellTaskSpec code_pk validation ===")
        print("Input code_pk: None")
        print("Expected: ValueError with 'code_pk cannot be None'")

        # Test the validator directly
        with pytest.raises(ValueError, match="code_pk cannot be None"):
            AiidaShellTaskSpec.validate_code_pk_not_none(None)


class TestAiidaIconTaskSpecValidation:
    """Test Pydantic validation for AiidaIconTaskSpec."""

    def test_code_pk_none_raises_error(self):
        """Test that PK validator raises ValueError when None."""

        print("\n=== Test AiidaIconTaskSpec PK validation ===")
        print("Input PK: None")
        print("Expected: ValueError with 'PKs cannot be None'")

        # Test the validator directly
        with pytest.raises(ValueError, match="PKs cannot be None"):
            AiidaIconTaskSpec.validate_pk_not_none(None)

    def test_model_namelist_pks_with_none_raises_error(self):
        """Test that model_namelist_pks validator raises ValueError for None values."""

        print("\n=== Test AiidaIconTaskSpec model_namelist_pks validation ===")
        print("Input model_namelist_pks: {'atm': None}")
        print("Expected: ValueError with 'Model namelist PK for atm cannot be None'")

        # Test the validator directly with a dict containing None
        with pytest.raises(ValueError, match="Model namelist PK for atm cannot be None"):
            AiidaIconTaskSpec.validate_model_pks_not_none({"atm": None})
