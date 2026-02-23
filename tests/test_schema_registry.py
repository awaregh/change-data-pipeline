"""Tests for SchemaRegistry and schema evolution."""

import pytest

from cdc.schema_registry import (
    Compatibility,
    SchemaEvolutionError,
    SchemaRegistry,
    SchemaVersion,
)

ORDER_SCHEMA_V1 = {
    "type": "object",
    "properties": {
        "id":     {"type": "integer"},
        "item":   {"type": "string"},
        "amount": {"type": "number"},
    },
    "required": ["id", "item", "amount"],
}

# Compatible extension: adds optional 'currency' field
ORDER_SCHEMA_V2 = {
    "type": "object",
    "properties": {
        "id":       {"type": "integer"},
        "item":     {"type": "string"},
        "amount":   {"type": "number"},
        "currency": {"type": "string"},
    },
    "required": ["id", "item", "amount"],
}

# Breaking: drops 'item' from properties and required
ORDER_SCHEMA_BREAKING = {
    "type": "object",
    "properties": {
        "id":     {"type": "integer"},
        "amount": {"type": "number"},
    },
    "required": ["id", "amount"],
}


class TestSchemaRegistryRegistration:
    def test_first_registration_succeeds(self):
        registry = SchemaRegistry()
        sv = registry.register("orders", ORDER_SCHEMA_V1)
        assert sv.version == 1
        assert sv.subject == "orders"

    def test_second_registration_returns_version_2(self):
        registry = SchemaRegistry()
        registry.register("orders", ORDER_SCHEMA_V1)
        sv2 = registry.register("orders", ORDER_SCHEMA_V2)
        assert sv2.version == 2

    def test_incompatible_schema_raises(self):
        registry = SchemaRegistry()
        registry.register("orders", ORDER_SCHEMA_V1)
        with pytest.raises(SchemaEvolutionError):
            registry.register("orders", ORDER_SCHEMA_BREAKING)

    def test_none_compatibility_allows_breaking_change(self):
        registry = SchemaRegistry(default_compatibility=Compatibility.NONE)
        registry.register("orders", ORDER_SCHEMA_V1)
        sv = registry.register("orders", ORDER_SCHEMA_BREAKING)
        assert sv.version == 2

    def test_set_compatibility_per_subject(self):
        registry = SchemaRegistry(default_compatibility=Compatibility.FULL)
        registry.set_compatibility("orders", Compatibility.NONE)
        registry.register("orders", ORDER_SCHEMA_V1)
        # Should NOT raise because subject is set to NONE
        sv = registry.register("orders", ORDER_SCHEMA_BREAKING)
        assert sv.version == 2


class TestSchemaRegistryLookup:
    def test_get_latest(self):
        registry = SchemaRegistry()
        registry.register("orders", ORDER_SCHEMA_V1)
        registry.register("orders", ORDER_SCHEMA_V2)
        latest = registry.get_latest("orders")
        assert latest.version == 2

    def test_get_latest_none_if_not_registered(self):
        registry = SchemaRegistry()
        assert registry.get_latest("unknown") is None

    def test_get_version_specific(self):
        registry = SchemaRegistry()
        registry.register("orders", ORDER_SCHEMA_V1)
        registry.register("orders", ORDER_SCHEMA_V2)
        sv1 = registry.get_version("orders", 1)
        assert sv1.version == 1
        assert "currency" not in sv1.schema.get("properties", {})

    def test_get_version_out_of_range(self):
        registry = SchemaRegistry()
        registry.register("orders", ORDER_SCHEMA_V1)
        with pytest.raises(KeyError):
            registry.get_version("orders", 99)

    def test_all_versions(self):
        registry = SchemaRegistry()
        registry.register("orders", ORDER_SCHEMA_V1)
        registry.register("orders", ORDER_SCHEMA_V2)
        versions = registry.all_versions("orders")
        assert len(versions) == 2


class TestSchemaCompatibilityModes:
    def test_backward_compatible_add_optional_field(self):
        registry = SchemaRegistry(default_compatibility=Compatibility.BACKWARD)
        registry.register("orders", ORDER_SCHEMA_V1)
        sv = registry.register("orders", ORDER_SCHEMA_V2)
        assert sv.version == 2

    def test_backward_incompatible_add_required_field(self):
        """Adding a required field is backward incompatible."""
        registry = SchemaRegistry(default_compatibility=Compatibility.BACKWARD)
        registry.register("orders", ORDER_SCHEMA_V1)
        new_required_schema = {
            "type": "object",
            "properties": {
                "id":     {"type": "integer"},
                "item":   {"type": "string"},
                "amount": {"type": "number"},
                "region": {"type": "string"},
            },
            "required": ["id", "item", "amount", "region"],  # new required field
        }
        with pytest.raises(SchemaEvolutionError):
            registry.register("orders", new_required_schema)

    def test_forward_compatible_add_optional_field(self):
        registry = SchemaRegistry(default_compatibility=Compatibility.FORWARD)
        registry.register("orders", ORDER_SCHEMA_V1)
        sv = registry.register("orders", ORDER_SCHEMA_V2)
        assert sv.version == 2

    def test_forward_incompatible_remove_required_field(self):
        """Removing a required field is forward incompatible."""
        registry = SchemaRegistry(default_compatibility=Compatibility.FORWARD)
        registry.register("orders", ORDER_SCHEMA_V1)
        with pytest.raises(SchemaEvolutionError):
            registry.register("orders", ORDER_SCHEMA_BREAKING)

    def test_full_compatibility_blocks_breaking_changes(self):
        registry = SchemaRegistry(default_compatibility=Compatibility.FULL)
        registry.register("orders", ORDER_SCHEMA_V1)
        with pytest.raises(SchemaEvolutionError):
            registry.register("orders", ORDER_SCHEMA_BREAKING)


class TestSchemaValidation:
    def test_valid_data_passes(self):
        registry = SchemaRegistry()
        registry.register("orders", ORDER_SCHEMA_V1)
        # Should not raise (the schema only requires id, item, amount)
        registry.validate("orders", {"id": 1, "item": "Widget", "amount": 9.99})

    def test_invalid_data_raises(self):
        registry = SchemaRegistry()
        registry.register("orders", ORDER_SCHEMA_V1)
        import jsonschema
        with pytest.raises(jsonschema.ValidationError):
            registry.validate("orders", {"id": "not-an-int", "item": "Widget", "amount": 9.99})

    def test_validate_specific_version(self):
        registry = SchemaRegistry()
        registry.register("orders", ORDER_SCHEMA_V1)
        registry.register("orders", ORDER_SCHEMA_V2)
        # v1 is valid with id, item, amount
        registry.validate("orders", {"id": 1, "item": "Widget", "amount": 9.99}, version=1)

    def test_validate_no_schema_raises(self):
        registry = SchemaRegistry()
        with pytest.raises(KeyError):
            registry.validate("unknown_topic", {"id": 1})
