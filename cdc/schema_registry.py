"""Schema registry with forward/backward compatibility enforcement.

The ``SchemaRegistry`` stores versioned JSON-Schema definitions for each
topic/table.  When a new schema version is registered the registry checks
that the evolution is *backward compatible* (new readers can read old data)
and *forward compatible* (old readers can read new data), unless the caller
explicitly opts into a breaking change.

Compatibility rules (aligned with Confluent Schema Registry conventions):

BACKWARD  – new schema can read data written with the previous schema.
            Allowed: add optional fields with defaults, remove fields.
FORWARD   – old schema can read data written with the new schema.
            Allowed: add fields (old readers will ignore them), remove
            optional fields that had defaults.
FULL      – both BACKWARD and FORWARD (default).
NONE      – no compatibility check.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

try:
    import jsonschema
    _JSONSCHEMA_AVAILABLE = True
except ImportError:  # pragma: no cover
    _JSONSCHEMA_AVAILABLE = False


class Compatibility(str, Enum):
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    NONE = "NONE"


@dataclass
class SchemaVersion:
    """A versioned schema entry in the registry."""

    subject: str
    version: int
    schema: dict[str, Any]
    compatibility: Compatibility = Compatibility.FULL


class SchemaEvolutionError(Exception):
    """Raised when a schema change violates the compatibility policy."""


class SchemaRegistry:
    """In-memory schema registry with compatibility enforcement.

    Parameters
    ----------
    default_compatibility:
        Compatibility mode applied to new subjects (can be overridden
        per-subject via ``set_compatibility``).
    """

    def __init__(self, default_compatibility: Compatibility = Compatibility.FULL) -> None:
        self._default_compat = default_compatibility
        # subject -> list of SchemaVersion (index 0 = version 1, etc.)
        self._schemas: dict[str, list[SchemaVersion]] = {}
        self._compat: dict[str, Compatibility] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, subject: str, schema: dict[str, Any]) -> SchemaVersion:
        """Register *schema* for *subject*, returning the new ``SchemaVersion``.

        If this is the first schema for the subject it is accepted without
        compatibility checks.  Otherwise the registry validates that the
        new schema is compatible with the latest registered schema.

        Raises
        ------
        SchemaEvolutionError
            If the new schema violates the compatibility policy.
        """
        compat = self._compat.get(subject, self._default_compat)

        if subject not in self._schemas:
            self._schemas[subject] = []

        versions = self._schemas[subject]
        new_version = len(versions) + 1

        if versions:
            latest = versions[-1]
            if compat != Compatibility.NONE:
                self._check_compatibility(compat, latest.schema, schema, subject)

        sv = SchemaVersion(subject=subject, version=new_version, schema=copy.deepcopy(schema), compatibility=compat)
        self._schemas[subject].append(sv)
        return sv

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get_latest(self, subject: str) -> Optional[SchemaVersion]:
        """Return the most recently registered schema for *subject*."""
        versions = self._schemas.get(subject)
        return versions[-1] if versions else None

    def get_version(self, subject: str, version: int) -> SchemaVersion:
        """Return the schema at *version* (1-based) for *subject*."""
        versions = self._schemas.get(subject, [])
        if version < 1 or version > len(versions):
            raise KeyError(f"No schema version {version} for subject '{subject}'")
        return versions[version - 1]

    def all_versions(self, subject: str) -> list[SchemaVersion]:
        """Return all schema versions for *subject* in registration order."""
        return list(self._schemas.get(subject, []))

    # ------------------------------------------------------------------
    # Compatibility configuration
    # ------------------------------------------------------------------

    def set_compatibility(self, subject: str, compatibility: Compatibility) -> None:
        """Override the compatibility mode for a specific subject."""
        self._compat[subject] = compatibility

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self, subject: str, data: dict[str, Any], version: Optional[int] = None) -> None:
        """Validate *data* against the schema for *subject*.

        Parameters
        ----------
        subject:
            The schema subject (e.g. table name).
        data:
            The data dict to validate.
        version:
            Schema version to validate against.  Defaults to the latest.

        Raises
        ------
        jsonschema.ValidationError
            If validation fails (only when ``jsonschema`` is installed).
        RuntimeError
            If ``jsonschema`` is not available.
        """
        if not _JSONSCHEMA_AVAILABLE:
            raise RuntimeError("jsonschema is required for validation: pip install jsonschema")

        sv = self.get_version(subject, version) if version else self.get_latest(subject)
        if sv is None:
            raise KeyError(f"No schema registered for subject '{subject}'")
        jsonschema.validate(instance=data, schema=sv.schema)

    # ------------------------------------------------------------------
    # Compatibility checks (structural / JSON-Schema based)
    # ------------------------------------------------------------------

    def _check_compatibility(
        self,
        compat: Compatibility,
        old_schema: dict[str, Any],
        new_schema: dict[str, Any],
        subject: str,
    ) -> None:
        """Check *new_schema* against *old_schema* for the given compat mode."""
        if compat in (Compatibility.BACKWARD, Compatibility.FULL):
            self._check_backward(old_schema, new_schema, subject)
        if compat in (Compatibility.FORWARD, Compatibility.FULL):
            self._check_forward(old_schema, new_schema, subject)

    def _check_backward(
        self, old: dict[str, Any], new: dict[str, Any], subject: str
    ) -> None:
        """New schema must be able to read data written with old schema.

        Rule: every *required* field in the new schema must have existed in
        the old schema (so old data without that field would be invalid).
        """
        old_props = set(old.get("properties", {}).keys())
        new_required = set(new.get("required", []))

        added_required = new_required - old_props
        if added_required:
            raise SchemaEvolutionError(
                f"[{subject}] BACKWARD incompatible: new required fields added "
                f"that don't exist in old schema: {sorted(added_required)}"
            )

    def _check_forward(
        self, old: dict[str, Any], new: dict[str, Any], subject: str
    ) -> None:
        """Old schema must be able to read data written with new schema.

        Rule: every *required* field in the old schema must still be present
        in the new schema (old readers expect those fields to exist).
        """
        old_required = set(old.get("required", []))
        new_props = set(new.get("properties", {}).keys())

        removed_required = old_required - new_props
        if removed_required:
            raise SchemaEvolutionError(
                f"[{subject}] FORWARD incompatible: required fields removed "
                f"that old readers depend on: {sorted(removed_required)}"
            )
