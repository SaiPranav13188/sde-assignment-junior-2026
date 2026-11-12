"""Workflow lead schemas and the disposition catalogue."""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path

from .normalize import normalize_header


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    required: bool = False
    pii: bool = False
    reference: bool = False
    values: tuple[str, ...] = ()
    default: str | None = None
    aliases: tuple[str, ...] = ()


@dataclass
class WorkflowSchema:
    workflow_id: str
    fields: list[Field]
    _header_index: dict[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        for f in self.fields:
            for label in (f.name, *f.aliases):
                key = normalize_header(label)
                existing = self._header_index.get(key)
                if existing is not None and existing != f.name:
                    raise ValueError(
                        f"{self.workflow_id}: header {label!r} is ambiguous "
                        f"({existing} vs {f.name})")
                self._header_index[key] = f.name

    def field_for_header(self, header: str) -> str | None:
        return self._header_index.get(normalize_header(header))

    @property
    def reference_field(self) -> str | None:
        return next((f.name for f in self.fields if f.reference), None)

    @property
    def phone_field(self) -> str:
        phones = [f.name for f in self.fields if f.type == "phone"]
        if len(phones) != 1:
            raise ValueError(f"{self.workflow_id}: expected exactly one phone field")
        return phones[0]


@dataclass(frozen=True)
class Disposition:
    code: str
    is_terminal: bool
    is_success: bool


def load_schemas(schema_dir: Path) -> dict[str, WorkflowSchema]:
    schemas: dict[str, WorkflowSchema] = {}
    for path in sorted(schema_dir.glob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        fields = [
            Field(
                name=f["name"], type=f["type"], required=bool(f.get("required", False)),
                pii=bool(f.get("pii", False)), reference=bool(f.get("reference", False)),
                values=tuple(f.get("values", ())), default=f.get("default"),
                aliases=tuple(f.get("aliases", ())),
            )
            for f in doc["fields"]
        ]
        schemas[doc["workflow_id"]] = WorkflowSchema(doc["workflow_id"], fields)
    if not schemas:
        raise FileNotFoundError(f"no workflow schemas found in {schema_dir}")
    return schemas


def _bool(s: str) -> bool:
    v = s.strip().lower()
    if v not in ("true", "false"):
        raise ValueError(f"bad boolean {s!r} in dispositions.csv")
    return v == "true"


def load_dispositions(schema_dir: Path) -> dict[str, dict[str, Disposition]]:
    """{workflow_id: {code: Disposition}}"""
    catalogue: dict[str, dict[str, Disposition]] = {}
    with open(schema_dir / "dispositions.csv", newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            wf, code = row["workflow_id"].strip(), row["code"].strip()
            catalogue.setdefault(wf, {})[code] = Disposition(
                code, _bool(row["is_terminal"]), _bool(row["is_success"]))
    return catalogue
