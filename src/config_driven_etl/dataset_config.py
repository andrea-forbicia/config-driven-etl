"""Dataset configuration models and YAML loader."""

from collections import Counter
from collections.abc import Iterable
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    StringConstraints,
    field_validator,
    model_validator,
)

# Names end up as BigQuery identifiers and directory names; lowercase
# snake_case avoids case-insensitive clashes in both.
Identifier = Annotated[
    str, StringConstraints(pattern=r"^[a-z_][a-z0-9_]*$", max_length=128)
]


class ColumnType(StrEnum):
    """Closed set of column types supported by the pipeline."""

    STRING = "string"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    DATE = "date"
    DATETIME = "datetime"


NUMERIC_TYPES = frozenset({ColumnType.INT, ColumnType.FLOAT})
TEMPORAL_TYPES = frozenset({ColumnType.DATE, ColumnType.DATETIME})


class _StrictModel(BaseModel):
    # Unknown keys are almost always typos in the YAML file.
    model_config = ConfigDict(extra="forbid", frozen=True)


class Source(_StrictModel):
    """Where the raw CSV comes from and how to read it."""

    url: HttpUrl | None = None
    path: Path | None = None
    # The Polars CSV reader needs a single-byte separator.
    delimiter: str = Field(default=",", pattern=r"^[\x01-\x7f]$")
    # Limited to what the Polars CSV reader decodes natively.
    encoding: Literal["utf-8", "utf-8-lossy"] = "utf-8"

    @field_validator("path", mode="before")
    @classmethod
    def _reject_empty_path(cls, value: object) -> object:
        # Path("") silently becomes Path("."), the current directory.
        if value == "":
            raise ValueError("path must not be empty")
        return value

    @model_validator(mode="after")
    def _check_location(self) -> Self:
        if (self.url is None) == (self.path is None):
            raise ValueError("set exactly one of 'url' or 'path'")
        return self


class Column(_StrictModel):
    """Mapping of one source column to a typed target column."""

    source: str = Field(min_length=1)
    name: Identifier
    type: ColumnType
    nullable: bool = True
    format: str | None = None
    # A NaN bound would make every range comparison false.
    min: float | None = Field(default=None, allow_inf_nan=False)
    max: float | None = Field(default=None, allow_inf_nan=False)

    @model_validator(mode="after")
    def _check_type_options(self) -> Self:
        problems = []
        has_bounds = self.min is not None or self.max is not None
        if has_bounds and self.type not in NUMERIC_TYPES:
            problems.append(
                f"'min' and 'max' are only allowed on int and float "
                f"columns, not {self.type}"
            )
        if (
            self.min is not None
            and self.max is not None
            and self.min > self.max
        ):
            problems.append(
                f"'min' ({self.min}) must not exceed 'max' ({self.max})"
            )
        # Inferring the format could silently misread ambiguous dates.
        if self.type in TEMPORAL_TYPES and self.format is None:
            problems.append(f"'format' is required on {self.type} columns")
        if self.type not in TEMPORAL_TYPES and self.format is not None:
            problems.append(
                f"'format' is only allowed on date and datetime columns, "
                f"not {self.type}"
            )
        _raise_if_any(problems)
        return self


class Rules(_StrictModel):
    """Dataset-wide cleaning rules."""

    deduplicate_on: tuple[Identifier, ...] = ()
    drop_unmapped_columns: bool = True


class Target(_StrictModel):
    """BigQuery destination table."""

    table: Identifier
    partition_by: Identifier | None = None
    # BigQuery accepts at most four clustering columns.
    cluster_by: tuple[Identifier, ...] = Field(default=(), max_length=4)
    # Only truncate keeps re-runs idempotent.
    write_mode: Literal["truncate"] = "truncate"


class DatasetConfig(_StrictModel):
    """Full description of one dataset, as read from its YAML file."""

    name: Identifier
    source: Source
    columns: tuple[Column, ...] = Field(min_length=1)
    rules: Rules = Field(default_factory=Rules)
    target: Target

    @model_validator(mode="after")
    def _check_references(self) -> Self:
        problems = [
            f"columns: name '{name}' is used more than once"
            for name in _duplicates(c.name for c in self.columns)
        ]
        problems += [
            f"columns: source '{source}' is mapped more than once"
            for source in _duplicates(c.source for c in self.columns)
        ]

        types = {column.name: column.type for column in self.columns}
        partition_by = self.target.partition_by
        if partition_by is not None:
            if partition_by not in types:
                problems.append(
                    f"target.partition_by: '{partition_by}' is not a column"
                )
            elif types[partition_by] not in TEMPORAL_TYPES:
                problems.append(
                    f"target.partition_by: '{partition_by}' must be a date "
                    f"or datetime column, not {types[partition_by]}"
                )

        for path, names in [
            ("target.cluster_by", self.target.cluster_by),
            ("rules.deduplicate_on", self.rules.deduplicate_on),
        ]:
            problems += [
                f"{path}: '{name}' is not a column"
                for name in names
                if name not in types
            ]
            problems += [
                f"{path}: '{name}' is listed more than once"
                for name in _duplicates(names)
            ]

        # BigQuery rejects FLOAT64 clustering columns at table creation.
        problems += [
            f"target.cluster_by: '{name}' must not be a float column"
            for name in dict.fromkeys(self.target.cluster_by)
            if types.get(name) is ColumnType.FLOAT
        ]

        _raise_if_any(problems)
        return self


def _duplicates(values: Iterable[str]) -> list[str]:
    """Return values seen more than once, each once, in first-seen order."""
    counts = Counter(values)
    return [value for value, count in counts.items() if count > 1]


def _raise_if_any(problems: list[str]) -> None:
    # One error listing every problem lets the author fix the file in a
    # single pass instead of one re-run per mistake.
    if problems:
        raise ValueError("\n".join(problems))
