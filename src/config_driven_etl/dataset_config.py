"""Dataset configuration models and YAML loader."""

from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    StringConstraints,
    field_validator,
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
