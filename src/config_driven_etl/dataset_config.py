"""Dataset configuration models and YAML loader."""

import warnings
from collections import Counter
from collections.abc import Hashable, Iterable, Sequence
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal, Self

import polars as pl
import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    StringConstraints,
    ValidationError,
    ValidationInfo,
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

# Validation context key: directory that relative source paths start from.
_BASE_DIR = "base_dir"


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

    @field_validator("path")
    @classmethod
    def _resolve_against_config_dir(
        cls, value: Path | None, info: ValidationInfo
    ) -> Path | None:
        # The loader passes the YAML file's directory, so a relative path
        # means the same file wherever the command is run from.
        base_dir = (info.context or {}).get(_BASE_DIR)
        if value is None or base_dir is None or value.is_absolute():
            return value
        return base_dir / value

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
        if self.type in TEMPORAL_TYPES and self.format is not None:
            reason = _format_problem(self.format, self.type)
            if reason is not None:
                problems.append(f"'format' {self.format!r} {reason}")
        _raise_if_any(problems)
        return self


# Day, month, hour, minute, second and microsecond all differ, so a
# format that drops or confuses any date part fails the round trip.
_SAMPLE = datetime(2026, 1, 2, 3, 4, 5, 6)


def _format_problem(format_: str, column_type: ColumnType) -> str | None:
    """Why format cannot parse a full date, or None if it can.

    The sample is written with the format and read back the way the
    transform step reads values, so only formats Polars accepts pass.
    """
    # The parser reads a %Z name but ignores it, so times would be
    # silently taken as UTC.
    if "%Z" in format_.replace("%%", ""):
        return "uses %Z, whose time zone name is ignored; use %z"
    # In UTC, so offset directives such as %z can be written.
    sample = pl.Series([_SAMPLE]).dt.replace_time_zone("UTC")
    try:
        # %#z can only be parsed; %z output is valid input for it.
        text = sample.dt.strftime(format_.replace("%#z", "%z"))
    except pl.exceptions.PolarsError:
        return "has an unknown or incomplete directive"
    try:
        with warnings.catch_warnings():
            # Polars warns about %f, which misreads "05.123456" as
            # 123 microseconds; the round trip alone cannot see it.
            warnings.simplefilter("error", pl.exceptions.ChronoFormatWarning)
            parsed = _read_back(text, format_, column_type)
    except pl.exceptions.ChronoFormatWarning:
        return "uses %f, which misreads fractions; use %.f or %6f"
    except pl.exceptions.ComputeError as error:
        # These carry a useful reason, such as an hour without minutes.
        reason = str(error).splitlines()[0]
        return f"cannot be used to read a {column_type}: {reason}"
    except pl.exceptions.PolarsError:
        return f"cannot read back a {column_type} written with it"
    if parsed.item() != _SAMPLE.date():
        return "must include the year, month and day"
    return None


def _read_back(
    text: pl.Series, format_: str, column_type: ColumnType
) -> pl.Series:
    if column_type is ColumnType.DATE:
        return text.str.to_date(format_)
    return text.str.to_datetime(format_, time_unit="us").dt.date()


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
    columns: tuple[Column, ...]
    rules: Rules = Field(default_factory=Rules)
    target: Target

    # Not Field(min_length=1): pydantic counts only the items that passed,
    # so one invalid column would also be reported as "no columns".
    @field_validator("columns")
    @classmethod
    def _require_columns(
        cls, columns: tuple[Column, ...]
    ) -> tuple[Column, ...]:
        if not columns:
            raise ValueError("at least one column is required")
        return columns

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


class DatasetConfigError(Exception):
    """A dataset file that cannot be read or is not a valid config."""

    def __init__(self, path: Path, problems: Sequence[str]) -> None:
        self.path = path
        self.problems = tuple(problems)
        super().__init__(
            "\n".join(f"{path}: {problem}" for problem in self.problems)
        )


def load_dataset_config(path: Path) -> DatasetConfig:
    """Read and validate a dataset YAML file.

    Raises DatasetConfigError listing every problem found.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise DatasetConfigError(
            path, [f"cannot read file: {error.strerror}"]
        ) from error
    except UnicodeDecodeError as error:
        raise DatasetConfigError(path, ["file is not valid UTF-8"]) from error

    try:
        # A SafeLoader subclass: python/* tags are still rejected.
        data = yaml.load(text, Loader=_UniqueKeyLoader)
    except yaml.YAMLError as error:
        raise DatasetConfigError(path, [_format_yaml_error(error)]) from error

    if data is None:
        raise DatasetConfigError(path, ["file is empty"])
    if not isinstance(data, dict):
        raise DatasetConfigError(
            path,
            [f"top level must be a mapping, not {type(data).__name__}"],
        )

    try:
        return DatasetConfig.model_validate(
            data, context={_BASE_DIR: path.parent}
        )
    except ValidationError as error:
        raise DatasetConfigError(
            path, _format_validation_error(error)
        ) from error


class _UniqueKeyLoader(yaml.SafeLoader):
    """SafeLoader that rejects repeated keys instead of keeping the last."""

    def construct_mapping(
        self, node: yaml.MappingNode, deep: bool = False
    ) -> dict[Any, Any]:
        seen = set()
        for key_node, _ in node.value:
            # Merge keys (<<) are resolved by the parent, and local keys
            # may override merged ones, as in plain YAML.
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue
            key = self.construct_object(key_node, deep=deep)
            # Unhashable keys are reported by the parent implementation.
            if not isinstance(key, Hashable):
                continue
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    problem=f"duplicate key '{key}'",
                    problem_mark=key_node.start_mark,
                )
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def _format_yaml_error(error: yaml.YAMLError) -> str:
    if not isinstance(error, yaml.MarkedYAMLError):
        # ReaderError and similar carry no line mark, only their text.
        return str(error).splitlines()[0]
    reason = error.problem or "invalid YAML"
    if error.problem_mark is None:
        return reason
    return f"line {error.problem_mark.line + 1}: {reason}"


def _format_validation_error(error: ValidationError) -> list[str]:
    problems = []
    for detail in error.errors():
        location = _format_location(detail["loc"])
        message = detail["msg"].removeprefix("Value error, ")
        # Model-level validators report several problems in one message.
        for line in message.splitlines():
            problems.append(f"{location}: {line}" if location else line)
    return problems


def _format_location(location: tuple[int | str, ...]) -> str:
    """Render a pydantic location as a path like columns[2].type."""
    path = ""
    for part in location:
        if isinstance(part, int):
            path += f"[{part}]"
        else:
            path += f".{part}" if path else part
    return path
