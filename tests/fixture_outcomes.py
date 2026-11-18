"""The fixture datasets and their expected outcome, stated once.

A change to a fixture CSV or YAML file updates its entry here; every test
that counts fixture rows reads the numbers from OUTCOMES.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from config_driven_etl.dataset_config import DatasetConfig, load_dataset_config

FIXTURES = Path(__file__).parent / "fixtures"


@dataclass(frozen=True)
class FixtureOutcome:
    """Rows a fixture yields through extract and transform."""

    rows_in: int
    rows_valid: int
    # A row with several reasons counts once under each of them.
    rejections: Mapping[str, int]

    @property
    def rows_rejected(self) -> int:
        return self.rows_in - self.rows_valid


OUTCOMES: Mapping[str, FixtureOutcome] = {
    "sensor_readings": FixtureOutcome(
        rows_in=14,
        rows_valid=5,
        rejections={
            "humidity_pct: above max": 1,
            "humidity_pct: invalid int": 1,
            "is_online: invalid bool": 1,
            "measured_at: invalid datetime": 1,
            "reading_id: duplicate": 1,
            "reading_id: no pattern match": 1,
            "sensor_id: null": 2,
            "temperature_c: above max": 1,
        },
    ),
    "library_loans": FixtureOutcome(
        rows_in=14,
        rows_valid=4,
        rejections={
            "branch_code: no pattern match": 1,
            "branch_code: not allowed": 1,
            "branch_code: null": 1,
            "fine_eur: below min": 1,
            "fine_eur: invalid float": 1,
            "loan_date: invalid date": 2,
            "loan_id: duplicate": 1,
            "loan_id: invalid int": 1,
            "renewals: above max": 1,
        },
    ),
}


def fixture_path(name: str) -> Path:
    """The YAML file of a fixture dataset."""
    return FIXTURES / name / f"{name}.yaml"


def fixture_config(name: str) -> DatasetConfig:
    return load_dataset_config(fixture_path(name))
