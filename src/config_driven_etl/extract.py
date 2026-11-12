"""Download or read the raw CSV of a dataset."""

import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import IO

import requests

# Read is the longest wait between two chunks, not the whole download.
TIMEOUT = (10, 60)  # (connect, read) in seconds
CHUNK_SIZE = 1024 * 1024


class ExtractError(Exception):
    """The raw data of a dataset could not be obtained."""


def raw_csv_path(data_dir: Path, name: str) -> Path:
    """Return where the raw CSV of a dataset is stored."""
    return data_dir / "raw" / name / f"{name}.csv"


def download_csv(
    url: str, destination: Path, session: requests.Session
) -> int:
    """Stream url into destination and return the number of bytes written.

    Raises ExtractError on HTTP, timeout or network errors; a failed
    download never leaves a truncated file or replaces the previous one.
    """
    return _write_atomically(
        destination, lambda file: _stream(url, file, session)
    )


def copy_csv(source: Path, destination: Path) -> int:
    """Copy a local CSV into destination and return the bytes written.

    Raises ExtractError if the source cannot be read; a failed copy never
    leaves a truncated file or replaces the previous one.
    """
    return _write_atomically(destination, lambda file: _copy(source, file))


def _write_atomically(
    destination: Path, write: Callable[[IO[bytes]], int]
) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Deleted when the block exits, which is a no-op after the rename and
    # removes the partial file on any error.
    with tempfile.NamedTemporaryFile(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".part",
        delete_on_close=False,
    ) as temp_file:
        size = write(temp_file)
        # Windows cannot rename a file that is still open.
        temp_file.close()
        Path(temp_file.name).replace(destination)
    return size


def _stream(url: str, file: IO[bytes], session: requests.Session) -> int:
    try:
        with session.get(url, stream=True, timeout=TIMEOUT) as response:
            response.raise_for_status()
            size = 0
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                file.write(chunk)
                size += len(chunk)
            return size
    except requests.HTTPError as error:
        response = error.response
        if response is None:
            raise ExtractError(f"{url}: HTTP error: {error}") from error
        status = f"{response.status_code} {response.reason or ''}".strip()
        raise ExtractError(f"{url}: HTTP {status}") from error
    except requests.Timeout as error:
        raise ExtractError(f"{url}: timed out") from error
    except requests.RequestException as error:
        raise ExtractError(f"{url}: download failed: {error}") from error


def _copy(source: Path, file: IO[bytes]) -> int:
    try:
        source_file = source.open("rb")
    except OSError as error:
        reason = error.strerror or str(error)
        raise ExtractError(f"{source}: cannot read file: {reason}") from error
    with source_file:
        shutil.copyfileobj(source_file, file, CHUNK_SIZE)
    return file.tell()
