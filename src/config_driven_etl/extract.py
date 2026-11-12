"""Download or read the raw CSV of a dataset."""

import tempfile
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

    The file is written under a temporary name and renamed when complete,
    so a failed download never leaves a truncated file or replaces the
    previous one. Raises ExtractError on HTTP, timeout or network errors.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Deleted when the block exits, which is a no-op after the rename and
    # removes the partial file on any error.
    with tempfile.NamedTemporaryFile(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".part",
        delete_on_close=False,
    ) as temp_file:
        size = _stream(url, temp_file, session)
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
