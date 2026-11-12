"""Download or read the raw CSV of a dataset."""

import hashlib
import shutil
import tempfile
from collections import Counter
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Literal

import polars as pl
import requests
from pydantic import BaseModel, ConfigDict, ValidationError

from config_driven_etl.dataset_config import Source
from config_driven_etl.errors import error_reason

# Read is the longest wait between two chunks, not the whole download.
TIMEOUT = (10, 60)  # (connect, read) in seconds
CHUNK_SIZE = 1024 * 1024


class ExtractError(Exception):
    """The raw data of a dataset could not be obtained."""


@dataclass(frozen=True)
class DownloadResult:
    """Outcome of download_csv; downloaded is False when skipped."""

    downloaded: bool
    size: int
    sha256: str


class _SourceMetadata(BaseModel):
    """What was downloaded last, stored next to the raw CSV."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    url: str
    last_modified: str | None
    size: int
    sha256: str


def raw_csv_path(data_dir: Path, name: str) -> Path:
    """Return where the raw CSV of a dataset is stored."""
    return data_dir / "raw" / name / f"{name}.csv"


def metadata_path(destination: Path) -> Path:
    """Return the metadata file kept next to a downloaded CSV."""
    return destination.with_name(f"{destination.stem}.meta.json")


def download_csv(
    url: str, destination: Path, session: requests.Session
) -> DownloadResult:
    """Stream url into destination unless it is unchanged since last time.

    The previous Last-Modified is sent as If-Modified-Since; on 304 the
    existing file is kept. Raises ExtractError on HTTP, timeout or network
    errors; a failed download never leaves a truncated file or replaces
    the previous one.
    """
    previous = _reusable_metadata(url, destination)
    # Only the date is sent: some portals return unstable ETags, and an
    # If-None-Match header would make the server ignore the date.
    headers = {}
    if previous is not None and previous.last_modified is not None:
        headers["If-Modified-Since"] = previous.last_modified

    with (
        _http_errors(url),
        session.get(
            url, stream=True, timeout=TIMEOUT, headers=headers
        ) as response,
    ):
        if response.status_code == requests.codes.not_modified:
            if previous is None:
                raise ExtractError(f"{url}: unexpected HTTP 304")
            return DownloadResult(False, previous.size, previous.sha256)
        response.raise_for_status()
        digest = hashlib.sha256()
        size = _write_atomically(
            destination,
            lambda file: _write_body(response, file, digest.update),
        )
        last_modified = response.headers.get("Last-Modified")

    metadata = _SourceMetadata(
        url=url,
        last_modified=last_modified,
        size=size,
        sha256=digest.hexdigest(),
    )
    payload = metadata.model_dump_json(indent=2).encode("utf-8")
    _write_atomically(metadata_path(destination), lambda f: f.write(payload))
    return DownloadResult(True, size, metadata.sha256)


def copy_csv(source: Path, destination: Path) -> int:
    """Copy a local CSV into destination and return the bytes written.

    Raises ExtractError if the source cannot be read; a failed copy never
    leaves a truncated file or replaces the previous one.
    """
    size = _write_atomically(destination, lambda file: _copy(source, file))
    # Download metadata no longer describes this file; a later download
    # from the URL must not be skipped because of it.
    metadata = metadata_path(destination)
    try:
        metadata.unlink(missing_ok=True)
    except OSError as error:
        raise ExtractError(
            f"{metadata}: cannot remove file: {error_reason(error)}"
        ) from error
    return size


def _reusable_metadata(url: str, destination: Path) -> _SourceMetadata | None:
    """Return the previous download's metadata if it still describes it."""
    try:
        text = metadata_path(destination).read_text(encoding="utf-8")
        size = destination.stat().st_size
    # Unreadable metadata only costs a fresh download.
    except (OSError, UnicodeDecodeError):
        return None
    try:
        metadata = _SourceMetadata.model_validate_json(text)
    except ValidationError:
        return None
    # A changed URL or a file altered since the download needs a fresh copy.
    if metadata.url != url or metadata.size != size:
        return None
    return metadata


def _write_atomically(
    destination: Path, write: Callable[[IO[bytes]], int]
) -> int:
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Deleted when the block exits, which is a no-op after the rename
        # and removes the partial file on any error.
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
    except requests.RequestException:
        # A subclass of OSError, but a network failure: _http_errors
        # reports it as a failed download.
        raise
    except OSError as error:
        # Permissions, a full disk or a file in the way of the directory.
        raise ExtractError(
            f"{destination}: cannot write file: {error_reason(error)}"
        ) from error
    return size


def _write_body(
    response: requests.Response,
    file: IO[bytes],
    on_chunk: Callable[[bytes], object],
) -> int:
    size = 0
    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
        file.write(chunk)
        on_chunk(chunk)
        size += len(chunk)
    return size


@contextmanager
def _http_errors(url: str) -> Generator[None]:
    """Turn requests exceptions into ExtractError with a clear message."""
    try:
        yield
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


_POLARS_ENCODINGS: dict[str, Literal["utf8", "utf8-lossy"]] = {
    "utf-8": "utf8",
    "utf-8-lossy": "utf8-lossy",
}


def scan_raw_csv(path: Path, source: Source) -> pl.LazyFrame:
    """Lazily read a raw CSV with every column as a string.

    Delimiter and encoding come from the source config. The header is read
    now, so a missing or empty file or a repeated column name raises
    ExtractError here. Errors in the body (invalid bytes, rows with extra
    fields) may only surface when the frame is collected.
    """
    try:
        # Without a header Polars returns the names as written; with one
        # it silently renames duplicates.
        header = _scan(path, source, has_header=False, n_rows=1)
        names = header.collect().row(0)
    except (OSError, pl.exceptions.PolarsError) as error:
        reason = error_reason(error)
        raise ExtractError(f"{path}: cannot read CSV: {reason}") from error

    # Empty names cannot be mapped by a config, so repeats do not matter.
    repeated = [
        name
        for name, count in Counter(n for n in names if n is not None).items()
        if count > 1
    ]
    if repeated:
        listed = ", ".join(f"'{name}'" for name in repeated)
        raise ExtractError(f"{path}: repeated column name in header: {listed}")
    return _scan(path, source)


def _scan(
    path: Path,
    source: Source,
    has_header: bool = True,
    n_rows: int | None = None,
) -> pl.LazyFrame:
    return pl.scan_csv(
        path,
        separator=source.delimiter,
        encoding=_POLARS_ENCODINGS[source.encoding],
        infer_schema=False,
        has_header=has_header,
        n_rows=n_rows,
    )
