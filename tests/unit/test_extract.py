import hashlib
import json
import tempfile
import unittest
from collections.abc import Iterable, Iterator
from pathlib import Path
from unittest import mock

import polars as pl
import requests

from config_driven_etl.dataset_config import Source
from config_driven_etl.extract import (
    CHUNK_SIZE,
    TIMEOUT,
    DownloadResult,
    ExtractError,
    copy_csv,
    download_csv,
    metadata_path,
    raw_csv_path,
    scan_raw_csv,
)

URL = "https://example.org/data.csv"


LAST_MODIFIED = "Thu, 12 Nov 2026 14:14:35 GMT"


def fake_response(
    status: int = 200,
    reason: str = "OK",
    chunks: Iterable[bytes] = (),
    last_modified: str | None = None,
) -> mock.MagicMock:
    response = mock.MagicMock(spec=requests.Response)
    response.status_code = status
    response.reason = reason
    response.headers = (
        {} if last_modified is None else {"Last-Modified": last_modified}
    )
    response.iter_content.return_value = iter(chunks)
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(
            response=response
        )
    return response


def fake_session(
    response: mock.MagicMock | None = None,
    error: Exception | None = None,
) -> mock.MagicMock:
    session = mock.MagicMock(spec=requests.Session)
    if error is not None:
        session.get.side_effect = error
    else:
        session.get.return_value.__enter__.return_value = response
    return session


def chunks_then(error: BaseException) -> Iterator[bytes]:
    yield b"partial,"
    raise error


class RawCsvPathTest(unittest.TestCase):
    def test_path_is_named_after_the_dataset(self) -> None:
        self.assertEqual(
            raw_csv_path(Path("data"), "sample"),
            Path("data/raw/sample/sample.csv"),
        )


class DownloadCsvTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.destination = Path(directory.name) / "raw" / "s" / "s.csv"

    def write_previous_run(self) -> None:
        self.destination.parent.mkdir(parents=True)
        self.destination.write_bytes(b"previous")

    def assert_no_partial_files(self) -> None:
        leftovers = list(self.destination.parent.glob("*.part"))
        self.assertEqual(leftovers, [])

    def download_error(self, session: mock.MagicMock) -> str:
        with self.assertRaises(ExtractError) as context:
            download_csv(URL, self.destination, session)

        self.assert_no_partial_files()
        return str(context.exception)

    def test_streams_chunks_into_destination(self) -> None:
        session = fake_session(fake_response(chunks=[b"a,b\n", b"1,2\n"]))

        result = download_csv(URL, self.destination, session)

        self.assertEqual(
            result,
            DownloadResult(True, 8, hashlib.sha256(b"a,b\n1,2\n").hexdigest()),
        )
        self.assertEqual(self.destination.read_bytes(), b"a,b\n1,2\n")
        session.get.assert_called_once_with(
            URL, stream=True, timeout=TIMEOUT, headers={}
        )
        response = session.get.return_value.__enter__.return_value
        response.iter_content.assert_called_once_with(chunk_size=CHUNK_SIZE)
        self.assert_no_partial_files()

    def test_replaces_previous_file(self) -> None:
        self.write_previous_run()
        session = fake_session(fake_response(chunks=[b"new"]))

        download_csv(URL, self.destination, session)

        self.assertEqual(self.destination.read_bytes(), b"new")

    def test_empty_body(self) -> None:
        session = fake_session(fake_response(chunks=[]))

        result = download_csv(URL, self.destination, session)

        self.assertEqual(result.size, 0)
        self.assertEqual(self.destination.read_bytes(), b"")

    def first_download(
        self, last_modified: str | None = LAST_MODIFIED
    ) -> None:
        response = fake_response(chunks=[b"v1"], last_modified=last_modified)
        download_csv(URL, self.destination, fake_session(response))

    def sent_headers(self, session: mock.MagicMock) -> dict[str, str]:
        return session.get.call_args.kwargs["headers"]

    def test_writes_metadata_next_to_the_file(self) -> None:
        self.first_download()

        metadata = json.loads(metadata_path(self.destination).read_text())

        self.assertEqual(
            metadata,
            {
                "url": URL,
                "last_modified": LAST_MODIFIED,
                "size": 2,
                "sha256": hashlib.sha256(b"v1").hexdigest(),
            },
        )
        self.assertEqual(metadata_path(self.destination).name, "s.meta.json")

    def test_unchanged_source_is_not_downloaded_again(self) -> None:
        self.first_download()
        session = fake_session(fake_response(304, "Not Modified"))

        result = download_csv(URL, self.destination, session)

        self.assertEqual(
            result,
            DownloadResult(False, 2, hashlib.sha256(b"v1").hexdigest()),
        )
        self.assertEqual(
            self.sent_headers(session),
            {"If-Modified-Since": LAST_MODIFIED},
        )
        self.assertEqual(self.destination.read_bytes(), b"v1")
        self.assert_no_partial_files()

    def test_changed_source_replaces_file_and_metadata(self) -> None:
        self.first_download()
        newer = "Fri, 13 Nov 2026 08:00:00 GMT"
        session = fake_session(
            fake_response(chunks=[b"v2!"], last_modified=newer)
        )

        result = download_csv(URL, self.destination, session)

        self.assertTrue(result.downloaded)
        self.assertEqual(self.destination.read_bytes(), b"v2!")
        metadata = json.loads(metadata_path(self.destination).read_text())
        self.assertEqual(
            (metadata["last_modified"], metadata["size"]), (newer, 3)
        )

    def test_no_condition_when_previous_metadata_is_unusable(self) -> None:
        cases = {
            "no last_modified": lambda: self.first_download(None),
            "other url": lambda: self.rewrite_metadata(url="https://x.org/a"),
            "file altered": lambda: self.destination.write_bytes(b"edited"),
            "corrupt metadata": lambda: metadata_path(
                self.destination
            ).write_text("{not json"),
            "metadata not utf-8": lambda: metadata_path(
                self.destination
            ).write_bytes(b"\xff\xfe"),
            "file missing": lambda: self.destination.unlink(),
        }
        for case, spoil in cases.items():
            with self.subTest(case=case):
                self.first_download()
                spoil()
                session = fake_session(fake_response(chunks=[b"v2"]))

                result = download_csv(URL, self.destination, session)

                self.assertEqual(self.sent_headers(session), {})
                self.assertTrue(result.downloaded)

    def rewrite_metadata(self, **changes: object) -> None:
        path = metadata_path(self.destination)
        metadata = json.loads(path.read_text())
        path.write_text(json.dumps({**metadata, **changes}))

    def test_unexpected_not_modified(self) -> None:
        session = fake_session(fake_response(304, "Not Modified"))

        message = self.download_error(session)

        self.assertEqual(message, f"{URL}: unexpected HTTP 304")
        self.assertFalse(self.destination.exists())

    def test_failed_download_keeps_previous_metadata(self) -> None:
        self.first_download()
        before = metadata_path(self.destination).read_text()
        session = fake_session(fake_response(500, "Server Error"))

        self.download_error(session)

        self.assertEqual(metadata_path(self.destination).read_text(), before)
        self.assertEqual(self.destination.read_bytes(), b"v1")

    def test_http_errors(self) -> None:
        for status, reason in [(404, "Not Found"), (503, "")]:
            with self.subTest(status=status):
                session = fake_session(fake_response(status, reason))

                message = self.download_error(session)

                self.assertEqual(
                    message, f"{URL}: HTTP {status} {reason}".strip()
                )
                self.assertFalse(self.destination.exists())

    def test_http_error_without_response(self) -> None:
        response = fake_response()
        response.raise_for_status.side_effect = requests.HTTPError("boom")

        message = self.download_error(fake_session(response))

        self.assertEqual(message, f"{URL}: HTTP error: boom")

    def test_timeout(self) -> None:
        session = fake_session(error=requests.ConnectTimeout("slow"))

        self.assertEqual(self.download_error(session), f"{URL}: timed out")

    def test_connection_error(self) -> None:
        session = fake_session(error=requests.ConnectionError("refused"))

        self.assertEqual(
            self.download_error(session),
            f"{URL}: download failed: refused",
        )

    def test_failure_mid_stream_keeps_previous_file(self) -> None:
        self.write_previous_run()
        error = requests.exceptions.ChunkedEncodingError("cut")
        session = fake_session(fake_response(chunks=chunks_then(error)))

        message = self.download_error(session)

        self.assertEqual(message, f"{URL}: download failed: cut")
        self.assertEqual(self.destination.read_bytes(), b"previous")

    def test_read_timeout_mid_stream(self) -> None:
        # requests wraps a read timeout inside iter_content in a
        # ConnectionError, so it is reported as a failed download.
        self.write_previous_run()
        error = requests.ConnectionError("Read timed out.")
        session = fake_session(fake_response(chunks=chunks_then(error)))

        message = self.download_error(session)

        self.assertEqual(message, f"{URL}: download failed: Read timed out.")
        self.assertEqual(self.destination.read_bytes(), b"previous")

    def test_unexpected_error_still_removes_partial_file(self) -> None:
        session = fake_session(
            fake_response(chunks=chunks_then(KeyboardInterrupt()))
        )

        with self.assertRaises(KeyboardInterrupt):
            download_csv(URL, self.destination, session)

        self.assert_no_partial_files()
        self.assertFalse(self.destination.exists())


class CopyCsvTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.source = self.root / "local" / "input.csv"
        self.source.parent.mkdir()
        self.destination = self.root / "raw" / "s" / "s.csv"

    def assert_no_partial_files(self) -> None:
        if self.destination.parent.exists():
            leftovers = list(self.destination.parent.glob("*.part"))
            self.assertEqual(leftovers, [])

    def copy_error(self, source: Path) -> str:
        with self.assertRaises(ExtractError) as context:
            copy_csv(source, self.destination)

        self.assert_no_partial_files()
        return str(context.exception)

    def test_copies_content_and_returns_size(self) -> None:
        self.source.write_bytes(b"a,b\n1,2\n")

        size = copy_csv(self.source, self.destination)

        self.assertEqual(size, 8)
        self.assertEqual(self.destination.read_bytes(), b"a,b\n1,2\n")
        self.assertEqual(self.source.read_bytes(), b"a,b\n1,2\n")
        self.assert_no_partial_files()

    def test_copies_files_larger_than_one_chunk(self) -> None:
        content = b"x" * (CHUNK_SIZE + 10)
        self.source.write_bytes(content)

        size = copy_csv(self.source, self.destination)

        self.assertEqual(size, len(content))
        self.assertEqual(self.destination.read_bytes(), content)

    def test_replaces_previous_file(self) -> None:
        self.destination.parent.mkdir(parents=True)
        self.destination.write_bytes(b"previous")
        self.source.write_bytes(b"new")

        copy_csv(self.source, self.destination)

        self.assertEqual(self.destination.read_bytes(), b"new")

    def test_source_is_destination(self) -> None:
        self.destination.parent.mkdir(parents=True)
        self.destination.write_bytes(b"same")

        size = copy_csv(self.destination, self.destination)

        self.assertEqual(size, 4)
        self.assertEqual(self.destination.read_bytes(), b"same")

    def test_missing_source_keeps_previous_file(self) -> None:
        self.destination.parent.mkdir(parents=True)
        self.destination.write_bytes(b"previous")
        missing = self.root / "local" / "missing.csv"

        message = self.copy_error(missing)

        self.assertTrue(message.startswith(f"{missing}: cannot read file: "))
        self.assertEqual(self.destination.read_bytes(), b"previous")

    def test_directory_as_source(self) -> None:
        message = self.copy_error(self.source.parent)

        self.assertTrue(
            message.startswith(f"{self.source.parent}: cannot read file: ")
        )
        self.assertFalse(self.destination.exists())

    def test_removes_download_metadata(self) -> None:
        # Otherwise switching back to the URL could keep this local copy
        # on a 304 from the server.
        meta = metadata_path(self.destination)
        self.destination.parent.mkdir(parents=True)
        meta.write_text("{}")
        self.source.write_bytes(b"local")

        copy_csv(self.source, self.destination)

        self.assertFalse(meta.exists())


class ScanRawCsvTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "raw.csv"

    def scan(self, content: bytes, **source: str) -> pl.DataFrame:
        self.path.write_bytes(content)
        config = Source.model_validate({"path": "raw.csv", **source})
        return scan_raw_csv(self.path, config).collect()

    def scan_error(self, content: bytes | None, **source: str) -> str:
        if content is not None:
            self.path.write_bytes(content)
        config = Source.model_validate({"path": "raw.csv", **source})
        with self.assertRaises(ExtractError) as context:
            scan_raw_csv(self.path, config)
        return str(context.exception)

    def test_every_column_is_a_string(self) -> None:
        frame = self.scan(b"id,price,when\n1,2.5,2026-01-01\n")

        self.assertEqual(
            frame.schema,
            pl.Schema(
                {"id": pl.String, "price": pl.String, "when": pl.String}
            ),
        )
        self.assertEqual(frame.rows(), [("1", "2.5", "2026-01-01")])

    def test_returns_a_lazy_frame(self) -> None:
        self.path.write_bytes(b"a\n1\n")
        source = Source.model_validate({"path": "raw.csv"})

        self.assertIsInstance(scan_raw_csv(self.path, source), pl.LazyFrame)

    def test_uses_configured_delimiter(self) -> None:
        for delimiter, content in [
            (";", b'a;b\n"x;y";2\n'),
            ("\t", b"a\tb\nx;y\t2\n"),
        ]:
            with self.subTest(delimiter=delimiter):
                frame = self.scan(content, delimiter=delimiter)

                self.assertEqual(frame.rows(), [("x;y", "2")])

    def test_raw_values_are_not_cleaned(self) -> None:
        frame = self.scan(b'a,b,c\n1,,"  "\n2,"",x\n')

        self.assertEqual(frame.rows(), [("1", None, "  "), ("2", "", "x")])

    def test_byte_order_mark_is_ignored(self) -> None:
        frame = self.scan(b"\xef\xbb\xbfa,b\n1,2\n")

        self.assertEqual(frame.columns, ["a", "b"])

    def test_header_only(self) -> None:
        frame = self.scan(b"a,b\n")

        self.assertEqual((frame.columns, frame.height), (["a", "b"], 0))

    def test_lossy_encoding_replaces_invalid_bytes(self) -> None:
        frame = self.scan(b"a\ncaf\xe9\n", encoding="utf-8-lossy")

        self.assertEqual(frame.rows(), [("caf\ufffd",)])

    def assert_body_error_is_not_silent(self, content: bytes) -> None:
        # Small files fail while the header is read, large ones only when
        # the body is collected; either way the error is raised.
        self.path.write_bytes(content)
        source = Source.model_validate({"path": "raw.csv"})

        with self.assertRaises((ExtractError, pl.exceptions.ComputeError)):
            scan_raw_csv(self.path, source).collect()

    def test_invalid_utf8_in_body_is_not_silent(self) -> None:
        self.assert_body_error_is_not_silent(b"a\nok\ncaf\xe9\n")

    def test_short_rows_are_filled_with_nulls(self) -> None:
        frame = self.scan(b"a,b\n1,2\n3\n")

        self.assertEqual(frame.rows(), [("1", "2"), ("3", None)])

    def test_long_rows_are_not_silent(self) -> None:
        self.assert_body_error_is_not_silent(b"a,b\n1,2\n3,4,5\n")

    def test_repeated_header_names(self) -> None:
        message = self.scan_error(b"a,b,a,b,c\n1,2,3,4,5\n")

        self.assertEqual(
            message,
            f"{self.path}: repeated column name in header: 'a', 'b'",
        )

    def test_repeated_empty_header_names_are_allowed(self) -> None:
        frame = self.scan(b"a,,\n1,2,3\n")

        self.assertEqual(frame.width, 3)

    def test_unreadable_files(self) -> None:
        cases = {
            "missing file": None,
            "empty file": b"",
            "invalid utf-8 header": b"caf\xe9\n1\n",
        }
        for case, content in cases.items():
            with self.subTest(case=case):
                self.path.unlink(missing_ok=True)

                message = self.scan_error(content)

                self.assertTrue(
                    message.startswith(f"{self.path}: cannot read CSV: "),
                    message,
                )
