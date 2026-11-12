import tempfile
import unittest
from collections.abc import Iterable, Iterator
from pathlib import Path
from unittest import mock

import requests

from config_driven_etl.extract import (
    CHUNK_SIZE,
    TIMEOUT,
    ExtractError,
    download_csv,
    raw_csv_path,
)

URL = "https://example.org/data.csv"


def fake_response(
    status: int = 200,
    reason: str = "OK",
    chunks: Iterable[bytes] = (),
) -> mock.MagicMock:
    response = mock.MagicMock(spec=requests.Response)
    response.status_code = status
    response.reason = reason
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

        size = download_csv(URL, self.destination, session)

        self.assertEqual(size, 8)
        self.assertEqual(self.destination.read_bytes(), b"a,b\n1,2\n")
        session.get.assert_called_once_with(URL, stream=True, timeout=TIMEOUT)
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

        size = download_csv(URL, self.destination, session)

        self.assertEqual(size, 0)
        self.assertEqual(self.destination.read_bytes(), b"")

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
