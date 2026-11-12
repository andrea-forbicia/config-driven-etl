import re
import unittest

from typer.testing import CliRunner

from config_driven_etl import __version__
from config_driven_etl.cli import app

# Rich forces colors on CI runners, splitting option names with ANSI codes.
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


class CliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()

    def test_version_prints_package_version(self) -> None:
        result = self.runner.invoke(app, ["--version"])

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output.strip(), __version__)

    def test_help_exits_successfully(self) -> None:
        result = self.runner.invoke(app, ["--help"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("--version", ANSI_ESCAPE.sub("", result.output))
