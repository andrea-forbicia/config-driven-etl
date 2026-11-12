import runpy
import unittest
from unittest import mock

from config_driven_etl import __version__


class MainModuleTest(unittest.TestCase):
    def test_python_dash_m_runs_the_cli(self) -> None:
        with (
            mock.patch("sys.argv", ["config-driven-etl", "--version"]),
            mock.patch("typer.echo") as echo,
            self.assertRaises(SystemExit) as context,
        ):
            runpy.run_module("config_driven_etl", run_name="__main__")

        self.assertEqual(context.exception.code, 0)
        echo.assert_called_once_with(__version__)


if __name__ == "__main__":
    unittest.main()
