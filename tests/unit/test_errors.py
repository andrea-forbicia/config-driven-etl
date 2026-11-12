import unittest

from config_driven_etl.errors import error_reason


class ErrorReasonTest(unittest.TestCase):
    def test_first_line_of_the_message(self) -> None:
        error = ValueError("first line\nsecond line")

        self.assertEqual(error_reason(error), "first line")

    def test_empty_message_gives_the_type_name(self) -> None:
        self.assertEqual(error_reason(ValueError("")), "ValueError")

    def test_blank_first_line_gives_the_type_name(self) -> None:
        self.assertEqual(error_reason(OSError("\ndetail")), "OSError")


if __name__ == "__main__":
    unittest.main()
