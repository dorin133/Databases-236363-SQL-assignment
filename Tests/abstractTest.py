import unittest
import Solution


class AbstractTest(unittest.TestCase):
    # before each test, setUp is executed
    def setUp(self) -> None:
        Solution.createTables()

    def tearDown(self) -> None:
        Solution.dropTables()
    # after each test, tearDown is executed



