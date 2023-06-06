import unittest

from algoseek_connector import Dataset
from mock_session import MockSession, sample_datagroups, sample_datasets, sample_columns


class TestSelect(unittest.TestCase):
    def setUp(self):
        session = MockSession()
        dgr_name = sample_datagroups[0]
        dts_name = sample_datasets[dgr_name][0]
        self.columns = sample_columns[dgr_name][dts_name][0]
        self.ds = Dataset(dgr_name, dts_name, session=session)

    def test_select_from_str_array(self):
        expr = self.ds.select(self.columns[:3])
        expected = "SELECT " + ", ".join(f'"{x}"' for x in self.columns[:3])
        self.assertEqual(expr.sql().split("FROM")[0].strip(), expected)

    def test_select_from_str_unpack(self):
        expr = self.ds.select(*self.columns[1:5])
        expected = "SELECT " + ", ".join(f'"{x}"' for x in self.columns[1:5])
        self.assertEqual(expr.sql().split("FROM")[0].strip(), expected)

    def test_select_from_str_dict(self):
        payload = {
            "colA": self.columns[0],
            "colB": self.columns[1],
        }
        expr = self.ds.select(payload)
        expected = "SELECT " + ", ".join(f'"{y}" AS "{x}"' for x, y in payload.items())
        self.assertEqual(expr.sql().split("FROM")[0].strip(), expected)

    def test_select_from_columns_array(self):
        columns = [self.ds._columns[x] for x in self.columns[:3]]
        expr = self.ds.select(columns[:3])
        expected = "SELECT " + ", ".join(f'"{x}"' for x in self.columns[:3])
        self.assertEqual(expr.sql().split("FROM")[0].strip(), expected)

    def test_select_from_single_string(self):
        expr = self.ds.select(self.columns[0])
        expected = f'SELECT "{self.columns[0]}"'
        self.assertEqual(expr.sql().split("FROM")[0].strip(), expected)

    def test_select_empty(self):
        with self.assertRaises(ValueError):
            self.ds.select()

    def test_select_from_two_arrays(self):
        with self.assertRaises(ValueError):
            self.ds.select(self.columns[:3], self.columns[:3])

    def test_select_from_invalid_object(self):
        with self.assertRaises(ValueError):
            self.ds.select(self.ds)
        with self.assertRaises(ValueError):
            self.ds.select(sample_columns)

    def test_select_from_unknown_string(self):
        expr = self.ds.select("Hello world!")
        expected = "SELECT 'Hello world!'"
        self.assertEqual(expr.sql().split("FROM")[0].strip(), expected)
