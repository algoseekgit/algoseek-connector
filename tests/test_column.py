import unittest

from algoconnect import Dataset
from algoconnect.resources import Column

from mock_session import MockSession, sample_datagroups, sample_datasets, sample_columns


class TestDataset(unittest.TestCase):

    def setUp(self):
        self.session = MockSession()
        dgr_name = sample_datagroups[0]
        dts_name = sample_datasets[dgr_name][0]
        self.dataset = Dataset(dgr_name + '.' + dts_name, session=self.session)

    def test_dataset_columns(self):
        names = sample_columns[self.dataset.datagroup.name][self.dataset.name][0]
        for name in names:
            self.assertTrue(hasattr(self.dataset, name))
            self.assertIsInstance(getattr(self.dataset, name), Column)
            self.assertIs(getattr(self.dataset, name)._parent, self.dataset)

    def test_existing_column(self):
        payload = sample_columns[self.dataset.datagroup.name][self.dataset.name]
        column = getattr(self.dataset, payload[0][0])
        self.assertEqual(column.name, payload[0][0])
        self.assertEqual(column.dtype, payload[1][0])
        self.assertEqual(column.description, payload[2][0])

    def test_get_invalid_column(self):
        with self.assertRaises(AttributeError):
            getattr(self.dataset, 'ColumnDoesNotExist')

    def test_create_invalid_column(self):
        with self.assertRaises(ValueError):
            Column('ColumnDoesNotExist', 'String', parent=self.dataset)

    def create_column_simple(self):
        column = Column('Ticker', 'String')
        self.assertEqual(column.name, 'Ticker')
        self.assertEqual(column.dtype, 'String')
        self.assertIsNote(column.descr)
        self.assertIsNote(column._parent)

        self.assertEqual(column.evaluated_name, column.name)
        self.assertEqual(column.evaluated_dtype, column.dtype)

    def test_str_repr(self):
        column = Column('Ticker', 'String')
        self.assertEqual(str(column), f'"{column.name}"')

    def test_cast(self):
        column = Column('Ticker', 'String').cast('Text')
        self.assertIsInstance(column, Column)
        self.assertEqual(column.dtype, 'String')
        self.assertEqual(column.evaluated_dtype, 'Text')

    def test_alias(self):
        column = Column('Ticker', 'String').alias('Symbol')
        self.assertIsInstance(column, Column)
        self.assertEqual(column.name, 'Ticker')
        self.assertEqual(column.evaluated_name, 'Symbol')

    def test_double_alias(self):
        column = Column('Ticker', 'String').alias('Symbol').alias('TickerName')
        self.assertIsInstance(column, Column)
        self.assertEqual(column.name, 'Ticker')
        self.assertEqual(column.evaluated_name, 'TickerName')

    def tearDown(self):
        self.session.close()
