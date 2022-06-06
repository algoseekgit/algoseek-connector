import datetime
import unittest

from algoconnect.resources import Column
from algoconnect.expressions import Literal


class TestDataset(unittest.TestCase):

    def test_simple(self):
        a = Literal('a')
        self.assertEqual(a.value, 'a')

        b = Literal('b')
        self.assertEqual(b.value, 'b')
        self.assertIsNone(b._parent)
        t = Literal('t', parent=a)
        self.assertIs(t._parent, a)

    def test_from_constant_numeric(self):

        number = Literal.wrap_constant(999)
        self.assertIsInstance(number, Literal)
        self.assertEqual(number.value, 999)
        self.assertEqual(str(number), '999')

        number = Literal.wrap_constant(1.0)
        self.assertIsInstance(number, Literal)
        self.assertEqual(number.value, 1.0)
        self.assertEqual(str(number), '1.0')

    def test_from_constant_str(self):

        s = Literal.wrap_constant('Testing')
        self.assertIsInstance(s, Literal)
        self.assertEqual(s.value, 'Testing')
        self.assertEqual(str(s), "'Testing'")

    def test_from_constant_date(self):

        s = Literal.wrap_constant(datetime.date(2022, 2, 3))
        self.assertIsInstance(s, Literal)
        self.assertEqual(s.value, datetime.date(2022, 2, 3))
        self.assertEqual(str(s), "'2022-02-03'")

    def test_from_constant_list(self):

        s = Literal.wrap_constant([1, 2, 3, 4])
        self.assertIsInstance(s, Literal)
        self.assertEqual(s.value, [1, 2, 3, 4])
        self.assertEqual(str(s), "(1, 2, 3, 4)")

        s = Literal.wrap_constant(('a', 'b', 'c'))
        self.assertIsInstance(s, Literal)
        self.assertEqual(s.value, ('a', 'b', 'c'))
        self.assertEqual(str(s), "('a', 'b', 'c')")

        s = Literal.wrap_constant({'AAPL', 'TSLA', "NFLX"})
        self.assertIsInstance(s, Literal)
        self.assertEqual(s.value, {'AAPL', 'TSLA', 'NFLX'})
        self.assertEqual(sorted(str(s)), sorted("('AAPL', 'TSLA', 'NFLX')"))

    def test_constant_from_expr(self):

        ticker = Column('Ticker', 'String')
        const_ticker = Literal.wrap_constant(ticker)
        self.assertIs(const_ticker, ticker)
