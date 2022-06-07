import unittest

from algoseek_connector.expressions import Literal, BooleanExpression, FuncExpression
from algoseek_connector.functions import toDate


class TestExpressions(unittest.TestCase):

    def test_and(self):
        expr = Literal.wrap_constant(0) & Literal.wrap_constant(1)
        self.assertIsInstance(expr, BooleanExpression)
        self.assertEqual(expr.sql(), "0 AND 1")

    def test_or(self):
        expr = Literal.wrap_constant(0) | Literal.wrap_constant(1)
        self.assertIsInstance(expr, BooleanExpression)
        self.assertEqual(expr.sql(), "0 OR 1")

    def test_between(self):
        expr = Literal.wrap_constant(1).between(Literal.wrap_constant(0), Literal.wrap_constant(2))
        self.assertIsInstance(expr, BooleanExpression)
        self.assertEqual(expr.sql(), "1 BETWEEN 0 AND 2")

    def test_function_apply(self):
        expr = toDate(Literal.wrap_constant('2022-01-01 02:03:04'))
        self.assertIsInstance(expr, FuncExpression)
        self.assertEqual(expr.sql(), "toDate('2022-01-01 02:03:04')")

    def test_function_apply_in_bool(self):
        expr = toDate(Literal.wrap_constant('2022-01-01 02:03:04')) > '2021-01-01'
        self.assertIsInstance(expr, BooleanExpression)
        self.assertEqual(expr.sql(), "toDate('2022-01-01 02:03:04') > '2021-01-01'")
