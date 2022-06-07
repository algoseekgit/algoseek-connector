import unittest

from algoseek_connector.expressions import Literal, BooleanExpression


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
