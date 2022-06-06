import unittest

import algoseek_connector.functions as fn


class TestDataset(unittest.TestCase):

    def test_function_name_integrity(self):
        for name in dir(fn):
            if isinstance(getattr(fn, name), fn.Function):
                self.assertEqual(getattr(fn, name).name, name)

    def test_function_name_unique(self):
        names = set()
        for name in dir(fn):
            if isinstance(getattr(fn, name), fn.Function):
                fn_name = getattr(fn, name).name
                self.assertNotIn(fn_name, names)
                names.add(fn_name)
