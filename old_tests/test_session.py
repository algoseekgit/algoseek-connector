import unittest

from algoseek_connector import Session, ExecutionError

import config


class TestSession(unittest.TestCase):
    def setUp(self):
        self.host = config.host
        self.user = config.user
        self.password = config.password

    def test_empty(self):
        session = Session()
        self.assertIsNone(session._host)
        self.assertIsNone(session._user)
        self.assertIsNone(session._password)

    def test_instance(self):
        session = Session(self.host, self.user, self.password)
        self.assertIsNotNone(session.ping())
        session.close()

    def test_operational(self):
        session = Session(self.host, self.user, self.password)
        self.assertEqual(session.execute("SELECT 2+2"), [(4,)])
        session.close()

    def test_context(self):
        with Session(self.host, self.user, self.password) as session:
            self.assertIsNotNone(session.ping())

    def test_invalid_password(self):
        invalid_password = self.password[:-1]
        session = Session(self.host, self.user, invalid_password)
        with self.assertRaises(ExecutionError) as e:
            session.ping()
            self.assertTrue("Authentication failed" in e.message)
        session.close()

    def test_invalid_table_name(self):
        session = Session(self.host, self.user, self.password)
        with self.assertRaises(ExecutionError) as e:
            session.execute("SELECT * FROM InvalidTableName LIMIT 1")
            self.assertTrue("doesn't exist" in e.message)
        session.close()


if __name__ == "__main__":
    unittest.main()
