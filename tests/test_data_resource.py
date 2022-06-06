import unittest

from algoseek_connector import Session, DataResource, Datagroup

import config


class TestDataResource(unittest.TestCase):

    def setUp(self):
        self.host = config.host
        self.user = config.user
        self.password = config.password

    def test_empty(self):
        resource = DataResource()
        self.assertIsNotNone(resource.session)

    def test_from_session(self):
        session = Session(self.host, self.user, self.password)
        resource = DataResource(session)
        self.assertIs(resource.session, session)
        session.close()

    def test_datagroups(self):
        session = Session(self.host, self.user, self.password)
        resource = DataResource(session)
        self.assertNotEqual(list(resource.datagroups.all()), [])
        for dgr in resource.datagroups.all():
            self.assertIsInstance(dgr, Datagroup)
        session.close()

    def test_datagroup_from_str(self):
        session = Session(self.host, self.user, self.password)
        resource = DataResource(session)
        dgr, *_ = list(resource.datagroups.all())
        self.assertEqual(dgr.name, resource.datagroup(dgr.name).name)
        session.close()


if __name__ == '__main__':
    unittest.main()
