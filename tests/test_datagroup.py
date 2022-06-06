import unittest

from algoconnect import Session, DataResource, Datagroup
from algoconnect.resources import DatagroupSelector, DatagroupsMeta

from mock_session import MockSession, sample_datagroups
import config


class TestDatagroup(unittest.TestCase):

    def setUp(self):
        self.session = MockSession()

    def test_from_data_resource(self):
        resource = DataResource(self.session)
        datagroups = resource.datagroups
        names = {item.name for item in datagroups.all()}
        self.assertEqual(set(sample_datagroups), names)
        for name in sample_datagroups:
            self.assertTrue(hasattr(datagroups, name))

    def test_single(self):
        name = sample_datagroups[0]
        dgr = Datagroup(name, session=self.session)
        self.assertEqual(name, dgr.name)

    def test_single_from_resource(self):
        name = sample_datagroups[0]
        resource = DataResource(self.session)
        dgr = resource.datagroup(name)
        self.assertEqual(name, dgr.name)

    def test_invalid_name(self):
        name = sample_datagroups[0] + sample_datagroups[0]
        resource = DataResource(self.session)
        with self.assertRaises(ValueError):
            resource.datagroup(name)

    def test_selector(self):
        selector = DatagroupSelector(session=self.session)
        resource = DataResource(self.session)
        datagroups = resource.datagroups
        self.assertEqual(set(selector.meta.names), set(datagroups.meta.names))

    def test_meta_from_real_db(self):
        session = Session(config.host, config.user, config.password)
        meta = DatagroupsMeta(session)
        self.assertTrue(hasattr(meta, 'names'))
        self.assertTrue(meta.names)
        session.close()

    def test_meta(self):
        meta = DatagroupsMeta(self.session)
        self.assertTrue(hasattr(meta, 'names'))
        self.assertEqual(set(meta.names), set(sample_datagroups))

    def tearDown(self):
        self.session.close()
