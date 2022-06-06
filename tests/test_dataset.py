import unittest

from algoconnect import DataResource, Datagroup, Dataset
from algoconnect.resources import ColumnsMeta

from mock_session import MockSession, sample_datagroups, sample_datasets


class TestDataset(unittest.TestCase):

    def setUp(self):
        self.session = MockSession()

    def test_selector_from_resource(self):
        resource = DataResource(self.session)
        dgr_name = sample_datagroups[0]
        datagroup = resource.datagroup(dgr_name)
        selector = datagroup.datasets
        self.assertEqual(set(selector.meta.names), set(sample_datasets[dgr_name]))
        for name in sample_datasets[dgr_name]:
            self.assertTrue(hasattr(selector, name))

    def test_dataset_from_datagroup_resource(self):
        resource = DataResource(self.session)
        dgr_name = sample_datagroups[0]
        dts_name = sample_datasets[dgr_name][0]
        datagroup = resource.datagroup(dgr_name)
        dataset = Dataset(datagroup, dts_name, session=self.session)
        self.assertEqual(dataset.name, dts_name)
        self.assertEqual(dataset.datagroup, datagroup)

    def test_dataset_from_datagroup_instance(self):
        dgr_name = sample_datagroups[0]
        dts_name = sample_datasets[dgr_name][0]
        datagroup = Datagroup(dgr_name, session=self.session)
        dataset = Dataset(datagroup, dts_name, session=self.session)
        self.assertEqual(dataset.name, dts_name)
        self.assertEqual(dataset.datagroup, datagroup)

    def test_dataset_from_str_datagroup(self):
        dgr_name = sample_datagroups[0]
        dts_name = sample_datasets[dgr_name][0]
        dataset = Dataset(dgr_name, dts_name, session=self.session)
        self.assertEqual(dataset.name, dts_name)
        self.assertEqual(dataset.datagroup.name, dgr_name)

    def test_dataset_from_str(self):
        dgr_name = sample_datagroups[0]
        dts_name = sample_datasets[dgr_name][0]
        dataset = Dataset(dgr_name + '.' + dts_name, session=self.session)
        self.assertEqual(dataset.name, dts_name)
        self.assertEqual(dataset.datagroup.name, dgr_name)

    def test_invalid_datagroup_name(self):
        dgr_name = sample_datagroups[0]
        dts_name = sample_datasets[dgr_name][0]
        dgr_name = 'some-nonexisting-name'
        with self.assertRaises(ValueError):
            Dataset(dgr_name + '.' + dts_name, session=self.session)

    def test_invalid_dataset_name(self):
        dgr_name = sample_datagroups[0]
        dts_name = 'some-nonexisting-name'
        with self.assertRaises(ValueError):
            Dataset(dgr_name + '.' + dts_name, session=self.session)

    def test_columns_meta(self):
        dgr_name = sample_datagroups[-1]
        dts_name = sample_datasets[dgr_name][-1]
        meta = ColumnsMeta(dgr_name, dts_name, self.session)
        self.assertIs(meta.session, self.session)
        self.assertTrue(hasattr(meta, 'names'))
        self.assertTrue(hasattr(meta, 'types'))
        self.assertTrue(hasattr(meta, 'descriptions'))
        self.assertIsInstance(meta.names, tuple)
        self.assertIsInstance(meta.types, tuple)
        self.assertIsInstance(meta.descriptions, tuple)

    def test_columns_attr(self):
        dgr_name = sample_datagroups[-1]
        dts_name = sample_datasets[dgr_name][-1]
        dataset = Dataset(dgr_name + '.' + dts_name, session=self.session)
        meta = ColumnsMeta(dgr_name, dts_name, self.session)
        for name in meta.names:
            self.assertTrue(hasattr(dataset, name))

    def tearDown(self):
        self.session.close()
