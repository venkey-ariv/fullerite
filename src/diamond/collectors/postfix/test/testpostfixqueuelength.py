#!/usr/bin/python
# coding=utf-8
################################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from mock import Mock
from mock import patch

from diamond.collector import Collector
from postfix import PostfixQueueSizeCollector

################################################################################


class TestPostfixQueueSizeCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('PostfixQueueSizeCollector', {})

        self.collector = PostfixQueueSizeCollector(config, None)

    def test_import(self):
        self.assertTrue(PostfixQueueSizeCollector)

    @patch.object(Collector, 'publish')
    def test_should_work_with_synthetic_data(self, publish_mock):

        # Overide config to point to fixture dir.
        self.config.update({
            'path': os.path.dirname(__file__) + '/fixtures/',
            'queue_names': 'a b',
        })

        self.collector.collect()

        metrics = {
                "postfix_a_queue_size": 2,
                "postfix_b_queue_size": 2,
        }

        self.setDocExample(collector=self.collector.__class__.__name__,
                           metrics=metrics,
                           defaultpath=self.collector.config['path'])
        self.assertPublishedMany(publish_mock, metrics)

    @patch.object(Collector, 'publish')
    def test_should_fail_gracefully(self, publish_mock):

        # Overide config to point to fixture dir.
        self.config.update({
            'path': os.path.dirname(__file__) + '/fixtures/',
            'queue_names': 'invalid_queue_name',
        })

        self.collector.collect()
        self.assertPublishedMany(publish_mock, {})


################################################################################
if __name__ == "__main__":
    unittest.main()
