# coding=utf-8

"""
Counts the queues for each postfix instance, and reports them
"""

import diamond.collector
import os
import subprocess
import time


class PostfixQueueSizeCollector(diamond.collector.Collector):

    PATH_DEFAULT = "/nail"
    QUEUE_NAMES_DEFAULT = ""
    MSG_DIRS = ["deferred", "active", "incoming"]

    def get_default_config_help(sef):
        config_help = super(PostfixQueueSizeCollector, self).get_default_config_help()
        config_help.update({
            'dir_path': 'Postfix directory path',
            'queue_names': 'Space delimited name of queues'+
                'for which length has to be collected',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(PostfixQueueSizeCollector, self).get_default_config()
        config.update({
            'path': 'postfix',
            'dir_path': self.PATH_DEFAULT,
            'queue_names': QUEUE_NAMES_DEFAULT,
        })
        return config

    def collect(self):
        """
        Collect the length of the postfix queues
        """

        def get_queue_length(queue_name):
            length = 0
            for dirs in self.MSG_DIRS:
                # Sum file counts in MSG_DIRS.
                # path: postfix-(queue_name)/MSG_DIR/
            return length

        try:
            # Collect and Publish Metrics
            self.log.debug("Postfix dir path %s and queue names: %s",
                    self.config['base_path'], self.config['queue_names'])
            
            queues_names = self.config['queue_names'].split()
            if len(queue_names) == 0:
                return

            for queue_name in queue_names:
                metric_name = 'postfix_{0}_queue_size'.format(queue_name)
                self.publish(metric_name, get_queue_length(queue_name))
        except Exception, e:
            self.log.error("PostfixQueueSizeCollector Error: %s", e)
