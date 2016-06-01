import re

from basiclogsearcher import BasicLogSearcher
from utils import Event, extract_nodename


class DiagParser(BasicLogSearcher):
    def __init__(self, log_file, timeline):
        """ Super into the parent class passing our file and ourselves. """
        super(DiagParser, self).__init__(log_file, timeline)

    @BasicLogSearcher.register_search('Started rebalancing bucket',)
    def _parse_rebalance_start(self, messages):
        self.timeline.add_events(
            [Event(message, 'rebalance', 'starting rebalance',
                   self.timeline.default_node_name)
             for message in messages])

    @BasicLogSearcher.register_search('Rebalance exited')
    def _parse_rebalance_exit(self, messages):
        self.timeline.add_events(
            [Event(message, 'rebalance', 'rebalance exited',
                   self.timeline.default_node_name)
             for message in messages])

    @BasicLogSearcher.register_search('Started node add transaction')
    def _parse_node_add(self, messages):
        pat_node_add = re.compile(r"adding node '(.*)' to nodes_wanted")
        for message in messages:
            m = pat_node_add.search(message)
            if m:
                added_node = extract_nodename(m.group(1),
                                              self.timeline.default_node_name)
            else:
                added_node = 'unknown_node'
            self.timeline.add_event(Event(message, 'topology',
                                          'added node {}'.format(added_node),
                                          self.timeline.default_node_name))

    @BasicLogSearcher.register_search('was automatically failovered')
    def _parse_auto_failover(self, messages):
        pat_failover = re.compile(r'Node (.*) was automatically failovered')
        pat_node_failover = re.compile(r'info:message\((.*)\) - Node')
        for message in messages:
            m = pat_failover.search(message)
            if m:
                action_node = extract_nodename(m.group(1),
                                               self.timeline.default_node_name)
            else:
                action_node = 'unknown_node'

            m = pat_node_failover.search(message)
            if m:
                on_node = extract_nodename(m.group(1),
                                           self.timeline.default_node_name)
            else:
                on_node = 'unknown_node'

            self.timeline.add_event(Event(
                message, 'topology',
                '{} was automatically failed over'.format(action_node),
                self.timeline.default_node_name, node_name=on_node))

    @BasicLogSearcher.register_search('Starting failing over')
    def _parse_starting_failover(self, messages):
        pat_failover_start = re.compile(r"Starting failing over '(.*)'")
        for message in messages:
            m = pat_failover_start.search(message)
            if m:
                action_node = extract_nodename(m.group(1),
                                               self.timeline.default_node_name)
            else:
                action_node = 'unknown_node'

            self.timeline.add_event(Event(
                message, 'topology',
                "starting failing over '{}'".format(action_node),
                self.timeline.default_node_name))

    @BasicLogSearcher.register_search('- Failed over')
    def _parse_failover_complete(self, messages):
        pat_manual_failover = re.compile(r'Failed over \'(.*)\': ok')
        pat_node_manual_failover = re.compile(r'info:message\((.*)\) - Failed')
        for message in messages:
            m = pat_manual_failover.search(message)
            if m:
                action_node = extract_nodename(m.group(1),
                                               self.timeline.default_node_name)
            else:
                action_node = 'unknown_node'

            m = pat_node_manual_failover.search(message)
            if m:
                on_node = extract_nodename(m.group(1),
                                           self.timeline.default_node_name)
            else:
                on_node = 'unknown_node'

            self.timeline.add_event(Event(
                message, 'topology',
                '{} was failed over'.format(action_node),
                self.timeline.default_node_name, node_name=on_node))

    @BasicLogSearcher.register_search('exited with status')
    def _parse_process_exit(self, messages):
        pat_serv_exit = re.compile(
            r'Port server .* on node .* exited with status [0-9]+\. Restarting')
        pat_port_serv = re.compile(r'Port server (.*) on node')
        pat_exit_status = re.compile(r'exited with status ([0-9]+)')
        for message in messages:
            m = pat_serv_exit.search(message)
            if m:
                m = pat_port_serv.search(message)
                if m:
                    exit_server = m.group(1)
                else:
                    exit_server = 'unknown_port_server'

                m = pat_exit_status.search(message)
                if m:
                    exit_status = m.group(1)
                else:
                    exit_status = 'unknown_exit_status'

                descr = '{} exited with status {} and restarted'.format(
                    exit_server, exit_status)
                self.timeline.add_event(Event(message, 'crash', descr,
                                              self.timeline.default_node_name))

    @BasicLogSearcher.register_search('Usage of disk')
    def _parse_disk_space(self, messages):
        pat_disk_usage = re.compile(
            r'Usage of disk "(.*)" on node "(.*)" is around ([0-9]+%)')
        for message in messages:
            m = pat_disk_usage.search(message)
            if m:
                disk = m.group(1)
                on_node = m.group(2)
                perc = m.group(3)
                descr = '{} usage on {} is {}'.format(disk, on_node, perc)
                self.timeline.add_event(Event(message, 'fail', descr,
                                              self.timeline.default_node_name))

    @BasicLogSearcher.register_search('saw that node')
    def _parse_saw_node(self, messages):
        pat_saw_node = re.compile(
            r"Node '(.*)' saw that node '(.*)' (went down|came up)")
        for message in messages:
            m = pat_saw_node.search(message)
            if m:
                on_node = extract_nodename(m.group(1),
                                           self.timeline.default_node_name)
                action_node = extract_nodename(m.group(2),
                                               self.timeline.default_node_name)
                action = m.group(3)
                descr = '{} {}'.format(action_node, action)
                self.timeline.add_event(Event(
                    message, 'fail', descr, self.timeline.default_node_name,
                    node_name=on_node))

    @BasicLogSearcher.register_search('Data has been lost for')
    def _parse_data_loss(self, messages):
        pat_data_lost = re.compile(
            r'Data has been lost for ([0-9]%) of vbuckets in bucket "(.*)"\.')
        for message in messages:
            m = pat_data_lost.search(message)
            if m:
                perc = m.group(1)
                bucket = m.group(2)
                descr = 'bucket {} has lost data for {} of vbuckets'.format(
                    bucket, perc)
                self.timeline.add_event(Event(message, 'fail', descr,
                                              self.timeline.default_node_name))

    @BasicLogSearcher.register_search('Write Commit Failure')
    def _parse_write_failure(self, messages):
        pat_write_failure = re.compile(
            r'Write Commit Failure. Disk write failed for item in Bucket '
            '"(.*)" on node (.*)\.')
        for message in messages:
            m = pat_write_failure.search(message)
            if m:
                bucket = m.group(1)
                on_node = m.group(2)
                descr = 'write commit failure for bucket {} on {}'.format(
                    bucket, on_node)
                self.timeline.add_event(Event(message, 'fail', descr,
                                              self.timeline.default_node_name,
                                              node_name=on_node))

    @BasicLogSearcher.register_search(
        "Haven't heard from a higher priority node or a master, so I'm taking "
        "over")
    def _parse_takeovers(self, messages):
        for message in messages:
            self.timeline.add_event(Event(message, 'master', 'became master',
                                          self.timeline.default_node_name))

    @BasicLogSearcher.register_search(
        "Current master is older and I'll try to takeover")
    def _parse_old_masters(self, messages):
        for message in messages:
            self.timeline.add_event(Event(message, 'master',
                                          'trying takeover from older master',
                                          self.timeline.default_node_name))

    @BasicLogSearcher.register_search('loaded on node')
    def _parse_bucket_loads(self, messages):
        pat_bucket_load = re.compile(
            r'Bucket "(.*)" loaded on node \'(.*)\' in ([0-9]+ seconds)\.')
        for message in messages:
            m = pat_bucket_load.search(message)
            if m:
                bucket = m.group(1)
                on_node = extract_nodename(m.group(2),
                                           self.timeline.default_node_name)
                t = m.group(3)
                descr = 'bucket {} loaded on {} in {}'.format(bucket, on_node,
                                                              t)
                self.timeline.add_event(Event(message, 'bucket', descr,
                                              self.timeline.default_node_name))

