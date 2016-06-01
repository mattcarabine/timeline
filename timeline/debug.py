import re

from basiclogsearcher import BasicLogSearcher
from utils import Event

pat_ms = re.compile(r'([0-9]+) milliseconds')


class DebugParser(BasicLogSearcher):
    def __init__(self, log_file, timeline):
        """ Super into the parent class passing our file and ourselves. """
        super(DebugParser, self).__init__(log_file, timeline)

    @BasicLogSearcher.register_search('Detected time forward jump')
    def _parse_time_jumps(self, time_jumps):
        for time_jump in time_jumps:
            m = pat_ms.search(time_jump)
            if m:
                action_time = m.group(1)
            else:
                action_time = '?'
            self.timeline.add_event(Event(
                time_jump, 'time_jump',
                'detected time jump / erlang latency of {}ms'
                .format(action_time), self.timeline.default_node_name))
