
import re

from basiclogsearcher import BasicLogSearcher
from utils import Event


class InfoParser(BasicLogSearcher):
    def __init__(self, log_file, timeline):
        """ Super into the parent class passing our file and ourselves. """
        super(InfoParser, self).__init__(log_file, timeline)

    @BasicLogSearcher.register_search('Doing local bucket flush')
    def _parse_flush_start(self, flushes):
        pat_bucket_flush_start = re.compile(
            r'janitor_agent-(.*)<.*Doing local bucket flush')
        for flush in flushes:
            m = pat_bucket_flush_start.search(flush)
            if m:
                bucket = m.group(1)
                self.timeline.add_event(Event(
                    flush, 'flush',
                    'starting to flush bucket `{}`'.format(bucket),
                    self.timeline.default_node_name
                ))

    @BasicLogSearcher.register_search('Local flush is done')
    def _parse_flush_start(self, flushes):
        pat_bucket_flush_end = re.compile(
            r'janitor_agent-(.*)<.*Local flush is done')
        for flush in flushes:
            m = pat_bucket_flush_end.search(flush)
            if m:
                bucket = m.group(1)
                self.timeline.add_event(Event(
                    flush, 'flush',
                    'flush complete for bucket `{}`'.format(bucket),
                    self.timeline.default_node_name
                ))

