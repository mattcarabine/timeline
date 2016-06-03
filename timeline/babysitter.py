import re

from basiclogsearcher import BasicLogSearcher
from utils import Event


class BabysitterParser(BasicLogSearcher):
    def __init__(self, log_file, timeline):
        """ Super into the parent class passing our file and ourselves. """
        super(BabysitterParser, self).__init__(log_file, timeline)

    @BasicLogSearcher.register_search(
        'Cushion managed supervisor for memcached failed:', multi_line=4)
    def _parse_assertions(self, assertions):
        extract_assert_regex = re.compile(r'(assertion|asssertion|Assert) '
                                          'failed \[(?P<assert>[^\[]+)\] at')
        for assertion in assertions:
            m = extract_assert_regex.search(assertion[4])
            if m:
                self.timeline.add_event(Event(
                    assertion[0], 'assert', 'Memcached assertion: `{}`'
                    .format(m.group('assert')),
                    self.timeline.default_node_name))
