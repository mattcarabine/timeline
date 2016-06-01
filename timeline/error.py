
import re

from basiclogsearcher import BasicLogSearcher
from utils import Event, extract_nodename


class ErrorParser(BasicLogSearcher):
    def __init__(self, log_file, timeline):
        """ Super into the parent class passing our file and ourselves. """
        super(ErrorParser, self).__init__(log_file, timeline)

    @BasicLogSearcher.register_search(
        r"The following buckets became not ready on node",
        until=r'those of them are active')
    def _parse_buckets_not_ready(self, instances):
        pat_bucket_not_ready = re.compile(
            r"The following buckets became not ready on node "
            "'(.*)': \[(.*)\], .*\]$")
        pat_bucket_not_ready_mult = re.compile(
            r"The following buckets became not ready on node '"
            "(.*)': \[\"([^\"]*)\",$")
        pat_bucket_not_ready_mult_middle = re.compile(r' *"(.*)",$')
        pat_bucket_not_ready_mult_end = re.compile(r' *"(.*)"\]')

        for instance in instances:
            single = pat_bucket_not_ready.search(instance[0])
            multi = pat_bucket_not_ready_mult.search(instance[0])

            if single:
                on = extract_nodename(single.group(1),
                                      self.timeline.default_node_name)
                buckets = single.group(2).replace('"', '')
                self.timeline.add_event(Event(
                    instance[0], 'fail',
                    'buckets not ready on {}: `{}`'.format(on, buckets),
                    self.timeline.default_node_name))

            elif multi:
                on = extract_nodename(multi.group(1),
                                      self.timeline.default_node_name)
                buckets = multi.group(2).replace('"', '')

                for line in instance[1:]:
                    m = pat_bucket_not_ready_mult_end.search(line)
                    if m:
                        buckets += ', ' + m.group(1)
                        self.timeline.add_event(Event(
                            instance[0], 'fail',
                            'buckets not ready on {}: `{}`'.format(on,
                                                                   buckets),
                            self.timeline.default_node_name))
                        continue

                    m = pat_bucket_not_ready_mult_middle.search(line)
                    if m:
                        buckets += ', ' + m.group(1)
                        continue
