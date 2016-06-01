from basiclogsearcher import BasicLogSearcher

from utils import Event


class CouchDBParser(BasicLogSearcher):
    def __init__(self, log_file, timeline):
        """ Super into the parent class passing our file and ourselves. """
        super(CouchDBParser, self).__init__(log_file, timeline)

    @BasicLogSearcher.register_search('Too many file descriptors open')
    def _parse_emfile(self, messages):
        self.timeline.add_events(
            [Event(message, 'failure', 'store hit file descriptor limit',
                   self.timeline.default_node_name) for message in messages])
