import re
from dateutil.parser import parse

pat_time = re.compile(r'(([0-9]{4}-[0-9]{2}-[0-9]{2}[T ]?[0-9][0-9]*:[0-9]{2}:'
                      '[0-9]{2}\.[0-9]{3})([-+][0-9]{2}:[0-9]{2})?)')
pat_nodename1 = re.compile(r'ns_1@([%!~\-_\*\.\w]+)')
pat_nodename2 = re.compile(r'([^\s]+@127.0.0.1)')


class Event(object):
    def __init__(self, line=None, event_type=None, description=None,
                 default_node_name=None, node_name=None, input_dict=None):
        if input_dict:
            self.timestamp = input_dict['timestamp']
            self.node_name = input_dict['node_name']
            self.type = input_dict['type']
            self.description = input_dict['description']
        else:
            self.timestamp = extract_time(line)
            # In the general case it is more convenient to
            # automatically parse the node name from the line
            # However in the case of diag, this does not work
            # all the time as it reports messages from multiple
            # nodes, not just the node the logs are for.
            if node_name:
                self.node_name = node_name
            else:
                self.node_name = extract_nodename(line, default_node_name)
            self.type = event_type
            self.description = description
        self.node_width = 20
        self.str_format = '{0:<26} {1:^{width}} {2}'

    def to_dict(self):
        return {'timestamp': self.timestamp.isoformat(),
                'node_name': self.node_name,
                'type': self.type,
                'description': self.description}

    def __cmp__(self, other):
        if isinstance(other, Event):
            if self.timestamp < other.timestamp:
                return -1
            elif self.timestamp == other.timestamp:
                return 0
            else:
                return 1
        else:
            raise TypeError

    def __str__(self):
        return self.str_format.format(self.timestamp.isoformat(),
                                      self.node_name, self.description,
                                      width=self.node_width)

    def __hash__(self):
        return (hash(self.timestamp) + hash(self.description) +
                hash(self.node_name))

    def __eq__(self, other):
        if isinstance(other, Event):
            return (self.timestamp == other.timestamp and
                    self.description == other.description)
        else:
            raise TypeError


def extract_time(line):
    match = pat_time.search(line)
    t = match.group(1)
    time = parse(t)
    return time


def extract_nodename(line, default_nodename):
    # remove any mention of the babysitter that could be picked up
    line = line.replace('babysitter_of_ns1@127.0.0.1', '')

    m = pat_nodename1.search(line)
    if m:
        n = m.group(1)
    else:
        # cb-YdeMJIMPznyUExnURcBY@127.0.0.1
        m = pat_nodename2.search(line)
        if m:
            return m.group(1)
        else:
            return default_nodename

    if n == '127.0.0.1':
        return default_nodename

    return n

