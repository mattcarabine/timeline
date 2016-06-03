import re

COUCHBASE_LOG_UUID_REGEX = re.compile(r'{uuid,[\n\r\s]+(?:.*[\n\r\s]+)+?'
                                      '<<"([\w]+)">>\]}')


class CBLogParser(object):
    def __init__(self, log_file, timeline):
        m = COUCHBASE_LOG_UUID_REGEX.search(log_file.read())
        if m:
            timeline.cluster_uuid = m.group(1)
