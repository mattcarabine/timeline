import re


class BasicLogSearcher(object):
    """Simple base class for searching for interesting strings in a text file.
    Child class must implement search_file and pass itself as reference to init
    """

    def __init__(self, log_file, timeline):
        self.log_file = log_file
        self.timeline = timeline
        self.perform_searches()

    def perform_searches(self):
        searches = {}
        for name, method in self.__class__.__dict__.iteritems():
            if hasattr(method, "string"):
                searches[method] = []

        for line in self.log_file:
            for search in searches:
                if search.regex:
                    result = re.search(search.string, line)
                else:
                    result = search.string in line

                if result:
                    if search.multi_line:
                        search_result = [line]
                        for _ in xrange(search.multi_line):
                            search_result.append(next(self.log_file))
                    elif search.until is not None:
                        search_result = [line]
                        while not ((re.search(search.until, line))
                                   if search.regex else
                                   (search.until in line)):
                            line = next(self.log_file)
                            search_result.append(line)
                    else:
                        search_result = line

                    searches[search].append(search_result)

                    # Now that we have a result it's safe to assume that
                    # this line won't match other searches
                    break

        for search in searches:
            search(self, searches[search])

    @classmethod
    def register_search(cls, string, regex=False, multi_line=0, until=None):
        def decorator(func):
            if regex:
                func.string = re.compile(string)
            else:
                func.string = string
            if regex and until is not None:
                func.until = re.compile(until)
            else:
                func.until = until
            func.regex = regex
            func.multi_line = multi_line
            return func
        return decorator
