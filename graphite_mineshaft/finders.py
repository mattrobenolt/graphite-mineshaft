from mineshaft import Mineshaft

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet  # noqa
    from graphite.node import LeafNode, BranchNode  # noqa


class MineshaftLeafNode(LeafNode):
    __fetch_multi__ = 'mineshaft'


class MineshaftBranchNode(BranchNode):
    pass


class MineshaftFinder(object):
    __fetch_multi__ = 'mineshaft'
    __slots__ = ('driver',)

    def __init__(self, config=None):
        self.driver = Mineshaft(config.get('mineshaft', {}).get('url'))

    def find_nodes(self, query):
        nodes = self.driver.resolve(query.pattern)
        for node in nodes:
            if node.leaf:
                yield MineshaftLeafNode(node.key, MineshaftReader(node.key, self))
            else:
                yield MineshaftBranchNode(node.key)

    def fetch_multi(self, nodes, start_time, end_time):
        step = end_time - start_time
        series = {}
        paths = [node.path for node in nodes]
        data = self.driver.metrics(paths, start_time, end_time)
        if data:
            # Collect all the series into their own dict
            series = {key: value['series'] for key, value in data.iteritems()}

            # Extract the time info out of the first key
            src = data.keys()[0]
            start_time = data[src]['from']
            end_time = data[src]['to']
            step = data[src]['step']

        return (start_time, end_time, step), series


class MineshaftReader(object):
    __slots__ = ('path', 'finder')

    def __init__(self, path, finder):
        self.path = path
        self.finder = finder

    def fetch(self, start_time, end_time):
        data = self.finder.driver.metrics(self.path, start_time, end_time).get(self.path, {})
        if not data:
            return (start_time, end_time, end_time - start_time), []
        return (data['from'], data['to'], data['step']), data['series']

    def get_intervals(self):
        return None
