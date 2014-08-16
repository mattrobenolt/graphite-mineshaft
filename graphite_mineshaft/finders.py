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
                yield BranchNode(node.key)

    def fetch_multi(self, nodes, start_time, end_time):
        targets = [node.path for node in nodes]
        data = self.driver.metrics(targets, start_time, end_time)



class MineshaftReader(object):
    __slots__ = ('path', 'finder')

    def __init__(self, path, finder):
        self.path = path
        self.finder = finder

    def fetch(self, start_time, end_time):
        print self.path, start_time, end_time
        data = self.finder.driver.metrics(self.path, start_time, end_time).get(self.path, {})
        if not data:
            return (start_time, end_time, end_time - start_time), []
        return (data['from'], data['to'], data['step']), data['series']

    def get_intervals(self):
        return None
