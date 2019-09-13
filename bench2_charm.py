from charm4py import charm, Chare, Group, Future, Reducer
from collections import defaultdict
import numpy as np
import time

num_trials = 5


# this is not strictly necessary but makes it more scalable
def myreducer(contribs):
    result = set()
    for c in contribs:
        result |= c
    return result

Reducer.addReducer(myreducer)


class StreamingPrefixCount(Chare):

    def reset(self):
        self.prefix_count = defaultdict(int)
        self.popular_prefixes = set()

    def add_document(self, document):
        for word in document:
            for i in range(1, len(word)):
                prefix = word[:i]
                self.prefix_count[prefix] += 1
                if self.prefix_count[prefix] > 3:
                    self.popular_prefixes.add(prefix)

    def get_popular(self, f):
        # a reduction is not strictly necessary but is more scalable
        self.reduce(f, self.popular_prefixes, Reducer.myreducer)


def main(args):
    t0 = time.time()
    durations2 = []
    num_cpus = charm.numPes()
    # same seed for fair comparison
    np.random.seed(seed=1234)
    streaming_actors = Group(StreamingPrefixCount)
    for _ in range(num_trials):
        streaming_actors.reset(awaitable=True).get()
        start_time = time.time()

        for i in range(num_cpus * 10):
            document = [np.random.bytes(20) for _ in range(10000)]
            #document = [np.random.bytes(5) for _ in range(1000)]   # smaller task size
            streaming_actors[i % num_cpus].add_document(document)

        # wait for quiescence
        charm.waitQD()
        # get the aggregates results
        results = Future()
        streaming_actors.get_popular(results)
        popular_prefixes = results.get()

        duration2 = time.time() - start_time
        durations2.append(duration2)
        print('Stateful computation workload took {} seconds.'.format(duration2))
    print('Total time=', time.time() - t0)
    exit()


charm.start(main)
