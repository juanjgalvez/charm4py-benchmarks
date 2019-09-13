from collections import defaultdict
import numpy as np
import psutil
import ray
import time

num_trials = 5

# Count the number of physical CPUs.
num_cpus = psutil.cpu_count(logical=False)
print('Using {} cores.'.format(num_cpus))

ray.init(num_cpus=num_cpus)


@ray.remote
class StreamingPrefixCount(object):

    def __init__(self):
        self.prefix_count = defaultdict(int)
        self.popular_prefixes = set()

    def add_document(self, document):
        for word in document:
            for i in range(1, len(word)):
                prefix = word[:i]
                self.prefix_count[prefix] += 1
                if self.prefix_count[prefix] > 3:
                    self.popular_prefixes.add(prefix)

    def get_popular(self):
        return self.popular_prefixes


t0 = time.time()
durations2 = []
# same seed for fair comparison
np.random.seed(seed=1234)
for _ in range(num_trials):
    streaming_actors = [StreamingPrefixCount.remote() for _ in range(num_cpus)]

    start_time = time.time()

    for i in range(num_cpus * 10):
        document = [np.random.bytes(20) for _ in range(10000)]
        #document = [np.random.bytes(5) for _ in range(1000)]   # smaller task size
        streaming_actors[i % num_cpus].add_document.remote(document)

    # Aggregate all of the results.
    results = ray.get([actor.get_popular.remote() for actor in streaming_actors])
    popular_prefixes = set()
    for prefixes in results:
        popular_prefixes |= prefixes

    duration2 = time.time() - start_time
    durations2.append(duration2)
    print('Stateful computation workload took {} seconds.'.format(duration2))

print('Total time=', time.time() - t0)
