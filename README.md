mrdomino
========

Implementation of mapreduce to run on http://dominoup.com.

Example usage (see complete example at examples/example.py):

```python
from mrdomino import MRJob, MRStep, MRSettings

class MyMapReduceJob(MRJob):

    def map1(self, _, line):
        """ defines mapper for the 1st step """
        yield key, val

    def combine1(self, key, vals):
        """ defines combiner for the 1st step """
        yield key, val

    def reduce1(self, key, vals):
        """ defines reducer for the 1st step """
        yield key, val

    def map2(self, key, val):
        """ defines mapper for the 2nd step """
        yield key, val

    def reduce2(self, key, vals):
        """ defines reducer for the 2nd step """
        yield key, val

    def steps(self):
        return [
            MRStep(
                mapper=map1,
                combiner=combine1,
                reducer=reduce1
            ),
            MRStep(
                mapper=map2,
                reducer=reduce2
            )
        ]

    def settings(self):
        return MRSettings(
            input_files=glob.glob('data/*.gz'),
            output_dir='out',
            tmp_dir='tmp',
            use_domino=False,
            n_concurrent_machines=3,
            n_shards_per_machine=3,
            step_config={
                0: dict(n_mappers=6, n_reducers=3),
                1: dict(n_mappers=4, n_reducers=2)
            }
        )


MyMapReduceJob.run()
```
