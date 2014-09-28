import re
import glob
import sys
from mrdomino import MRJob, MRStep, MRSettings, protocol as mr_protocol


def get_tld(domain):
    return re.match(r'^.*\b([^\.]+\.[^\.]+)$', domain).group(1)


class MRSummary(MRJob):

    INPUT_PROTOCOL = mr_protocol.JSONValueProtocol
    INTERNAL_PROTOCOL = mr_protocol.JSONProtocol
    OUTPUT_PROTOCOL = mr_protocol.JSONProtocol

    def map1(self, _, j):
        key = j[u'object'][u'user_id']
        _, domain = key.split("@")
        tld = get_tld(domain)
        self.increment_counter("TLD map1", tld, 1)
        yield key, 1

    def combine1(self, key, vals):
        total = sum(vals)
        yield key, total

    def reduce1(self, key, vals):
        total = sum(vals)
        _, domain = key.split("@")
        tld = get_tld(domain)
        self.increment_counter("TLD reduce1", tld, total)
        yield key, total    # username -> count of posts

    def combine2(self, key, vals):
        total = sum(vals)
        yield key, total

    def map2(self, key, val):
        _, domain = key.split("@")
        tld = get_tld(domain)
        self.increment_counter("TLD map2", tld, val)
        yield domain, val

    def reduce2(self, key, vals):
        total = sum(vals)
        tld = get_tld(key)
        self.increment_counter("TLD reduce2", tld, total)
        yield key, total

    def steps(self):
        return [
            MRStep(
                mapper=self.map1,
                combiner=self.combine1,
                reducer=self.reduce1,
            ),
            MRStep(
                mapper=self.map2,
                combiner=self.combine2,
                reducer=self.reduce2,
            )
        ]

    def settings(self):
        return MRSettings(
            input_files=glob.glob(sys.argv[1]),
            output_dir='out',
            tmp_dir='tmp',
            use_domino=True,
            n_concurrent_machines=2,
            n_shards_per_machine=3,
            step_config={
                0: dict(n_mappers=6, n_reducers=3),
                1: dict(n_mappers=4, n_reducers=2)
            }
        )


if __name__ == '__main__':
    MRSummary.run()
