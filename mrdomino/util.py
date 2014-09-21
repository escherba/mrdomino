import collections
import json
import time
import gzip
import re
import imp
import operator
import logging
import sys
import functools


NestedCounter = functools.partial(collections.defaultdict, collections.Counter)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class protocol(object):
    JSONProtocol = 0
    JSONValueProtocol = 1
    PickleProtocol = 2       # unsupported
    PickleValueProtocol = 3  # unsupported
    RawProtocol = 4          # unsupported
    RawValueProtocol = 5     # unsupported
    ReprProtocol = 6         # unsupported
    ReprValueProtocol = 7    # unsupported


def get_instance(args):
    job_module = imp.load_source('job_module', args.job_module)
    job_class = getattr(job_module, args.job_class)
    return job_class()


def get_step(args):
    return get_instance(args).steps()[args.step_idx]


class MRCounter(collections.Iterable):
    """Two-story counter
    """
    def __init__(self):
        self.counter = NestedCounter()

    def __iter__(self):
        return self.counter.__iter__()

    def iteritems(self):
        return self.counter.iteritems()

    def incr(self, key, sub_key, incr):
        self.counter[key][sub_key] += incr

    def __iadd__(self, counter2):
        """Add another counter (allows += operator)

        >>> c = MRCounter()
        >>> c.incr("a", "b", 2)
        >>> c.incr("b", "c", 3)
        >>> d = MRCounter()
        >>> d.incr("c", "b", 2)
        >>> d.incr("b", "c", 30)
        >>> d += c
        >>> d.counter['b']['c']
        33

        """
        counter = self.counter
        for key, val in counter2.iteritems():
            counter[key].update(val)
        return self

    def show(self):
        """Display counter content in a human-friendly way"""
        output = []
        for key, sub_dict in sorted(self.iteritems()):
            output.append('  %s:' % key)
            for sub_key, count in sorted(sub_dict.iteritems()):
                output.append('    %s: %d' % (sub_key, count))
        return '\n'.join(output)

    def serialize(self):
        """
        >>> c = MRCounter()
        >>> c.incr("a", "b", 2)
        >>> c.incr("b", "c", 3)
        >>> type(c.serialize())
        <type 'str'>

        """
        arr = []
        for key, sub_dict in sorted(self.iteritems()):
            for sub_key, count in sorted(sub_dict.iteritems()):
                arr.append({
                    'key': key,
                    'sub_key': sub_key,
                    'count': count,
                })
        return json.dumps({
            'counters': arr,
        })

    @classmethod
    def deserialize(cls, string):
        """

        >>> c = MRCounter()
        >>> c.incr("a", "b", 2)
        >>> c.incr("b", "c", 3)
        >>> d = MRCounter.deserialize(c.serialize())
        >>> c.counter == d.counter
        True

        """
        counter = MRCounter()
        j = json.loads(string)
        for obj in j['counters']:
            key = obj['key']
            sub_key = obj['sub_key']
            count = obj['count']
            counter.incr(key, sub_key, count)
        return counter

    @classmethod
    def sum(cls, iterable):
        """Sum a series of instances of cls

        >>> c = MRCounter()
        >>> c.incr("a", "b", 2)
        >>> c.incr("b", "c", 3)
        >>> d = MRCounter()
        >>> d.incr("c", "b", 2)
        >>> d.incr("b", "c", 30)
        >>> e = MRCounter.sum([c, d])
        >>> e.counter['b']['c']
        33

        """
        return reduce(operator.__iadd__, iterable, cls())


class MRTimer(object):
    """Context class for benchmarking"""
    def __enter__(self):
        self.clock_start = time.clock()
        self.wall_start = time.time()
        return self

    def __exit__(self, *args):
        clock_end = time.clock()
        wall_end = time.time()
        self.clock_interval = clock_end - self.clock_start
        self.wall_interval = wall_end - self.wall_start

    def __str__(self):
        return "clock: %0.03f sec, wall: %0.03f sec." \
            % (self.clock_interval, self.wall_interval)


def create_cmd(parts):
    """Join together a command line represented as list"""
    sane_parts = []
    for part in parts:
        if not isinstance(part, str):
            # note: python subprocess module raises a TypeError instead
            # of converting everything to string
            part = str(part)
        sane_parts.append(part)

    return ' '.join(sane_parts)


def read_files(filenames):
    """Returns an iterator over files in a list of files"""
    for filename in filenames:
        with open(filename, 'r') as filehandle:
            yield filehandle.read()


def read_lines(filenames):
    """Returns an iterator over lines in a list of files"""
    for filename in filenames:
        with open(filename, 'r') as filehandle:
            for line in filehandle:
                yield line


def open_input(filename, mode='r'):
    """Transparently open input files, whether plain text or gzip"""
    if re.match(r'.*\.gz$', filename) is None:
        # no .gz extension, assume a plain text file
        return open(filename, mode)
    else:
        # got a gzipped file
        return gzip.open(filename, mode + 'b')


if __name__ == "__main__":
    import doctest
    doctest.testmod()
