import sys
import json
from os.path import join as path_join
from argparse import ArgumentParser, FileType
from mrdomino.util import logger, get_instance, open_gz


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--job_module', type=str, required=True)
    parser.add_argument('--job_class', type=str, required=True)
    parser.add_argument('--step_idx', type=int, required=True)
    parser.add_argument('--input', type=FileType('r'), default=sys.stdin,
                        help='string that input files are prefixed with')
    parser.add_argument('--work_dir', type=str, required=True,
                        help='directory containing map output files')
    parser.add_argument('--output_prefix', type=str, default='map.out.gz',
                        help='string to prefix output files')
    parser.add_argument('--shard', type=int, required=True,
                        help='which shart are we at')

    namespace = parser.parse_args()
    return namespace


def run(args):

    # find the combine function.
    job = get_instance(args)
    step = job.get_step(args.step_idx)
    combine_func = step.combiner

    shard = args.shard
    in_fh = args.input
    out_fn = path_join(args.work_dir, args.output_prefix % str(shard))
    logger.info("combiner {}: output -> {}".format(shard, out_fn))

    last_key = None
    values = []

    count_written = 0
    count_seen = 0
    with open_gz(out_fn, 'w') as out_fh:
        for line in in_fh:
            count_seen += 1
            key, value = json.loads(line)
            if key == last_key:
                # extend previous run
                values.append(value)
            else:
                # end previous run
                if values:
                    for kv in combine_func(last_key, values):
                        count_written += 1
                        out_fh.write(json.dumps(kv) + '\n')

                # start new run
                last_key = key
                values = [value]
        # dump any remaining values
        if values:
            for kv in combine_func(last_key, values):
                count_written += 1
                out_fh.write(json.dumps(kv) + '\n')

    counters = job._counters
    counters.incr("combiner", "seen", count_seen)
    counters.incr("combiner", "written", count_written)

    # write the counters to file.
    fname = path_join(args.work_dir, 'combine-%d.counters' % shard)
    logger.info("combiner {}: counters -> {}".format(shard, fname))
    with open(fname, 'w') as fhandle:
        fhandle.write(counters.serialize())

    # write how many entries were written for reducer balancing purposes.
    fname = path_join(args.work_dir, (args.output_prefix % str(shard)) + '.count')
    logger.info("combiner {}: lines written -> {}".format(shard, fname))
    with open(fname, 'w') as fhandle:
        fhandle.write(str(count_written))


if __name__ == '__main__':
    run(parse_args())
