import heapq
import json
from glob import glob
from argparse import ArgumentParser
from os.path import join as path_join
from itertools import imap
from mrdomino.util import read_files, get_step, logger


def parse_args():
    ap = ArgumentParser()
    ap.add_argument('--work_dir', type=str, required=True,
                    help='directory containing files to shuffle')
    ap.add_argument('--job_module', type=str, required=True)
    ap.add_argument('--job_class', type=str, required=True)
    ap.add_argument('--step_idx', type=int, required=True)
    ap.add_argument('--input_prefix', type=str, default='map.out',
                    help='string that input files are prefixed with')
    ap.add_argument('--output_prefix', type=str, default='reduce.in',
                    help='string to prefix output files')
    args = ap.parse_args()
    return args


def main():

    args = parse_args()

    # count exactly how many input lines we have so we can balance work.
    glob_pattern = path_join(args.work_dir,
                             args.input_prefix + '_count.[0-9]*')
    count_ff = glob(glob_pattern)
    if not count_ff:
        raise RuntimeError("Step {} shuffler: not input files found matching "
                           "pattern {}".format(args.step_idx, glob_pattern))
    logger.info("Step {} shuffler: counting entries from {}"
                .format(args.step_idx, count_ff))
    num_entries = sum(imap(int, read_files(count_ff)))

    in_ff = sorted(glob(path_join(args.work_dir,
                                  args.input_prefix + '.[0-9]*')))
    sources = [open(f, 'r') for f in in_ff]

    step = get_step(args)
    n_output_files = step.n_reducers

    out_format = path_join(args.work_dir, args.output_prefix + '.%d')
    outputs = [open(out_format % i, 'w') for i in range(n_output_files)]

    # To cleanly separate reducer outputs by key groups we need to unpack
    # values on shuffling and compare keys. Every index change has to be
    # accompanied by a key change, otherwise index change is postponed.
    old_key = None
    old_index = 0
    lines_written = 0
    for count, line in enumerate(heapq.merge(*sources)):
        key = json.loads(line)[0]
        index = count * n_output_files / num_entries

        # postpone switching to new index until a change in key also observed
        if old_index != index and old_key != key:
            old_index = index
        outputs[old_index].write(line)
        lines_written += 1

        old_key = key

    for source in sources:
        source.close()

    for output in outputs:
        output.close()

    logger.info("Step {} shuffler: lines written: {}"
                .format(args.step_idx, lines_written))

if __name__ == "__main__":
    main()
