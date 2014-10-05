from argparse import ArgumentParser
import os
import re
import time
import json
import sys
import subprocess
from glob import glob
from itertools import imap
from mrdomino import map_one_machine, reduce_one_machine
from mrdomino.shuffle import run_shuffle, parse_args as shuffle_args
from mrdomino.util import MRCounter, read_files, read_lines, get_instance, \
    protocol, logger, format_cmd


DOMINO_EXEC = 'domino'

PREFIX_MAP_OUT = 'map-%s.out.gz'
PREFIX_REDUCE_IN = 'reduce-%s.in.gz'
PREFIX_REDUCE_OUT = 'reduce-%s.out.gz'


def parse_args(args=None):
    parser = ArgumentParser()
    parser.add_argument('--input_files', type=str, nargs='+', required=True,
                        help='list of input files to mappers')
    parser.add_argument('--output', type=str, default='-',
                        help='file path to write output to')
    parser.add_argument('--work_dir', type=str, required=True,
                        help='temporary working directory')
    parser.add_argument('--exec_script', type=str, required=True,
                        help='script to use for execution')
    parser.add_argument('--job_module', type=str, required=True)
    parser.add_argument('--job_class', type=str, required=True)
    parser.add_argument('--n_mappers', type=int, default=1,
                        help="number of mappers")
    parser.add_argument('--n_reducers', type=int, default=1,
                        help="number of reducers")
    parser.add_argument('--step_idx', type=int, required=True,
                        help='Index of this step (zero-base)')
    parser.add_argument('--total_steps', type=int, required=True,
                        help='total number of steps')
    parser.add_argument('--use_domino', type=int, default=0,
                        help='whether to run on Domino platform')
    parser.add_argument('--no_clean', type=int, default=0,
                        help='do not clean temporary files')
    parser.add_argument('--sync_domino', type=int, default=0,
                        help='call Domino CLI with no-sync option')
    parser.add_argument('--n_concurrent_machines', type=int, default=2,
                        help='maximum number of domino jobs to be running '
                        'at once')
    parser.add_argument('--n_shards_per_machine', type=int, default=4,
                        help='number of processes to spawn per domino job '
                        '(-1 for all)')
    parser.add_argument('--poll_done_interval_sec', type=int, default=45,
                        help='interval between successive checks that we '
                        'are done')
    namespace = parser.parse_args(args)
    return namespace


class ShardState(object):
    NOT_STARTED = 0
    IN_PROGRESS = 1
    DONE = 2


def combine_counters(work_dir, n_map_shards, n_reduce_shards):
    filenames = map(lambda (work_dir, shard):
                    os.path.join(work_dir, 'map-%d.counters' % shard),
                    zip([work_dir] * n_map_shards, range(n_map_shards)))
    filenames += map(lambda (work_dir, shard):
                     os.path.join(work_dir, 'combine-%d.counters' % shard),
                     zip([work_dir] * n_map_shards, range(n_map_shards)))
    filenames += map(lambda (work_dir, shard):
                     os.path.join(work_dir, 'reduce-%d.counters' % shard),
                     zip([work_dir] * n_reduce_shards, range(n_reduce_shards)))
    return MRCounter.sum(
        imap(MRCounter.deserialize,
             read_files(filter(os.path.exists, filenames))))


def update_shards_done(args, done_pattern, num_shards, shard2state):
    """go to disk and determine which shards are completed"""
    if args.use_domino:
        proc = subprocess.Popen([DOMINO_EXEC, 'download'])
        proc.communicate()
    for i in range(num_shards):
        filename = done_pattern % i
        if os.path.exists(filename):
            shard2state[i] = ShardState.DONE


def are_all_shards_done(shard2state):
    return list(set(shard2state.itervalues())) == [ShardState.DONE]


def get_shard_groups_to_start(
        n_concurrent_machines, n_shards_per_machine, shard2state):
    """get the list of shards to start now.  update state accordingly."""
    # get the state of each domino job (group of shards).
    shards = sorted(shard2state)
    machines = []
    for i in range(0, len(shards), n_shards_per_machine):
        machine_shards = shards[i:i + n_shards_per_machine]
        machine_status = min(map(lambda shard: shard2state[shard],
                                 machine_shards))
        machines.append(machine_status)

    # get how many domino jobs to start up.
    n_machines_in_progress = \
        len(filter(lambda m: m == ShardState.IN_PROGRESS, machines))
    n_todos = n_concurrent_machines - n_machines_in_progress

    # get up to n_todos domino jobs to start.
    start_me = []
    count = 0
    for i, machine in enumerate(machines):
        if machine == ShardState.NOT_STARTED:
            machine_shards = range(i * n_shards_per_machine,
                                   (i + 1) * n_shards_per_machine)
            machine_shards = filter(lambda n: n < len(shards), machine_shards)
            start_me.append(machine_shards)
            count += 1
            if count == n_todos:
                break

    return start_me


def show_shard_state(shard2state, n_shards_per_machine):
    shards = sorted(shard2state)
    output = ['Shard state:']
    for i in range(0, len(shards), n_shards_per_machine):
        machine_shards = shards[i:i + n_shards_per_machine]
        output.append('%s' % map(lambda i: shard2state[i], machine_shards))
    return ' '.join(output)


def schedule_machines(args, cmd, done_file_pattern, n_shards):

    def wrap_cmd(command):
        if args.use_domino:
            if args.sync_domino:
                prefix = [DOMINO_EXEC, 'run', args.exec_script]
            else:
                prefix = [DOMINO_EXEC, 'run', '--no-sync', args.exec_script]
        else:
            prefix = [args.exec_script]
        return prefix + command

    shard2state = dict(zip(
        range(n_shards),
        [ShardState.NOT_STARTED] * n_shards))

    if args.use_domino and not args.sync_domino:
        # upload everything before we start, since subtasks are run with
        # --no-sync
        proc = subprocess.Popen([DOMINO_EXEC, 'sync'])
        proc.communicate()

    while True:
        # go to disk and look for shard done files.
        update_shards_done(args, done_file_pattern, n_shards, shard2state)

        logger.info(show_shard_state(shard2state, args.n_shards_per_machine))

        if are_all_shards_done(shard2state):
            break

        # if we can start any more domino jobs (per n_concurrent_machines
        # restriction), get the ones to start.
        start_me = get_shard_groups_to_start(
            args.n_concurrent_machines, args.n_shards_per_machine, shard2state)

        # start the jobs.
        if start_me:
            logger.info('Starting shard groups: %s', start_me)

        procs = []
        for shards in start_me:
            # execute command.
            cmd_lst = wrap_cmd(cmd + ['--shards', ','.join(map(str, shards))])
            logger.info("Starting process: {}".format(' '.join(cmd_lst)))
            proc = subprocess.Popen(cmd_lst)
            procs.append(proc)

            # w/o terminating, will get '.dominoignore already locked' error
            if args.use_domino:
                proc.communicate()

            # note them as started.
            for shard in shards:
                shard2state[shard] = ShardState.IN_PROGRESS

        try:
            # wait to poll.
            time.sleep(args.poll_done_interval_sec)
        except KeyboardInterrupt:
            # User pressed Ctrl-C
            logger.warn("Keyboard interrupt received")
            for proc in procs:
                proc.terminate()
            sys.exit(1)


def run_step(args):

    logger.info('Starting step %d with options: %s', args.step_idx, args)
    logger.info('%d input files.', len(args.input_files))

    work_dir = args.work_dir
    logger.info('Working directory: %s', work_dir)

    job = get_instance(args)

    # perform mapping
    logger.info('Starting %d mappers.', args.n_mappers)
    schedule_machines(
        args,
        cmd=format_cmd([
            map_one_machine.__name__,
            '--step_idx', args.step_idx,
            '--input_files', args.input_files,
            '--output_prefix', PREFIX_MAP_OUT,
            '--job_module', args.job_module,
            '--job_class', args.job_class,
            '--work_dir', work_dir,
            '--n_mappers', args.n_mappers
        ]),
        done_file_pattern=os.path.join(work_dir, 'map-%d.done'),
        n_shards=args.n_mappers)

    if not args.no_clean:
        # clean up mapper inputs if step_idx > 0
        if args.step_idx > 0:
            logger.info('Deleting mapper inputs')
            for fname in args.input_files:
                logger.info('    Deleting %s', fname)
                os.unlink(fname)

    # shuffle mapper outputs to reducer inputs
    # check if shuffle-%d.done files are present
    done_pattern = os.path.join(args.work_dir, "shuffle-%d.done")
    shuffle_states = dict(zip(
        range(args.n_reducers),
        [ShardState.NOT_STARTED] * args.n_reducers))
    for i in range(args.n_reducers):
        name = done_pattern % i
        if os.path.exists(name):
            shuffle_states[i] = ShardState.DONE
    shuffle_states_done = list(set(shuffle_states.itervalues())) == \
        [ShardState.DONE]
    if not shuffle_states_done:
        logger.info("Shuffling...")
        run_shuffle(shuffle_args(format_cmd([
            '--step_idx', args.step_idx,
            '--input_prefix', PREFIX_MAP_OUT,
            '--output_prefix', PREFIX_REDUCE_IN,
            '--job_module', args.job_module,
            '--job_class', args.job_class,
            '--work_dir', work_dir,
            '--n_reducers', args.n_reducers
        ])))

    if not args.no_clean:
        # clean up mapper outputs
        logger.info('Deleting mapper outputs')
        mapper_out_glob = os.path.join(work_dir, PREFIX_MAP_OUT) + '.[0-9]*'
        for fname in glob(mapper_out_glob):
            logger.info('    Deleting %s', fname)
            os.unlink(fname)

    # perform reduction
    logger.info('Starting %d reducers.', args.n_reducers)
    schedule_machines(
        args,
        cmd=format_cmd([
            reduce_one_machine.__name__,
            '--step_idx', args.step_idx,
            '--input_prefix', PREFIX_REDUCE_IN,
            '--output_prefix', PREFIX_REDUCE_OUT,
            '--job_module', args.job_module,
            '--job_class', args.job_class,
            '--work_dir', work_dir,
            '--n_reducers', args.n_reducers
        ]),
        done_file_pattern=os.path.join(work_dir, 'reduce-%d.done'),
        n_shards=args.n_reducers)

    # collect counters
    counter = combine_counters(
        work_dir, args.n_mappers, args.n_reducers)
    logger.info('Step %d counters:\n%s', args.step_idx, counter.show())

    if not args.no_clean:
        # clean up reducer inputs
        logger.info('Deleting reducer inputs')
        reducer_in_glob = os.path.join(work_dir, PREFIX_REDUCE_IN) + '.[0-9]*'
        for fname in glob(reducer_in_glob):
            logger.info('    Deleting %s', fname)
            os.unlink(fname)

    if args.step_idx == args.total_steps - 1:

        logger.info('Joining reduce outputs')

        if job.INTERNAL_PROTOCOL == protocol.JSONProtocol and \
                job.OUTPUT_PROTOCOL == protocol.JSONValueProtocol:
            unpack_tuple = True
        elif job.INTERNAL_PROTOCOL == protocol.JSONValueProtocol and \
                job.OUTPUT_PROTOCOL == protocol.JSONProtocol:
            raise RuntimeError("if internal protocol is value-based, "
                               "output protocol must also be so")
        elif job.INTERNAL_PROTOCOL == protocol.JSONProtocol and \
                job.OUTPUT_PROTOCOL == protocol.JSONProtocol:
            unpack_tuple = False
        elif job.INTERNAL_PROTOCOL == protocol.JSONValueProtocol and \
                job.OUTPUT_PROTOCOL == protocol.JSONValueProtocol:
            unpack_tuple = False
        else:
            raise ValueError("unsupported output protocol: {}"
                             .format(job.OUTPUT_PROTOCOL))

        # make sure that files are sorted by shard number
        filenames = glob(os.path.join(work_dir, PREFIX_REDUCE_OUT % '[0-9]*'))
        prefix_match = re.compile('.*\\b' +
                                  (PREFIX_REDUCE_OUT % '(\\d+)') + '$')
        presorted = []
        for filename in filenames:
            match = prefix_match.match(filename)
            if match is not None:
                presorted.append((int(match.group(1)), filename))
        filenames = [filename[1] for filename in sorted(presorted)]

        def write_to_fhandle(fhandle):
            for key_value in read_lines(filenames):
                if unpack_tuple:
                    _, value = json.loads(key_value)
                    value = json.dumps(value) + "\n"
                else:
                    value = key_value
                fhandle.write(value)

        if args.output != '-':
            with open(args.output, 'w') as out_fh:
                write_to_fhandle(out_fh)
        else:
            write_to_fhandle(sys.stdout)

        if not args.no_clean:
            # clean up reducer outputs
            logger.info('Deleting reducer outputs')
            for fname in filenames:
                logger.info('    Deleting %s', fname)
                os.unlink(fname)

    # done.
    logger.info('Mapreduce step done.')


if __name__ == '__main__':
    run_step(parse_args())
