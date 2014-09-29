import os
import sys
import abc
import stat
import argparse
from tempfile import mkdtemp
from mrdomino.util import MRCounter, protocol, logger, format_cmd
from mrdomino.step import run_step, parse_args as step_args, PREFIX_REDUCE_OUT

__version__ = '0.1.0'


def parse_args(args=None, namespace=None, known=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=str, default='-',
                        help='file to write output to')
    parser.add_argument('--tmp_dir', type=str, default='tmp',
                        help='temporary working directory')
    parser.add_argument('--use_domino', action="store_true",
                        help='whether to run this on Domino')
    parser.add_argument('--n_concurrent_machines', type=int, default=2,
                        help='maximum number of domino jobs to be running '
                        'at one time')
    parser.add_argument('--n_shards_per_machine', type=int, default=4,
                        help='number of processes to spawn per domino job '
                        '(-1 for all)')
    parser.add_argument('--step_config', type=str, nargs='+', default=[],
                        help="comma-delimited string of tuples"
                        " where 1st integer is numer of mappers, 2nd number "
                        "of reducers")

    if known:
        namespace, input_files = parser.parse_known_args(
            args=args, namespace=namespace)
        return namespace, input_files
    else:
        namespace = parser.parse_args(args=args, namespace=namespace)
        return namespace


class MRStep(object):
    def __init__(self, mapper, reducer, combiner=None):

        # do some basic type checking to verify that we pass callables.
        assert mapper is not None and hasattr(mapper, '__call__')
        self.mapper = mapper
        assert reducer is not None and hasattr(reducer, '__call__')
        self.reducer = reducer
        assert combiner is None or hasattr(combiner, '__call__')
        self.combiner = combiner


def create_exec_script(tmp_dir):
    """
    Creates an exec script to allow execution of `python -m` lines with
    Domino

    The sole reason this script exists is because DominoUp CLI launcher does
    not accept interpreter name as its first parameter
    """
    exec_script = os.path.join(tmp_dir, 'exec.sh')
    with open(exec_script, 'w') as fhandle:
        fhandle.write("#!/bin/bash\nexec python -m $*\n")
    curr_stat = os.stat(exec_script)
    os.chmod(exec_script, curr_stat.st_mode | stat.S_IEXEC)
    return exec_script


def mapreduce(job_class):

    job = job_class()
    step_count = len(job._steps)

    # if temporary directory root does not exist, create one
    tmp_root = job._settings.tmp_dir
    if not os.path.exists(tmp_root):
        os.makedirs(tmp_root)

    exec_script = create_exec_script(tmp_root)
    tmp_dirs = [mkdtemp(dir=tmp_root, prefix="step%d." % i)
                for i in range(step_count)]

    input_file_lists = [job._input_files]
    for i, (step, out_dir) in enumerate(zip(job._steps, tmp_dirs)):
        step_config = map(int, job._settings.step_config[i].split(':'))
        n_reducers = step_config[1]
        reduce_format = os.path.join(out_dir, PREFIX_REDUCE_OUT + '.%d')
        input_file_lists.append([reduce_format % n for n in range(n_reducers)])

    logger.info("Input files: {}".format(input_file_lists))

    for i, step in enumerate(job._steps):
        step_config = map(int, job._settings.step_config[i].split(':'))
        n_mappers = step_config[0]
        n_reducers = step_config[1]
        cmd_opts = format_cmd([
            '--step_idx', i,
            '--total_steps', step_count,
            '--input_files', input_file_lists[i],
            '--work_dir', tmp_dirs[i],
            '--exec_script', exec_script,
            '--n_mappers', n_mappers,
            '--n_reducers', n_reducers,
            '--output', job._settings.output,
            '--job_module', sys.modules[job.__module__].__file__,
            '--job_class', job.__class__.__name__,
            '--use_domino', job._settings.use_domino,
            '--n_concurrent_machines', job._settings.n_concurrent_machines,
            '--n_shards_per_machine', job._settings.n_shards_per_machine
        ])
        logger.info("Starting step %d with options: %s", i, cmd_opts)
        run_step(step_args(cmd_opts))
    logger.info('All done.')


class MRJob(object):

    __metaclass__ = abc.ABCMeta

    INPUT_PROTOCOL = protocol.JSONValueProtocol
    INTERNAL_PROTOCOL = protocol.JSONProtocol
    OUTPUT_PROTOCOL = protocol.JSONValueProtocol

    def __init__(self):
        settings1 = parse_args(args=format_cmd(self.settings()), known=False)
        settings2, input_files = parse_args(namespace=settings1, known=True)
        self._settings = settings2
        self._input_files = input_files
        self._steps = self.steps()
        assert len(self._steps) == len(self._settings.step_config)
        self._counters = MRCounter()

    @classmethod
    def run(cls):
        mapreduce(cls)

    @abc.abstractmethod
    def steps(self):
        """define steps necessary to run the job"""

    @abc.abstractmethod
    def settings(self):
        """define settings"""

    def increment_counter(self, group, counter, amount=1):
        self._counters.incr(group, counter, amount)

    def get_step(self, step_idx):
        return self.steps()[step_idx]
