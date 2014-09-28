import os
import sys
import abc
import stat
from tempfile import mkdtemp
from mrdomino.util import MRCounter, protocol, logger, format_cmd
from mrdomino.step import run_step, parse_args as step_args, PREFIX_REDUCE_OUT


class MRStep(object):
    def __init__(self, mapper, reducer, combiner=None):

        # do some basic type checking to verify that we pass callables.
        assert mapper is not None and hasattr(mapper, '__call__')
        self.mapper = mapper
        assert reducer is not None and hasattr(reducer, '__call__')
        self.reducer = reducer
        assert combiner is None or hasattr(combiner, '__call__')
        self.combiner = combiner


class MRSettings(object):
    def __init__(self, input_files, output_dir, tmp_dir, use_domino=False,
                 n_concurrent_machines=2, n_shards_per_machine=4,
                 step_config=None):

        assert isinstance(input_files, list)
        self.input_files = input_files
        assert isinstance(output_dir, str)
        self.output_dir = output_dir
        assert isinstance(tmp_dir, str)
        self.tmp_dir = tmp_dir
        assert isinstance(use_domino, bool)
        self.use_domino = use_domino
        assert isinstance(step_config, dict)
        self.step_config = step_config
        assert isinstance(n_concurrent_machines, int)
        self.n_concurrent_machines = n_concurrent_machines
        assert isinstance(n_shards_per_machine, int)
        self.n_shards_per_machine = n_shards_per_machine


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

    input_file_lists = [job._settings.input_files]
    for i, (step, out_dir) in enumerate(zip(job._steps, tmp_dirs)):
        step_config = job._settings.step_config[i]
        n_reducers = step_config.get('n_reducers') or 1
        reduce_format = os.path.join(out_dir, PREFIX_REDUCE_OUT + '.%d')
        input_file_lists.append([reduce_format % n for n in range(n_reducers)])

    logger.info("Input files: {}".format(input_file_lists))

    # if output directory root does not exist, create one
    output_dir = job._settings.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for i, step in enumerate(job._steps):
        step_config = job._settings.step_config[i]
        n_mappers = step_config.get('n_mappers') or 1
        n_reducers = step_config.get('n_reducers') or 1
        cmd_opts = format_cmd([
            '--step_idx', i,
            '--total_steps', step_count,
            '--input_files', input_file_lists[i],
            '--work_dir', tmp_dirs[i],
            '--exec_script', exec_script,
            '--n_mappers', n_mappers,
            '--n_reducers', n_reducers,
            '--output_dir', output_dir,
            '--job_module', sys.modules[job.__module__].__file__,
            '--job_class', job.__class__.__name__,
            '--use_domino', int(job._settings.use_domino),
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
        self._settings = self.settings()
        self._steps = self.steps()
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
