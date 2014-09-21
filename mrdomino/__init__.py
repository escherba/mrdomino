import os
import sys
from tempfile import mkdtemp
from abc import abstractmethod
from mrdomino.util import MRCounter, get_step, get_instance, protocol, logger
from mrdomino.step import parse_args, run_step


class MRStep(object):
    def __init__(self, mapper, reducer, combiner=None, n_mappers=2,
                 n_reducers=2):

        # do some basic type checking to verify that we pass callables.
        assert hasattr(mapper, '__call__')
        self.mapper = mapper
        assert hasattr(reducer, '__call__')
        self.reducer = reducer
        assert combiner is None or hasattr(combiner, '__call__')
        self.combiner = combiner
        assert isinstance(n_mappers, int)
        self.n_mappers = n_mappers
        assert isinstance(n_reducers, int)
        self.n_reducers = n_reducers


class MRSettings(object):
    def __init__(self, input_files, output_dir, tmp_dir,
                 exec_script, use_domino=False,
                 n_concurrent_machines=2, n_shards_per_machine=4):

        self.exec_script = exec_script
        assert isinstance(input_files, list)
        self.input_files = input_files
        assert isinstance(output_dir, str)
        self.output_dir = output_dir
        assert isinstance(tmp_dir, str)
        self.tmp_dir = tmp_dir
        assert isinstance(use_domino, bool)
        self.use_domino = use_domino
        assert isinstance(n_concurrent_machines, int)
        self.n_concurrent_machines = n_concurrent_machines
        assert isinstance(n_shards_per_machine, int)
        self.n_shards_per_machine = n_shards_per_machine


def mapreduce(job_class):

    job = job_class()
    step_count = len(job._steps)

    # if temporary directory root does not exist, create one
    tmp_root = job._settings.tmp_dir
    if not os.path.exists(tmp_root):
        os.makedirs(tmp_root)
    tmp_dirs = [mkdtemp(dir=tmp_root, prefix="step%d." % i)
                for i in range(step_count)]

    input_file_lists = [job._settings.input_files]
    for step, out_dir in zip(job._steps, tmp_dirs):
        n_reducers = step.n_reducers
        reduce_format = os.path.join(out_dir, 'reduce.out.%d')
        ff = [reduce_format % n for n in range(n_reducers)]
        input_file_lists.append(ff)

    logger.info("Input files: {}".format(input_file_lists))

    # if output directory root does not exist, create one
    output_dir = job._settings.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for i, step in enumerate(job._steps):
        cmd_opts = [
            '--step_idx', str(i),
            '--total_steps', str(step_count),
            '--input_files', ' '.join(input_file_lists[i]),
            '--work_dir', tmp_dirs[i],
            '--exec_script', job._settings.exec_script,
            '--output_dir', output_dir,
            '--job_module', sys.modules[job.__module__].__file__,
            '--job_class', job.__class__.__name__,
            '--use_domino', str(int(job._settings.use_domino)),
            '--n_concurrent_machines', str(job._settings.n_concurrent_machines),
            '--n_shards_per_machine', str(job._settings.n_shards_per_machine)
        ]
        logger.info("Starting step %d with options: %s" % (i, cmd_opts))
        run_step(parse_args(cmd_opts))
    logger.info('All done.')


class MRJob(object):

    INPUT_PROTOCOL = protocol.JSONValueProtocol
    INTERNAL_PROTOCOL = protocol.JSONProtocol
    OUTPUT_PROTOCOL = protocol.JSONValueProtocol

    def __init__(self, counters=None):
        self._settings = self.settings()
        self._steps = self.steps()
        self._counters = MRCounter()

    @classmethod
    def run(cls):
        mapreduce(cls)

    @abstractmethod
    def steps(self):
        """define steps necessary to run the job"""

    @abstractmethod
    def settings(self):
        """define settings"""

    def increment_counter(self, group, counter, amount=1):
        self._counters.incr(group, counter, amount)

    def get_step(self, step_idx):
        return self.steps()[step_idx]
