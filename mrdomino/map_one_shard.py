import json
import re
import math
import itertools
from os.path import join as path_join
from subprocess import Popen, PIPE
from mrdomino.util import open_gz, logger, get_instance, protocol, \
    format_cmd


def each_input_line(input_files, shard, n_shards):
    # assign slices of each file to shards.
    slice_assignments = []
    num_files = len(input_files)
    for i in range(n_shards):
        slice_assignments += [i] * num_files

    # get which files this shard is using (partially or the whole file).
    a = num_files * shard / n_shards
    z = int(math.ceil(num_files * (shard + 1) / float(n_shards)))

    # for each input file, yield the slices we want from it.
    for i in range(a, z):
        aa = n_shards * i
        zz = n_shards * (i + 1)
        assign = slice_assignments[aa:zz]
        inf_gen = itertools.cycle(range(n_shards))
        with open_gz(input_files[i], 'r') as fhandle:
            for j, line in itertools.izip(inf_gen, fhandle):
                if shard == assign[j]:
                    yield line


def map(shard, args):

    # find the map function.
    job = get_instance(args)
    step = job.get_step(args.step_idx)
    map_func = step.mapper
    n_shards = args.n_mappers
    combine_func = step.combiner

    assert 0 <= shard < n_shards

    if combine_func is None:
        out_fn = path_join(args.work_dir, args.output_prefix % str(shard))
        logger.info("mapper {}: output -> {}".format(shard, out_fn))
        if re.match(r'.*\.gz$', out_fn) is None:
            proc_sort = Popen(['sort', '-o', out_fn], bufsize=4096, stdin=PIPE)
            proc = proc_sort
        else:
            proc_gzip = Popen(['gzip', '-c'], bufsize=4096, stdin=PIPE,
                              stdout=open_gz(out_fn, 'w'))
            proc_sort = Popen(['sort'], bufsize=4096, stdin=PIPE,
                              stdout=proc_gzip.stdin)
            proc = proc_gzip
    else:
        cmd_opts = format_cmd([
            'python', '-m', 'mrdomino.combine',
            '--job_module', args.job_module,
            '--job_class', args.job_class,
            '--step_idx', args.step_idx,
            '--work_dir', args.work_dir,
            '--output_prefix', args.output_prefix,
            '--shard', shard
        ])
        logger.info("mapper {}: starting combiner: {}"
                    .format(shard, cmd_opts))
        proc_combine = Popen(cmd_opts, bufsize=4096, stdin=PIPE)
        proc_sort = Popen(['sort'], bufsize=4096, stdin=PIPE,
                          stdout=proc_combine.stdin)
        proc = proc_combine

    if args.step_idx == 0:
        # first step
        if job.INPUT_PROTOCOL == protocol.JSONProtocol:
            unpack_tuple = True
        elif job.INPUT_PROTOCOL == protocol.JSONValueProtocol:
            unpack_tuple = False
        else:
            raise ValueError("unsupported protocol: {}"
                             .format(job.INPUT_PROTOCOL))
    elif args.step_idx > 0:
        # intermediate step
        if job.INTERNAL_PROTOCOL == protocol.JSONProtocol:
            unpack_tuple = True
        elif job.INTERNAL_PROTOCOL == protocol.JSONValueProtocol:
            unpack_tuple = False
        else:
            raise ValueError("unsupported protocol: {}"
                             .format(job.INTERNAL_PROTOCOL))
    else:
        raise ValueError("step_idx={} cannot be negative"
                         .format(args.step_idx))

    # process each line of input and sort for the merge step.
    # using with block here ensures that proc_sort.stdin is closed on exit and
    # that it won't block the pipeline
    count_written = 0
    count_seen = 0
    with proc_sort.stdin as in_fh:
        for line in each_input_line(args.input_files, shard, n_shards):
            count_seen += 1
            key_value = json.loads(line)
            key, value = key_value if unpack_tuple else (None, key_value)
            for key_value in map_func(key, value):
                in_fh.write(json.dumps(key_value) + '\n')
                count_written += 1

    counters = job._counters
    counters.incr("mapper", "seen", count_seen)
    counters.incr("mapper", "written", count_written)

    # write the counters to file.
    fname = path_join(args.work_dir, 'map-%d.counters' % shard)
    logger.info("mapper {}: counters -> {}".format(shard, fname))
    with open(fname, 'w') as fhandle:
        fhandle.write(counters.serialize())

    # write how many entries were written for reducer balancing purposes.
    # note that if combiner is present, we delegate this responsibility to it.
    if combine_func is None:
        fname = path_join(args.work_dir, (args.output_prefix % str(shard)) + '.count')
        logger.info("mapper {}: lines written -> {}".format(shard, fname))
        with open(fname, 'w') as fhandle:
            fhandle.write(str(count_written))

    # `communicate' will wait for subprocess to terminate
    proc.communicate()

    # finally note that we are done.
    fname = path_join(args.work_dir, 'map-%d.done' % shard)
    logger.info("mapper {}: done -> {}".format(shard, fname))
    with open(fname, 'w') as fhandle:
        fhandle.write('')
