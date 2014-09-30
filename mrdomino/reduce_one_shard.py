import json
from os.path import join as path_join
from functools import partial
from contextlib import nested as nested_context
from mrdomino.util import logger, get_instance, protocol, open_gz


def reduce(shard, args):

    # find the reduce function.
    job = get_instance(args)
    step = job.get_step(args.step_idx)
    reduce_func = step.reducer

    work_dir = args.work_dir

    # process each (key, value) pair.
    out_fn = path_join(work_dir, args.output_prefix + '.%d' % shard)
    logger.info("reducer {}: output -> {}".format(shard, out_fn))

    assert args.input_prefix is not None
    in_f = path_join(work_dir, args.input_prefix + '.%d' % shard)
    logger.info("reducer {}: input <- {}".format(shard, in_f))
    input_stream = partial(open_gz, in_f, 'r')

    if args.step_idx >= 0:
        if job.INTERNAL_PROTOCOL == protocol.JSONProtocol:
            unpack_tuple = False
        elif job.INTERNAL_PROTOCOL == protocol.JSONValueProtocol:
            unpack_tuple = True
        else:
            raise ValueError("unsupported protocol: {}"
                             .format(job.INTERNAL_PROTOCOL))
    else:
        raise ValueError("step_idx={} cannot be negative"
                         .format(args.step_idx))

    count_written = 0
    count_seen = 0
    with nested_context(input_stream(),
                        open_gz(out_fn, 'w')) as (in_fh, out_fh):
        last_key = None
        values = []
        for line in in_fh:
            count_seen += 1
            key, value = json.loads(line)
            if key == last_key:
                # extend previous run
                values.append(value)
            else:
                # end previous run
                if values:
                    for kv in reduce_func(last_key, values):
                        k, v = kv if unpack_tuple else (None, kv)
                        count_written += 1
                        out_fh.write(json.dumps(v) + '\n')

                # start new run
                last_key = key
                values = [value]
        # dump any remaining values
        if values:
            for kv in reduce_func(last_key, values):
                k, v = kv if unpack_tuple else (None, kv)
                count_written += 1
                out_fh.write(json.dumps(v) + '\n')

    counters = job._counters
    counters.incr("reducer", "seen", count_seen)
    counters.incr("reducer", "written", count_written)

    # write the counters to file.
    fname = path_join(work_dir, 'reduce.counters.%d' % shard)
    logger.info("reducer {}: counters -> {}".format(shard, fname))
    with open(fname, 'w') as fhandle:
        fhandle.write(counters.serialize())

    # finally note that we are done.
    fname = path_join(work_dir, 'reduce.done.%d' % shard)
    logger.info("reducer {}: done -> {}".format(shard, fname))
    with open(fname, 'w') as fhandle:
        fhandle.write('')
