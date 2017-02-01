#!/usr/bin/env python
import argparse
import json

from simplequeue import Pipeline


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Dispatch Queue.')
    parser.add_argument("-r", "--runtime", type=str, required=True, help="Path to the runtime configuration file.")
    parser.add_argument("-i", "--id", type=str, required=True, help="Module ID.")
    args = parser.parse_args()

    module_name, module_id = args.id.split('_')

    with open(args.runtime, 'r') as f:
        runtime = json.load(f)
    pipeline = Pipeline(runtime, module_name)

    nb = 0

    while True:
        message = pipeline.receive()
        if message is not None:
            pipeline.log.debug(module_name + ': Got a message')
            pipeline.send(message)
            nb += 1
            if nb % 100 == 0:
                pipeline.log.info('{} ({}): {} messages processed, {} to go.'.format(
                    module_name, module_id, nb, pipeline.count_queued_messages()))
        else:
            pipeline.log.debug(module_name + ": Empty Queues: Waiting...")
            pipeline.sleep(1)
