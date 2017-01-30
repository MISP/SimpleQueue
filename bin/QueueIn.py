#!/usr/bin/env python
# -*-coding:UTF-8 -*

import argparse
import signal
import sys

from simplequeue import Process


def signal_term_handler(signal, frame):
    sys.exit(0)


def run(pipeline, module, runtime):
    p = Process(pipeline, module, runtime)
    print('Entry queue for {} started. Receive on {}, populate to {}.'.format(module, p.source, p.in_set))
    p.populate_set_in()


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_term_handler)

    parser = argparse.ArgumentParser(description='Input queue for a module.')
    parser.add_argument("-p", "--pipeline", type=str, required=True, help="Path to the pipeline configuration file.")
    parser.add_argument("-m", "--module", type=str, required=True, help="Module to use.")
    parser.add_argument("-r", "--runtime", type=str, required=True, help="Path to the runtime configuration file.")
    args = parser.parse_args()

    run(args.pipeline, args.module, args.runtime)
