#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
import shlex
import argparse
import texttable
import os
import redis
import time


class Manager():

    def __init__(self, pipeline_path, runtime_path, startup_path):
        with open(pipeline_path) as f:
            self.pipeline_path = pipeline_path
            self.pipeline = json.load(f)
        with open(runtime_path) as f:
            self.runtime_path = runtime_path
            self.runtime = json.load(f)
        with open(startup_path) as f:
            self.startup_path = startup_path
            self.startup = json.load(f)
        self.queues = {}
        self.modules = {}
        self.default_redis = redis.StrictRedis(host=self.runtime['Default']['host'],
                                               port=self.runtime['Default']['port'],
                                               db=self.runtime['Default']['db'],
                                               decode_responses=True)
        self.cleanup_mgmt()

    def _is_pid_running(self, pid):
        if pid is None or pid.poll() is not None:
            return False
        return True

    def launch_queues(self):
        for module in self.pipeline.keys():
            pin = subprocess.Popen(['QueueIn.py', '-p', self.pipeline_path, '-m', module, '-r', self.runtime_path])
            pout = subprocess.Popen(['QueueOut.py', '-p', self.pipeline_path, '-m', module, '-r', self.runtime_path])
            self.queues[module] = (pin, pout)

    def update_running_queues(self):
        if not self.queues:
            return
        cur_queues = {}
        for module, p in self.queues.items():
            pin, pout = p
            if not self._is_pid_running(pin):
                pin = None
            if not self._is_pid_running(pout):
                pout = None
            if pin or pout:
                cur_queues[module] = (pin, pout)
        self.queues = cur_queues

    def stop_queues(self):
        if not self.queues:
            return
        for pin, pout in self.queues.values():
            if pin:
                pin.kill()
            if pout:
                pout.kill()
        self.queues = {}

    def launch_modules(self):
        for module in self.startup.keys():
            nb_processes = self.startup[module].get('processes')
            if not nb_processes:
                nb_processes = 1
            self.modules[module] = []
            for i in range(nb_processes):
                cmd = "python -m {} -r {} -i {}_{}".format(self.startup[module]['module'], self.runtime_path, module, i)
                args = shlex.split(cmd)
                pid = subprocess.Popen(args)
                self.modules[module].append(pid)

    def update_running_modules(self):
        if not self.modules:
            return
        cur_modules = {}
        for module, ps in self.modules.items():
            cur_pids = [p for p in ps if self._is_pid_running(p)]
            if cur_pids:
                cur_modules[module] = cur_pids
        self.modules = cur_modules

    def stop_modules(self):
        if not self.modules:
            return
        for ps in self.modules.values():
            [p.kill() for p in ps if p]
        self.modules = {}

    def show_status(self):
        modules_sizes = texttable.Texttable()
        modules_sizes.header(["Queue name", "#Items"])
        rows = []
        for name, size in self.default_redis.hgetall("queues").items():
            rows.append((name, size))
        rows.sort()
        modules_sizes.add_rows(rows, header=False)
        modules_status = texttable.Texttable()
        modules_status.header(["Queue name", "Process ID", 'Last pop', 'Last push'])
        rows = []
        for m in self.default_redis.smembers('modules'):
            for p in self.default_redis.smembers('module_{}'.format(m)):
                details = self.default_redis.hgetall('module_{}_{}'.format(m, p))
                rows.append([m, p, details['in'], details['out']])
        rows.sort()
        modules_status.add_rows(rows, header=False)

        os.system('clear')
        print(modules_sizes.draw())
        print(modules_status.draw())

    def cleanup_mgmt(self):
        pipe = self.default_redis.pipeline(False)
        for m in self.default_redis.smembers('modules'):
            for p in self.default_redis.smembers('module_{}'.format(m)):
                pipe.delete('module_{}_{}'.format(m, p))
            pipe.delete('module_{}'.format(m))
        pipe.delete('modules')
        pipe.delete('queues')
        pipe.execute()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start and manage all queues.')
    parser.add_argument("-p", "--pipeline", type=str, required=True, help="Path to the pipeline configuration file.")
    parser.add_argument("-r", "--runtime", type=str, required=True, help="Path to the runtime configuration file.")
    parser.add_argument("-s", "--startup", type=str, required=True, help="Path to the startup configuration file.")
    args = parser.parse_args()
    m = Manager(args.pipeline, args.runtime, args.startup)
    m.launch_queues()
    m.launch_modules()
    try:
        while m.modules or m.queues:
            m.update_running_queues()
            m.update_running_modules()
            m.show_status()
            time.sleep(1)
    except KeyboardInterrupt:
        m.stop_queues()
        m.stop_modules()
    except Exception as e:
        print(e)
        m.stop_queues()
        m.stop_modules()
