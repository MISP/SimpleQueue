#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
import shlex
import argparse
import os
import redis
import time
from datetime import datetime
import uuid

try:
    from terminaltables import AsciiTable
    HAS_TAB = True
except:
    HAS_TAB = False


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
        self.default_redis = redis.StrictRedis(host=self.runtime['Default']['host'],
                                               port=self.runtime['Default']['port'],
                                               db=self.runtime['Default']['db'],
                                               decode_responses=True)
        self.cleanup_mgmt()

    def _is_pid_running(self, pid):
        try:
            os.kill(int(pid), 0)
        except OSError:
            return False
        else:
            return True

    def launch_queues(self):
        for module in self.pipeline.keys():
            pin = subprocess.Popen(['QueueIn.py', '-p', self.pipeline_path, '-m', module, '-r', self.runtime_path])
            pout = subprocess.Popen(['QueueOut.py', '-p', self.pipeline_path, '-m', module, '-r', self.runtime_path])
            self.queues[module] = (pin.pid, pout.pid)

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
                os.kill(pin, 9)
            if pout:
                os.kill(pout, 9)
        self.queues = {}

    def get_module_status(self, module):
        nb_processes = self.default_redis.hget('config_{}'.format(module), 'nb_processes')
        pids = self.default_redis.smembers('pids_{}'.format(module))
        return int(nb_processes), [int(p) for p in pids]

    def launch_modules(self):
        p = self.default_redis.pipeline(False)
        for module in self.startup.keys():
            nb_processes = self.startup[module].get('processes')
            if not nb_processes:
                nb_processes = 1
            p.sadd('running_modules', module)
            p.hset('config_{}'.format(module), 'nb_processes', nb_processes)
            pids = []
            for i in range(nb_processes):
                pid = self._start_process(module)
                pids.append(pid)
            p.sadd('pids_{}'.format(module), *pids)
        p.execute()

    def _start_process(self, module):
        cmd = "python -m {} -r {} -i {}_{}".format(self.startup[module]['module'],
                                                   self.runtime_path, module, uuid.uuid4())
        args = shlex.split(cmd)
        return subprocess.Popen(args).pid

    def update_running_modules(self):
        if not self.default_redis.exists('running_modules'):
            return
        # Clean zombies
        os.waitpid(-1, os.WNOHANG)
        pipe = self.default_redis.pipeline()
        for module in self.default_redis.smembers('running_modules'):
            expected_processes, running_processes = self.get_module_status(module)
            cur_pids = []
            for p in running_processes:
                if self._is_pid_running(p):
                    cur_pids.append(p)
            to_start = expected_processes - len(cur_pids)
            if to_start > 0:
                for i in range(to_start):
                    pid = self._start_process(module)
                    cur_pids.append(pid)
            pipe.delete('pids_{}'.format(module))
            pipe.sadd('pids_{}'.format(module), *cur_pids)
        pipe.execute()

    def stop_modules(self):
        if not self.default_redis.exists('running_modules'):
            return
        pipe = self.default_redis.pipeline()
        for module in self.default_redis.smembers('running_modules'):
            expected_processes, running_processes = self.get_module_status(module)
            [os.kill(p, 9) for p in running_processes if p]
            pipe.delete('config_{}'.format(module))
            pipe.delete('pids_{}'.format(module))
        pipe.delete('running_modules')
        pipe.execute()

    def update_status(self):
        status = {}
        for m in self.default_redis.smembers('modules'):
            status[m] = {}
            for p in self.default_redis.smembers('module_{}'.format(m)):
                if not self._is_pid_running(p):
                    self.default_redis.delete('module_{}_{}'.format(m, p))
                    self.default_redis.srem('module_{}'.format(m), p)
                    continue
                details = self.default_redis.hgetall('module_{}_{}'.format(m, p))
                status[m][p] = {'last_pop': details['in'], 'size_in': details['size_in'],
                                'last_push': details['out'], 'size_out': details['size_out'],
                                'delayed': self.default_redis.zcard('{}in_delayed'.format(m)),
                                'processing': details['uuid']}
        self.default_redis.set('status', json.dumps(status), ex=600)

    def update_status_queues(self):
        status_queues = {}
        for m in self.default_redis.smembers('modules'):
            # Intermediate queues
            status_queues['{}in'.format(m)] = []
            status_queues['{}out'.format(m)] = []
            status_queues['{}in_delayed'.format(m)] = []
            inqueue = self.default_redis.sscan('{}in'.format(m), count=3)[1]
            delayed_queue = self.default_redis.zscan('{}in_delayed'.format(m), count=7)[1]
            outqueue = self.default_redis.sscan('{}out'.format(m), count=3)[1]
            if inqueue:
                for iq in inqueue:
                    job = json.loads(iq)
                    status_queues['{}in'.format(m)].append(job['uuid'])
            if delayed_queue:
                for dq in delayed_queue:
                    job = json.loads(dq[0])
                    status_queues['{}in_delayed'.format(m)].append([job['uuid'], datetime.fromtimestamp(dq[1]).isoformat()])
            if outqueue:
                for oq in outqueue:
                    job = json.loads(oq)
                    status_queues['{}out'.format(m)].append(job['uuid'])
        self.default_redis.set('status_queues', json.dumps(status_queues), ex=600)

    def show_status_queues(self):
        if not HAS_TAB:
            os.system('clear')
            print('terminaltables no installed, running headless.')
            return
        if not self.default_redis.exists('status'):
            os.system('clear')
            print('No status key available, nothing to display.')
            return
        status_queues = json.loads(self.default_redis.get('status_queues'))
        for m in self.default_redis.smembers('modules'):
            if status_queues.get('{}in_delayed'.format(m)):
                table_delayed = [['{}in_delayed'.format(m), 'Until']]
                if status_queues.get('{}in_delayed'.format(m)):
                    for l in status_queues['{}in_delayed'.format(m)]:
                        table_delayed.append(l)
                table_delayed = AsciiTable(table_delayed)
                print(table_delayed.table)
            if status_queues.get('{}in'.format(m)):
                table_in = [['{}in'.format(m)]]
                for l in status_queues['{}in'.format(m)]:
                    table_in.append([l])
                table_in = AsciiTable(table_in)
                print(table_in.table)
            if status_queues.get('{}out'.format(m)):
                table_out = [['{}out'.format(m)]]
                for l in status_queues['{}out'.format(m)]:
                    table_out.append([l])
                table_out = AsciiTable(table_out)
                print(table_out.table)

    def show_status(self):
        if not HAS_TAB:
            os.system('clear')
            print('terminaltables no installed, running headless.')
            return
        if not self.default_redis.exists('status'):
            os.system('clear')
            print('No status key available, nothing to display.')
            return
        status = json.loads(self.default_redis.get('status'))

        table = [["Queue name", "Delayed", "Process ID", 'Processing', 'Last pop', 'Input Size', 'Last push', 'Output Size']]
        rows = []
        for m, d in status.items():
            for p, values in d.items():
                rows.append([m, values['delayed'], p, values.get('processing'), values['last_pop'], values['size_in'], values['last_push'], values['size_out']])
        rows.sort()
        table += rows
        table = AsciiTable(table)
        print(table.table)

    def cleanup_mgmt(self):
        pipe = self.default_redis.pipeline(False)
        for m in self.default_redis.smembers('modules'):
            for p in self.default_redis.smembers('module_{}'.format(m)):
                pipe.delete('module_{}_{}'.format(m, p))
            pipe.delete('module_{}'.format(m))
            pipe.delete('pids_{}'.format(m))
            pipe.delete('config_{}'.format(m))
        pipe.delete('modules')
        pipe.delete('status')
        pipe.delete('status_queues')
        pipe.execute()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start and manage all queues.')
    parser.add_argument("-p", "--pipeline", type=str, required=True, help="Path to the pipeline configuration file.")
    parser.add_argument("-r", "--runtime", type=str, required=True, help="Path to the runtime configuration file.")
    parser.add_argument("-s", "--startup", type=str, required=True, help="Path to the startup configuration file.")
    parser.add_argument("-q", "--quiet", default=False, action='store_true', help="Run in quiet mode, no display.")
    args = parser.parse_args()
    m = Manager(args.pipeline, args.runtime, args.startup)
    m.launch_queues()
    m.launch_modules()
    try:
        while m.queues:
            m.update_running_queues()
            m.update_running_modules()
            m.update_status()
            m.update_status_queues()
            if not args.quiet or not HAS_TAB:
                os.system('clear')
                m.show_status()
                m.show_status_queues()
            time.sleep(1)
    except KeyboardInterrupt:
        m.stop_queues()
        m.stop_modules()
    # except Exception as e:
    #    print(e)
    #    m.stop_queues()
    #    m.stop_modules()
