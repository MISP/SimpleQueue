#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import redis
from datetime import datetime

class Log():

    def __init__(self, runtime, queue_name, process_id):
        self.r = redis.StrictRedis(host=runtime['Log']['host'],
                                   port=runtime['Log']['port'],
                                   db=runtime['Log']['db'])
        self.name = queue_name
        self.pid = process_id
        self.length = runtime['Log']['length']

    def _log(self, level, entry):
        queue = '{}_{}'.format(self.name, level)
        to_print = '{} - {} - {}'.format(datetime.now().isoformat(), self.pid, entry)
        p = self.r.pipeline(False)
        p.sadd('all_logs', queue)
        p.lpush(queue, to_print)
        p.ltrim(queue, 0, self.length)
        p.execute()

    def debug(self, entry):
        self._log('debug', entry)

    def info(self, entry):
        self._log('info', entry)

    def warning(self, entry):
        self._log('warning', entry)

    def error(self, entry):
        self._log('error', entry)

