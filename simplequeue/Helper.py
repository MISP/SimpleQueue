#!/usr/bin/env python
# -*-coding:UTF-8 -*
"""
Queue helper module
===================

This module subscribe to a Publisher stream and put the received messages
into a Redis-list waiting to be popped later by others scripts.
"""
import redis
import time
import json
import os
from datetime import datetime

from .logging import Log


class ModuleConnector(object):

    def __init__(self, runtime, module_name):
        self.log = Log(runtime, module_name, os.getpid())
        self.r = redis.StrictRedis(host=runtime['Default']['host'],
                                   port=runtime['Default']['port'],
                                   db=runtime['Default']['db'])
        self.module_name = module_name
        self.in_set = self.module_name + 'in'
        self.out_set = self.module_name + 'out'
        self.log.info('New {} for {} started.'.format(self.__class__.__name__, self.module_name))
        self.r.sadd('modules', self.module_name)
        self.r.sadd('module_{}'.format(self.module_name), os.getpid())
        self.r.hmset('module_{}_{}'.format(self.module_name, os.getpid()),
                     {'in': 0, 'out': 0, 'size_in': 0, 'size_out': 0})

    def sleep(self, interval):
        """Requests the pipeline to sleep for the given interval"""
        time.sleep(interval)

    def send(self, msg):
        '''Push a messages to the temporary exit queue (multiprocess)'''
        self.r.sadd(self.out_set, msg)
        self.r.hmset('module_{}_{}'.format(self.module_name, os.getpid()),
                     {'out': datetime.now().isoformat(), 'size_out': self.r.scard(self.out_set)})

    def receive(self):
        '''Pop a messages from the temporary queue (multiprocess)'''
        # Update the size of the current waiting queue (for information purposes)
        self.r.hmset('module_{}_{}'.format(self.module_name, os.getpid()),
                     {'in': datetime.now().isoformat(), 'size_in': self.r.scard(self.in_set),
                      'size_out': self.r.scard(self.out_set)})
        return self.r.spop(self.in_set)


class QueueManager(object):

    class PubSub(object):

        def __init__(self):
            self.subscriber = None
            self.publishers = []

        def setup_subscribe(self, queue_name, queue_config):
            r = redis.StrictRedis(host=queue_config['host'],
                                  port=queue_config['port'],
                                  db=queue_config['db'])
            self.subscriber = r.pubsub(ignore_subscribe_messages=True)
            self.subscriber.psubscribe(queue_name)

        def subscribe(self):
            msg = self.subscriber.get_message()
            if not msg:
                return None
            if msg.get('data'):
                return json.loads(msg['data'].decode())

        def setup_publish(self, queue_name, queue_config):
            r = redis.StrictRedis(host=queue_config['host'],
                                  port=queue_config['port'],
                                  db=queue_config['db'])
            self.publishers.append((r, queue_name))

        def publish(self, message):
            for p, queue_name in self.publishers:
                p.publish(queue_name, message)

    def __init__(self, pipeline, module_name, runtime):
        with open(runtime, 'r') as f:
            self.runtime = json.load(f)
        self.log = Log(self.runtime, module_name, os.getpid())
        self.log.info('Intializing Queue for {}'.format(module_name))
        with open(pipeline, 'r') as f:
            self.modules = json.load(f)
        self.module_name = module_name
        self.pubsub = self.PubSub()
        # Setup the intermediary redis connector that makes the queues multiprocessing-ready
        self.r_temp = redis.StrictRedis(host=self.runtime['Default']['host'],
                                        port=self.runtime['Default']['port'],
                                        db=self.runtime['Default']['db'])
        self.in_set = self.module_name + 'in'
        self.out_set = self.module_name + 'out'
        self.source = self.modules[self.module_name].get('source-queue')
        self.destinations = self.modules[self.module_name].get('destination-queues')
        self.log.info('Queue for {} initialized.'.format(self.module_name))

    def check_delayed(self):
        now = int(time.time())
        for value in self.r_temp.zrange('{}_delayed'.format(self.in_set), 0, now):
            if value:
                msg = json.loads(value.decode())
                msg.pop('run_at', None)
                self.r_temp.sadd(self.in_set, json.dumps(msg))
        self.r_temp.zremrangebyscore('{}_delayed'.format(self.in_set), 0, now)

    def populate_set_in(self):
        '''Push all the messages addressed to the queue in a temporary redis set (mono process)'''
        queue_config = self.runtime.get(self.source)
        if queue_config is None:
            queue_config = self.runtime['Default']
        self.pubsub.setup_subscribe(self.source, queue_config)
        self.log.info('{} subscribing to input queue: {}.'.format(self.module_name, self.source))
        while True:
            msg = self.pubsub.subscribe()
            if msg:
                if not msg.get('run_at'):
                    self.r_temp.sadd(self.in_set, json.dumps(msg))
                else:
                    self.r_temp.zadd('{}_delayed'.format(self.in_set), msg.get('run_at'), json.dumps(msg))
            else:
                time.sleep(0.5)
            self.check_delayed()

    def publish(self):
        '''Push all the messages processed by the module to the next queue (mono process)'''
        if self.destinations is None:
            self.log.info('{} has no output queue.'.format(self.module_name))
            return False
        # We can have multiple publisher
        for dst in self.destinations:
            queue_config = self.runtime.get(dst)
            if queue_config is None:
                queue_config = self.runtime['Default']
            self.pubsub.setup_publish(dst, queue_config)
        self.log.info('{} ready to publish to {}.'.format(self.module_name, ', '.join(self.destinations)))
        while True:
            message = self.r_temp.spop(self.out_set)
            if message is None:
                time.sleep(1)
                continue
            self.pubsub.publish(message)
            # self.log.debug('{} sent a message.'.format(self.module_name))
