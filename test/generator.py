#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis
import time
import json

nb = 0

with open('etc/pipeline.conf', 'r') as f:
    pipeline = json.load(f)

with open('etc/runtime.conf', 'r') as f:
    runtime = json.load(f)

r = redis.StrictRedis(host=runtime['Default']['host'],
                      port=runtime['Default']['port'],
                      db=runtime['Default']['db'])


while True:
    message = time.time()
    r.publish(pipeline['Entry']['source-queue'], message)
    nb += 1
    if nb % 1000 == 0:
        print(nb)

    if nb % 10000 == 0:
        print('Done.')
        break
