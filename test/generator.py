#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis
import time
import json
import random
import uuid

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
    r.publish(pipeline['Entry']['source-queue'],
              json.dumps({'uuid': str(uuid.uuid4()),
                          'run_at': message + random.randint(0, 20),
                          'content': message}))
    nb += 1
    if nb % 100 == 0:
        print(nb)

    if nb % 1000 == 0:
        print('Done.')
        break
