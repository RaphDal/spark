#!/usr/bin/env python3

import statistics

file = open('logs')

trials = {}
while True:
    line = file.readline()
    if not line:
        break
    if line.startswith('Iteration'):
        [_, trial, iteration, time, _] = line.split('-')
        iters = trials.get(trial, [])
        iters.append(float(time))
        trials[trial] = iters

keys = [*trials.keys()]
keys.sort()
for trial in keys:
    iters = trials[trial]
    print("%s: %sms (stddev %s)" %
          (trial, statistics.mean(iters), statistics.stdev(iters)))
