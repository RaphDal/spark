#!/usr/bin/env python3

file = open('logs')

trials = {}
while True:
    line = file.readline()
    if not line:
        break
    if line.startswith('Iteration'):
        [_, trial, iteration, time, _] = line.split('-')
        currentTrialSum, currentTrialCount = trials.get(trial, (0.0, 0))
        currentTrialSum += float(time)
        currentTrialCount += 1
        trials[trial] = (currentTrialSum, currentTrialCount)

for trial, (trialSum, trialCount) in trials.items():
    print(f'{trial}: {trialSum/trialCount}ms')
