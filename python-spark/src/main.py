import os
import sys
import argparse
import importlib

import pyspark

if os.path.exists('job.zip'):
    sys.path.insert(0, 'job.zip')
else:
    sys.path.insert(0,'./job.zip')

parser = argparse.ArgumentParser();
parser.add_argument('--job', type=str, help='Job name to execute', required=True)
parser.add_argument('--job-args', help='arguments need to execute the job', nargs='*')
args = vars(parser.parse_args())

sc = pyspark.SparkContext(appName=args.get('job'))
job_module = importlib.import_module('jobs.%s' % args.get('job'))
job_args = ','.join(map(str, args.get('job_args')))
job_args_dict = dict(item.split("=") for item in job_args.split(","))
job_module.analyze(sc, **job_args_dict)