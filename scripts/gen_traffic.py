#!/usr/bin/python

import argparse
import time
import random
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--job-list', dest="job_list", default="jobs.txt", help="list of jobs to issue")
parser.add_argument('-v', '--verbose', dest="verbose", default=False, action='store_true', help="verbose output")
parser.add_argument('--log', dest="logfile", default="log.txt", help="file to save the log")
parser.add_argument('-t', "--time", dest="time", default=60, help="how many minutes should it keep generating traffic")
parser.add_argument("--issue-interval", dest="issue_interval", default=10, help="schedule new jobs every N seconds")
parser.add_argument("--bulk-size", dest="bulk_size", default=5, help="number of jobs to issue at each interval")
parser.add_argument('--save-traffic', dest="save_traffic", action="store_true", help="save traffic to a file")
parser.add_argument('--traffic-outfile', dest="traffic_output_file", default="out_traffic.txt", help="file to save the generated traffic")
parser.add_argument('--replay-traffic', dest="replay_traffic", action="store_true", help="replay previously saved traffic from traffic file (it will ignore all other traffic variables)")
parser.add_argument('--traffic-infile', dest="traffic_input_file", default="in_traffic.txt", help="file to replay traffic from")
parser.add_argument('--random-traffic', dest="random_traffic", action="store_true", help="generate completely random traffic at random intervals (it will ingore issue-interval and bulk-size)")
parser.add_argument("--time-unit", dest="time_unit", default=1, help="time unit fogr emulation in seconds (can be floating point)")
parser.add_argument("--force-quit", dest="force_quit", action="store_true", help="do not wait for all jobs to finish before quiting")
parser.add_argument('--traffic-scenario', dest="traffic", default="normal", help="traffic scenario type for random generation (normal, light, heavy, bursty)")

class job:

	def __init__(self, job_name, req_hosts, est_time):
		self.name = job_name	
		self.req_hosts = req_hosts
		self.est_time = est_time

#--------------------------------#

args = parser.parse_args()

issue_intervals = []
bulk_sizes = []
if args.traffic == "normal":
	issue_intervals = [20.00, 40.00, 40.00, 60.00, 60.00, 120.00]
	bulk_sizes = [16, 32, 64, 64, 128, 256] 
elif args.traffic == "light":
	issue_intervals = [40.00, 60.00, 120.00]
	bulk_sizes = [16, 32, 32, 64] 
elif args.traffic == "heavy":
	issue_intervals = [20.00, 40.00, 60.00]
	bulk_sizes = [64, 128, 128, 256] 
elif args.traffic == "bursty":
	issue_intervals = [60.00, 120.00]
	bulk_sizes = [32, 64, 64, 128, 128, 256] 
else:
	print "Traffic scenarion not supported!"
	sys.exit(0)


def generate_traffic(issue_interval, bulk_size, total_time):

	start_time = time.time()
	last_issue_time = time.time()
	elapsed_issue_time = 0
	elapsed_time = 0 
	if args.random_traffic:
		issue_interval = 0 #random.choice(issue_intervals) 
	
	while elapsed_time < total_time:
	
		if elapsed_issue_time > issue_interval:

			if args.random_traffic:
				#bulk_size = random.randint(1, 128)
				bulk_size = random.choice(bulk_sizes)
				issue_interval = random.choice(issue_intervals) 
				issue_interval = issue_interval * float(args.time_unit) 

			if args.save_traffic:
				traffic_outfile.write("** elapsed time: " + str(float(elapsed_issue_time) / float(args.time_unit)) + " - bulk_size: " + str(bulk_size)  + "\n") 
		
			#sys.stdout.write("** elapsed time: " + str(float(elapsed_issue_time) / float(args.time_unit)) + " - bulk_size: " + str(bulk_size)  + "\n") 
			hosts_used = 0
			while(hosts_used < bulk_size):
				job = random.choice(job_list)

				if (hosts_used + job.req_hosts) > bulk_size:
					continue

				#sys.stdout.write(job.name + "\t" + str(job.req_hosts) + "\t" + str(job.est_time) + "\n")
 				hosts_used += job.req_hosts

				if args.save_traffic:
					traffic_outfile.write(job.name + "\t" + str(job.req_hosts) + "\t" + str(job.est_time) + "\n")
					traffic_outfile.flush()

			sys.stdout.flush()
		
			last_issue_time = time.time()

		elapsed_issue_time = time.time() - last_issue_time
		elapsed_time = time.time() - start_time
	
	#sys.stdout.write("quit")
	sys.stdout.flush()

def replay_traffic():
	traffic = open(args.traffic_input_file, "r")	
	for line in traffic:
		#parse elapsed time
		if len(line.split()) > 3:
			time.sleep(float(line.split()[3]) * float(args.time_unit))
		else:
			sys.stdout.write(line)	
			sys.stdout.flush()

	time.sleep(1)
	if args.force_quit:
		sys.stdout.write("quit -f")
	else:
		sys.stdout.write("quit")
	sys.stdout.flush()


# MAIN
if args.replay_traffic:
	replay_traffic()
else:
	job_list = list()
	jobs = open(args.job_list, "r")

	for line in jobs:
		if line.startswith("#"):
			continue
		job_list.append(job(line.split()[0], int(line.split()[1]), float(line.split()[2]) ))
  	#job_list = f.read().splitlines()

	#open traffic file
	traffic_outfile = open(args.traffic_output_file, "w")
	generate_traffic(float(args.issue_interval) * float(args.time_unit), int(args.bulk_size), float(args.time) * float(args.time_unit))

