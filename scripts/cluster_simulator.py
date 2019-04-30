#!/usr/bin/python

import argparse
import pandas as pd
import sys
from subprocess import PIPE, Popen
from threading  import Thread
from logger import Logger
from common import Job
from common import JobProfilerManager
from scheduler import Scheduler
from scheduler import SLURMScheduler
from budget_aware_sched import BudgetAwareOptimalScheduler
from budget_aware_sched import BudgetAwarePredictionScheduler
from budget_aware_sched import BudgetAwareSpeculationScheduler
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x
from time import sleep
from time import time
from stats import StatsLog

parser = argparse.ArgumentParser()
parser.add_argument('--scheduler', dest="scheduler", default="random", help="scheduler to use")
parser.add_argument('--host-list', dest="host_list", default="hosts.txt", help="host list file")
parser.add_argument('-v', '--verbose', dest="verbose", action='store_true', help="verbose output")
parser.add_argument('--log', dest="logfile", default="log.txt", help="file to save the log")
parser.add_argument("--time-limit", dest="time_limit", default=None, help="time in seconds to run the simulation")
parser.add_argument("--time-unit", dest="time_unit", default=0.1, help="time unit for emulation in seconds (can be floating point)")
parser.add_argument('--enable-statistics', dest="enable_stats", action='store_true', help="enable runtime statistics and performance measurements")
parser.add_argument('--stats-log', dest="stats_logfile", default="stats_log.txt", help="file to save the stastics")
parser.add_argument('--stats-trace-file', dest="trace_file", default="trace.txt", help="file to save statistics traces")
parser.add_argument("--job-power-cap", dest="job_power_cap", default=115.0, help="Power peak for a single job allowed in watts")
parser.add_argument("--global-power-budget", dest="global_power_budget", default=20000.0, help="Total power available to the cluster")
parser.add_argument("--schedule-interval", dest="schedule_interval", default=1, help="Check for new jobs every N seconds")
parser.add_argument('--show-elapsed-time', dest="show_elapsed_time", action='store_true', help="print elapsed time")
parser.add_argument('--sched-conf', dest="sched_conf", default="./conf/sched_default.conf", help="scheduling policy configuration file")
parser.add_argument('--enable-safety-capping', dest="enable_safety_capping", action='store_true', help="forces predicted/speculated power peaks so that system does not exceed global power budget")

args = parser.parse_args()

def input_handler(stream, queue):
	while _running: 
		for line in iter(stream.readline, b''):
			queue.put(line)
	stream.close()

# increase elapsed time and move execution forward
def tick(tick_unit):
	sleep(tick_unit)
	#TODO: we could join the two list
	pending_jobs = sched.get_pending_jobs()
	for job in pending_jobs:
		job.progress(1)
	#progress running jobs
	running_jobs = sched.get_running_jobs().keys()
	for job in running_jobs:
		#log.write("tick\n")
		job.progress(1)
		#log.write(job.name + " (" + str(job.pid) + ") elapsed time: " + str(job.get_elapsed_time()) + "(est time:" + str(job.get_est_time()) + ")\n")
		if job.get_elapsed_time() <= 0:
			log.write("* job " + job.name + " (" + str(job.pid) + ") finished\n")
			if args.enable_stats:
				stats.retire_job(job, sched.get_hosts(job))
			sched.job_finished(job)	
		#log_message(log,  "tack\n")
	if args.enable_stats:
		stats.inc_time()
		stats.trace_stats(sched.get_running_jobs())
	
	
_running = True
num_sockets = 2
#open log file for writing
log = Logger(args.logfile, args.verbose)
#log = open(args.logfile, 'w')

log.write("* simulator starting...\n")
#get hosts
hosts_file = open(args.host_list, 'r')
hosts = list()
for line in hosts_file:
	host = line.strip('\n')
	for i in range(0, num_sockets):
		hosts.append(host + "_" + str(i))
		i += 1

#get job execution times
job_elapsed_time = dict() #done at creation of new job

#power_profiles = ["none", "100", "90", "80", "75", "70"]
#power_profiles = [115.0, 100.0, 90.0, 80.0, 70.0, 60.0, 50.0]
power_profiles = [115.0]
#create job profiler data manager
jp = JobProfilerManager(hosts, "./conf/job_profiler_manager.conf", power_profiles)

#create stats manager
if args.enable_stats:
	stats = StatsLog(jp, args.stats_logfile, args.trace_file)

#create new scheduler
if args.scheduler == "naive":
	#sched = RandomScheduler("naive",  hosts, jp, log, args.verbose)
	sched = SLURMScheduler("naive",  hosts, jp, log, stats, args.verbose)
elif args.scheduler == "ideal_pred_model":
	sched = BudgetAwareOptimalScheduler("ideal_pred_model", hosts, jp, log, stats, args.verbose)
elif args.scheduler == "optimal_pred_model":
	sched = BudgetAwarePredictionScheduler("optimal_pred_model", hosts, jp, log, stats, args.verbose)
	#jp.set_prediction_model("optimal_model");
	jp.set_prediction_model("opt_model_var");
elif args.scheduler == "generic_pred_model":
	sched = BudgetAwarePredictionScheduler("generic_pred_model", hosts, jp, log, stats, args.verbose)
	jp.set_prediction_model("generic_model");
elif args.scheduler == "speculation_naive":
	sched = BudgetAwareSpeculationScheduler("speculation_naive", hosts, jp, log, stats, args.verbose)
elif args.scheduler == "speculation_ordered":
	sched = BudgetAwareSpeculationScheduler("speculation_ordered", hosts, jp, log, stats, args.verbose)
	sched.enable_ordered_hosts()
elif args.scheduler == "lazy_pred_model":
	sched = BudgetAwarePredictionScheduler("lazy_pred_model", hosts, jp, log, stats, args.verbose)
	#jp.set_prediction_model("opt_model_cb");
	jp.set_prediction_model("opt_model_sparse");
else:
	print("Sceduling policy not found, exiting.")
	exit()
sched.load_confing(args.sched_conf)
#set power limit if applicable
#if "budget_" in args.scheduler:
#	if args.power_limit:
#		sched.set_power_budget(float(args.power_limit))
#	else:
#		print("Need to speciy a power limit when using a budget aware policy!")
#		exit()	
sched.set_job_power_budget(args.job_power_cap)
sched.set_global_power_budget(args.global_power_budget)
if args.enable_safety_capping:
	sched.enable_safety_capping()


log.write("* scheduler initialized!\n" \
					"========================\n" \
					"- number of hosts: " + str(len(hosts)) + "\n" \
					"- scheduler: " + args.scheduler + "\n" \
					"- global power budget: " + str(args.global_power_budget) + "\n" \
					"- job power budget: " + str(args.job_power_cap) + "\n" \
					"========================\n")

#create input manager
log.write("* starting up input reader daemon\n")
new_job_q = Queue()
job_monitor = Thread(target=input_handler, args=(sys.stdin, new_job_q))
job_monitor.daemon = True # thread dies with the program
job_monitor.start()


log.write("* simulator running...\n")
job_num=0
elapsed_time = 0
while True:
	#start_time = time()
	#check if time limit is reached
	#print "elapsed time:" + str(elapsed_time)
	if args.show_elapsed_time:
		sys.stdout.write("elapsed time %d sec   \r" % (elapsed_time) )
		sys.stdout.flush()

	if args.time_limit != None:
		if float(elapsed_time) > float(args.time_limit):
			break
	
	try: line = new_job_q.get_nowait()
	except Empty:
		sched.run()
		tick(float(args.time_unit))	
		elapsed_time += 1
		#pass
	else:
		if not line:
			continue
		_job = line.strip("\n")
		if _job == "quit" or _job == "quit -f":
			if _job == "quit" and sched.busy():
				log.write("* simulator is still running jobs, waiting for them to finish...\n")
				while sched.busy():
					#print "elapsed time:" + str(elapsed_time)
					if args.show_elapsed_time:
						sys.stdout.write("elapsed time %d sec   \r" % (elapsed_time) )
						sys.stdout.flush()

					if args.time_limit != None:
						if float(elapsed_time) > float(args.time_limit):
							break
					sched.run()
					tick(float(args.time_unit))	
					elapsed_time += 1
			elif _job == "quit -f" and sched.busy():	
				log.write("* simulator is still running jobs, force exiting...\n")
			log.write("* simulator is stopping and exiting!\n")
			running = False
			break

		# Parse job string
		job_descr = _job.split()
		job_name = job_descr[0]
		job_req_nodes = job_descr[1]
		job_est_time = job_descr[2]
		
		log.write("* creating new job object: " + str(job_num) + " ("  + job_name + ")\n")
		jp.add_job(job_name, job_req_nodes, job_est_time, power_profiles)
		#exec_time = jp.get_execution_time(_job, "none")
		job = Job(job_name, job_req_nodes, job_est_time, job_num)
		sched.push(job)
		if args.enable_stats:
			stats.new_job(job)
		job_num += 1
	#end_time = time()
	#print "tick: " + str(end_time - start_time)  
	#end of input

sys.stdout.write("total simulated time %d sec\n" % (elapsed_time))
	
if args.enable_stats:
	stats.finish()

