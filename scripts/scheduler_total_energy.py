#!/usr/bin/python

import argparse
import pandas as pd
import operator
import random

parser = argparse.ArgumentParser()
parser.add_argument('-j', '--jobs', dest="jobs", nargs='+', required=True, help="applications to schedule")
parser.add_argument('--host-list', dest="host_list", default="hosts.txt", help="host list file")
parser.add_argument('--scheduler', dest="scheduler", default="random", help="scheduling policy (random | random_optimal | perdiction | optimal | worst)")
parser.add_argument('-v', '--verbose', dest="verbose", default=False, action='store_true', help="verbose output")
parser.add_argument('--run-simulation', dest="simulate", default=False, action='store_true', help='simulate runs given cached execution results')
parser.add_argument('--save-schedule', dest="save_allocation", default=False, action='store_true', help='save the node allocation in file')
parser.add_argument('--schedule-file', dest='allocation_file', default='allocated_nodes.csv', help='node allocation file to be saved')

args = parser.parse_args()

def print_v1(message):
	if args.verbose: print(message)

if not args.simulate:
	print("Actual run not supported! Run in simulation mode.")
	exit()

if not (args.scheduler == "random" or args.scheduler == "random_optimal" or args.scheduler == "prediction" or args.scheduler == "optimal" or args.scheduler == "worst"):
	print("Invalid scheduling policy, choose one of the following: random, random_optimal, prediction, optimal.")
	exit()

hosts_file = open(args.host_list, 'r')

hosts = list()
for line in hosts_file:
	hosts.append(line.strip('\n'))

#print hosts

if args.save_allocation:
	allocation_file = open(args.allocation_file, 'w') 
	allocation_file.write("app,node,energy\n")

njobs = len(args.jobs)
#print njobs
job_df = dict()
job_energy = dict()
reserved_nodes = set()

_field='mean_power'
if args.scheduler == 'prediction':
	_field='mean_pred_power' 

for job in args.jobs:
	#get execution time
	df = pd.read_csv("../measurements/data/perf_stats/" + str(job) + "_perf.csv")
	exec_time = df['exec_time'].iloc[0]
	#print exec_time
	if exec_time > 1000000:
		exec_time = exec_time/1000000.00
	elif exec_time > 1000:
		exec_time = exec_time/1000.00
	#print "exec_time: " + str(exec_time)

	#compute energy consumption
	df = pd.read_csv("../power_prediction/data/power_stats/" + str(job) + "_ncores12_power_stats.csv")
	df['actual_energy'] = df['mean_power'] * exec_time
	df['projected_energy'] = df[_field] * exec_time
	if args.scheduler == "worst":
		df = df.sort_values('projected_energy', ascending=False)
	else:
		df = df.sort_values('projected_energy')
	#check if job id is already present and create new id if so
	_job = job
	id_cnt = 1
	while job in job_energy:
		job = _job + "_" + str(id_cnt)
		id_cnt += 1
	#get min energy consumption in order to order the energy needs of each job
	job_energy[job] = df['projected_energy'].iloc[0]	
	#print df
	#print df['actual_energy'].head(njobs)
	job_df[job] = df

job_energy_sorted = list()
#if args.scheduler == "random_optimal":
	#job_energy_sorted = job_energy.items()
#	job_energy_sorted = sorted(job_energy.items(), key=operator.itemgetter(1), reverse=True)
#else:
#	job_energy_sorted = sorted(job_energy.items(), key=operator.itemgetter(1), reverse=True)
job_energy_sorted = sorted(job_energy.items(), key=operator.itemgetter(1), reverse=True)
#print len(job_energy_sorted)

total_consumption=0
for job in job_energy_sorted:
	#get ordered nodelist
	df = job_df[job[0]]
	nodes = df['hostname']	
	#try to reserve first available node with minimun energy consumption
	for node in nodes:
		if args.scheduler == 'random':
			node = nodes.iloc[random.randint(0, 255)]
		elif args.scheduler == 'random_optimal':
			node = nodes.iloc[random.randint(0, len(job_energy_sorted)-1)]
		print_v1("..." + job[0] + " trying to reserve node " + node + "...")
		if node in reserved_nodes:
			continue
		reserved_nodes.add(node)
		#get the actual energy consumption
		energy_consumption = df[df['hostname'] == node]['actual_energy'].iloc[0]
		print_v1("* running " + job[0] + " on " + node + " consumed " + str(energy_consumption) + "J")
		if args.save_allocation:
			allocation_file.write(job[0] + "," + node + "," + str(energy_consumption) + "\n")
		total_consumption += energy_consumption
		break

print "total energy consumption with " + args.scheduler + " scheduling policy: " + str(total_consumption) + "J"

