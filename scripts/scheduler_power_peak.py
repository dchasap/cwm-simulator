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
	allocation_file.write("app,node,power\n")

njobs = len(args.jobs)
#print njobs
job_df = dict()
job_power = dict()
reserved_nodes = set()

_field='max_power'
if args.scheduler == 'prediction':
	_field='max_pred_power' 

for job in args.jobs:
	#compute energy consumption
	df = pd.read_csv("../power_prediction/data/power_stats/" + str(job) + "_ncores12_power_stats.csv")
	df['actual_power'] = df['max_power']
	df['projected_power'] = df[_field]
	if args.scheduler == "worst":
		df = df.sort_values('projected_power', ascending=False)
	else:
		df = df.sort_values('projected_power')
	#check if job id is already present and create new id if so
	_job = job
	id_cnt = 1
	while job in job_power:
		job = _job + "_" + str(id_cnt)
		id_cnt += 1
	#get min energy consumption in order to order the energy needs of each job
	job_power[job] = df['projected_power'].iloc[0]	
	#print df
	#print df['actual_energy'].head(njobs)
	job_df[job] = df

job_power_sorted = list()
#if args.scheduler == "random_optimal":
	#job_energy_sorted = job_energy.items()
#	job_energy_sorted = sorted(job_energy.items(), key=operator.itemgetter(1), reverse=True)
#else:
#	job_energy_sorted = sorted(job_energy.items(), key=operator.itemgetter(1), reverse=True)
job_power_sorted = sorted(job_power.items(), key=operator.itemgetter(1), reverse=True)
#print len(job_energy_sorted)

peak_power=0
for job in job_power_sorted:
	#get ordered nodelist
	df = job_df[job[0]]
	nodes = df['hostname']	
	#try to reserve first available node with minimun energy consumption
	for node in nodes:
		if args.scheduler == 'random':
			node = nodes.iloc[random.randint(0, 255)]
		elif args.scheduler == 'random_optimal':
			node = nodes.iloc[random.randint(0, len(job_power_sorted)-1)]
		print_v1("..." + job[0] + " trying to reserve node " + node + "...")
		if node in reserved_nodes:
			continue
		reserved_nodes.add(node)
		#get the actual energy consumption
		power_consumption = df[df['hostname'] == node]['actual_power'].iloc[0]
		print_v1("* running " + job[0] + " on " + node + " consumed " + str(power_consumption) + "J")
		if args.save_allocation:
			allocation_file.write(job[0] + "," + node + "," + str(power_consumption) + "\n")
		peak_power += power_consumption 
		#if peak_power < power_consumption:
		#	peak_power = power_consumption 
		break

print "peak power consumption with " + args.scheduler + " scheduling policy: " + str(peak_power) + "J"

