import random
from common import log_message
from common import get_execution_time
from common import Job
import pandas as pd
import operator
from scheduler import Scheduler

#optimal scheduling policy 
class PowerAwareOptimalScheduler(Scheduler):

	def run(self):

		njobs = len(self.ready_queue)
		job_df = dict()
		job_energy = dict()

		#sort nodes for all pending jobs
		_field='max_power'
		for job in self.ready_queue:		
			#compute energy consumption
			df = pd.read_csv("../power_prediction/data/power_stats/" + str(job.name) + "_ncores12_power_stats.csv")
			df['actual_power'] = df['max_power']
			df['projected_power'] = df[_field]
			#sort to find best nodes	
			df = df.sort_values('projected_power')
			#get min energy consumption in order to compute the energy needs of each job
			job_energy[job] = df['projected_power'].iloc[0]	
			job_df[job] = df

		job_energy_sorted = list()
		job_energy_sorted = sorted(job_energy.items(), key=operator.itemgetter(1), reverse=True)
		for job in job_energy_sorted:
			#get ordered nodelist
			df = job_df[job[0]]
			#check if there are any available nodes left
			if not self.available_hosts:
				log_message(self.log, self.verbose, "* all hosts are busy!\n")
				break
			#try to reserve first available node with minimun energy consumption
			nodes = df['hostname']	
			for node in nodes:
				#log_message(self.log, self.verbose, "..." + job[0].name + " trying to reserve node " + node + "...\n")
				if node in self.available_hosts:
					self.available_hosts.remove(node)
					job = self.ready_queue.pop(0)
					self.running_jobs[job] = node
					log_message(self.log, self.verbose, "* running " + job.name + " on " + node + "\n")
					return 


#worst scheduling policy 
class PowerAwareWorstScheduler(Scheduler):

	def run(self):

		job_df = dict()
		job_energy = dict()

		_field='max_power'
		for job in self.ready_queue:		
			#get execution time
			exec_time = get_execution_time(job.name)
			#compute energy consumption
			df = pd.read_csv("../power_prediction/data/power_stats/" + str(job.name) + "_ncores12_power_stats.csv")
			df['actual_power'] = df['max_power'] * exec_time
			df['projected_power'] = df[_field] * exec_time
			#sort to find best nodes	
			df = df.sort_values('projected_power', ascending=False)
			#get min energy consumption in order to compute the energy needs of each job
			job_energy[job] = df['projected_power'].iloc[0]	
			job_df[job] = df

		job_energy_sorted = list()
		job_energy_sorted = sorted(job_energy.items(), key=operator.itemgetter(1), reverse=True)
		for job in job_energy_sorted:
			#get ordered nodelist
			df = job_df[job[0]]
			#check if there are any available nodes left
			if not self.available_hosts:
				log_message(self.log, self.verbose, "* all hosts are busy!\n")
				break

			#try to reserve first available node with minimun energy consumption
			nodes = df['hostname']	
			for node in nodes:
				#log_message(self.log, self.verbose, "..." + job[0].name + " trying to reserve node " + node + "...\n")
				if node in self.available_hosts:
					self.available_hosts.remove(node)
					job = self.ready_queue.pop(0)
					self.running_jobs[job] = node
					#get the actual energy consumption
					log_message(self.log, self.verbose, "* running " + job.name + " on " + node + "\n")
					break

#Prediction energy aware scheduling policy 
class PowerAwarePredictionScheduler(Scheduler):

	def run(self):

		njobs = len(self.ready_queue)
		job_df = dict()
		job_energy = dict()

		_field='max_pred_power'
		for job in self.ready_queue:		
			#get execution time
			exec_time = get_execution_time(job.name)
			#compute energy consumption
			df = pd.read_csv("../power_prediction/data/power_stats/" + str(job.name) + "_ncores12_power_stats.csv")
			df['actual_power'] = df['max_power'] * exec_time
			df['projected_power'] = df[_field] * exec_time
			#sort to find best nodes	
			df = df.sort_values('projected_power')
			#get min energy consumption in order to compute the energy needs of each job
			job_energy[job] = df['projected_power'].iloc[0]	
			job_df[job] = df

		#check if there are any available nodes left
		if not self.available_hosts:
			log_message(self.log, self.verbose, "* all hosts are busy!\n")
			return

		job_energy_sorted = list()
		job_energy_sorted = sorted(job_energy.items(), key=operator.itemgetter(1), reverse=True)
		for job in job_energy_sorted:
			#get ordered nodelist
			df = job_df[job[0]]
			#check if there are any available nodes left
			if not self.available_hosts:
				log_message(self.log, self.verbose, "* all hosts are busy!\n")
				break
			#try to reserve first available node with minimun energy consumption
			nodes = df['hostname']	
			for node in nodes:
				#log_message(self.log, self.verbose, "..." + job[0].name + " trying to reserve node " + node + "...\n")
				if node in self.available_hosts:
					self.available_hosts.remove(node)
					job = self.ready_queue.pop(0)
					self.running_jobs[job] = node
					#get the actual energy consumption
					log_message(self.log, self.verbose, "* running " + job.name + " on " + node + "\n")
					break

