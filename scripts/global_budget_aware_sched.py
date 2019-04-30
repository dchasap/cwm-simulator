import random
from common import Job
from common import JobProfilerManager
import pandas as pd
import operator
from scheduler import Scheduler
from time import time

# base class for budget aware algorithms
class BudgetAwareScheduler(Scheduler):

	def load_conf(self, config_file):
		conf = open(config_file, 'w')
		for line in conf:
			pass #TODO:implement parsing for future options

	def set_power_budget(self, power_budget):
		self.power_budget = power_budget
		try:
			self.power_limit = float(power_budget)
		except ValueError:
			print "Error: Budget aware scheduling requires a power budget set!"
			exit()

#optimal scheduling policy 
class BudgetAwareOptimalScheduler(BudgetAwareScheduler):

	def run(self):
		
		self.ready_queue = self.jp.sort_jobs(self.ready_queue, "max_power", True, "none")

		for job in self.ready_queue:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			#check if there is any node that meets the budget criteria
			if float(self.jp.get_min(job.name, "max_power", "none")) > self.power_limit:
				self.log.write("* job " + job.name + " cannot meet budget constrains, running under power constraint!\n")
				#remove job without running it
				#job = self.ready_queue.pop(0)
				#job.set_power_budget(self.power_budget, self.jp.get_execution_time(job.name, host, self.power_budget)) 
				#continue
		
			self.available_hosts = self.jp.sort_hosts(self.available_hosts, job.name, "max_power", True, "none")
			#check if there are any available nodes left
			host_found = False	
			for host in self.available_hosts:
				#try to reserve first available node with lower power peak
				power_peak = self.jp.get_power_peak(job.name, host, "none")
				if float(power_peak) < self.power_limit:
					self.available_hosts.remove(host)
					self.ready_queue.remove(job)
					#job.set_power_budget(self.power_budget, self.jp.get_execution_time(job.name, self.power_budget)) 
					#job.set_power_budget(self.power_budget, self.jp.get_execution_time(job.name, "none")) 
					job.set_power_budget("none", self.jp.get_execution_time(job.name, host, "none")) 
					job.running = True
					self.running_jobs[job] = host
					host_found = True
					self.log.write("* running " + job.name + " on " + host + "\n")
					break 
				else: #since  
					self.available_hosts.remove(host)
					self.ready_queue.remove(job)
					job.set_power_budget(self.power_budget, self.jp.get_execution_time(job.name, host, self.power_budget)) 
					job.running = True
					self.running_jobs[job] = host
					host_found = True
					self.log.write("* running " + job.name + " on " + host + " under power constrains\n")
					break 	

			if not host_found:
				self.log.write("* job " + job.name + " could not be scheduled at this moment\n")

#prediction scheduling policy 
class BudgetAwarePredictionScheduler(BudgetAwareScheduler):

	def run(self):
		
		self.ready_queue = self.jp.sort_jobs_by_prediction(self.ready_queue, "max_power", True, "none")

		for job in self.ready_queue:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			#check if there is any node that meet the budget criteria
			if float(self.jp.get_min(job.name, "max_power", "none")) > self.power_limit: #FIX: use prediction here
				self.log.write("* job " + job.name + " cannot meet budget constrains, running under power constraint!\n")
		
			#self.available_hosts = self.jp.sort_hosts(self.available_hosts, job.name, "max_pred_power", True, "none")
			self.available_hosts = self.jp.sort_hosts_by_prediction(self.available_hosts, job.name, 'max_power', True, "none")
			#check if there are any available nodes left
			host_found = False	
			for host in self.available_hosts:
				#try to reserve first available node with lower power peak
				power_peak = self.jp.get_power_peak_prediction(job.name, host, "none")
				if float(power_peak) < self.power_limit:
					self.available_hosts.remove(host)
					self.ready_queue.remove(job)
					job.set_power_budget("none", self.jp.get_execution_time(job.name, host, "none")) 
					job.running = True
					self.running_jobs[job] = host
					host_found = True
					self.log.write("* running " + job.name + " on " + host + "\n")
					power_peak = self.jp.get_power_peak(job.name, host, "none")
					if power_peak > self.power_limit:
						self.log.write("* WARNING! Actual power peak is " + str(power_peak)  + "\n")
					break
				else: #since  
					self.available_hosts.remove(host)
					self.ready_queue.remove(job)
					job.set_power_budget(self.power_budget, self.jp.get_execution_time(job.name, host, self.power_budget)) 
					job.running = True
					self.running_jobs[job] = host
					host_found = True
					self.log.write("* running " + job.name + " on " + host + " under power constrains\n")
					break 	


			if not host_found:
				self.log.write("* job " + job.name + " could not be scheduled at this moment\n")
#####

#speculate scheduling policy 
class BudgetAwareLazyPredictionScheduler(BudgetAwareScheduler):

	def run(self):
	
		host = "catalyst45_0"	
		self.ready_queue = self.jp.sort_jobs_by_node(self.ready_queue, host, "max_power", True, "none")
		self.available_hosts = self.jp.sort_hosts(self.available_hosts, "swaptions", "max_power", True, "none")

		for job in self.ready_queue:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			#TODO: no handling of jobs	
			#check if there is any node that meet the budget criteria
			#self.jp.get_power_peak(job.name, host, "max_power")
			#if float() > float(self.power_budget):
			#	self.log.write("* job " + job.name + " cannot meet budget constrains!\n")
			#	#remove job without running it
			#	job = self.ready_queue.pop(0)
			#	continue
		
			#check if there are any available nodes left
			power_peak = self.jp.get_power_peak(job.name, host, "none")
			host_found = False	
			for host in self.available_hosts:
				power_variation = self.jp.get_node_variation(host)
				est_power_peak = power_peak * power_variation
				#try to reserve first available node with lower power peak
				if float(est_power_peak) < self.power_limit:
					self.available_hosts.remove(host)
					self.ready_queue.remove(job)
					job.set_power_budget("none", self.jp.get_execution_time(job.name, host, "none")) 
					job.running = True
					self.running_jobs[job] = host
					host_found = True
					self.log.write("* running " + job.name + " on " + host + "\n")
					real_power_peak = self.jp.get_power_peak(job.name, host, "none")
					if real_power_peak > self.power_limit:
						self.log.write("* WARNING! Actual power peak is " + str(real_power_peak)  + "\n")
					break
				else: #since  
					self.available_hosts.remove(host)
					self.ready_queue.remove(job)
					job.set_power_budget(self.power_budget, self.jp.get_execution_time(job.name, host, self.power_budget)) 
					job.running = True
					self.running_jobs[job] = host
					host_found = True
					self.log.write("* running " + job.name + " on " + host + " under power constrains\n")
					break 	


			if not host_found:
				self.log.write("* job " + job.name + " could not be scheduled at this moment\n")
#####

#speculate scheduling policy 
class BudgetAwareSpeculationScheduler(BudgetAwareScheduler):

	def run(self):
	
		host = "catalyst45_0"	
		self.ready_queue = self.jp.sort_jobs_by_node(self.ready_queue, host, "max_power", True, "none")
		self.available_hosts = self.jp.sort_hosts(self.available_hosts, "swaptions", "max_power", True, "none")

		for job in self.ready_queue:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			#check if there is any node that meet the budget criteria
			if float(self.jp.get_power_peak(job.name, host, "none")) > self.power_limit:
				self.log.write("* job " + job.name + " cannot meet budget constrains!\n")
				#remove job without running it
				job = self.ready_queue.pop(0)
				continue
		
			#check if there are any available nodes left
			power_peak = self.jp.get_power_peak(job.name, host, "none")
			host_found = False	
			for host in self.available_hosts:
				#try to reserve first available node with lower power peak
				if float(power_peak) < self.power_limit:
					self.available_hosts.remove(host)
					self.ready_queue.remove(job)
					job.set_power_budget("none", self.jp.get_execution_time(job.name, host, "none")) 
					job.running = True
					self.running_jobs[job] = host
					host_found = True
					self.log.write("* running " + job.name + " on " + host + "\n")
					real_power_peak = self.jp.get_power_peak(job.name, host, "none")
					if real_power_peak > self.power_limit:
						self.log.write("* WARNING! Actual power peak is " + str(real_power_peak)  + "\n")
					break
				else: #since  
					self.available_hosts.remove(host)
					self.ready_queue.remove(job)
					job.set_power_budget(self.power_budget, self.jp.get_execution_time(job.name, host, self.power_budget)) 
					job.running = True
					self.running_jobs[job] = host
					host_found = True
					self.log.write("* running " + job.name + " on " + host + " under power constrains\n")
					break 	


			if not host_found:
				self.log.write("* job " + job.name + " could not be scheduled at this moment\n")
#####


#Prediction energy aware scheduling policy
class BudgetAwareWorstScheduler(BudgetAwareScheduler):

	def run(self):
		
		jobs = self.jp.sort_jobs(self.ready_queue, "max_power", False, "none")

		for job in jobs:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			#check if there is any node that meet the budget criteria
			if self.jp.get_min(job.name, "max_power", "none") > self.power_limit:
				self.log.write("* job " + job.name + " cannot meet budget constrains!\n")
				#remove job without running it
				job = self.ready_queue.pop(0)
				continue
		
			self.available_hosts = self.jp.sort_hosts(self.available_hosts, job.name, "max_power", False, "none")
			#check if there are any available nodes left
			for host in self.available_hosts:
				#try to reserve first available node with lower power peak
				host_found = False	
				power_peak = self.jp.get_power_peak(job.name, host, "max_power", "none")
				if power_peak < self.power_limit:
					self.available_hosts.remove(host)
					job = self.ready_queue.pop(0)
					job.set_power_budget(self.power_budget, self.jp.get_execution_time(job.name, self.power_budget)) 
					job.running = True
					self.running_jobs[job] = host
					host_found = True
					self.log.write("* running " + job.name + " on " + host + "\n")
					break 
		
			if not host_found:
				self.log.write("* job " + job[0].name + " could not be scheduled at this moment\n")

		
