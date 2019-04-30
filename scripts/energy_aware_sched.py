import random
from common import Job
from common import JobProfilerManager
import pandas as pd
import operator
from scheduler import Scheduler

#optimal scheduling policy 
class EnergyAwareOptimalScheduler(Scheduler):

	def run(self):
	
		self.ready_queue = self.jp.sort_jobs(self.ready_queue, "total_energy", True)

		for job in self.ready_queue:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			self.available_hosts = self.jp.sort_hosts(self.available_hosts, job.name, "total_energy", True)
			#try to reserve first available node with lower power peak
			host = self.available_hosts.pop(0)
			self.ready_queue.remove(job)
			job.running = True
			self.running_jobs[job] = host
			self.log.write("* running " + job.name + " on " + host + "\n") 

#worst scheduling policy 
class EnergyAwareWorstScheduler(Scheduler):

	def run(self):
	
		self.ready_queue = self.jp.sort_jobs(self.ready_queue, "total_energy", False)

		for job in self.ready_queue:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			self.available_hosts = self.jp.sort_hosts(self.available_hosts, job.name, "total_energy", False)
			#try to reserve first available node with lower power peak
			host = self.available_hosts.pop(0)
			self.ready_queue.remove(job)
			job.running = True
			self.running_jobs[job] = host
			self.log.write("* running " + job.name + " on " + host + "\n") 


#Prediction energy aware scheduling policy 
class EnergyAwarePredictionScheduler(Scheduler):

	def run(self):
	
		self.ready_queue = self.jp.sort_jobs(self.ready_queue, "total_pred_energy", True)

		for job in self.ready_queue:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			self.available_hosts = self.jp.sort_hosts(self.available_hosts, job.name, "total_pred_energy", True)
			#try to reserve first available node with lower power peak
			host = self.available_hosts.pop(0)
			self.ready_queue.remove(job)
			job.running = True
			self.running_jobs[job] = host
			self.log.write("* running " + job.name + " on " + host + "\n") 

