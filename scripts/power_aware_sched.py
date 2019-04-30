import random
from common import Job
from common import JobProfilerManager
import pandas as pd
import operator
from scheduler import Scheduler

#optimal scheduling policy 
class PowerAwareOptimalScheduler(Scheduler):

	def run(self):
	
		self.ready_queue = self.jp.sort_jobs(self.ready_queue, "max_power", True)

		for job in self.ready_queue:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			self.available_hosts = self.jp.sort_hosts(self.available_hosts, job.name, "max_power", True)
			#try to reserve first available node with lower power peak
			host = self.available_hosts.pop(0)
			self.ready_queue.remove(job)
			job.running = True
			self.running_jobs[job] = host
			self.log.write("* running " + job.name + " on " + host + "\n") 


#worst scheduling policy 
class PowerAwareWorstScheduler(Scheduler):

	def run(self):
	
		self.ready_queue = self.jp.sort_jobs(self.ready_queue, "max_power", False)

		for job in self.ready_queue:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			self.available_hosts = self.jp.sort_hosts(self.available_hosts, job.name, "max_power", False)
			#try to reserve first available node with lower power peak
			host = self.available_hosts.pop(0)
			self.ready_queue.remove(job)
			job.running = True
			self.running_jobs[job] = host
			self.log.write("* running " + job.name + " on " + host + "\n") 


#Prediction energy aware scheduling policy 
class PowerAwarePredictionScheduler(Scheduler):

	def run(self):
	
		self.ready_queue = self.jp.sort_jobs(self.ready_queue, "max_pred_power", True)

		for job in self.ready_queue:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			self.available_hosts = self.jp.sort_hosts(self.available_hosts, job.name, "max_pred_power", True)
			#try to reserve first available node with lower power peak
			host = self.available_hosts.pop(0)
			self.ready_queue.remove(job)
			job.running = True
			self.running_jobs[job] = host
			self.log.write("* running " + job.name + " on " + host + "\n") 

