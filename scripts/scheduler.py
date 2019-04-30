import random
from common import Job
from common import JobProfilerManager
from stats import StatsLog 
import pandas as pd
import operator

class Scheduler:
	
	def __init__(self, name, hosts, job_profiler_manager, log, stats, verbose):
		self.ready_queue = list()
		self.running_jobs = dict()
		self.available_hosts = list(hosts)
		self.log = log
		self.verbose = verbose
		self.hosts_busy = False
		self.name = name	
		self.jp = job_profiler_manager
		#self.job_power_budget = 'none'
		self.job_power_cap = 115
		self.global_power_consumption = 0 # should be the sum of all idle power on the nodes
		self.ordered_hosts_enabled = False
		self.safety_capping_enabled = False
		self.queue_depth  = 10
		self.sched_interval = 60
		self.elapsed_time = 0
		self.force_sched = True
		self.stats = stats
		

	def load_confing(self, conf_file):
		conf = open(conf_file, 'r')
		for line in conf:
			if line.startswith("estimation_error"):
				val = line.split()
				self.estimation_error = float(val[1])
			elif line.startswith("queue_depth"):
				val = line.split()
				self.queue_depth = int(val[1])	
			elif line.startswith("sched_interval"):
				val = line.split()
				self.sched_interval = int(val[1])	
			#if line.startswith("safety_capping_enabled"):
			#	val = line.split()
			#	self.safety_capping_enabled = bool(safety_capping_enabled)

			#if line.startswith("ordered_hosts_enabled"):
			#	val = line.split()
			#	self.ordered_hosts_enabled = bool(ordered_hosts_enabled)

	
	def get_running_jobs(self):
		return self.running_jobs
	
	def get_pending_jobs(self):
		return self.ready_queue

	def get_hosts(self, job):
		return self.running_jobs[job]

	def job_finished(self, job):
		job.running = False
		self.hosts_busy = False
		hosts = self.running_jobs[job]
		for host in hosts:
			self.available_hosts.append(host)
		self.global_power_consumption -= job.power_peak
		del self.running_jobs[job]
		self.force_sched = True

	def drop_job(self, job):
		self.ready_queue.remove(job)
		self.stats.drop_job()	

	def busy(self):
		return len(self.ready_queue) or len(self.running_jobs)

	def push(self, job):
		self.log.write("* adding new job: " + str(job.pid) + " (" + job.name + ")\n")
		self.ready_queue.append(job)

	def set_job_power_budget(self, job_power_budget):
		self.job_power_cap = float(job_power_budget)

	def set_global_power_budget(self, global_power_budget):
		self.global_power_budget = global_power_budget

	def set_power_consumption(self, global_power_consumption):
		self.global_power_consumption = global_power_consumption

	def enable_ordered_hosts(self):
		self.ordered_hosts_enabled = True

	def disable_ordered_hosts(self):
		self.ordered_hosts_enabled = False

	def enable_safety_capping(self):
		self.safety_capping_enabled = True
	
	def disable_safety_capping(self):
		self.safety_capping_enabled = False

	def get_available_hosts(self, job):
		return self.available_hosts[:job.req_hosts]

	def get_power_peak(self, job, hosts):
		return self.job_power_cap * job.req_hosts

	def run(self):
		if (self.sched_interval < self.elapsed_time) or self.force_sched:
			self.force_sched = False
			self.elapsed_time = 0
			self._run()
		self.elapsed_time += 1

	def _run(self):
		#self.ready_queue = self.jp.sort_jobs(self.ready_queue, "max_power", True, 115.0, False)
		running_backfilling = False
		_ready_queue = self.ready_queue[:self.queue_depth]
		for job in _ready_queue:
		
			#check if there are any available nodes left
			if not self.available_hosts and not self.hosts_busy:
				self.log.write("* all hosts are busy!\n")
				self.hosts_busy = True
				break
			elif not self.available_hosts:
				break
	
			#check if there are enough available hosts for this job, if not start backfilling
			if running_backfilling:
				if critical_job.elapsed_time < job.elapsed_time:
					continue 	

			if len(self.available_hosts) < job.req_hosts:
				self.log.write("* not enough hosts available for job " + job.name + "(" + str(job.pid) + ")!\n")
				# backfilling, get N jobs closer to completion and get max_time from them
				nearing_completion_jobs = sorted(list(self.running_jobs.keys()), key=lambda x: x.elapsed_time)
				critical_job = nearing_completion_jobs[-1] #job with longest elapsed time
				running_backfilling = True
				continue #TODO: implement backfilling

			hosts = self.get_available_hosts(job)
			host_found = False	
			power_peak = self.get_power_peak(job, hosts)
			
			#first condition we need to meet is that we do not get over the global power budget
			if (float(power_peak) + float(self.global_power_consumption)) > float(self.global_power_budget):

				#drop job if it can never be scheduled
				if float(power_peak) > float(self.global_power_budget):
					self.log.write("* dropping job " + job.name + ", not enough power available system wide!\n")
					self.drop_job(job)
					continue
				#start power backfilling
				self.log.write("* cannot schedule " + job.name + " at this moment, will exceed global power budget!\n")
				nearing_completion_jobs = sorted(list(self.running_jobs.keys()), key=lambda x: x.elapsed_time)
				critical_job = nearing_completion_jobs[-1] #job with longest elapsed time
				running_backfilling = True
				continue
				#TODO: Power Backfilling
				#break

			if float(power_peak) <= (float(self.job_power_cap) * job.req_hosts):
				for host in hosts:
					self.available_hosts.remove(host)
				self.ready_queue.remove(job)
				job.set_power_budget(115.0, self.jp.get_execution_time(job.name, hosts[0], 115.0))
				job.set_power_peak(power_peak) 
				job.running = True
				self.running_jobs[job] = hosts
				host_found = True
				self.global_power_consumption += job.power_peak
				self.log.write("* running " + job.name + " on " + str(hosts) + "\n") 
				#break
			else:
				for host in hosts:  
					self.available_hosts.remove(host)
				self.ready_queue.remove(job)
				job.set_power_budget(self.job_power_cap, self.jp.get_execution_time(job.name, hosts[0], self.job_power_cap)) 
				job.set_power_peak(self.job_power_cap * job.req_hosts) 
				job.running = True
				self.running_jobs[job] = hosts
				host_found = True
				self.global_power_consumption += job.power_cap * job.req_hosts
				self.log.write("* running " + job.name + " on " + str(hosts) + " under power constrains\n")
				#break 	

			if not host_found:
				self.log.write("* job " + job.name + " could not be scheduled at this moment\n")



	#
	def __run(self):
		
		while len(self.ready_queue):		
			#get next job and required nodes
			job = self.ready_queue[0]
			num_nodes = job.req_hosts
			#if there are available hosts proceed
			if len(self.available_hosts) >= num_nodes:
				hosts = list()
				for n in range(num_nodes):
					hosts.append(self.available_hosts.pop(0))
				
				job = self.ready_queue.pop(0)
				job.running = True
				self.running_jobs[job] = hosts
				self.log.write("* running job " + str(job.pid) + " on " + str(hosts) + "\n")			
			elif self.hosts_busy:
				self.log.write("* not enough available hosts!\n")
				self.hosts_busy = True
				break

class SLURMScheduler(Scheduler):

	def get_available_hosts(self, job):
		return self.available_hosts[:job.req_hosts]

	def get_power_peak(self, job, hosts):
		return self.job_power_cap * job.req_hosts

