
from common import Job
from common import JobProfilerManager

class StatsLog:	
	 
	def __init__(self, job_profiler_manager, stats_file, trace_file):
		self.jp = job_profiler_manager
		self.stats = open(stats_file, "w")
		self.trace = open(trace_file, "w")
		self.total_running_time = 0.0
		self.jobs_issued = 0
		self.retired_jobs = 0
		self.total_energy_consumed = 0.0
		self.max_power = 0.0
		self.max_nodes_used = 0
		self.sum_running_jobs = 0.0
		self.trace.write("time,power,peak_power,usage\n")
		self.trace.flush()
		self.jobs = list()
		self.jobs_dropped = 0

	def inc_time(self):
		self.total_running_time += 1

	def retire_job(self, job, hosts):
		self.retired_jobs += 1
		for host in hosts:
			self.total_energy_consumed += self.jp.get_energy_consumption(job.name, host, job.power_cap)
		#print job.wait_time
		#self.jobs.append(job)
		#TODO: measure also the peak power reached


	def new_job(self, job):
		self.jobs_issued += 1
		self.jobs.append(job)

	def drop_job(self):
		self.jobs_dropped += 1

	def trace_stats(self, running_jobs):
		total_power = 0.0
		peak_power = 0.0
		total_busy_hosts = 0
		for job, hosts in running_jobs.items():
			progress = job.get_progress()
			total_job_power = 0
			for host in hosts:
				peak_power = float(self.jp.get_current_power(job.name, host, progress, job.power_cap))
				total_job_power += peak_power

			#get max power 
			if self.max_power < peak_power:
				self.max_power = peak_power
			
			total_power += total_job_power		

			total_busy_hosts += job.req_hosts

			if self.max_nodes_used < total_busy_hosts:
				self.max_nodes_used = total_busy_hosts

			self.sum_running_jobs = total_busy_hosts

		#print ("time,power,peak_power,usage\n")
		#print (str(self.total_running_time) + "," + str(total_power) + "," + str(self.max_power) + "," + str(self.sum_running_jobs) + "\n")
		self.trace.write(str(self.total_running_time) + "," + str(total_power) + "," + str(self.max_power) + "," + str(self.sum_running_jobs) + "\n")
		self.trace.flush()
		#print "peak power reached at " + str(peak_power) + " by running " + j1.name + " on " + h1 + " capped at " + j1.power_cap

	def finish(self):
		#compute job average waiting time
		total_wait_time = 0
		total_turnaround_time = 0
		product_turnaround_time = 0
		for job in self.jobs:
			total_wait_time += job.wait_time
			total_turnaround_time += job.total_exec_time + job.wait_time
			product_turnaround_time *= job.total_exec_time + job.wait_time
		average_wait_time = total_wait_time / len(self.jobs)
		average_turnaround_time = total_turnaround_time / len(self.jobs)
		gm_turnaround_time = product_turnaround_time ** (1/len(self.jobs))

		self.stats.write("total running time: " + str(self.total_running_time) + " sec\n")	
		self.stats.write("total issued  jobs: " + str(self.jobs_issued) + "\n")	
		self.stats.write("total completed  jobs: " + str(self.retired_jobs) + "\n")
		self.stats.write("total dropped  jobs: " + str(self.jobs_dropped) + "\n")
		self.stats.write("average job turnaround time: " + str(average_turnaround_time) + "\n")
		self.stats.write("geom mean job turnaround time: " + str(gm_turnaround_time) + "\n")
		self.stats.write("average job wait time: " + str(average_wait_time) + "\n")
		self.stats.write("total energy consumed: " + str(self.total_energy_consumed) + " Joule\n")	
		self.stats.write("average energy consumed: " + str(self.total_energy_consumed / self.total_running_time) + " Joule\n")	
		self.stats.write("max power reached: " + str(self.max_power) + " Watt\n")
		self.stats.write("average sys usage: " + str(((self.sum_running_jobs / self.total_running_time) / 256) * 100) + "%\n")
		self.stats.write("max sys usage reached: " + str((self.max_nodes_used / 256) * 100) + "%\n")

		self.stats.flush()
		self.stats.close()
		self.trace.close()

		if False:
			print ("total running time: " + str(self.total_running_time) + " sec\n")	
			print ("total issued  jobs: " + str(self.jobs_issued) + "\n")	
			print ("total completed  jobs: " + str(self.retired_jobs) + "\n")
			print ("total dropped  jobs: " + str(self.jobs_dropped) + "\n")
			print ("average job turnaround time: " + str(average_turnaround_time) + "\n")
			print ("average job wait time: " + str(average_wait_time) + "\n")
			print ("total energy consumed: " + str(self.total_energy_consumed) + " Joule\n")	
			print ("average energy consumed: " + str(self.total_energy_consumed / self.total_running_time) + " Joule\n")	
			print ("max power reached: " + str(self.max_power) + " Watt\n")
			print ("average sys usage: " + str(((self.sum_running_jobs / self.total_running_time) / 256) * 100) + "%\n")
			print ("max sys usage reached: " + str((self.max_nodes_used / 256) * 100) + "%\n")

