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

#optimal policy 
class BudgetAwareOptimalScheduler(BudgetAwareScheduler):

	def get_available_hosts(self, job):
		self.available_hosts = self.jp.sort_hosts(self.available_hosts, job.name, "max_power", True, 115.0, False)
		return self.available_hosts[:job.req_hosts]

	def get_power_peak(self, job, hosts):
		power_peak = 0
		for host in hosts:
			power_peak += self.jp.get_power_peak(job.name, host, 115.0, False)

		return power_peak


#prediction scheduling policy 
class BudgetAwarePredictionScheduler(BudgetAwareScheduler):

	def get_available_hosts(self, job):
		self.available_hosts = self.jp.sort_hosts(self.available_hosts, job.name, "max_power", True, 115.0, True)
		return self.available_hosts[:job.req_hosts]

	def get_power_peak(self, job, hosts):
		power_peak = 0
		for host in hosts:
			power_peak += self.jp.get_power_peak(job.name, host, 115.0, True)

		return power_peak

#speculate scheduling policy 
class BudgetAwareSpeculationScheduler(BudgetAwareScheduler):

	def get_available_hosts(self, job):
		if self.ordered_hosts_enabled:
			self.available_hosts = self.jp.sort_hosts(self.available_hosts, self.jp.get_test_job(), "max_power", True, 115.0, False)

		return self.available_hosts[:job.req_hosts]


	def get_power_peak(self, job, hosts):
		power_peak = (self.jp.get_power_peak(job.name, self.jp.get_test_host(), 115.0, False) + self.estimation_error) * job.req_hosts 
		return power_peak

#####

