import random
import numpy
import pandas as pd

#class PredictionModel:

#	def __init__(self):
#		self.prediction

#class PMCsPredictionModel(PredictionModel):

#class LazyPredictionModel(PredictionModel):	

####

data_dir = "/p/lscratchh/dchasap/data"

class JobProfile:
	
	def __init__(self):
		self.exec_time = dict()
		self.power_trace = dict()
		self.power_stats = dict()
		self.power_peak = dict() #used for faster access than data frames
		self.power_min = dict()
		self.power_per_node = dict()
		self.sorted_hosts = dict()
		
		self.power_pred_stats = dict()
		self.power_pred_trace = dict()
		self.power_pred_peak = dict() #used for faster access than data frames
		self.power_pred_min = dict()
		self.sorted_pred_hosts = dict()

#####

#
class JobProfilerManager:

	def __init__(self, hosts, config_file, power_profiles):
		self.hosts = hosts
		self.power_prof = dict()
		for power_index in power_profiles:
			self.power_prof[power_index] = JobProfile()

		self.uncapped_power_stats = dict()
		
		self.node_power_variation = dict()
		self.load_conf(config_file)
		self.prediction_model ='none'
		#pre load test job data
		#self.add_job("ferret", power_profiles)
		self.add_job(self.test_job, 1, 200, power_profiles)
		self.power_budgets = power_profiles
#		self.compute_node_variation()
		#self.job_added = False
	
	#
	def load_conf(self, config_file):
		conf = open(config_file, 'r')
		for line in conf:
			if line.startswith("test_node"):
				val = line.split()
				self.test_node = val[1]
			elif line.startswith("test_job"):
				val = line.split()
				self.test_job = val[1]
	
	#FIXME: maybe extra info not required
	def add_job(self, job, job_req_hosts, job_est_time, power_budgets):
	
		self.job_added = True

		#get power and performance stats for all power profiles
		for power_index in power_budgets:

			#clear floating point digits from power cup
			#power_index_str = str(int(power_index))
			power_index_str = 'none'
			#fetch exec time
			if not job in self.power_prof[power_index].exec_time:
				self.power_prof[power_index].exec_time[job] = dict()
				self.power_prof[power_index].power_trace[job] = dict()
				self.power_prof[power_index].power_peak[job] = dict() #used for faster access
				total_energy = list()
				mean_power = list()
				max_power = list()
				min_power = list()
				for host in self.hosts:
					node = host.split("_")
					#get execution time
					df = pd.read_csv(data_dir + "/perf_stats/PERF_" + node[0] + "_" + job + "_ncores12_pl" + power_index_str + "_socket" + node[1] + "_iter0.csv")
					#FIXME: we cannot deal with missing data points
					if df.shape[0] == 0:
						#print "Warning! Could not find perf stats for " + job + " when run on " + host + "@" + power_index_str + "W - switching to estimated time"
						#exit(0)
						self.power_prof[power_index].exec_time[job][host] = float(job_est_time)
					else:	
						self.power_prof[power_index].exec_time[job][host] = df['time'].iloc[0]
					#get power stats
					df = pd.read_csv(data_dir + "/component_stats/PKG_POWER_" + node[0] + "_" + job + "_ncores12_pl" + power_index_str + "_socket" + node[1] + "_iter0.csv")
					#self.power_prof[power_index].exec_time[job][host] = df.shape[0]
					#self.power_prof[power_index].exec_time[job][host] = exec_time_prof[job][power_index]
					self.power_prof[power_index].power_trace[job][host] = df
					total_energy.append(df['PKG_POWER'].sum())
					mean_power.append(df['PKG_POWER'].mean())
					max_power.append(df['PKG_POWER'].max())
					min_power.append(df['PKG_POWER'].min())
					self.power_prof[power_index].power_peak[job][host] = df["PKG_POWER"].max()
				
				stats_data = [("hostname", self.hosts), ("total_energy", total_energy), ("mean_power", mean_power), ("max_power", max_power), ("min_power", min_power)]
				stats_df = pd.DataFrame.from_items(stats_data)
				self.power_prof[power_index].power_stats[job] = stats_df
	
		#print self.power_prof["none"].exec_time["ferret"]["catalyst45_0"]
		#print self.power_prof["80"].exec_time["ferret"]["catalyst45_0"]
		#print self.power_prof["70"].exec_time["ferret"]["catalyst45_0"]
		self.uncapped_power_stats = self.power_prof[115.0].power_stats
		#get power prediction stats
		power_index = 115.0
		#power_index_str = str(int(power_index))
		power_index_str = 'none'
#		if not job in self.power_prof[power_index].power_pred_stats:
#			df = pd.read_csv("../power_prediction/data/power_stats/" + job + "_ncores12_pl" + power_index + "_power_stats.csv")
#			print df
#			exit()
#			self.power_prof[power_index].power_stats_df[job] = df
		if self.prediction_model != "none" and job != self.test_job:
	
			if not job in self.power_prof[power_index].power_pred_trace:
				self.power_prof[power_index].power_pred_trace[job] = dict()
				self.power_prof[power_index].power_pred_peak[job] = dict() #if power_trace_df does not contain job, then neither power_peak_d does
				total_energy = list()
				mean_power = list()
				max_power = list()
				min_power = list()
		
				for host in self.hosts:
					node = host.split("_")
					df = pd.read_csv(data_dir + "/power_stats/" + job  + "/" + self.prediction_model + "_" + job + "_" + node[0] + "_ncores12_pl" + power_index_str + "_socket" + node[1] + ".csv")
					self.power_prof[power_index].power_pred_trace[job][host] = df #maybe clean this up a bit of unnecessary fields
					total_energy.append(df["prediction"].sum())
					mean_power.append(df['prediction'].mean())
					max_power.append(df['prediction'].max())
					min_power.append(df['prediction'].min())
					self.power_prof[power_index].power_pred_peak[job][host] = df["prediction"].max() #for faster access
					
				stats_data = [("hostname", self.hosts), ("total_energy", total_energy), ("mean_power", mean_power), ("max_power", max_power), ("min_power", min_power)]
				stats_df = pd.DataFrame.from_items(stats_data)
				self.power_prof[power_index].power_pred_stats[job] = stats_df	

####
	def get_test_host(self):
		return self.test_node

	def get_test_job(self):
		return self.test_job

	def set_prediction_model(self, prediction_model):
		self.prediction_model = prediction_model
			
	#			
#	def compute_node_variation(self): 
#		test_host = self.hosts[0]
#		hosts = self.hosts[1:]
#		node = test_host.split('_')
	
		#load test host data
#		df = pd.read_csv("../power_prediction/data/power_stats/" + self.test_job + "_ncores12_pl" + "115" + "_power_stats.csv")
#		df['variation'] = df['mean_power'] / df.loc[df['hostname'] == self.test_node]['mean_power'].iloc[0]

#		for host in self.hosts:
#			self.node_power_variation[host]	= df.loc[df["hostname"] == host]["variation"].iloc[0]
	
	#
#	def get_node_variation(self, host):
#		return self.node_power_variation[host]

	#get execution time from performance data file
	def get_execution_time(self, job, host, power_index):
		return self.power_prof[power_index].exec_time[job][host]

	#compute energy consumption
	def get_energy_consumption(self, job, host, power_index):
		#FIXME: shouldn't this also support total_pred_energy, depending on scheduler?
		df = self.power_prof[power_index].power_stats[job] 
		return df.loc[df['hostname'] == host]["total_energy"].iloc[0]

	#
	def get_current_power(self, job, host, time, power_index):
		df = self.power_prof[power_index].power_trace[job][host]
		if time < len(df['PKG_POWER']):
			return df['PKG_POWER'].iloc[int(time)]
		return 0.0

	#
	def sort_hosts(self, hosts, job, by_field, ascending, power_index, use_prediction):
		
		if use_prediction:
			if job in self.power_prof[power_index].sorted_pred_hosts:
				_hosts = self.power_prof[power_index].sorted_pred_hosts[job]
			else:
				df = self.power_prof[power_index].power_pred_stats[job]
				df = df.sort_values(by_field, ascending=ascending)
				_hosts = self.power_prof[power_index].sorted_pred_hosts[job] = df["hostname"].tolist()
		else:
			if job in self.power_prof[power_index].sorted_hosts:
				_hosts = self.power_prof[power_index].sorted_hosts[job]
			else:
				df = self.power_prof[power_index].power_stats[job]
				df = df.sort_values(by_field, ascending=ascending)
				_hosts = self.power_prof[power_index].sorted_hosts[job] = df["hostname"].tolist()
 		
		#return list(set(hosts).intersection(_hosts))
		return filter(set(hosts).__contains__, _hosts) #this seems to be slightly faster

	#
	#def sort_hosts_by_prediction(self, hosts, job, by_field, ascending, power_index):
	#	df = self.power_prof[power_index].power_pred_stats[job]
		#get only the requested nodes
	#	df = df[df["hostname"].isin(hosts)]
		#sort to find best nodes	
	#	df = df.sort_values(by_field, ascending=ascending)
		#self.power_stats_df[job] = df
	#	return df["hostname"].tolist()


	#
	def sort_jobs(self, jobs, by_field, reverse, power_index, use_prediction):

		if not self.job_added: return jobs #fix to only calculate when new job is added to ready queue, to gain performance
		#print "recalc job order" 
		#print "normal"
		self.job_added = False

		if not jobs: return jobs

		power_stats = list()
		for job in jobs:
			if use_prediction:				
				if job not in self.power_prof[power_index].power_pred_min:
					df = self.power_prof[power_index].power_pred_stats[job.name]
					self.power_prof[power_index].power_pred_min[job] = df[by_field].min()
				#sort to find best nodes	
				power_stats.append(self.power_prof[power_index].power_pred_min[job])
			else:
				if job not in self.power_prof[power_index].power_min:
					df = self.uncapped_power_stats[job.name]
					self.power_prof[power_index].power_min[job] = df[by_field].min()
				#sort to find best nodes	
				power_stats.append(self.power_prof[power_index].power_min[job])

		power_stats, jobs = (list(t) for t in zip(*sorted(zip(power_stats, jobs), reverse=reverse)))
		return jobs

	
	#
	#def sort_jobs_by_prediction(self, jobs, by_field, reverse, power_index):

	#	if not self.job_added: return jobs #fix to only calculate when new job is added to ready queue, to gain performance
	#	self.job_added = False

	#	if not jobs: return jobs

	#	power_stats = list()
	#	for job in jobs:
	#		df = self.power_prof[power_index].power_pred_stats[job.name]
			#sort to find best nodes	
	#		power_stats.append(df[by_field].min())
	
	#	power_stats, jobs = (list(t) for t in zip(*sorted(zip(power_stats, jobs), reverse=reverse)))
	#	return jobs


	#
	def sort_jobs_by_node(self, jobs, node, by_field, reverse, power_index):

		if not self.job_added: return jobs #fix to only calculate when new job is added to ready queue, to gain performance
		self.job_added = False

		if not jobs: return jobs

		power_stats = list()
		for job in jobs:
			df = self.uncapped_power_stats[job.name]
			#sort to find best nodes	
	
			if job not in self.power_prof[power_index].power_per_node:
					self.power_prof[power_index].power_per_node[job] = dict()

			if node not in self.power_prof[power_index].power_per_node[job]:
					df = self.uncapped_power_stats[job.name]
					self.power_prof[power_index].power_per_node[job][node] = df.loc[df["hostname"] == node][by_field].iloc[0]
				#sort to find best nodes	
			#power_stats.append(df.loc[df["hostname"] == node][by_field].iloc[0])
			power_stats.append(self.power_prof[power_index].power_per_node[job][node])


		power_stats, jobs = (list(t) for t in zip(*sorted(zip(power_stats, jobs), reverse=reverse)))
		return jobs

	#
	def get_min(self, job, by_field, power_index):
		df = self.power_prof[power_index].power_stats[job]
		return df[by_field].min()

	#
	def get_power_peak(self, job, host, power_index, use_prediction):
		if use_prediction:
			return self.power_prof[power_index].power_pred_peak[job][host]
		else:
			return self.power_prof[power_index].power_peak[job][host]
	
	#
	def get_available_power_cap(self, power_cap):
	
		#print "desired power_cap: " + str(power_cap)	
		#power profiles should be ordered!!!
		self.power_budgets.sort();
		#power_cap_choice = 115
		for available_power_cap in self.power_budgets:
			if power_cap < available_power_cap + 5: #a couple of vats will not hurt
				break
	
		#print "available power cap: " + str(available_power_cap)
		return available_power_cap



#####

class Job:

	def __init__(self, name, req_nodes, est_time, pid):
		self.name = name
		self.pid = pid
		self.elapsed_time = self.total_exec_time = 0
		self.est_time = float(est_time)
		self.req_hosts = int(req_nodes)
		self.running = False
		self.power_bound = False
		self.power_cap = 115.0
		self.power_peak = 115.0 #conservative set to max 
		self.wait_time = 0

	def progress(self, time):
		if self.running:
			#print "progress"
			self.elapsed_time -= time
		else:
			#print "wait"
			self.wait_time += time

	def get_progress(self):
		#print self.name + ":" + str(self.total_exec_time)
		return self.total_exec_time - self.elapsed_time

	def get_elapsed_time(self):
		return self.elapsed_time

	def get_est_time(self):
		return self.est_time

	def get_waiting_time(self):
		return self.waiting_time

	def get_total_execution_time(self):
		return self.total_exec_time

	def set_power_budget(self, power_cap, bound_exec_time):
		self.power_bound = True
		self.power_cap = power_cap
		self.elapsed_time = self.total_exec_time = bound_exec_time

	def set_power_peak(self, power_peak):
		self.power_peak = power_peak

	def get_power_budget(self):
		return self.power_cap

	def is_power_bound(self):
		return self.power_bound


