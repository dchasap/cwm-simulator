# cwm-simulator
A SLURM-like cluster workload manager simulator, for evaluating power-aware scehduling policies.

Simulator allows users to implement their own policies and test them.  Also offers scripts
to create their job load, given a number of jobs.  The simulator uses application traces
to run jobs (it does not actually run them).


* Configuration Files: The following files need to created in the conf directory.
	These files describe the workload, the cluster and workload manager settings.
	
	- job-list: a list of jobs that are to be used to create a workload (used to generate
	  traffic, not by the simulator itself).  Should contain tuplets of job name and
    estimated execution time  

	- host-list: a list of host machine names

  - job_profiler_manager.txt: setting options for job profiler (job profiler is used to
    process traces and make power predictions for jobs)

  - sched_default.txt: settings for scheduler options

  - traffic: a file describing traffic, used by gen_traffic.py to replay a traffic
    scenario and send jobs to be scheduled by the simulator. 

* Application Traces and predictions:
	Traces are list of tuplets that contain a timestamp and a corresponding power consumption.
	The actual exectuion time is also required.  Example traces and statistics used for making
	power predictions can be found in the data/ folder. 


* Generate/Replay Traffic: ./script/gen_traffic.py
	This script is used to either generate a traffic load and/or replay it to send jobs to
  the simulator for execution.  Without replaying, users need to manually send jobs.  See 
	.scripts/gen_traffic.py --help for more information.


* Simulator: ./scripts/cluster_simulator.py
  Accepts incoming jobs and schedules them according to user implemented scheduling
  policies (some already implemented examples exist in scripts).   Terminates when "quit"
	job is received. 
  See ./scripts/cluster_simulator.py --help for more information.


An example of running the simulator with a given pre-generated traffic file.

./scripts/gen_traffic.py 	--job-list ./conf/jobs.txt \
													--time-unit 1 \
													--replay-traffic \
													--traffic-infile ./conf/traffic_bursty.txt | \
./scripts/cluster_simulator.py \
													--host-list ./conf/hosts.txt \
																	-v --time-unit 1 \
																	--enable-statistics \
																	--stats-log stats.txt \
																	--stats-trace-file stats_trace.csv \
																	--log log.txt \
																	--scheduler naive \ # this is SLURM extended 
																	--global-power-budget 20000 \
																	--time-limit 3600 \
																	--job-power-cap 120


