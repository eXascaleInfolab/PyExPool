# PyExPool

A Lightweight Multi-Process Execution Pool to schedule Jobs execution with *per-job timeout*, optionally grouping them into Tasks and specifying optional execution parameters considering NUMA architecture:

- automatic CPU affinity management and maximization of the dedicated CPU cache for a worker process
- minimal amount of RAM per a worker process
- automatic rescheduling of the worker processes and modification of their queue parameters on low memory condition for the in-RAM computations (requires [psutil](https://pypi.python.org/pypi/psutil), can be disabled)
- timeout per each Job (it was the main initial motivation to implement this module, because this feature is not provided by any Python implementation out of the box)
- onstart/ondone callbacks, ondone is called only on successful completion (not termination) for both Jobs and Tasks (group of jobs)
- stdout/err output, which can be redirected to any custom file or PIPE
- custom parameters for each Job and respective owner Task besides the name/id

> Automatic rescheduling of the workers on low memory condition for the in-RAM computations is an optional and the only feature that requires external package, namely [psutil](https://pypi.python.org/pypi/psutil).

Implemented as a *single-file module* to be *easily included into your project and customized as a part of your distribution* (like in [PyCaBeM](//github.com/eXascaleInfolab/PyCABeM)), not as a separate library.  
The main purpose of this single-file module is the **asynchronous execution of modules and external executables with cache / parallelization tuning and optional automatic rescheduler adjusting for the in-RAM computations**.  
In case asynchronous execution of the *Python functions* is required and usage of external dependences is not a problem, or automatic jobs scheduling for in-RAM computations is not required, then more handy and straightforward approach is to use [Pebble](https://pypi.python.org/pypi/Pebble) library.

\author: (c) Artem Lutov <artem@exascale.info>  
\organizations: [eXascale Infolab](http://exascale.info/), [Lumais](http://www.lumais.com/), [ScienceWise](http://sciencewise.info/)  
\date: 2016-01  

## Content
- [Dependencies](#dependencies)
- [API](#api)
	- [Job](#job)
	- [Task](#task)
	- [ExecPool](#execpool)
- [Usage](#usage)
	- [Usage Example](#usage-example)
	- [Failsafe Termination](#failsafe-termination)
- [Related Projects](#related-projects)

## Dependencies

[psutil](https://pypi.python.org/pypi/psutil) only in case of dynamic jobs queue adaption is required for the in-RAM computations, otherwise there are no any dependencies.  
To install `psutil`:
```
$ sudo pip install psutil
```

## API

Flexible API provides *automatic CPU affinity management, maximization of the dedicated CPU cache, limitation of the minimal dedicated RAM per a worker process, dynamic rescheduling of the worker processes on low memoty condition and optimization of their queue for the in-RAM computations*, optional automatic restart of jobs on timeout, access to job's process, parent task, start and stop execution time and more...  
`ExecPool` represents a pool of worker processes to execute `Job`s that can be grouped into `Tasks`s for more flexible management.

### Job

```python
Job(name, workdir=None, args=(), timeout=0, ontimeout=False, task=None, startdelay=0
, onstart=None, ondone=None, params=None, stdout=sys.stdout, stderr=sys.stderr, omitafn=False):
	"""Initialize job to be executed

	name  - job name
	workdir  - working directory for the corresponding process, None means the dir of the benchmarking
	args  - execution arguments including the executable itself for the process
		NOTE: can be None to make make a stub process and execute the callbacks
	timeout  - execution timeout. Default: 0, means infinity
	ontimeout  - action on timeout:
		False  - terminate the job. Default
		True  - restart the job
	task  - origin task if this job is a part of the task
	startdelay  - delay after the job process starting to execute it for some time,
		executed in the CONTEXT OF THE CALLER (main process).
		ATTENTION: should be small (0.1 .. 1 sec)
	onstart  - callback which is executed on the job starting (before the execution
		started) in the CONTEXT OF THE CALLER (main process) with the single argument,
		the job. Default: None
		ATTENTION: must be lightweight
		NOTE: can be executed a few times if the job is restarted on timeout
	ondone  - callback which is executed on successful completion of the job in the
		CONTEXT OF THE CALLER (main process) with the single argument, the job. Default: None
		ATTENTION: must be lightweight
	params  - additional parameters to be used in callbacks
	stdout  - None or file name or PIPE for the buffered output to be APPENDED
	stderr  - None or file name or PIPE or STDOUT for the unbuffered error output to be APPENDED
		ATTENTION: PIPE is a buffer in RAM, so do not use it if the output data is huge or unlimited
	omitafn  - Omit affinity policy of the scheduler, which is actual when the affinity is enabled
	 	and the process has multiple treads

	tstart  - start time is filled automatically on the execution start (before onstart). Default: None
	tstop  - termination / completion time after ondone
	proc  - process of the job, can be used in the ondone() to read it's PIPE
	"""
```
### Task
```python
Task(name, timeout=0, onstart=None, ondone=None, params=None, stdout=sys.stdout, stderr=sys.stderr):
	"""Initialize task, which is a group of jobs to be executed

	name  - task name
	timeout  - execution timeout. Default: 0, means infinity
	onstart  - callback which is executed on the task starting (before the execution
		started) in the CONTEXT OF THE CALLER (main process) with the single argument,
		the task. Default: None
		ATTENTION: must be lightweight
	ondone  - callback which is executed on successful completion of the task in the
		CONTEXT OF THE CALLER (main process) with the single argument, the task. Default: None
		ATTENTION: must be lightweight
	params  - additional parameters to be used in callbacks
	stdout  - None or file name or PIPE for the buffered output to be APPENDED
	stderr  - None or file name or PIPE or STDOUT for the unbuffered error output to be APPENDED
		ATTENTION: PIPE is a buffer in RAM, so do not use it if the output data is huge or unlimited

	tstart  - start time is filled automatically on the execution start (before onstart). Default: None
	tstop  - termination / completion time after ondone
	"""
```
### ExecPool
```python
def ramfracs(fracsize):
	"""Evaluate the minimal number of RAM fractions of the specified size in GB

	Used to estimate the reasonable number of processes with the specified minimal
	dedicated RAM.

	fracsize  - minimal size of each fraction in GB, can be a fractional number
	return the minimal number of RAM fractions having the specified size in GB
	"""

def cpucorethreads():
	"""The number of hardware treads per a CPU core

	Used to specify CPU afinity step dedicating the maximal amount of CPU cache.
	"""

def cpunodes():
	"""The number of NUMA nodes, where CPUs are located

	Used to evaluate CPU index from the affinity table index considerin the NUMA architectore.
	"""

def afnicpu(iafn, corethreads=1, nodes=1, crossnodes=True):
	"""Affinity table index mapping to the CPU index

	Affinity table is a reduced CPU table by the non-primary HW treads in each core.
	Typically CPUs are evumerated across the nodes:
	NUMA node0 CPU(s):     0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30
	NUMA node1 CPU(s):     1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31
	So, in case the number of threads per core is 2 then the following CPUs should be bound:
	0, 1, 4, 5, 8, ...
	2 -> 4, 4 -> 8
	#i ->  i  +  i // cpunodes() * cpunodes() * (cpucorethreads() - 1)

	iafn  - index in the affinity table to be mapped into the respective CPU index
	corethreads  - HW threads per CPU core or just some affinity step,
		1  - maximal parallelization with the minimal CPU cache size
	nodes  - NUMA nodes containing CPUs
	crossnodes  - cross-nodes enumeration of the CPUs in the NUMA nodes

	return CPU index respective to the specified index in the affinity table
	"""

ExecPool(workers=cpu_count(), afnstep=None)
	"""Multi-process execution pool of jobs

	workers  - number of resident worker processes, >=1. The reasonable value is
		<= NUMA nodes * node CPUs, which is typically returned by cpu_count(),
		where node CPUs = CPU cores * HW treads per core.
		To guarantee minimal number of RAM per a process, for example 2.5 GB:
			workers = min(cpu_count(), max(ramfracs(2.5), 1))
	afnstep  - affinity step, integer if applied. Used to bound whole CPU cores
		instead of the hardware treads to have more dedicated cache.
		Typical values:
			None  - do not use affinity at all (recommended for multi-threaded workers),
			1  - maximize parallelization (the number of worker processes = CPU units),
			cpucorethreads()  - maximize the dedicated CPU cache (the number of
				worker processes = CPU cores = CPU units / hardware treads per CPU core).
		NOTE: specification of the afnstep might cause reduction of the workers.
	"""

	execute(job, async=True):
		"""Schedule the job for the execution

		job  - the job to be executed, instance of Job
		async  - async execution or wait until execution completed
		  NOTE: sync tasks are started at once
		return  - 0 on successful execution, process return code otherwise
		"""

	join(timeout=0):
		"""Execution cycle

		timeout  - execution timeout in seconds before the workers termination, >= 0.
			0 means absence of the timeout. The time is measured SINCE the first job
			was scheduled UNTIL the completion of all scheduled jobs.
		return  - True on graceful completion, False on termination by the specified timeout
		"""

	__del__():
		"""Force termination of the pool"""

	__finalize__():
		"""Force termination of the pool"""
```


## Usage

Target version of the Python is 2.7+ including 3.x, also works fine on PyPy.

The workflow consists of the following steps:

1. Create Execution Pool.
1. Create and schedule Jobs with required parameters, callbacks and optionally packing them into Tasks.
1. Wait on Execution pool until all the jobs are completed or terminated, or until the global timeout is elapsed.

### Usage Example
```python
from multiprocessing import cpu_count
from sys import executable as PYEXEC  # Full path to the current Python interpreter

# 1. Create Multi-process execution pool with the optimal affinity step to maximize the dedicated CPU cache size
execpool = ExecPool(max(cpu_count() - 1, 1), cpucorethreads())
global_timeout = 30 * 60  # 30 min, timeout to execute all scheduled jobs or terminate them


# 2. Schedule jobs execution in the pool

# 2.a Job scheduling using external executable: "ls -la"
execpool.execute(Job(name='list_dir', args=('ls', '-la')))


# 2.b Job scheduling using python function / code fragment,
# which is not a goal of the design, but is possible.

# 2.b.1 Create the job with specified parameters
jobname = 'NetShuffling'
jobtimeout = 3 * 60  # 3 min

# The network shuffling routine to be scheduled as a job,
# which can also be a call of any external executable (see 2.alt below)
args = (PYEXEC, '-c',
"""import os
import subprocess

basenet = '{jobname}' + '{_EXTNETFILE}'
#print('basenet: ' + basenet, file=sys.stderr)
for i in range(1, {shufnum} + 1):
	netfile = ''.join(('{jobname}', '.', str(i), '{_EXTNETFILE}'))
	if {overwrite} or not os.path.exists(netfile):
		# sort -R pgp_udir.net -o pgp_udir_rand3.net
		subprocess.call(('sort', '-R', basenet, '-o', netfile))
""".format(jobname=jobname, _EXTNETFILE='.net', shufnum=5, overwrite=False))

# 2.b.2 Schedule the job execution, which might be postponed
# if there are no any free executor processes available
execpool.execute(Job(name=jobname, workdir='this_sub_dir', args=args, timeout=jobtimeout
	# Note: onstart/ondone callbacks, custom parameters and others can be also specified here!
))

# Add another jobs
# ...


# 3. Wait for the jobs execution for the specified timeout at most
execpool.join(global_timeout)  # 30 min
```

### Failsafe Termination
To perform *graceful termination* of the Jobs in case of external termination of your program, signal handlers can be set:
```python
import signal  # Intercept kill signals

# Use execpool as a global variable, which is set to None when all jobs are done,
# and recreated on jobs scheduling
execpool = None

def terminationHandler(signal=None, frame=None, terminate=True):
	"""Signal termination handler

	signal  - raised signal
	frame  - origin stack frame
	terminate  - whether to terminate the application
	"""
	global execpool

	if execpool:
		del execpool  # Destructors are caled later
		# Define _execpool to avoid unnessary trash in the error log, which might
		# be caused by the attempt of subsequent deletion on destruction
		execpool = None  # Note: otherwise _execpool becomes undefined
	if terminate:
		sys.exit()  # exit(0), 0 is the default exit code.

# Set handlers of external signals, which can be the first lines inside
# if __name__ == '__main__':
signal.signal(signal.SIGTERM, terminationHandler)
signal.signal(signal.SIGHUP, terminationHandler)
signal.signal(signal.SIGINT, terminationHandler)
signal.signal(signal.SIGQUIT, terminationHandler)
signal.signal(signal.SIGABRT, terminationHandler)

# Define execpool to schedule some jobs
execpool = ExecPool(max(cpu_count() - 1, 1))

# Failsafe usage of execpool ...
```
Also it is recommended to register the termination handler for the normal interpreter termination using [**atexit**](https://docs.python.org/2/library/atexit.html):
```python
import atexit
...
# Set termination handler for the internal termination
atexit.register(terminationHandler, terminate=False)
```

**Note:** Please, [star this project](//github.com/eXascaleInfolab/PyExPool) if you use it.

## Related Projects
- [ExecTime](https://bitbucket.org/lumais/exectime)  -  *failover* lightweight resource consumption profiler (*timings and memory*), applicable to multiple processes with optional *per-process results labeling* and synchronized *output to the specified file* or `stderr`: https://bitbucket.org/lumais/exectime
- [PyCABeM](https://github.com/eXascaleInfolab/PyCABeM) - Python Benchmarking Framework for the Clustering Algorithms Evaluation. Uses extrinsic (NMIs) and intrinsic (Q) measures for the clusters quality evaluation considering overlaps (nodes membership by multiple clusters).
