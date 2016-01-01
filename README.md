# PyExPool

Lightweight Multi-Process Execution Pool to schedule Jobs execution with *per-job timeout*, optionally grouping them into Tasks and specifying execution paremeters:

- timeout per each Job (it was the main motivation to implemtent this module, because this feature is not provided by any Python implementation out of the box)
- onstart/ondone callbacks, ondone is called only on successful completion (not termination) for both Jobs and Tasks (group of jobs)
- stdout/err output, which can be redireted to any custom file or PIPE
- custom parameters for each Job and embracing Task besides the name/id
	
The implementation is a ***single file module* to be easily included into your project and *customized* as a part of your distribution** (like in [PyCaBeM](//github.com/XI-lab/PyCABeM)), not as a separate library.  
The main purpose of this single-file module is *asynchronious execution of modules and external executables*. When asynchronious execution of Python functions is required and usage of external dependences is not a problem, then more handy and straightforward approach is to use [Pebble](https://pypi.python.org/pypi/Pebble) library.

\author: (c) Artem Lutov <artem@exascale.info>  
\organizations: [eXascale Infolab](http://exascale.info/), [Lumais](http://www.lumais.com/), [ScienceWise](http://sciencewise.info/)  
\date: 2016-01  

## Content
- [API](#api)
	- [Job](#job)
	- [Task](#task)
	- [ExecPool](#execpool)
- [Usage](#usage)
	- [Usage Example](#usage-example)
	- [Failover Example](#failover-example)
- [Related Projects](#related-projects)

## API

Flexible API provides optional automatic restart of jobs on timeout, access to job's process, parent task, start and stop execution time and much more...
### Job

```python
Job(name, workdir=None, args=(), timeout=0, ontimeout=False, task=None
, startdelay=0, onstart=None, ondone=None, params=None, stdout=sys.stdout, stderr=sys.stderr):
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
ExecPool(workers=cpu_count())
	"""Multi-process execution pool of jobs
	
	workers  - number of resident worker processes
	"""

	execute(job, async=True):
		"""Schecule the job for the execution

		job  - the job to be executed, instance of Job
		async  - async execution or wait until execution completed
		  NOTE: sync tasks are started at once
		return  - 0 on successful execution, proc. returncode otherwise
		"""

	join(timeout=0):
		"""Execution cycle

		timeout  - execution timeout in seconds before the workers termination, >= 0.
			0 means absebse of the timeout. The time is measured SINCE the first job
			was scheduled UNTIL the completion of all scheduled jobs.
		return  - True on graceful completion, Flase on termination by the specified timeout
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
1. Wait on Execution pool untill all the jobs are completed or terminated, or until the global timeout is elapsed.

### Usage Example
```python
from multiprocessing import cpu_count
from sys import executable as PYEXEC  # Full path to the current Python interpreter

# 1. Create Multi-process execution pool
execpool = ExecPool(max(cpu_count() - 1, 1))
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

### Failover Example
To perform *graceful termination* of the Jobs in case of external terminatoin of your program, signal handlers can be set:
```python
import signal  # Intercept kill signals

# Use execpool as a global variable, which is set to None when all jobs are done,
# and recreated on jobs scheduling
execpool = None

def terminationHandler(signal, frame):
	"""Signal termination handler"""
	global execpool

	if execpool:
		del execpool  # Destructors are caled later
	sys.exit(0)

# Set handlers of external signals, which can be the first lines inside
# if __name__ == '__main__':
signal.signal(signal.SIGTERM, terminationHandler)
signal.signal(signal.SIGHUP, terminationHandler)
signal.signal(signal.SIGINT, terminationHandler)
signal.signal(signal.SIGQUIT, terminationHandler)
signal.signal(signal.SIGABRT, terminationHandler)


# Define execpool to schedule some jobs
execpool = ExecPool(max(cpu_count() - 1, 1))

# Failsafe sage of execpool ...
```

**Note:** Please, [star this project](//github.com/XI-lab/PyExPool) if you use it.

## Related Projects
- [ExecTime](https://bitbucket.org/lumais/exectime)  -  *failover* lightweight resource consumption profiler (*timings and memory*), applicable to multiple processes with optional *per-process results labeling* and sycnchronized *output to the specified file* or `stderr`: https://bitbucket.org/lumais/exectime
