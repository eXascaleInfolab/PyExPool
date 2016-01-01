# PyExPool

Lightweight Multi-Process Execution Pool to schedule Jobs execution with *per-job timeout*, optionally grouping them into Tasks and specifying execution paremeters:

- timeout per each Job (it was the main motivation to implemtent this module, because this feature is not provided by any Python implementation out of the box)
- onstart/ondone callbacks, ondone is called only on successful completion (not termination) for both Jobs and Tasks (group of jobs)
- stdout/err output, which can be redireted to any custom file or PIPE
- custom parameters for each Job and embracing Task besides the name/id
	
The implementation is a ***single file module* to be easily included into your project and *customized* as a part of your distribution** (like in [PyCaBeM](//github.com/XI-lab/PyCABeM)), not as a separate library.

\author: (c) Artem Lutov <artem@exascale.info>  
\organizations: [eXascale Infolab](http://exascale.info/), [Lumais](http://www.lumais.com/), [ScienceWise](http://sciencewise.info/)  
\date: 2016-01  


## API

Flexible API provides optional automatic restart of jobs on timeout, access to job's process, parent task, start and stop execution time and much more...

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

```python
ExecPool(workers=cpu_count())
	"""Multi-process execution pool of jobs
	
	workers  - number of resident worker processes
	"""
```


## Usage

```python
from multiprocessing import cpu_count
from sys import executable as PYEXEC  # Full path to the current Python interpreter

# Create Multi-process execution pool
execpool = ExecPool(max(cpu_count() - 1, 1))
global_timeout = 30 * 60  # 30 min, timeout to execute all scheduled jobs or terminate them

# Fill the pool with jobs
jobname = 'NetShuffling'
jobtimeout = 3 * 60  # 3 min

args = (PYEXEC, '-c',
# The network shuffling procedure to be sccheduled as a job, which can also be a call of any external executable
"""import os
import subprocess

basenet = '{jobname}' + '{_EXTNETFILE}'
#print('basenet: ' + basenet, file=sys.stderr)
for i in range(1, {shufnum} + 1):
	# sort -R pgp_udir.net -o pgp_udir_rand3.net
	netfile = ''.join(('{jobname}', '.', str(i), '{_EXTNETFILE}'))
	if {overwrite} or not os.path.exists(netfile):
		subprocess.call(('sort', '-R', basenet, '-o', netfile))
# Remove existent redundant shuffles if any
#i = {shufnum} + 1
#while i < 100:  # Max number of shuffles
#	netfile = ''.join(('{jobname}', '.', str(i), '{_EXTNETFILE}'))
#	if not os.path.exists(netfile):
#		break
#	else:
#		os.remove(netfile)
""".format(jobname=jobname, _EXTNETFILE='.net', shufnum=5, overwrite=False))

# Schedule the job execution, which might be postponed if there are no free executor processes available
execpool.execute(Job(name=jobname, workdir='this_sub_dir', args=args, timeout=jobtimeout
	# Note: onstart/ondone callbacks, custom parameters and others can be also specified here!
))


# Add another jobs
# ...

# Wait for the jobs execution for the specified timeout at most
execpool.join(global_timeout)  # 30 min
```
