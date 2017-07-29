#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
\descr:  Multi-Process Execution Pool to schedule Jobs execution with per-job timeout,
optionally grouping them into Tasks and specifying optional execution parameters
considering NUMA architecture:
	- automatic rescheduling and *load balancing* (reduction) of the worker processes
		and on low memory condition for the *in-RAM computations* (requires
		[psutil](https://pypi.python.org/pypi/psutil), can be disabled)
	- *chained termination* of the related worker processes (started jobs) and
		non-started jobs rescheduling to satisfy *timeout* and *memory limit* constraints
	- automatic CPU affinity management and maximization of the dedicated CPU cache
		vs parallelization for a worker process
	- *timeout per each Job* (it was the main initial motivation to implement this
		module, because this feature is not provided by any Python implementation out of the box)
	- onstart/ondone *callbacks*, ondone is called only on successful completion
		(not termination) for both Jobs and Tasks (group of jobs)
	- stdout/err output, which can be redirected to any custom file or PIPE
	- custom parameters for each Job and respective owner Task besides the name/id

	Flexible API provides optional automatic restart of jobs on timeout, access to job's process,
	parent task, start and stop execution time and much more...


	Core parameters specified as global variables:
	_LIMIT_WORKERS_RAM  - limit the amount of memory consumption (<= RAM) by worker processes,
		requires psutil import
	_CHAINED_CONSTRAINTS  - terminate related jobs on terminating any job by the execution
		constraints (timeout or RAM limit)

	The load balancing is enabled when global variables _LIMIT_WORKERS_RAM and _CHAINED_CONSTRAINTS
	are set, jobs categories and relative size (if known) specified. The balancing is performed
	to use as much RAM and CPU resources as possible performing in-RAM computations and meeting
	timeout, memory limit and CPU cache (processes affinity) constraints.
	Large executing jobs are rescheduled for the later execution with less number of worker
	processes after the completion of smaller jobs. The number of workers is reduced automatically
	(balanced) on the jobs queue processing. It is recommended to add jobs in the order of the
	increasing memory/time complexity if possible to reduce the number of worker process
	terminations for the jobs execution postponing on rescheduling.

\author: (c) Artem Lutov <artem@exascale.info>
\organizations: eXascale Infolab <http://exascale.info/>, Lumais <http://www.lumais.com/>, ScienceWise <http://sciencewise.info/>
\date: 2015-07 v1, 2017-06 v2
"""

from __future__ import print_function, division  # Required for stderr output, must be the first import
import sys
import time
import collections
import os
import ctypes  # Required for the multiprocessing Value definition
import types  # Required for instance methods definition
import traceback  # Stacktrace
# To print a stacktrace fragment:
# traceback.print_stack(limit=5, file=sys.stderr) or
# print(traceback.format_exc(5), file=sys.stderr)
import subprocess
import errno

from multiprocessing import cpu_count, Value, Lock  #, active_children

# Required to efficiently traverse items of dictionaries in both Python 2 and 3
try:
	from future.utils import viewvalues  #viewitems, viewkeys, viewvalues  # External package: pip install future
	from future.builtins import range
except ImportError:
	def viewMethod(obj, method):
		"""Fetch view method of the object

		obj  - the object to be processed
		method  - name of the target method, str

		return  target method or AttributeError

		>>> callable(viewMethod(dict(), 'items'))
		True
		"""
		viewmeth = 'view' + method
		ometh = getattr(obj, viewmeth, None)
		if not ometh:
			ometh = getattr(obj, method)
		return ometh

	#viewitems = lambda dct: viewMethod(dct, 'items')()
	#viewkeys = lambda dct: viewMethod(dct, 'keys')()
	viewvalues = lambda dct: viewMethod(dct, 'values')()

	# Replace range() implementation for Python2
	try:
		range = xrange
	except NameError:
		pass  # xrange is not defined in Python3, which is fine


def timeheader(timestamp=time.gmtime()):
	"""Timestamp header string

	timestamp  - timestamp

	return  - timetamp string for the file header
	"""
	assert isinstance(timestamp, time.struct_time), 'Unexpected type of timestamp'
	# ATTENTION: MPE pool timestamp [prefix] intentionally differs a bit from the
	# benchmark timestamp to easily find/filter each of them
	return time.strftime('# ----- %Y-%m-%d %H:%M:%S ' + '-'*30, timestamp)


# Limit the amount of memory consumption by worker processes.
# NOTE:
#  - requires import of psutils
#  - automatically reduced to the RAM size if the specidied limit is larger
_LIMIT_WORKERS_RAM = True
if _LIMIT_WORKERS_RAM:
	try:
		import psutil
	except ImportError as err:
		_LIMIT_WORKERS_RAM = False
		# Note: import occurs before the execution of the main application, so show
		# the timestamp to outline when the error occurred and separate reexecutions
		print(timeheader(), file=sys.stderr)
		print('WARNING, RAM constraints are disabled because the psutil module import failed: ', err, file=sys.stderr)

# Use chained constraints (timeout and memory limitation) in jobs to terminate
# also related worker processes and/or reschedule jobs, which have the same
# category and heavier than the origin violating the constraints
_CHAINED_CONSTRAINTS = True


_RAM_SIZE = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 1024.**3  # RAM (physical memory) size in Gb
# Dedicate at least 256 Mb for the OS consuming not more than 98% of RAM
_RAM_LIMIT = _RAM_SIZE * 0.98 - 0.25  # Maximal consumption of RAM in Gb (< _RAM_SIZE to avoid/reduce swapping)
_AFFINITYBIN = 'taskset'  # System app to set CPU affinity if required, should be preliminarry installed (taskset is present by default on NIX systems)
_DEBUG_TRACE = False  # Trace start / stop and other events to stderr;  1 - brief, 2 - detailed, 3 - in-cycles


def secondsToHms(seconds):
	"""Convert seconds to hours, mins, secs

	seconds  - seconds to be converted, >= 0

	return hours, mins, secs
	"""
	assert seconds >= 0, 'seconds validation failed'
	hours = int(seconds // 3600)
	mins = int((seconds - hours * 3600) // 60)
	secs = seconds - hours * 3600 - mins * 60
	return hours, mins, secs


def inGigabytes(nbytes):
	"""Convert bytes to gigabytes"""
	return nbytes / (1024. ** 3)


def inBytes(gb):
	"""Convert bytes to gigabytes"""
	return gb * 1024. ** 3


class Task(object):
	"""Task is a managing container for Jobs"""
	#TODO: Implement timeout support in add/delJob
	def __init__(self, name, timeout=0, onstart=None, ondone=None, params=None, stdout=sys.stdout, stderr=sys.stderr):
		"""Initialize task, which is a group of jobs to be executed

		name  - task name
		timeout  - execution timeout in seconds. Default: 0, means infinity
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
		tstop  - termination / completion time after ondone.
		"""
		assert isinstance(name, str) and timeout >= 0, 'Arguments are invalid'
		self.name = name
		self.timeout = timeout
		self.params = params
		# Add member handlers if required
		self.onstart = types.MethodType(onstart, self) if onstart else None
		self.ondone = types.MethodType(ondone, self) if ondone else None
		self.stdout = stdout
		self.stderr = stderr
		self.tstart = None
		self.tstop = None  # SyncValue()  # Termination / completion time after ondone
		# Private attributes
		self._jobsnum = Value(ctypes.c_uint)
		# Graceful completion of all tasks or at least one of the tasks was terminated
		self._graceful = Value(ctypes.c_bool)
		self._graceful.value = True


	def addJob(self):
		"""Add one more job to the task

		return  - updated task
		"""
		initial = False
		with self._jobsnum.get_lock():
			if self._jobsnum.value == 0:
				initial = True
			self._jobsnum.value += 1
		# Run onstart if required
		if initial:
			self.tstart = time.time()  # ATTENTION: .clock() should not be used, because it does not consider "sleep" time
			if self.onstart:
				self.onstart()
		return self


	def delJob(self, graceful):
		"""Delete one job from the task

		graceful  - whether the job is successfully completed or it was terminated
		return  - None
		"""
		final = False
		with self._jobsnum.get_lock():
			self._jobsnum.value -= 1
			if self._jobsnum.value == 0:
				final = True
		# Finalize if required
		if not graceful:
			self._graceful.value = False
		elif final:
			if self.ondone and self._graceful.value:
				self.ondone()
			self.tstop = time.time()
		return None


class Job(object):
	_RTM = 0.85  # Memory retention ratio, used to not drop the memory info fast on temporal releases, E [0, 1)
	assert 0 <= _RTM < 1, 'Memory retention ratio should E [0, 1)'

	# Note: the same job can be executed as Popen or Process object, but ExecPool
	# should use some wrapper in the latter case to manage it
	"""Job is executed in a separate process via Popen or Process object and is
	managed by the Process Pool Executor
	"""
	# NOTE: keyword-only arguments are specified after the *, supported only since Python 3
	def __init__(self, name, workdir=None, args=(), timeout=0, ontimeout=False, task=None #,*
	, startdelay=0, onstart=None, ondone=None, params=None, category=None, size=0, slowdown=1.
	, omitafn=False, memkind=1, stdout=sys.stdout, stderr=sys.stderr):
		"""Initialize job to be executed

		# Main parameters
		name  - job name
		workdir  - working directory for the corresponding process, None means the dir of the benchmarking
		args  - execution arguments including the executable itself for the process
			NOTE: can be None to make make a stub process and execute the callbacks
		timeout  - execution timeout in seconds. Default: 0, means infinity
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
		stdout  - None or file name or PIPE for the buffered output to be APPENDED.
			The path is interpreted in the CONTEXT of the CALLER
		stderr  - None or file name or PIPE or STDOUT for the unbuffered error output to be APPENDED
			ATTENTION: PIPE is a buffer in RAM, so do not use it if the output data is huge or unlimited.
			The path is interpreted in the CONTEXT of the CALLER

		# Scheduling parameters
		omitafn  - omit affinity policy of the scheduler, which is actual when the affinity is enabled
			and the process has multiple treads
		category  - classification category, typically semantic context or part of the name;
			used for _CHAINED_CONSTRAINTS to identify related jobs
		size  - expected relative memory complexity of the jobs of the same category,
			typically it is size of the processing data, >= 0, 0 means undefined size
			and prevents jobs chaining on constraints violation;
			used on _LIMIT_WORKERS_RAM or _CHAINED_CONSTRAINTS
		slowdown  - execution slowdown ratio, >= 0, where (0, 1) - speedup, > 1 - slowdown; 1 by default;
			used for the accurate timeout estimation of the jobs having the same .category and .size.
		memkind  - kind of memory to be evaluated (average of virtual and resident memory
			to not overestimate the instant potential consumption of RAM):
			0  - mem for the process itself omitting the spawned sub-processes (if any)
			1  - mem for the heaviest process of the process tree spawned by the original process
				(including the origin itself)
			2  - mem for the whole spawned process tree including the origin process

		# Execution parameters, initialized automatically on execution
		tstart  - start time is filled automatically on the execution start (before onstart). Default: None
		tstop  - termination / completion time after ondone
			NOTE: onstart() and ondone() callbacks execution is included in the job execution time
		proc  - process of the job, can be used in the ondone() to read it's PIPE
		mem  - consuming memory (smooth max of average of vms and rss, not just the current value)
			or the least expected value inherited from the jobs of the same category having non-smaller size;
			requires _LIMIT_WORKERS_RAM
		"""
		assert isinstance(name, str) and timeout >= 0 and (task is None or isinstance(task, Task)
			) and size >= 0 and slowdown > 0 and 0 <= memkind <= 2, 'Arguments are invalid'
		#if not args:
		#	args = ("false")  # Create an empty process to schedule it's execution

		# Properties specified by the input parameters -------------------------
		self.name = name
		self.workdir = workdir
		self.args = args
		self.params = params
		self.timeout = timeout
		self.ontimeout = ontimeout
		self.task = task.addJob() if task else None
		# Delay in the callers context after starting the job process. Should be small.
		self.startdelay = startdelay  # 0.2  # Required to sync sequence of started processes
		# Callbacks ------------------------------------------------------------
		self.onstart = types.MethodType(onstart, self) if onstart else None
		self.ondone = types.MethodType(ondone, self) if ondone else None
		# I/O redirection ------------------------------------------------------
		self.stdout = stdout
		self.stderr = stderr
		# Internal properties --------------------------------------------------
		self.tstart = None  # start time is filled automatically on the execution start, before onstart. Default: None
		self.tstop = None  # SyncValue()  # Termination / completion time after ondone
		# Internal attributes
		self.proc = None  # Process of the job, can be used in the ondone() to read it's PIPE
		self.terminates = 0  # The number of received termination requirests (generated because of the constraints violation)
		# Process-related file descriptors to be closed
		self._fstdout = None
		self._fstderr = None
		# Omit scheduler affinity policy (actual when some process is computed on all treads, etc.)
		self._omitafn = omitafn
		self.memkind = memkind
		if _LIMIT_WORKERS_RAM or _CHAINED_CONSTRAINTS:
			self.size = size  # Expected memory complexity of the job, typcally it's size of the processing data
			# Consumed implementation-defined type of memory on execution in gigabytes or the least expected
			# (inherited from the related jobs having the same category and non-smaller size)
			self.mem = 0.
		if _CHAINED_CONSTRAINTS:
			self.category = category  # Job name
			self.slowdown = slowdown  # Execution slowdown ratio, >= 0, where (0, 1) - speedup, > 1 - slowdown
			self.chtermtime = None  # Chained termination by time: None, False - by memory, True - by time
		if _LIMIT_WORKERS_RAM:
			# Note: wkslim is used only internally for the cross-category ordering
			# of the jobs queue by reducing resource consumption
			self.wkslim = None  # Worker processes limit (max number) on the job postponing if any


	def _updateMem(self):
		"""Update memory consumption (implementation-defined type) using smooth max

		Actual memory (not the historical max) is retrieved and updated
		using:
		a) smoothing filter in case of the decreasing consumption and
		b) direct update in case of the increasing consumption.

		Prerequisites: job must have defined proc (otherwise AttributeError is raised)
			and psutil should be available (otherwise NameError is raised)

		self.memkind defines the kind of memory to be evaluated:
			0  - mem for the process itself omitting the spawned sub-processes (if any)
			1  - mem for the heaviest process of the process tree spawned by the original process
				(including the origin)
			2  - mem for the whole spawned process tree including the origin process

		return  - smooth max of job mem
		"""
		# Current consumption of memory by the job
		curmem = 0  # Evaluating memory
		try:
			up = psutil.Process(self.proc.pid)
			pmem = up.memory_info()
			# Note: take average of mem and rss to not over reserve RAM especially for Java apps
			curmem = (pmem.vms + pmem.rss) / 2
			if self.memkind:
				amem = curmem  # Memory consumption of the whole process tree
				xmem = curmem  # Memory consumption of the heaviest process in the tree
				for ucp in up.children(recursive=True):  # Note: fetches only children procs
					pmem = ucp.memory_info()
					mem = (pmem.vms + pmem.rss) / 2  # Mb
					amem += mem
					if xmem < mem:
						xmem = mem
				curmem = amem if self.memkind == 2 else xmem
		except psutil.Error as err:
			# The process is finished and such pid does not exist
			print('WARNING, _updateMem() failed, current proc mem set to 0: ', err, file=sys.stderr)
		# Note: even if curmem = 0 updte mem smoothly to avoid issues on internal
		# fails of psutil even thought they should not happen
		curmem = inGigabytes(curmem)
		self.mem = max(curmem, self.mem * Job._RTM + curmem * (1-Job._RTM))
		return self.mem


	def lessmem(self, job):
		"""Whether the [estimated] memory consumption is less than in the specified job

		job  - another job for the .mem or .size comparison

		return  - [estimated] mem is less
		"""
		assert self.category is not None and self.category == job.category, (
			'Only jobs of the same initialized category can be compared')
		return self.size < job.size if not self.mem or not job.mem else self.mem < job.mem


	def complete(self, graceful=None):
		"""Completion function
		ATTENTION: This function is called after the destruction of the job-associated process
		to perform cleanup in the context of the caller (main thread).

		graceful  - the job is successfully completed or it was terminated / crashed, bool.
			None means use "not self.proc.returncode" (i.e. whether errcode is 0)
		"""
		# Close process-related file descriptors
		for fd in (self._fstdout, self._fstderr):
			if fd and fd is not sys.stdout and fd is not sys.stderr:  #  and hasattr(fd, 'close')
				try:
					fd.close()
				except Exception as err:
					print('ERROR, job "{}" I/O channel closing failed: {}. {}'.format(
						self.name, err, traceback.format_exc(5)), file=sys.stderr)
		self._fstdout = None
		self._fstderr = None

		#if self.proc is not None:
		#	if self.proc.poll() is None:  # poll() None means the process has not been terminated / completed
		#		self.proc.kill()
		#		self.proc.wait()  # Join the completed orocess to remove the entry from the children table and avoid zombie
		#		print('WARNING, completion of the non-finished process is called for "{}", killed'
		#			.format(self.name), file=sys.stderr)

		# Note: files should be closed before any assertions or exceptions
		assert self.tstop is None and self.tstart is not None, (  # and self.proc.poll() is not None
			'A job ({}) should be already started and can be completed only once, tstart: {}, tstop: {}'
			.format(self.name, self.tstart, self.tstop))
		# Job-related post execution
		if graceful is None:
			graceful = self.proc is not None and not self.proc.returncode
		if graceful:
			if self.ondone:
				try:
					self.ondone()
				except Exception as err:
					print('ERROR in ondone callback of "{}": {}. {}'.format(
						self.name, err, traceback.format_exc(5)), file=sys.stderr)
			# Clean up empty logs (can be left for the terminatig process to avoid delays)
			# Remove empty logs skipping the system devnull
			tpaths = []  # Base dir of the output
			if (self.stdout and isinstance(self.stdout, str) and self.stdout != os.devnull
			and os.path.exists(self.stdout) and os.path.getsize(self.stdout) == 0):
				tpath = os.path.split(self.stdout)[0]
				if tpath:
					tpaths.append(tpath)
				os.remove(self.stdout)
			if (self.stderr and isinstance(self.stderr, str) and self.stderr != os.devnull
			and os.path.exists(self.stderr) and os.path.getsize(self.stderr) == 0):
				tpath = os.path.split(self.stderr)[0]
				if tpath and (not tpaths or tpath not in tpaths):
					tpaths.append(tpath)
				os.remove(self.stderr)
			# Also remove the directory if it is empty
			for tpath in tpaths:
				try:
					os.rmdir(tpath)
				except OSError:
					pass  # The dir is not empty, just skip it
		# Check whether the job is associated with any task
		if self.task:
			self.task = self.task.delJob(graceful)
		# Updated execution status
		self.tstop = time.time()
		#if _DEBUG_TRACE:  # Note: terminated jobs are traced in __reviseWorkers()
		print('Completed {} "{}" #{} with errcode {}, executed {} h {} m {:.4f} s'
			.format('gracefully' if graceful else '(ABNORMALLY)'
			, self.name, '-' if self.proc is None else str(self.proc.pid)
			, '-' if self.proc is None else str(self.proc.returncode)
			, *secondsToHms(self.tstop - self.tstart))
			, file=sys.stderr if _DEBUG_TRACE else sys.stdout)
		# Note: the callstack is shown on the termination
		#traceback.print_stack(limit=5, file=sys.stderr)


def ramfracs(fracsize):
	"""Evaluate the minimal number of RAM fractions of the specified size in Gb

	Used to estimate the reasonable number of processes with the specified minimal
	dedicated RAM.

	fracsize  - minimal size of each fraction in Gb, can be a fractional number
	return the minimal number of RAM fractions having the specified size in Gb
	"""
	return int(_RAM_SIZE / fracsize)


def cpucorethreads():
	"""The number of hardware treads per a CPU core

	Used to specify CPU afinity dedicating the maximal amount of CPU cache L1/2.
	"""
	# -r or -E  - extended regex syntax, -n  - quet output, /p  - print the match
	return int(subprocess.check_output(
		[r"lscpu | sed -rn 's/^Thread\(s\).*(\w+)$/\1/p'"], shell=True))


def cpunodes():
	"""The number of NUMA nodes, where physical CPUs are located.

	Used to evaluate CPU index from the affinity table index considering the
	NUMA architecture.
	Usually NUMA nodes = physical CPUs.
	"""
	return int(subprocess.check_output(
		[r"lscpu | sed -rn 's/^NUMA node\(s\).*(\w+)$/\1/p'"], shell=True))


def cpusequential(ncpunodes=cpunodes()):
	"""Enumeration type of the logical CPUs: crossnodes or sequential

	The enumeration can be crossnodes starting with one hardware thread per each
	NUMA node, or sequential by enumerating all cores and hardware threads in each
	NUMA node first.
	For two hardware threads per a physical CPU core, where secondary hw threads
	are taken in brackets:
		Crossnodes enumeration, often used for the server CPUs
		NUMA node0 CPU(s):     0,2(,4,6)		=> PU L#1 (P#4)
		NUMA node1 CPU(s):     1,3(,5,7)
		Sequential enumeration, often used for the laptop CPUs
		NUMA node0 CPU(s):     0(,1),2(,3)		=> PU L#1 (P#1)  - indicates sequential
		NUMA node1 CPU(s):     4(,5),6(,7)
	ATTENTION: `hwloc` utility is required to detect the type of logical CPUs
	enumeration:  `$ sudo apt-get install hwloc`
	See details: http://www.admin-magazine.com/HPC/Articles/hwloc-Which-Processor-Is-Running-Your-Service

	ncpunodes  - the number of cpu nodes in the system to assume sequential
		enumeration for multinode systems only; >= 1

	return  - enumeration type of the logical CPUs, bool or None:
		False  - crossnodes
		True  - sequential
	"""
	# Fetch index of the second hardware thread / CPU core / CPU on the first NUMA node
	res = subprocess.check_output(
		[r"lstopo-no-graphics | sed -rn 's/\s+PU L#1 \(P#([0-9]+)\)/\1/p'"], shell=True)
	try:
		return int(res) == 1
	except ValueError as err:
		# res is not a number, i.e. hwloc (lstopo*) is not installed
		print('WARNING, "lstopo-no-graphics ("hwloc" utilities) call failed: {}'
			', assuming that multinode systems have nonsequential CPU enumeration.', err, file=sys.stderr)
	return ncpunodes == 1


class AffinityMask(object):
	"""Affinity mask

	Affinity table is a reduced CPU table by the non-primary HW treads in each core.
	Typically, CPUs are enumerated across the nodes:
	NUMA node0 CPU(s):     0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30
	NUMA node1 CPU(s):     1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31
	In case the number of hw threads per core is 2 then the physical CPU cores are 1 .. 15:
	NUMA node0 CPU(s):     0,2,4,6,8,10,12,14	(16,18,20,22,24,26,28,30  - 2nd hw treads)
	NUMA node1 CPU(s):     1,3,5,7,9,11,13,15	(17,19,21,23,25,27,29,31  - 2nd hw treads)
	But the enumeration can be also sequential:
	NUMA node0 CPU(s):     0,(1),2,(3),...
	...

	Hardware threads share all levels of the CPU cache, physical CPU cores share only the
	last level of the CPU cache (L2/3).
	The number of worker processes in the pool should be equal to the:
	- physical CPU cores for the cache L1/2 maximization
	- NUMA nodes for the cache L2/3 maximization

	NOTE: `hwloc` utility can be used to detect the type of logical CPUs enumeration:
	`$ sudo apt-get install hwloc`
	See details: http://www.admin-magazine.com/HPC/Articles/hwloc-Which-Processor-Is-Running-Your-Service

	# Doctests -----------------------------------------------------------------
	# Mask for all sequential logical CPU having index #1
	>>> AffinityMask(1, False, sequential=True)(1) == \
		str(AffinityMask.CORE_THREADS if AffinityMask.NODES == 1 else AffinityMask.NODE_CPUS)
	True

	# Mask for the first crossnode logical CPU in the group #1
	>>> AffinityMask(AffinityMask.CORE_THREADS, sequential=False)(1) == '1'
	True

	# Mask for all crossnode logical CPUs in the group #1
	>>> AffinityMask(AffinityMask.CORE_THREADS, first=False, sequential=False)(1) == \
		','.join([str(1 + c*(AffinityMask.CPUS // (AffinityMask.NODES * AffinityMask.CORE_THREADS))) \
		for c in range(AffinityMask.CORE_THREADS)])
	True

	# Mask for all sequential logical CPUs in the group #1
	>>> AffinityMask(AffinityMask.CORE_THREADS, False, sequential=True)(1) == \
		'-'.join([str(1*(AffinityMask.CPUS // (AffinityMask.NODES * AffinityMask.CORE_THREADS)) + c) \
		for c in range(AffinityMask.CORE_THREADS)])
	True

	# Mask for all crossnode logical CPU on the NUMA node #0
	>>> AffinityMask(AffinityMask.CPUS // AffinityMask.NODES, False, sequential=False)(0) == \
		','.join(['{}-{}'.format(c*(AffinityMask.CPUS // AffinityMask.CORE_THREADS) \
		, (c+1)*(AffinityMask.CPUS // AffinityMask.CORE_THREADS) - 1) for c in range(AffinityMask.CORE_THREADS)])
	True

	# Mask for all sequential logical CPU on the NUMA node #0
	>>> AffinityMask(AffinityMask.CPUS // AffinityMask.NODES, first=False, sequential=True)(0) == \
		'0-{}'.format(AffinityMask.NODE_CPUS-1)
	True

	# Exception on too large input index
	>>> AffinityMask(AffinityMask.CPUS // AffinityMask.NODES, False, sequential=False)(100000)
	Traceback (most recent call last):
	IndexError

	# Exception on float afnstep
	>>> AffinityMask(2.1, False)(1)
	Traceback (most recent call last):
	AssertionError

	# Exception on afnstep not multiple to the CORE_THREADS
	>>> AffinityMask(AffinityMask.CORE_THREADS + 1, False)(0)
	Traceback (most recent call last):
	AssertionError
	"""
	CPUS = cpu_count()  # Logical CPUs: all hardware threads in all physical CPU cores in all physical CPUs in all NUMA nodes
	NODES = cpunodes()  # NUMA nodes (typically, physical CPUs)
	CORE_THREADS = cpucorethreads()  # Hardware threads per CPU core
	SEQUENTIAL = cpusequential(NODES)  # Sequential enumeration of the logical CPUs or crossnode enumeration

	CORES = CPUS//CORE_THREADS  # Total number of physical CPU cores
	NODE_CPUS = CPUS//NODES  # Logical CPUs per each NUMA node
	#CPU_CORES = NODE_CPUS//CORE_THREADS  # The number of physical cores in each CPU
	if NODE_CPUS*NODES != CPUS or CORES*CORE_THREADS != CPUS:
		raise ValueError('Only uniform NUMA nodes are supported:'
			'  CORE_THREADS: {}, CORES: {}, NODE_CPUS: {}, NODES: {}, CPUS: {}'
			.format(CORE_THREADS, CORES, NODE_CPUS, NODES, CPUS))

	def __init__(self, afnstep, first=True, sequential=SEQUENTIAL):
		"""Affinity mask initialization

		afnstep  - affinity step, integer if applied, allowed values:
			1, CORE_THREADS * n,  n E {1, 2, ... CPUS / (NODES * CORE_THREADS)}

			Used to bind worker processes to the logical CPUs to have warm cache and,
			optionally, maximize cache size per a worker process.
			Groups of logical CPUs are selected in a way to maximize the cache locality:
			the single physical CPU is used taking all it's hardware threads in each core
			before allocating another core.

			Typical Values:
			1  - maximize parallelization for the single-threaded apps
				(the number of worker processes = logical CPUs)
			CORE_THREADS  - maximize the dedicated CPU cache L1/2
				(the number of worker processes = physical CPU cores)
			CPUS / NODES  - maximize the dedicated CPU cache L3
				(the number of worker processes = physical CPUs)
		first  - mask the first logical unit or all units in the selected group.
			One unit per the group maximizes the dedicated CPU cache for the
			single-threaded worker, all units should be used for the multi-threaded
			apps.
		sequential  - sequential or cross nodes enumeration of the CPUs in the NUMA nodes:
			None  - undefined, interpreted as crossnodes (the most widely used on servers)
			False  - crossnodes
			True  - sequential

			For two hardware threads per a physical CPU core, where secondary hw threads
			are taken in brackets:
			Crossnodes enumeration, often used for the server CPUs
			NUMA node0 CPU(s):     0,2(,4,6)
			NUMA node1 CPU(s):     1,3(,5,7)
			Sequential enumeration, often used for the laptop CPUs
			NUMA node0 CPU(s):     0(,1),2(,3)
			NUMA node1 CPU(s):     4(,5),6(,7)
		"""
		assert ((afnstep == 1 or (afnstep >= self.CORE_THREADS
			and not afnstep % self.CORE_THREADS)) and isinstance(first, bool)
			and (sequential is None or isinstance(sequential, bool))
			), ('Arguments are invalid:  afnstep: {}, first: {}, sequential: {}'
			.format(afnstep, first, sequential))
		self.afnstep = int(afnstep)  # To convert 2.0 to 2
		self.first = first
		self.sequential = sequential


	def __call__(self, i):
		"""Evaluate CPUs affinity mask for the specified group indes of size afnstep

		i  - index of the selecting group of logical CPUs
		return  - mask of the first or all logical CPUs
		"""
		if i < 0 or (i + 1) * self.afnstep > self.CPUS:
			raise IndexError('Index is out of range for the given affinity step:'
				'  i: {}, afnstep: {}, cpus: {} vs {} (afnstep * (i+1))'
				.format(i, self.afnstep, self.CPUS, i * self.afnstep))

		inode = i % self.NODES  # Index of the NUMA node
		if self.afnstep == 1:
			if self.sequential:
				# 1. Identify shift inside the NUMA node traversing over the same number of
				# the hardware threads in all physical cores starting from the first hw thread
				indcpu = i//self.NODES * self.CORE_THREADS  # Traverse with step = CORE_THREADS
				indcpu = indcpu%self.NODE_CPUS + indcpu//self.NODE_CPUS  # Normalize to fit the actual indices
				# 2. Identify index of the NUMA node and conver it to the number of logical CPUs
				#indcpus = inode * self.NODE_CPUS
				i = inode*self.NODE_CPUS + indcpu
				assert i < self.CPUS, 'Index out of range: {} >= {}'.format(i, self.CPUS)
			cpumask = str(i)
		else:  # afnstep = CORE_THREADS * n,  n E N
			if self.sequential:
				# NUMA node0 CPU(s):     0(,1),2(,3)
				# NUMA node1 CPU(s):     4(,5),6(,7)

				# 1. Identify index of the NUMA node and conver it to the number of logical CPUs
				#indcpus = inode * self.NODE_CPUS
				i = (inode*self.NODE_CPUS
					# 2. Identify shift inside the NUMA node traversing over the same number of
					# the hardware threads in all physical cores starting from the first hw thread
					+ i//self.NODES * self.afnstep)
				assert i + self.afnstep <= self.CPUS, ('Mask out of range: {} > {}'
					.format(i + self.afnstep, self.CPUS))
				if self.first:
					cpumask = str(i)
				else:
					cpumask = '{}-{}'.format(i, i + self.afnstep - 1)
			else:
				# NUMA node0 CPU(s):     0,2,4,6,8,10,12,14	(16,18,20,22,24,26,28,30  - 2nd hw treads)
				# NUMA node1 CPU(s):     1,3,5,7,9,11,13,15	(17,19,21,23,25,27,29,31  - 2nd hw treads)
				# afnstep <= 1 or hwthreads = 1  -> direct mapping
				# afnstep = 2 [th=2]  -> 0,16; 1,17; 2,18; ...
				#
				# NUMA node0 CPU(s):     0,3,6,9	(12,15,18,21  - 2nd hw treads)
				# NUMA node1 CPU(s):     1,4,7,10	(13,16,19,22  - 2nd hw treads)
				# NUMA node2 CPU(s):     2,5,8,11	(14,17,20,23  - 2nd hw treads)
				# afnstep = 3 [th=2]  -> 0,12,3; 1,13,4; ... 15,6,18;
				#
				# NUMA node0 CPU(s):     0,3,6,9	(12,15,18,21  24... ...45)
				# NUMA node1 CPU(s):     1,4,7,10	(13,16,19,22  25... ...46)
				# NUMA node2 CPU(s):     2,5,8,11	(14,17,20,23  26... ...47)
				# afnstep = 3 [th=4]  -> 0,12,24,36; ... 4,16,28,40; ...
				ncores = self.afnstep//self.CORE_THREADS  # The number of physical cores to dedicate
				# Index of the logical CPU (1st hw thread of the physical core) with shift of the NUMA node
				indcpu = i//self.NODES * self.NODES * ncores + inode
				cpus = []
				for hwt in range(self.CORE_THREADS):
					# Index of the logical cpu:
					# innode + ihwthread_shift
					i = indcpu + hwt * self.CORES
					cpus.append(str(i) if ncores <= 1 else '{}-{}'.format(i, i + ncores-1))
					if self.first:
						break
				assert i + ncores <= self.CPUS, (
					'Index is out of range for the given affinity step:'
					'  i: {}, afnstep: {}, ncores: {}, imapped: {}, CPUS: {}'
					.format(i, self.afnstep, ncores, i + ncores, self.CPUS))
				cpumask = ','.join(cpus)
		return cpumask


class ExecPool(object):
	"""Execution Pool of workers for jobs

	A worker in the pool executes only the one job, a new worker is created for
	each subsequent job.
	"""
	_CPUS = cpu_count()  # The number of logical CPUs in the system
	_KILLDELAY = 3  # 3 cycles of self.latency, termination wait time
	_MEMLOW = _RAM_SIZE - _RAM_LIMIT  # Low RAM(RSS) memory condition
	assert _MEMLOW >= 0, '_RAM_SIZE should be >= _RAM_LIMIT'
	# Memory threshold ratio, multiplier for the job to have a gap and
	# reduce the number of reschedulings, reccommended value: 1.2 .. 1.6
	_JMEMTRR = 1.5
	assert _JMEMTRR >= 1, 'Memory threshold retio should be >= 1'

	def __init__(self, wksnum=max(_CPUS-1, 1), afnmask=None, memlimit=0., latency=0., name=None):  # afnstep=None
		"""Execution Pool constructor

		wksnum  - number of resident worker processes, >=1. The reasonable value is
			<= logical CPUs (returned by cpu_count()) = NUMA nodes * node CPUs,
			where node CPUs = CPU cores * HW treads per core.
			The recomended value is max(cpu_count() - 1, 1) to leave one logical
			CPU for the benchmarking framework and OS applications.

			To guarantee minimal average RAM per a process, for example 2.5 Gb
			without _LIMIT_WORKERS_RAM flag (not using psutil for the dynamic
			control of memory consumption):
				wksnum = min(cpu_count(), max(ramfracs(2.5), 1))
		afnmask  - affinity mask for the worker processes, AffinityMask
			None if not applied
		memlimit  - limit total amount of Memory (automatically reduced to
			the amount of physical RAM if the larger value is specified) in gigabytes
			that can be used by worker processes to provide in-RAM computations, >= 0.
			Dynamically reduces the number of workers to consume not more memory
			than specified. The workers are rescheduled starting from the
			most memory-heavy processes.
			NOTE:
				- applicable only if _LIMIT_WORKERS_RAM
				- 0 means unlimited (some jobs might be [partially] swapped)
				- value > 0 is automatically limited with total physical RAM to process
					jobs in RAM almost without the swapping
		latency  - approximate minimal latency of the workers monitoring in sec, float >= 0;
			0 means automatically defined value (recommended, typically 2-3 sec)
		name  - name of the execution pool to distinguish traces from subsequently
			created execution pools (only on creation or termination)

		Internal attributes:
		alive  - whether the execution pool is alive or terminating, bool.
			Should be reseted to True on resuse after the termination.
			NOTE: should be reseted to True if the execution pool is reused
			after the joining or termination.
		"""
		assert (wksnum >= 1 and (afnmask is None or isinstance(afnmask, AffinityMask))
			and memlimit >= 0 and latency >= 0 and (name is None or isinstance(name, str))
			), ('Arguments are invalid:  wksnum: {}, afnmask: {}, memlimit: {}'
			', latency: {}, name: {}'.format(wksnum, afnmask, memlimit, latency, name))
		self.name = name

		# Verify and update wksnum and afnstep if required
		if afnmask:
			# Check whether _AFFINITYBIN exists in the system
			try:
				with open(os.devnull, 'wb') as fdevnull:
					subprocess.call([_AFFINITYBIN, '-V'], stdout=fdevnull)
				if afnmask.afnstep * wksnum > afnmask.CPUS:
					print('WARNING{}, the number of worker processes is reduced'
						' ({wlim0} -> {wlim} to satisfy the affinity step'
						.format('' if not self.name else ' ' + self.name
						,wlim0=wksnum, wlim=afnmask.CPUS//afnmask.afnstep), file=sys.stderr)
					wksnum = afnmask.CPUS // afnmask.afnstep
			except OSError as err:
				afnmask = None
				print('WARNING{}, {afnbin} does not exists in the system to fix affinity: {err}'
					.format('' if not self.name else ' ' + self.name
					, afnbin=_AFFINITYBIN, err=err), file=sys.stderr)
		self._wkslim = wksnum  # Max number of resident workers
		self._workers = set()  # Scheduled and started jobs, i.e. worker processes:  {executing_job, }
		self._jobs = collections.deque()  # Scheduled jobs that have not been started yet:  deque(job)
		self._tstart = None  # Start time of the execution of the first task
		# Affinity scheduling attributes
		self._afnmask = afnmask  # Affinity mask functor
		self._affinity = None if not self._afnmask else [None]*self._wkslim
		assert (self._wkslim * (1 if not self._afnmask else self._afnmask.afnstep)
			<= self._CPUS), ('_wkslim or afnstep is too large:'
			'  _wkslim: {}, afnstep: {}, CPUs: {}'.format(self._wkslim
			, 1 if not self._afnmask else self._afnmask.afnstep, self._CPUS))
		# Execution rescheduling attributes
		self.memlimit = 0. if not _LIMIT_WORKERS_RAM else max(0, min(memlimit, _RAM_LIMIT))  # in Gb
		self.latency = latency if latency else 1 + (self.memlimit != 0.)  # Seconds of sleep on pooling
		# Predefined private attributes
		self.__termlock = Lock()  # Lock for the __terminate() to avoid simultaneous call by the signal and normal execution flow
		self.alive = True  # The execution pool is in the working state (has not been terminated)

		if self.memlimit and self.memlimit != memlimit:
			print('WARNING{}, total memory limit is reduced to guarantee the in-RAM'
				' computations: {:.6f} -> {:.6f} Gb'.format('' if not self.name else ' ' + self.name
				, memlimit, self.memlimit), file=sys.stderr)


	def __enter__(self):
		"""Context entrence"""
		# Reuse execpool if possible
		if not self.alive:
			if not self._workers and not self._jobs:
				print('WARNING{}, the non-cleared execution pool is reused'
					.format('' if not self.name else ' ' + self.name))
				self._tstart = None
				self.alive = True
			else:
				raise ValueError('Terminating dirty execution pool can not be reentered:'
					'  alive: {}, {} workers, {} jobs'.format(self.alive
					, len(self._workers), len(self._jobs)))
		return self


	def __exit__(self, etype, evalue, traceback):
		"""Contex exit

		etype  - exception type
		evalue  - exception value
		traceback  - exception traceback
		"""
		self.__terminate()
		# Note: the exception (if any) is propagated if True is not returned here


	def __del__(self):
		"""Destructor"""
		self.__terminate()


	def __finalize__(self):
		"""Late clear up called after the garbage collection (unlikely to be used)"""
		self.__terminate()


	def __terminate(self):
		"""Force termination of the pool"""
		# Wait for the new worker registration on the job starting if required,
		# which shold be done << 10 ms
		acqlock = self.__termlock.acquire(True, 0.01)  # 10 ms
		self.alive = False  # The execution pool is terminating, should be always set on termination
		# The lock can't be acquired (takes more than 10 ms) only if the termination was already called
		if not acqlock or not (self._workers or self._jobs):
			if acqlock:
				self.__termlock.release()
			return
		print('WARNING{}, terminating the execution pool with {} nonstarted jobs and {} workers'
			', executed {} h {} m {:.4f} s, callstack fragment:'
			.format('' if not self.name else ' ' + self.name, len(self._jobs), len(self._workers)
			, *secondsToHms(0 if self._tstart is None else time.time() - self._tstart)), file=sys.stderr)
		traceback.print_stack(limit=5, file=sys.stderr)

		# Shut down all [nonstarted] jobs
		for job in self._jobs:
			# Note: only executing jobs, i.e. workers might have activated affinity
			print('  Scheduled nonstarted "{}" is removed'.format(job.name), file=sys.stderr)
		self._jobs.clear()

		# Shut down all workers
		for job in self._workers:
			print('  Terminating "{}" #{} ...'.format(job.name, job.proc.pid), file=sys.stderr)
			job.proc.terminate()
		# Wait a few sec for the successful process termitaion before killing it
		i = 0
		active = True
		while active and i < self._KILLDELAY:
			i += 1
			active = False
			for job in self._workers:
				if job.proc.poll() is None:
					active = True
					break
			time.sleep(self.latency)
		# Kill nonterminated processes
		if active:
			for job in self._workers:
				if job.proc.poll() is None:
					print('  Killing "{}" #{} ...'.format(job.name, job.proc.pid), file=sys.stderr)
					job.proc.kill()
		# Tidy jobs
		for job in self._workers:
			self.__complete(job, False)
		self._workers.clear()
		## Set _wkslim to 0 to not start any jobs
		#self._wkslim = 0  # ATTENTION: reset of the _wkslim can break silent subsequent reuse of the execution pool
		self.__termlock.release()


	def __postpone(self, job, priority=False):
		"""Schedule this job for the later execution

		Schedule this job for the later execution if it does not violates timeout
		and memory limit (if it was terminated because of the group violation made
		not by a single worker process).

		job  - postponing (rescheduling) job
		priority  - priority scheduling (to the queue begin instead of the end).
			Used only when the job should should be started, but was terminated
			earlier (because of the timeout with restart or group memory limit violation)
		"""
		if not self.alive:
			print('WARNING, postponing of the job "{}" is cancelled because'
				  ' the execution pool is not alive'.format(job.name))
			return
		# Note:
		# - postponing jobs are terminated jobs only, can be called for !_CHAINED_CONSTRAINTS;
		# - wksnum < self._wkslim
		wksnum = len(self._workers)  # The current number of worker processes
		assert ((job.terminates or job.tstart is None) and (priority or self._workers)
			# and _LIMIT_WORKERS_RAM and not job in self._workers and not job in self._jobs  # Note: self._jobs scanning is time-consuming
			and (not self.memlimit or job.mem < self.memlimit)  # and wksnum < self._wkslim
			and (job.tstart is None) == (job.tstop is None) and (not job.timeout
			or (True if job.tstart is None else job.tstop - job.tstart < job.timeout)
			) and (not self._jobs or self._jobs[0].wkslim >= self._jobs[-1].wkslim)), (
			'A terminated non-rescheduled job is expected that doest not violate constraints.'
			' "{}" terminates: {}, started: {} jwkslim: {} vs {} pwkslim, pripority: {}, {} workers, {} jobs: {};'
			'\nmem: {:.4f} / {:.4f} Gb, exectime: {:.4f} ({} .. {}) / {:.4f} sec'.format(
			job.name, job.terminates, job.tstart is not None, job.wkslim, self._wkslim
			, priority, wksnum, len(self._jobs)
			, ', '.join(['#{} {}: {}'.format(ij, j.name, j.wkslim) for ij, j in enumerate(self._jobs)])
			, 0 if not self.memlimit else job.mem, self.memlimit
			, 0 if job.tstop is None else job.tstop - job.tstart, job.tstart, job.tstop, job.timeout))
		# Postpone only the goup-terminated jobs by memory limit, not a single worker that exceeds the (time/memory) constraints
		# (except the explicitly requirested restart via ontimeout, which results the priority rescheduling)
		# Note: job wkslim should be updated before adding to the _jobs to handle correctly the case when _jobs were empty
		if _DEBUG_TRACE >= 2:
			print('  Nonstarted initial jobs: ', ', '.join(['{} ({})'.format(pj.name, pj.wkslim) for pj in self._jobs]))
		# Note: it does not impact on the existence of zombie procs
		## Reset proc to remove it from the subproceeses table and avoid zombies for the postponed jobs
		#if job.terminates:
		#	job.proc = None  # Reset old job process if any
		#	#job.terminates = 0  # Reset termination requests counter
		#	#job.tstop = None  # Reset the completion / termination time
		#	#job.tstart = None  # Reset the start times
		jobsnum = len(self._jobs)
		i = 0
		if priority:
			# Add to the begin of jobs with the same wkslim
			while i < jobsnum and self._jobs[i].wkslim > job.wkslim:
				i += 1
		else:
			# Add to the end of jobs with the same wkslim
			i = jobsnum - 1
			while i >= 0 and self._jobs[i].wkslim < job.wkslim:
				i -= 1
			i += 1
		if i != jobsnum:
			self._jobs.rotate(-i)
			self._jobs.appendleft(job)
			self._jobs.rotate(i)
		else:
			self._jobs.append(job)

		# Update limit of the worker processes of the other larger nonstarted jobs
		# of the same category as the added job has
		if _CHAINED_CONSTRAINTS and job.category is not None:
			k = 0
			kend = i
			while k < kend:
				pj = self._jobs[k]
				if pj.category == job.category and pj.size >= job.size:
					# Set mem in for the related nonstarted heavier jobs
					if pj.mem < job.mem:
						pj.mem = job.mem
					if job.wkslim < pj.wkslim:
						pj.wkslim = job.wkslim
						# Update location of the jobs in the queue, move the updated
						# job to the place before the origin
						if kend - k >= 2:  # There is no sence to reschedule pair of subsequent jobs
							self._jobs.rotate(-k)
							self._jobs.popleft()  # == pj; -1
							self._jobs.rotate(1 + k-i)  # Note: 1+ because one job is removed
							self._jobs.appendleft(pj)
							self._jobs.rotate(i-1)
							kend -= 1  # One more job is added before i
							k -= 1  # Note: k is incremented below
				k += 1
		if _DEBUG_TRACE >= 2:
			print('  Nonstarted updated jobs: ', ', '.join(['{} ({})'.format(pj.name, pj.wkslim) for pj in self._jobs]))


	def __start(self, job, async=True):
		"""Start the specified job by one of the worker processes

		job  - the job to be executed, instance of Job
		async  - async execution or wait intill execution completed
		return  - 0 on successful execution, proc.returncode otherwise
		"""
		assert isinstance(job, Job), 'Job type is invalid: {}'.format(job)
		assert job.tstop is None or job.terminates, ('The starting job "{}" is'
			' expected to be non-completed'.format(job.name))
		wksnum = len(self._workers)  # The current number of worker processes
		if (async and wksnum >= self._wkslim) or not self._wkslim or not self.alive:
			# Note: can be cause by the execution pool termination
			raise ValueError('Free workers should be available ({} busy workers of {}), alive: {}'
				.format(wksnum, self._wkslim, self.alive))
		#if _DEBUG_TRACE:
		print('Starting "{}"{}, workers: {} / {}...'.format(job.name, '' if async else ' in sync mode'
			, wksnum, self._wkslim), file=sys.stderr if _DEBUG_TRACE else sys.stdout)

		# Reset automatically defined values for the restarting job, which is possible only if it was terminated
		if job.terminates:
			job.terminates = 0  # Reset termination requests counter
			job.proc = None  # Reset old job process if any
			job.tstop = None  # Reset the completion / termination time
			# Note: retain previous value of mem for better scheduling, it is the valid value for the same job
		job.tstart = time.time()
		if job.onstart:
			if _DEBUG_TRACE >= 2:
				print('  Starting onstart() for job "{}"'.format(job.name), file=sys.stderr)
			try:
				job.onstart()
			except Exception as err:
				print('ERROR in onstart() callback of "{}": {}, the job is discarded. {}'
					.format(job.name, err, traceback.format_exc(5)), file=sys.stderr)
				errinf = getattr(err, 'errno', None)
				return -1 if errinf is None else errinf.errorcode
		# Consider custom output channels for the job
		fstdout = None
		fstderr = None
		acqlock = False  # The lock is aquired and should be released
		try:
			# Initialize fstdout, fstderr by the required output channel
			timestamp = None
			for joutp in (job.stdout, job.stderr):
				if joutp and isinstance(joutp, str):
					basedir = os.path.split(joutp)[0]
					if basedir and not os.path.exists(basedir):
						os.makedirs(basedir)
					try:
						fout = None
						if joutp == job.stdout:
							fout = open(joutp, 'a')
							job._fstdout = fout
							fstdout = fout
							outcapt = 'stdout'
						elif joutp == job.stderr:
							fout = open(joutp, 'a')
							job._fstderr = fout
							fstderr = fout
							outcapt = 'stderr'
						else:
							raise ValueError('Ivalid output stream value: ' + str(joutp))
						# Add a timestamp if the file is not empty to distinguish logs
						if fout is not None and os.fstat(fout.fileno()).st_size:
							if timestamp is None:
								timestamp = time.gmtime()
							print(timeheader(timestamp), file=fout)  # Note: prints also newline unlike fout.write()
					except IOError as err:
						print('ERROR on opening custom {} "{}" for "{}": {}. Default is used.'
							.format(outcapt, joutp, job.name, err), file=sys.stderr)
				else:
					if joutp == job.stdout:
						fstdout = joutp
					elif joutp == job.stderr:
						fstderr = joutp
					else:
						raise ValueError('Ivalid output stream buffer: ' + str(joutp))

			if _DEBUG_TRACE >= 2 and (fstdout or fstderr):
				print('"{}" output channels:\n\tstdout: {}\n\tstderr: {}'.format(job.name
					, job.stdout, job.stderr))  # Note: write to log, not to the stderr
			if(job.args):
				# Consider CPU affinity
				# Note: the exception is raised by .index() if the _affinity table is corrupted (doesn't have the free entry)
				iafn = -1 if not self._affinity or job._omitafn else self._affinity.index(None)  # Index in the affinity table to bind process to the CPU/core
				if iafn >= 0:
					job.args = [_AFFINITYBIN, '-c', self._afnmask(iafn)] + list(job.args)
				if _DEBUG_TRACE >= 2:
					print('  Opening proc for "{}" with:\n\tjob.args: {},\n\tcwd: {}'.format(job.name
						, ' '.join(job.args), job.workdir), file=sys.stderr)
				acqlock = self.__termlock.acquire(False)
				if not acqlock or not self.alive:
					# Note: it just interrupts job start, but does not cause termination of the whole (already terminated) execution pool
					raise EnvironmentError((errno.EINTR, 'Jobs can not be started beause the execution pool has been terminated'))  # errno.ERESTART
				job.proc = subprocess.Popen(job.args, bufsize=-1, cwd=job.workdir, stdout=fstdout, stderr=fstderr)  # bufsize=-1 - use system default IO buffer size
				if async:
					self._workers.add(job)
				# ATTENTION: the exception could be raised before the lock releasing on process creation
				self.__termlock.release()
				acqlock = False  # Note: an exception can be thrown below, but the lock is already released and should not be released again
				if iafn >= 0:
					try:
						self._affinity[iafn] = job.proc.pid
						print('"{jname}" #{pid}, iafn: {iafn} (CPUs #: {icpus})'
							.format(jname=job.name, pid=job.proc.pid, iafn=iafn
							, icpus=self._afnmask(iafn)))  # Note: write to log, not to the stderr
					except IndexError as err:
						# Note: BaseException is used to terminate whole execution pool
						raise BaseException('Affinity table is inconsistent: {}'.format(err))
				# Wait a little bit to start the process besides it's scheduling
				if job.startdelay > 0:
					time.sleep(job.startdelay)
		except BaseException as err:  # Should not occur: subprocess.CalledProcessError
			# ATTENTION: the exception could be raised on process creation or on not self.alive
			# with acquired lock, which should be released
			if acqlock:
				self.__termlock.release()
			print('ERROR on "{}" start occurred: {}, the job is discarded. {}'.format(
				job.name, err, traceback.format_exc(5)), file=sys.stderr)
			# Note: process-associated file descriptors are closed in complete()
			if job.proc is not None and job.proc.poll() is None:  # Note: this is an extra rare, but possible case
				# poll None means the process has not been terminated / completed,
				# which can be if sleep() generates exception or if the system
				# interrupted called [and the sync] process already created
				job.proc.terminate()
			self.__complete(job, False)
			# ATTENTION: reraise exception for the BaseException but not Exception subclusses
			# to have termination of the whole pool by the system interruption
			if not isinstance(err, Exception):
				raise
		else:
			if async:
				return 0
			# Synchronous job processing
			err = None
			try:
				job.proc.wait()
			except BaseException as err:  # Should not occur: subprocess.CalledProcessError
				print('ERROR on the synchronous execution of "{}" occurred: {}, the job is discarded. {}'
					.format(job.name, err, traceback.format_exc(5)), file=sys.stderr)
			finally:
				self.__complete(job)
			# ATTENTION: reraise exception for the BaseException but not Exception subclusses
			# to have termination of the whole pool by the system interruption
			if err and not isinstance(err, Exception):
				raise
		if job.proc.returncode:
			print('WARNING, "{}" failed to start, errcode: {}'.format(job.name, job.proc.returncode), file=sys.stderr)
		return job.proc.returncode


	def __complete(self, job, graceful=None):
		"""Complete the job clearing affinity if required

		job  - the job to be completed
		graceful  - the completion is graceful (job was not terminated internally
			due to some error or externally).
			None means unknown and should be identified automatically.
		"""
		if self._affinity and not job._omitafn and job.proc is not None:
			try:
				self._affinity[self._affinity.index(job.proc.pid)] = None
			except ValueError:
				print('WARNING, affinity clearup is requested to the job "{}" without the activated affinity'
					.format(job.name), file=sys.stderr)
				pass  # Do nothing if the affinity was not set for this process
		job.complete(graceful)


	def __reviseWorkers(self):
		"""Rewise the workers

		Check for the comleted jobs and their timeouts, update corresponding
		workers and start the nonstarted jobs if possible.
		Apply chained termination and rescheduling on tiomeout and memory
		constraints violation if _CHAINED_CONSTRAINTS.
		"""
		# Process completed jobs, check timeouts and memory constraints matching
		completed = set()  # Completed workers:  {proc,}
		memall = 0.  # Consuming memory by workers
		jtorigs = {}  # Timeout caused terminating origins (jobs) for the chained termination, {category: lightweightest_job}
		jmorigs = {}  # Memory limit caused terminating origins (jobs) for the chained termination, {category: smallest_job}
		for job in self._workers:
			if job.proc.poll() is not None:  # Not None means the process has been terminated / completed
				completed.add(job)
				continue

			exectime = time.time() - job.tstart
			# Update memory statistics (if required) and skip jobs that do not exceed the specified time/memory constraints
			if not job.terminates and (not job.timeout or exectime < job.timeout
			) and (not self.memlimit or job.mem < self.memlimit):
				# Update memory consumption statistics if applicable
				if self.memlimit:
					# NOTE: Evaluate memory consuption for the heaviest process in the process tree
					# of the origin job process to allow additional intermediate apps for the evaluations like:
					# ./exectime ./clsalg ca_prm1 ca_prm2
					job._updateMem()  # Consider mem consumption of the past runs if any
					if job.mem < self.memlimit:
						memall += job.mem  # Consider mem consumption of past runs if any
						#if _DEBUG_TRACE >= 3:
						#	print('  "{}" consumes {:.4f} Gb, memall: {:.4f} Gb'.format(job.name, job.mem, memall), file=sys.stderr)
						continue
					# The memory limits violating worker will be terminated
				else:
					continue

			# Terminate the worker because of the timeout/memory constraints violation
			job.terminates += 1
			# Save the most lighgtweight terminating chain origins for timeouts and memory overusage by the single process
			if _CHAINED_CONSTRAINTS and job.category is not None and job.size:
				# ATTENTION: do not terminate related jobs of the process that should be restarted by timeout,
				# because such processes often have non-deterministic behavior and specially scheduled to be
				# rexecuted until success
				if job.timeout and exectime >= job.timeout and not job.ontimeout:
					# Timeout constraints
					jorg = jtorigs.get(job.category, None)
					if jorg is None or job.size * job.slowdown < jorg.size * jorg.slowdown:
						jtorigs[job.category] = job
				elif self.memlimit and job.mem >= self.memlimit:
					# Memory limit constraints
					jorg = jmorigs.get(job.category, None)
					if jorg is None or job.size < jorg.size:
						jmorigs[job.category] = job
				# Otherwise this job is terminated because of multiple processes together overused memory,
				# it should be reschedulted, but not removed completely

			# Force killing when the termination does not work
			if job.terminates >= self._KILLDELAY:
				job.proc.kill()
				completed.add(job)
				if _DEBUG_TRACE:  # Note: anyway completing terminated jobs are traced
					exectime = time.time() - job.tstart
					print('WARNING, "{}" #{} is killed because of the {} violation'
						' consuming {:.4f} Gb with timeout of {:.4f} sec, executed: {:.4f} sec ({} h {} m {:.4f} s)'
						.format(job.name, job.proc.pid
						, 'timeout' if job.timeout and exectime >= job.timeout else (
							('' if job.mem >= self.memlimit else 'group ') + 'memory limit')
						, 0 if not self.memlimit else job.mem
						, job.timeout, exectime, *secondsToHms(exectime)), file=sys.stderr)
			else:
				job.proc.terminate()  # Schedule the worker completion to the next revise
		#self.memall = memall

		# Terminate chained related workers and jobs of the single jobs that violate timeout/memory constraints
		if _CHAINED_CONSTRAINTS and (jtorigs or jmorigs):
			# Traverse over the workers with defined job category and size
			for job in self._workers:
				# Note: even in the seldom case of the terminating job, it should be marked if chained-dependent
				# of the constraints violating job, to not be restarted (by request on timeout) or postponed
				if job.category is not None and job.size:
					# Travers over the chain origins and check matches skipping the origins themselves
					# Timeout chains
					for jorg in viewvalues(jtorigs):
						# Note: job !== jorg, because jorg terminates and job does not
						if (job.category == jorg.category  # Skip already terminating items
						and job is not jorg
						and job.size * job.slowdown >= jorg.size * jorg.slowdown):
							job.chtermtime = True  # Chained termination by time
							if job.terminates:
								break  # Switch to the following job
							# Terminate the worker
							job.terminates += 1
							job.proc.terminate()  # Schedule the worker completion to the next revise
							memall -= job.mem  # Reduce total memory consumed by the active workers
							break  # Switch to the following job
					else:
						# Memory limit chains
						for jorg in viewvalues(jmorigs):
							# Note: job !== jorg, because jorg terminates and job does not
							if (job.category == jorg.category  # Skip already terminating items
							and job is not jorg
							and not job.lessmem(jorg)):
								job.chtermtime = False  # Chained termination by memory
								if job.terminates:
									break  # Switch to the following job
								# Terminate the worker
								job.terminates += 1
								job.proc.terminate()  # Schedule the worker completion to the next revise
								memall -= job.mem  # Reduce total memory consumed by the active workers
								break  # Switch to the following job
			# Traverse over the nonstarted jobs with defined job category and size
			if _DEBUG_TRACE >= 2:
				print('  Updating chained constraints in nonstarted jobs: ', ', '.join([job.name for job in self._jobs]))
			jrot = 0  # Accumulated rotation
			ij = 0  # Job index
			while ij < len(self._jobs) - jrot:
				job = self._jobs[ij]
				if job.category is not None and job.size:
					# Travers over the chain origins and check matches skipping the origins themselves
					# Time constraints
					for jorg in viewvalues(jtorigs):
						if (job.category == jorg.category  # Skip already terminating items
						and job.size * job.slowdown >= jorg.size * jorg.slowdown):
							# Remove the item
							self._jobs.rotate(-ij)
							jrot += ij
							self._jobs.popleft()  # == job
							ij = -1  # Later +1 is added, so the index will be 0
							print('WARNING, nonstarted "{}" with weight {} is cancelled by timeout chain from "{}" with weight {}'.format(
								job.name, job.size * job.slowdown, jorg.name, jorg.size * jorg.slowdown), file=sys.stderr)
							break
					else:
						# Memory limit constraints
						for jorg in viewvalues(jmorigs):
							if (job.category == jorg.category  # Skip already terminating items
							and not job.lessmem(jorg)):
								# Remove the item
								self._jobs.rotate(-ij)
								jrot += ij
								self._jobs.popleft()  # == job
								ij = -1  # Later +1 is added, so the index will be 0
								print('WARNING, nonstarted "{}" with size {} is cancelled by memory limit chain from "{}" with size {}'
									' and mem {:.4f}'.format(job.name, job.size, jorg.name, jorg.size, jorg.mem), file=sys.stderr)
								break
				ij += 1
			# Recover initial order of the jobs
			self._jobs.rotate(jrot)
		# Remove completed jobs from worker processes
		# ATTENTINON: it should be done after the _CHAINED_CONSTRAINTS check to
		# mark the completed dependent jobs to not be restarted / postponed
		# Note: jobs complete execution relatively seldom, so set with fast
		# search is more suitable than full scan of the list
		for job in completed:
			self._workers.remove(job)
		# self._workers = [w for w in self._workers if w not in completed]

		# Check memory limitation fulfilling for all remained processes
		if self.memlimit:
			memfree = inGigabytes(psutil.virtual_memory().available)  # Amount of free RAM (RSS) in Gb; skip it if memlimit is not requested
		if self.memlimit and (memall >= self.memlimit or memfree <= self._MEMLOW):  # Jobs should use less memory than the limit
			# Terminate the largest workers and reschedule jobs or reduce the workers number
			wksnum = len(self._workers)
			# Overused memory with some gap (to reschedule less) to be released by worker(s) termination
			memov = memall - self.memlimit * (wksnum / (wksnum + 1.)) if memall >= self.memlimit else (self._MEMLOW + self.memlimit / (wksnum + 1.))
			pjobs = set()  # The heaviest jobs to be postponed to satisfy the memory limit constraint
			# Remove the heaviest workers until the memory limit constraints are sutisfied
			hws = []  # Heavy workers
			# ATTENTION: at least one worker should be remained after the reduction
			# Note: at least one worker shold be remained
			while memov >= 0 and len(self._workers) - len(pjobs) > 1:  # Overuse should negative, i.e. underuse to starr any another job, 0 in practice is insufficient of the subsequent execution
				# Reinitialize the heaviest remained jobs and continue
				for job in self._workers:
					if not job.terminates and job not in pjobs:
						hws.append(job)
						break
				assert self._workers and hws, 'Non-terminated worker processes must exist here'
				for job in self._workers:
					# Note: use some threshold for mem evaluation and consider starting time on scheduling
					# to terminate first the least worked processes (for approximately the same memory consumption)
					dr = 0.1  # Threshold parameter ratio, recommended value: 0.05 - 0.15; 0.1 means delta of 10%
					if not job.terminates and (job.mem * (1 - dr) >= hws[-1].mem or
					(job.mem * (1 + dr/2) >= hws[-1].mem and job.tstart > hws[-1].tstart)) and job not in pjobs:
						hws.append(job)
				# Move the largest jobs to postponed until memov is negative
				while memov >= 0 and hws and wksnum - len(pjobs) > 1:  # Retain at least a single worker
					job = hws.pop()
					pjobs.add(job)
					memov -= job.mem
				if _DEBUG_TRACE >= 2:
					print('  Group mem limit violation removing jobs: {}, remained: {} (from the end)'
						.format(', '.join([j.name for j in pjobs]), ', '.join([j.name for j in hws])))
			# Terminate and remove worker processes of the postponing jobs
			wkslim = self._wkslim - len(pjobs)  # New workers limit for the postponing job  # max(self._wkslim, len(self._workers))
			assert wkslim >= 1, 'The number of workers should not be less than 1'
			while pjobs:
				job = pjobs.pop()
				# Terminate the worker
				job.terminates += 1
				# Schedule the worker completion (including removement from the workers) to the next revise
				job.proc.terminate()
				# Upate wkslim
				job.wkslim = wkslim
			# Update amount of estimated memall
			memall = self.memlimit + memov  # Note: memov is negative here

		# Process completed (and terminated) jobs: execute callbacks and remove the workers
		#cterminated = False  # Completed terminated procs processed
		for job in completed:
			# The completion is graceful only if the termination requests were not received
			self.__complete(job, not job.terminates and not job.proc.returncode)
			exectime = job.tstop - job.tstart
			# Restart the job if it was terminated and should be restarted
			if not job.terminates:
				#cterminated = True
				continue
			print('WARNING, "{}" #{} is terminated because of the {} violation'
				', chtermtime: {}, consumes {:.4f} / {:.4f} Gb, timeout {:.4f} sec, executed: {:.4f} sec ({} h {} m {:.4f} s)'
				.format(job.name, job.proc.pid
				, 'timeout' if job.timeout and exectime >= job.timeout else (
					('' if job.mem >= self.memlimit else 'group ') + 'memory limit')
				, None if not _CHAINED_CONSTRAINTS else job.chtermtime
				, 0 if not self.memlimit else job.mem, self.memlimit
				, job.timeout, exectime, *secondsToHms(exectime)), file=sys.stderr)
			# Skip memory limit and timeout violating jobs that do not require autorestart (applicable only for the timeout)
			if (exectime >= job.timeout and not job.ontimeout) or (_CHAINED_CONSTRAINTS
			and job.chtermtime is not None) or (self.memlimit and job.mem >= self.memlimit):
				continue
			# Reschedule job having the group violation of the memory limit
			# if timeout is not violated or restart on timeout if requested
			# Note: memall to not postpone the single existing job
			if self.memlimit and ((memall and memall + job.mem * self._JMEMTRR >= self.memlimit)
			or memfree - job.mem * self._JMEMTRR <= self._MEMLOW) and (
			not job.timeout or exectime < job.timeout or job.ontimeout):
				self.__postpone(job)
			# Restart the job on timeout if requested
			elif exectime >= job.timeout and job.ontimeout:  # ATTENTION: restart on timeout only and if required
				# Note: if the job was terminated by timeout then memory limit was not met
				# Note: earlier executed job might not fit into the RAM now because of the inreasing mem consumption by the workers
				#if _DEBUG_TRACE >= 3:
				#	print('  "{}" is being rescheduled, workers: {} / {}, estimated mem: {:.4f} / {:.4f} Gb'
				#		.format(job.name, len(self._workers), self._wkslim, memall + job.mem, self.memlimit)
				#		, file=sys.stderr)
				assert len(self._workers) < self._wkslim, 'Completed jobs formed from the reduced workers'
				#assert not self.memlimit or memall + job.mem * self._JMEMTRR < self.memlimit, (
				#	'Group exceeding of the memory limit should be already processed')
				if not self.__start(job) and self.memlimit:
					memall += job.mem  # Reuse .mem from the previous run if exists
				# Note: do not call complete() on failed restart
			else:
				assert exectime < job.timeout, 'Timeout violating jobs should be already skipped'
				# The job was terminated (by group violation of memory limit or timeout with restart),
				# but now can be started successfully and will be satrted soon
				self.__postpone(job, True)
		# Note: the number of workers is not reduced to less than 1

		# Note: active_children() does not impact on the existence of zombie procs
		#if cterminated:
		#	# Note: required to join terminated child procs and avoid zombies
		#	active_children()   # Return list of all live children of the current process, joining any processes which have already finished

		# Start subsequent job if it is required
		if _DEBUG_TRACE >= 2:
			print('  Nonstarted jobs: ', ', '.join(['{} ({})'.format(job.name, job.wkslim) for pj in self._jobs]))
		while self._jobs and len(self._workers) < self._wkslim:
			#if _DEBUG_TRACE >= 3:
			#	print('  "{}" (expected totmem: {:.4f} / {:.4f} Gb) is being resheduled, {} nonstarted jobs: {}'
			#		.format(self._jobs[0].name, 0 if not self.memlimit else memall + job.mem, self.memlimit
			#		, len(self._jobs), ', '.join([j.name for j in self._jobs])), file=sys.stderr)
			job = self._jobs.popleft()
			# Jobs should use less memory than the limit, a worker process violating (time/memory) constaints are already filtered out
			# Note: memall to not postpone the single existing job
			if self.memlimit:
				jmemx = (job.mem if job.mem else memall / (1 + len(self._workers))) * self._JMEMTRR  # Extended estimated job mem
			if self.memlimit and ((memall and memall + jmemx >= self.memlimit
			# Note: omit the low memory condition for a single worker, otherwise the pool can't be executed
			) or (memfree - jmemx <= self._MEMLOW and self._workers)):
				# Note: only restarted jobs have defined mem
				# Postpone the job updating it's workers limit
				assert job.mem < self.memlimit, 'The workers exceeding memory constraints were already filtered out'
				self.__postpone(job)
				break
			elif not self.__start(job) and self.memlimit:
				memall += job.mem  # Reuse .mem from the previous run if exists
		assert (self._workers or not self._jobs) and self._wkslim, (
			'Worker processes should always exist if nonstarted jobs are remained:'
			'  workers: {}, wkslim: {}, jobs: {}'.format(len(self._workers)
			, self._wkslim, len(self._jobs)))


	def execute(self, job, async=True):
		"""Schecule the job for the execution

		job  - the job to be executed, instance of Job
		async  - async execution or wait until execution completed
		  NOTE: sync tasks are started at once
		return  - 0 on successful execution, process return code otherwise
		"""
		if not self.alive:
			print('WARNING, scheduling of the job "{}" is cancelled because'
				  ' the execution pool is not alive'.format(job.name))
			return errno.EINTR
		assert isinstance(job, Job) and job.name, (
			'Job type is invalid or the instance is not initialized: {}'.format(job))
		# Note: _wkslim an be 0 only on/after the termination
		assert len(self._workers) <= self._wkslim and self._wkslim >= 1, (
			'Number of workers exceeds the limit or the pool has been terminated:'
			'  workers: {}, wkslim: {}, alive: {}'
			.format(len(self._workers), self._wkslim, self.alive))

		if _DEBUG_TRACE:
			print('Scheduling the job "{}" with timeout {}'.format(job.name, job.timeout))
		errcode = 0
		# Initialize the [latest] value of job workers limit
		if self.memlimit:
			job.wkslim = self._wkslim
		if async:
			# Start the execution timer
			if self._tstart is None:
				self._tstart = time.time()
			# Evaluate total memory consumed by the worker processes
			if self.memlimit:
				memall = 0.
				for wj in self._workers:
					memall += wj.mem
				memfree = inGigabytes(psutil.virtual_memory().available)  # Amount of free RAM (RSS) in Gb; skip it if memlimit is not requested
				jmemx = (job.mem if job.mem else memall / (1 + len(self._workers))) * self._JMEMTRR  # Extended estimated job mem
			# Schedule the job, postpone it if already nonstarted jobs exist or there are no any free workers
			if self._jobs or len(self._workers) >= self._wkslim or (
			self.memlimit and ((memall and memall + jmemx >= self.memlimit
			# Note: omit the low memory condition for a single worker, otherwise the pool can't be executed
			) or (memfree - jmemx <= self._MEMLOW and self._workers))):
				if _DEBUG_TRACE >= 2:
					print('  Postponing "{}", {} jobs, {} workers, {} wkslim'
						', group memlim violation: {}, lowmem: {}'.format(job.name, len(self._jobs)
						, len(self._workers), self._wkslim, self.memlimit and (memall and memall + (job.mem if job.mem else
						memall / (1 + len(self._workers))) * self._JMEMTRR >= self.memlimit)
						, self.memlimit and (memfree - jmemx <= self._MEMLOW and self._workers)))
				if not self.memlimit or not self._jobs or self._jobs[-1].wkslim >= job.wkslim:
					self._jobs.append(job)
				else:
					jnum = len(self._jobs)
					i = 0
					while i < jnum and self._jobs[i].wkslim >= job.wkslim:
						i += 1
					self._jobs.rotate(-i)
					self._jobs.appendleft(job)
					self._jobs.rotate(i)
				#self.__reviseWorkers()  # Anyway the workers are revised if exist in the working cycle
			else:
				if _DEBUG_TRACE >= 2:
					print('  Starting "{}", {} jobs, {} workers, {} wkslim'.format(job.name, len(self._jobs)
						, len(self._workers), self._wkslim))
				errcode = self.__start(job)
		else:
			errcode = self.__start(job, False)
			# Note: sync job is completed automatically on any fails
		return errcode



	def join(self, timeout=0.):
		"""Execution cycle

		timeout  - execution timeout in seconds before the workers termination, >= 0.
			0 means unlimited time. The time is measured SINCE the first job
			was scheduled UNTIL the completion of all scheduled jobs.
		return  - True on graceful completion, Flase on termination by the specified
			constrainets (timeout, memory limit, etc.)
		"""
		assert timeout >= 0., 'timeout valiadtion failed'
		if self._tstart is None:
			assert not self._jobs and not self._workers, (
				'Start time should be defined for non-empty execution pool')
			return False

		self.__reviseWorkers()
		while self.alive and (self._jobs or self._workers):
			if timeout and time.time() - self._tstart > timeout:
				print('WARNING, the execution pool is terminated on timeout', file=sys.stderr)
				self.__terminate()
				return False
			time.sleep(self.latency)
			self.__reviseWorkers()
		self._tstart = None  # Be ready for the following execution

		assert not self._jobs and not self._workers, 'All jobs should be finalized'
		return True


if __name__ == '__main__':
	"""Doc tests execution"""
	import doctest
	#doctest.testmod()  # Detailed tests output
	flags = doctest.REPORT_NDIFF | doctest.REPORT_ONLY_FIRST_FAILURE | doctest.IGNORE_EXCEPTION_DETAIL
	failed, total = doctest.testmod(optionflags=flags)
	if failed:
		print("Doctest FAILED: {} failures out of {} tests".format(failed, total), file=sys.stderr)
	else:
		print('Doctest PASSED')
	# Note: to check specific testcase use:
	# $ python -m unittest mpepool.TestExecPool.test_jobTimeoutChained
	if len(sys.argv) <= 1:
		try:
			import mpetests
			suite = mpetests.unittest.TestLoader().loadTestsFromModule(mpetests)
			if mpetests.mock is not None:
				print('')  # Indent from doctests
				if not mpetests.unittest.TextTestRunner().run(suite).wasSuccessful():  # TextTestRunner(verbosity=2)
				#if unittest.main().result:  # verbosity=2
					print('Try to reexecute the tests (hot run) or set x2-3 larger TEST_LATENCY')
			else:
				print('WARNING, the unit tests are skipped because the mock module is not installed', file=sys.stderr)
		except ImportError as err:
			print('WARNING, Unit tests skipped because of the failed import: ', err, file=sys.stderr)
