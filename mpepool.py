#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
\descr:  Multi-Process Execution Pool to schedule Jobs execution with per-job timeout,
optionally grouping them into Tasks and specifying optional execution parameters
considering NUMA architecture:
	- automatic CPU affinity management and maximization of the dedicated CPU cache
		for a worker process
	- automatic rescheduling and balancing (reduction) of the worker processes and on
		low memory condition for the in-RAM computations (requires psutil, can be disabled)
	- chained termination of related worker processes and jobs rescheduling to satisfy
		timeout and memory limit constraints
	- timeout per each Job (it was the main initial motivation to implement this module,
		because this feature is not provided by any Python implementation out of the box)
	- onstart/ondone callbacks, ondone is called only on successful completion
		(not termination) for both Jobs and Tasks (group of jobs)
	- stdout/err output, which can be redirected to any custom file or PIPE
	- custom parameters for each Job and respective owner Task besides the name/id

	Flexible API provides optional automatic restart of jobs on timeout, access to job's process,
	parent task, start and stop execution time and much more...


	Core parameters specified as global variables:
	_LIMIT_WORKERS_RAM  - limit the amount of virtual memory (<= RAM) used by worker processes,
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
\date: 2015-07 (v1), 2017-06 (v2)
"""

from __future__ import print_function, division  # Required for stderr output, must be the first import
import sys
import time
import collections
import os
import ctypes  # Required for the multiprocessing Value definition
import types  # Required for instance methods definition
import traceback  # Stacktrace;  To print a stacktrace fragment: traceback.print_stack(limit=5, file=sys.stderr)
import subprocess

from multiprocessing import cpu_count, Value, Lock

# Required to efficiently traverse items of dictionaries in both Python 2 and 3
try:
	from future.utils import viewvalues  #viewitems, viewkeys, viewvalues  # External package: pip install future
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


# Limit the amount of virtual memory used by worker processes.
# NOTE:
#  - requires import of psutils
#  - automatically reduced to the RAM size if the specidied limit is larger
_LIMIT_WORKERS_RAM = True
if _LIMIT_WORKERS_RAM:
	try:
		import psutil
	except ImportError:
		_LIMIT_WORKERS_RAM = False

# Use chained constraints (timeout and memory limitation) in jobs to terminate
# also related worker processes and/or reschedule jobs, which have the same
# category and heavier than the origin violating the constraints
_CHAINED_CONSTRAINTS = True


_RAM_SIZE = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 1024.**3  # RAM (physical memory) size in Gb
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
		assert isinstance(name, str) and timeout >= 0, 'Parameters validaiton failed'
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
	rtm = 0.85  # Memory retention ratio, used to not drop the memory info fast on temporal releases, E [0, 1)
	assert 0 <= rtm < 1, 'Memory retention ratio should E [0, 1)'

	# Note: the same job can be executed as Popen or Process object, but ExecPool
	# should use some wrapper in the latter case to manage it
	"""Job is executed in a separate process via Popen or Process object and is
	managed by the Process Pool Executor
	"""
	# NOTE: keyword-only arguments are specified after the *, supported only since Python 3
	def __init__(self, name, workdir=None, args=(), timeout=0, ontimeout=False, task=None #,*
	, startdelay=0, onstart=None, ondone=None, params=None, category=None, size=0, slowdown=1.
	, omitafn=False, stdout=sys.stdout, stderr=sys.stderr):
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
		category  - classification category, typically context or part of the name; requires _CHAINED_CONSTRAINTS
		size  - size of the processing data, >= 0; requires _LIMIT_WORKERS_RAM or _CHAINED_CONSTRAINTS
			0 means undefined size and prevents jobs chaining on constraints violation
		slowdown  - execution slowdown ratio (inversely to the [estimated] execution speed), E (0, inf)

		# Execution parameters, initialized automatically on execution
		tstart  - start time is filled automatically on the execution start (before onstart). Default: None
		tstop  - termination / completion time after ondone
			NOTE: onstart() and ondone() callbacks execution is included in the job execution time
		proc  - process of the job, can be used in the ondone() to read it's PIPE
		vmem  - consuming virtual memory (smooth max, not just the current value) or the least expected value
			inherited from the jobs of the same category having non-smaller size; requires _LIMIT_WORKERS_RAM
		"""
		assert isinstance(name, str) and timeout >= 0 and (task is None or isinstance(task, Task)
			) and size >= 0 and slowdown > 0, 'Parameters validaiton failed'
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
		if _LIMIT_WORKERS_RAM or _CHAINED_CONSTRAINTS:
			self.size = size  # Size of the processing data
			# Consumed VM on execution in gigabytes or the least expected (inherited from the
			# related jobs having the same category and non-smaller size)
			self.vmem = 0.
		if _CHAINED_CONSTRAINTS:
			self.category = category  # Job name
			self.slowdown = slowdown  # Execution slowdown ratio, ~ 1 / exec_speed
		if _LIMIT_WORKERS_RAM:
			self.wkslim = None  # Worker processes limit (max number) on the job postponing if any


	def _updateVmem(self, tree=False, heaviest=False):
		"""Update virtual memory consumption using smooth max

		Actual virtual memory (not the historical max) is retrieved and updated
		using:
		a) smoothing filter in case of the decreasing consumption and
		b) direct update in case of the increasing consumption

		Prerequisites: job must have defined proc and psutil should be available

		tree  - evaluate virtual memory consumption for the whole process tree
			spawned by the current process
		maxchild  - evaluate virtual memory consumption only for the heaviest in
			the process tree. Actual only for the enabled 'tree' option.

		return  - smooth max of job vmem
		"""
		# Current consumption of virtual memory (vms) by the job
		curvmem = 0  # Evaluating virtual memory
		try:
			up = psutil.Process(self.proc.pid)
			curvmem = up.memory_info().vms
			if tree:
				avmem = curvmem  # Memory consumption of the whole process tree
				if heaviest:
					xvmem = curvmem  # Memory consumption of the heaviest process in the tree
				for ucp in up.children(recursive=True):  # Note: fetches only children procs
					vmem = ucp.memory_info().vms  # Mb; Resident Set Size
					avmem += vmem
					if heaviest and xvmem < vmem:
						xvmem = vmem
				curvmem = avmem if not heaviest else xvmem
			# Check also
		except psutil.Error as err:
			# The process is finished and such pid does not exist
			print('WARNING, _updateVmem() failed, current proc vmem set to 0: ', err, file=sys.stderr)
		# Note: even if curvmem = 0 updte vmem smoothly to avoid issues on internal
		# fails of psutil even thought they should not happen
		curvmem = inGigabytes(curvmem)
		self.vmem = max(curvmem, self.vmem * Job.rtm + curvmem * (1-Job.rtm))
		return self.vmem


	def lessVmem(self, job):
		"""Whether vmem or estimated vmem is less than in the specified job

		job  - another job for the vmem comparison

		return  - [estimated] vmem is less
		"""
		assert self.category is not None and self.category == job.category, (
			'Only jobs of the same initialized category can be compared')
		return self.size < job.size if not self.vmem or not job.vmem else self.vmem < job.vmem


	def complete(self, graceful=None):
		"""Completion function
		ATTENTION: This function is called after the destruction of the job-associated process
		to perform cleanup in the context of the caller (main thread).

		graceful  - the job is successfully completed or it was terminated / crashed, bool.
			None means use "not self.proc.returncode" (i.e. whether errcode is 0)
		"""
		assert self.tstop is None and self.tstart is not None, (
			'A job ({}) should be already started and can be completed only once, tstart: {}, tstop: {}'
			.format(self.name, self.tstart, self.tstop))
		# Close process-related file descriptors
		for fd in (self._fstdout, self._fstderr):
			if fd and hasattr(fd, 'close'):
				fd.close()
		self._fstdout = None
		self._fstderr = None

		# Job-related post execution
		if graceful is None:
			graceful = self.proc is not None and not self.proc.returncode
		if graceful:
			if self.ondone:
				try:
					self.ondone()
				except Exception as err:
					print('ERROR in ondone callback of "{}": {}. {}'.format(
						self.name, err, traceback.format_exc()), file=sys.stderr)
			# Clean up
			# Remove empty logs skipping the system devnull
			tpaths = []  # Base dir of the output
			if (self.stdout and isinstance(self.stdout, str) and self.stdout != os.devnull
			and os.path.exists(self.stdout) and os.path.getsize(self.stdout) == 0):
				tpaths.append(os.path.split(self.stdout)[0])
				os.remove(self.stdout)
			if (self.stderr and isinstance(self.stderr, str) and self.stderr != os.devnull
			and os.path.exists(self.stderr) and os.path.getsize(self.stderr) == 0):
				tpath = os.path.split(self.stderr)[0]
				if not tpaths or tpath not in tpaths:
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

	Used to specify CPU afinity step dedicating the maximal amount of CPU cache.
	"""
	return int(subprocess.check_output([r"lscpu | sed -rn 's/^Thread\(s\).*(\w+)$/\1/p'"], shell=True))


def cpunodes():
	"""The number of NUMA nodes, where CPUs are located

	Used to evaluate CPU index from the affinity table index considerin the NUMA architectore.
	"""
	return int(subprocess.check_output([r"lscpu | sed -rn 's/^NUMA node\(s\).*(\w+)$/\1/p'"], shell=True))


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

	>>> afnicpu(2, 2, 2, True)
	4
	>>> afnicpu(4, 2, 2, True)
	8
	>>> afnicpu(2, 4, 2, True)
	8
	>>> afnicpu(2, 2, 3, True)
	2
	>>> afnicpu(4, 2, 3, True)
	7
	>>> afnicpu(1, 2, 1, True)
	2
	>>> afnicpu(3, 2, 1, True)
	6
	>>> afnicpu(4, 1, 3, True)
	4
	>>> afnicpu(3, 2, 2, False)
	6
	>>> afnicpu(3, 2, 3, False)
	6
	"""
	if crossnodes:
		return iafn + iafn // nodes * nodes * (corethreads - 1)
	return iafn * corethreads


class ExecPool(object):
	'''Execution Pool of workers for jobs

	A worker in the pool executes only the one job, a new worker is created for
	each subsequent job.
	'''

	def __init__(self, wksnum=cpu_count(), afnstep=None, vmlimit=0., latency=0.):
		"""Execution Pool constructor

		wksnum  - number of resident worker processes, >=1. The reasonable value is
			<= NUMA nodes * node CPUs, which is typically returned by cpu_count(),
			where node CPUs = CPU cores * HW treads per core.
			To guarantee minimal average RAM per a process, for example 2.5 Gb:
				wksnum = min(cpu_count(), max(ramfracs(2.5), 1))
		afnstep  - affinity step, integer if applied. Used to bind worker to the
			processing units to have warm cache for single thread workers.
			Typical values:
				None  - do not use affinity at all (recommended for multi-threaded workers),
				1  - maximize parallelization (the number of worker processes = CPU units),
				cpucorethreads()  - maximize the dedicated CPU cache (the number of
					worker processes = CPU cores = CPU units / hardware treads per CPU core).
			NOTE: specification of the afnstep might cause reduction of the workers number.
		vmlimit  - limit total amount of Virtual Memory (automatically reduced to
			the amount of physical RAM if the larger value is specified) in gigabytes
			that can be used by worker processes to provide in-RAM computations, >= 0.
			Dynamically reduces the number of workers to consume total virtual memory
			not more than specified. The workers are rescheduled starting from the
			most memory-heavy processes.
			NOTE:
				- applicable only if _LIMIT_WORKERS_RAM
				- 0 means unlimited (some jobs might be [partially] swapped)
				- value > 0 is automatically limited with total physical RAM to process
					jobs in RAM almost without the swapping
		latency  - approximate minimal latency of the workers monitoring in sec, float >= 0.
			0 means automatically defined value (recommended, typically 2-3 sec).
		"""
		assert wksnum >= 1 and (afnstep is None or afnstep <= cpu_count()
			) and vmlimit >= 0 and latency >= 0, 'Input parameters are invalid'

		# Verify and update wksnum and afnstep if required
		if afnstep:
			# Check whether _AFFINITYBIN exists in the system
			try:
				subprocess.call([_AFFINITYBIN, '-V'])
				if afnstep > cpu_count() / wksnum:
					print('WARNING, the number of worker processes is reduced'
						' ({wlim0} -> {wlim} to satisfy the affinity step'
						.format(wlim0=wksnum, wlim=cpu_count() // afnstep), file=sys.stderr)
					wksnum = cpu_count() // afnstep
			except OSError as err:
				afnstep = None
				print('WARNING, {afnbin} does not exists in the system to fix affinity: {err}'
					.format(afnbin=_AFFINITYBIN, err=err), file=sys.stderr)
		self._wkslim = wksnum  # Max number of resident workers
		self._workers = set()  # Scheduled and started jobs, i.e. worker processes:  {executing_job, }
		self._jobs = collections.deque()  # Scheduled jobs that have not been started yet:  deque(job)
		self._tstart = None  # Start time of the execution of the first task
		# Affinity scheduling attributes
		self._afnstep = afnstep  # Affinity step for each worker process
		self._affinity = None if not self._afnstep else [None]*self._wkslim
		assert self._wkslim * (self._afnstep if self._afnstep else 1) <= cpu_count(), (
			'_wkslim or _afnstep is too large')
		self._numanodes = cpunodes()  # Defines sequence of the CPU ids on affinity table mapping for the crossnodes enumeration
		# Virtual memory tracing attributes
		# Dedicate at least 256 Mb for OS using not more than 99% of RAM
		self._vmlimit =  0. if not _LIMIT_WORKERS_RAM else max(0, min(vmlimit, _RAM_SIZE * 0.99 - 0.25))  # in Gb
		# Execution rescheduling attributes
		self._latency = latency if latency else 1 + (not not self._vmlimit)  # Seconds of sleep on pooling
		# Predefined private attributes
		self._killCount = 3  # 3 cycles of self._latency, termination wait time
		self.__termlock = Lock()  # Lock for the __terminate() to avoid simultaneous call by the signal and normal execution flow

		if self._vmlimit != vmlimit:
			print('WARNING, total memory limit is reduced to guarantee the in-RAM'
				' computations: {} -> {} Gb'.format(vmlimit, self._vmlimit), file=sys.stderr)


	def __enter__(self):
		"""Context entrence"""
		return self


	def __exit__(self, type, value, traceback):
		"""Contex exit

		type  - exception type
		value  - exception value
		traceback  - exception traceback
		"""
		self.__terminate()


	def __clearAffinity(self, job):
		"""Clear job affinity

		job  - the job to be processed
		"""
		if self._affinity and not job._omitafn and job.proc is not None:
			try:
				self._affinity[self._affinity.index(job.proc.pid)] = None
			except ValueError:
				print('WARNING, affinity clearup is requested to the job "{}" without the activated affinity'
					.format(job.name), file=sys.stderr)
				pass  # Do nothing if the affinity was not set for this process


	def __del__(self):
		self.__terminate()


	def __finalize__(self):
		self.__del__()


	def __terminate(self):
		"""Force termination of the pool"""
		if not self.__termlock.acquire(block=False) or not (self._workers or self._jobs):
			return
		# # Note: On python3 del might already release the objects
		# if not hasattr(self, '_jobs') or not hasattr(self, '_workers') or (not self._jobs and not self._workers):
		# 	return
		print('WARNING, terminating the execution pool with {} nonstarted jobs and {} workers'
			', executed {} h {} m {:.4f} s, callstack fragment:'
			.format(len(self._jobs), len(self._workers), *secondsToHms(
			0 if self._tstart is None else time.time() - self._tstart)), file=sys.stderr)
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
		while active and i < self._killCount:
			i += 1
			active = False
			for job in self._workers:
				if job.proc.poll() is None:
					active = True
					break
			time.sleep(self._latency)
		# Kill nonterminated processes
		if active:
			for job in self._workers:
				if job.proc.poll() is None:
					print('  Killing "{}" #{} ...'.format(job.name, job.proc.pid), file=sys.stderr)
					job.proc.kill()
		# Tidy jobs
		for job in self._workers:
			self.__clearAffinity(job)
			job.complete(False)
		self._workers.clear()
		self.__termlock.release()


	def __postpone(self, job, reduced, priority=False):
		"""Schedule this job for the later execution, possibly reducing the number of workers

		Schedule this job for the later execution if it does not violates timeout
		and memory limit (if it was terminated because of the group violation made
		not by a single worker process).
		Reduce the workers if they have not been reduced yet and all nonstarted jobs
		are postponed on the same number of workers.

		job  - postponing (rescheduling) job
		reduced  - whether the worker pocesses were already reduced on current
			their revision. Only a single reduction per each revision is allowed.
		priority  - priority scheduling (to start instead of the end of the queue).
			Used only when the job should, but can't be scheduled to the workers
			because of the insufficient number of the workers not violating any constraints

		return  whether worker processes has been reduced (or were already reduced)
		"""
		# Note: postponing jobs are terminated jobs only, can be called for !_CHAINED_CONSTRAINTS
		wksnum = len(self._workers)  # The current number of worker processes
		assert (job.terminates and (not priority or self._workers)
			and (not self._vmlimit or job.vmem < self._vmlimit) and (not job.timeout
			or (job.tstop if job.tstop is not None else time.time())
			- (job.tstart if job.tstart is not None else 0) < job.timeout)
			), (  #  and _LIMIT_WORKERS_RAM and not job in self._workers and not job in self._jobs  # Note: self._jobs scanning is time-consuming
			'A terminated non-rescheduled job is expected that doest not violate constraints.'
			' "{}" terminates: {}, wkslim: {} vs {} pwl, pripority: {}, {} workers, vmem: {:.4f} / {:.4f} Gb'
			', exectime: {:.4f} / {:.4f} sec'.format(job.name, job.terminates, job.wkslim, self._wkslim
			, priority, wksnum, 0 if not not self._vmlimit else job.vmem, self._vmlimit
			, (job.tstop if job.tstop is not None else time.time())
			- (job.tstart if job.tstart is not None else 0), job.timeout))
		# Postpone only the goup-terminated jobs by memory limit, not a single worker that exceeds the (time/memory) constraints
		# Note: job wkslim should be updated before adding to the _jobs to handle correctly the case when _jobs were empty
		if priority:
			self._jobs.appendleft(job)  # Now _jobs contain at least one job
		else:
			self._jobs.append(job)  # Now _jobs contain at least one job

		if _DEBUG_TRACE == 2:
			print('  Postponing-initial non-scheduled jobs ({} workers): {}'
				.format(wksnum, ', '.join(['{} ({})'.format(pj.name, pj.wkslim) for pj in self._jobs])))
		# Update limit of the worker processes of the other larger nonstarted jobs of the same category as this job has,
		# and move jobs with the lowest wkslim to the end.
		# Note: the update should be made for all nonstarted jobs, not only for the one caused the reduction.
		if _CHAINED_CONSTRAINTS and job.category is not None:
			jobsnum = len(self._jobs)  # The number of jobs
			ij = 1 if priority else 0  # Job index
			while ij < jobsnum:
				pj = self._jobs[ij]  # Processing job
				# Update wkslim of the other larger nonstarted jobs of the same category
				if pj.category == job.category and not pj.lessVmem(job):
					# Set vmem in for the related nonstarted heavier jobs
					if not pj.vmem:
						pj.vmem = job.vmem
					if job.wkslim < pj.wkslim:
						pj.wkslim = job.wkslim
				# Move jobs with the lowest wkslim to the end
				ijc = ij
				while ij >= 1 and self._jobs[ij].wkslim > self._jobs[ij-1].wkslim:
					self._jobs[ij-1], self._jobs[ij] = self._jobs[ij], self._jobs[ij-1]
					ij -= 1
				ij = ijc + 1
		if _DEBUG_TRACE == 2:
			print('  Postponing-updated non-scheduled jobs: ', ', '.join(['{} ({})'.format(pj.name, pj.wkslim) for pj in self._jobs]))

		# Reduce workers size if the first nonstarted job was rescheduled (and current job has been rescheduled)
		if not reduced and self._jobs[0].terminates and self._jobs[0].wkslim < self._wkslim:
			print('  _wkslim is reduced to {} from {} on "{}" (vmem: {:.4f} / {:.4f} Gb) postponing'
				', total exectime: {} h {} m {:.4f} s'
				.format(self._jobs[0].wkslim, self._wkslim, job.name, job.vmem, self._vmlimit
				, *secondsToHms(time.time() - self._tstart))
				, file=sys.stderr if _DEBUG_TRACE else sys.stdout)
			self._wkslim = max(self._jobs[0].wkslim, wksnum)
			reduced = True
			assert self._wkslim >= 0, '_wkslim should be non-negative'
		return reduced


	def __start(self, job, async=True):
		"""Start the specified job by one of the worker processes

		job  - the job to be executed, instance of Job
		async  - async execution or wait intill execution completed
		return  - 0 on successful execution, proc.returncode otherwise
		"""
		assert isinstance(job, Job), 'Job type is invalid'
		assert job.tstop is None or job.terminates, 'The starting job "{}" is expected to be non-completed'.format(job.name)
		if async and len(self._workers) > self._wkslim:
			raise AssertionError('Free workers must be available ({} busy workers of {})'
				.format(len(self._workers), self._wkslim))
		#if _DEBUG_TRACE:
		print('Scheduling "{}"{}, workers: {} / {}...'.format(job.name, '' if async else ' in sync mode'
			, len(self._workers), self._wkslim), file=sys.stderr if _DEBUG_TRACE else sys.stdout)

		# Reset automatically defined values for the restarting job, which is possible only if it was terminated
		if job.terminates:
			job.terminates = 0  # Reset termination requests counter
			job.proc = None  # Reset old job process if any
			job.tstop = None  # Reset the completion / termination time
			# Note: retain previous value of vmem for better scheduling, it is the valid value for the same job
		job.tstart = time.time()
		if job.onstart:
			if _DEBUG_TRACE == 2:
				print('  Starting onstart() for job "{}"'.format(job.name), file=sys.stderr)
			try:
				job.onstart()
			except Exception as err:
				print('ERROR in onstart() callback of "{}": {}. The job has not been started: {}'
					.format(job.name, err, traceback.format_exc()), file=sys.stderr)
				return -1
		# Consider custom output channels for the job
		fstdout = None
		fstderr = None
		try:
			# Initialize fstdout, fstderr by the required output channel
			for joutp in (job.stdout, job.stderr):
				if joutp and isinstance(joutp, str):
					basedir = os.path.split(joutp)[0]
					if not os.path.exists(basedir):
						os.makedirs(basedir)
					try:
						if joutp == job.stdout:
							self._fstdout = open(joutp, 'a')
							fstdout = self._fstdout
							outcapt = 'stdout'
						elif joutp == job.stderr:
							self._fstderr = open(joutp, 'a')
							fstderr = self._fstderr
							outcapt = 'stderr'
						else:
							raise ValueError('Ivalid output stream value: ' + str(joutp))
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

			if _DEBUG_TRACE and (fstdout or fstderr):
				print('"{}" output channels:\n\tstdout: {}\n\tstderr: {}'.format(job.name
					, job.stdout, job.stderr))  # Note: write to log, not to the stderr
			if(job.args):
				# Consider CPU affinity
				iafn = -1 if not self._affinity or job._omitafn else self._affinity.index(None)  # Index in the affinity table to bind process to the CPU/core
				if iafn >= 0:
					job.args = [_AFFINITYBIN, '-c', str(afnicpu(iafn, self._afnstep, self._numanodes))] + list(job.args)
				if _DEBUG_TRACE == 2:
					print('  Opening proc for "{}" with:\n\tjob.args: {},\n\tcwd: {}'.format(job.name
						, ' '.join(job.args), job.workdir), file=sys.stderr)
				job.proc = subprocess.Popen(job.args, bufsize=-1, cwd=job.workdir, stdout=fstdout, stderr=fstderr)  # bufsize=-1 - use system default IO buffer size
				if iafn >= 0:
					self._affinity[iafn] = job.proc.pid
					print('"{jname}" #{pid}, affinity {afn} (CPU #{icpu})'
						.format(jname=job.name, pid=job.proc.pid, afn=iafn
						, icpu=afnicpu(iafn, self._afnstep, self._numanodes)))  # Note: write to log, not to the stderr
				# Wait a little bit to start the process besides it's scheduling
				if job.startdelay > 0:
					time.sleep(job.startdelay)
		except Exception as err:  # Should not occur: subprocess.CalledProcessError
			print('ERROR on "{}" execution occurred: {}, skipping the job. {}'.format(
				job.name, err, traceback.format_exc()), file=sys.stderr)
			# Note: process-associated file descriptors are closed in complete()
			if job.proc is not None:  # Note: this is an extra rare, but possible case
				self.__clearAffinity(job)  # Note: process can both exists here and does not exist, i.e. the process state is undefined
			job.complete(False)
		else:
			if async:
				self._workers.add(job)
				return 0
			else:
				job.proc.wait()
				self.__clearAffinity(job)
				job.complete()
		if job.proc.returncode:
			print('WARNING, "{}" failed to start, errcode: {}'.format(job.name, job.proc.returncode), file=sys.stderr)
		return job.proc.returncode


	def __reviseWorkers(self):
		"""Rewise the workers

		Check for the comleted jobs and their timeouts, update corresponding
		workers and start the nonstarted jobs if possible.
		Apply chained termination and rescheduling on tiomeout and memory
		constraints violation if _CHAINED_CONSTRAINTS.
		"""
		# Process completed jobs, check timeouts and memory constraints matching
		completed = set()  # Completed workers:  {proc,}
		vmtotal = 0.  # Consuming virtual memory by workers
		jtorigs = {}  # Timeout caused terminating origins (jobs) for the chained termination, {category: lightweightest_job}
		jmorigs = {}  # Memory limit caused terminating origins (jobs) for the chained termination, {category: smallest_job}
		for job in self._workers:  # .items()  Note: the number of _workers is small
			if job.proc.poll() is not None:  # Not None means the process has been terminated / completed
				completed.add(job)
				continue

			exectime = time.time() - job.tstart
			# Update memory statistics (if required) an skip jobs that do not exceed the specified time/memory constraints
			if not job.terminates and (not job.timeout or exectime < job.timeout
			) and (not self._vmlimit or job.vmem < self._vmlimit):
				# Update memory consumption statistics if applicable
				if self._vmlimit:
					# NOTE: Evaluate memory consuption for the heaviest process in the process tree
					# of the origin job process to allow additional intermediate apps for the evaluations like:
					# ./exectime ./clsalg ca_prm1 ca_prm2
					job._updateVmem(True, True)  # Consider vm consumption of past runs if any
					if job.vmem < self._vmlimit:
						vmtotal += job.vmem  # Consider vm consumption of past runs if any
						#if _DEBUG_TRACE == 3:
						#	print('  "{}" consumes {:.4f} Gb, vmtotal: {:.4f} Gb'.format(job.name, job.vmem, vmtotal), file=sys.stderr)
						continue
					# The memory limits violating worker will be terminated
				else:
					continue

			# Terminate the worker because of the timeout/memory constraints violation
			job.terminates += 1
			# Save the most lighgtweight terminating chain origins for timeouts and memory overusage by the single process
			if _CHAINED_CONSTRAINTS and job.category is not None and job.size:
				if job.timeout and exectime >= job.timeout:
					# Timeout constraints
					jorg = jtorigs.get(job.category, None)
					if jorg is None or job.size * job.slowdown < jorg.size * jorg.slowdown:
						jtorigs[job.category] = job
				elif self._vmlimit and job.vmem >= self._vmlimit:
					# Memory limit constraints
					jorg = jmorigs.get(job.category, None)
					if jorg is None or job.lessVmem(jorg):
						jmorigs[job.category] = job
				# Otherwise this job is terminated because of multiple processes together overused memory,
				# it should be reschedulted, but not removed completely

			# Force killing when the termination does not work
			if job.terminates >= self._killCount:
				job.proc.kill()
				completed.add(job)
				if _DEBUG_TRACE:  # Note: anyway completing terminated jobs are traced
					exectime = time.time() - job.tstart
					print('WARNING, "{}" #{} is killed because of the {} violation'
						' consuming {:.4f} Gb with timeout of {:.4f} sec, executed: {:.4f} sec ({} h {} m {:.4f} s)'
						.format(job.name, job.proc.pid
						, 'timeout' if job.timeout and exectime >= job.timeout else (
							('' if job.vmem >= self._vmlimit else 'group ') + 'memory limit')
						, 0 if not self._vmlimit else job.vmem
						, job.timeout, exectime, *secondsToHms(exectime)), file=sys.stderr)
			else:
				job.proc.terminate()  # Schedule the worker completion to the next revise
		#self.vmtotal = vmtotal
		# Remove completed jobs from worker processes
		for job in completed:
			self._workers.remove(job)

		# Terminate chained related workers and jobs of the single jobs that violate timeout/memory constraints
		if _CHAINED_CONSTRAINTS and (jtorigs or jmorigs):
			# Traverse over the workers with defined job category and size
			for job in self._workers:
				if not job.terminates and job.category is not None and job.size:
					# Travers over the chain origins and check matches skipping the origins themselves
					# Timeout chains
					for jorg in viewvalues(jtorigs):
						# Note: job !== jorg, because jorg terminates and job does not
						if (job.category == jorg.category  # Skip already terminating items
						and job.size * job.slowdown >= jorg.size * jorg.slowdown):
							# Terminate the worker
							job.terminates += 1
							job.proc.terminate()  # Schedule the worker completion to the next revise
							vmtotal -= job.vmem  # Reduce total memory consumed by the active workers
							break  # Switch to the following job
					else:
						# Memory limit chains
						for jorg in viewvalues(jmorigs):
							# Note: job !== jorg, because jorg terminates and job does not
							if (job.category == jorg.category  # Skip already terminating items
							and not job.lessVmem(jorg)):
								# Terminate the worker
								job.terminates += 1
								job.proc.terminate()  # Schedule the worker completion to the next revise
								vmtotal -= job.vmem  # Reduce total memory consumed by the active workers
								break  # Switch to the following job
			# Traverse over the nonstarted jobs with defined job category and size
			if _DEBUG_TRACE == 2:
				print('  Updating chained constraints in non-scheduled jobs: ', ', '.join([job.name for job in self._jobs]))
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
							and not job.lessVmem(jorg)):
								# Remove the item
								self._jobs.rotate(-ij)
								jrot += ij
								self._jobs.popleft()  # == job
								ij = -1  # Later +1 is added, so the index will be 0
								print('WARNING, nonstarted "{}" with size {} is cancelled by memory limit chain from "{}" with size {}'
									' and vmem {:.4f}'.format(job.name, job.size, jorg.name, jorg.size, jorg.vmem), file=sys.stderr)
								break
				ij += 1
			# Recover initial order of the jobs
			self._jobs.rotate(jrot)

		# Check memory limitation fulfilling for all processes
		if self._vmlimit and vmtotal >= self._vmlimit:  # Jobs should use less memory than the limit
			# Terminate the largest workers and reschedule jobs or reduce the workers number
			memov = vmtotal - self._vmlimit  # Overused memory to be released by worker(s) termination
			pjobs = set()  # The heaviest jobs to be postponed to satisfy the memory limit constraint
			# Remove the heaviest workers until the memory limit constraints are sutisfied
			hws = []  # Heavy workers
			while memov >= 0:  # Overuse should negative, i.e. underuse to starr any another job, 0 in practice is insufficient of the subsequent execution
				# Reinitialize the heaviest remained jobs and continue
				for job in self._workers:
					if not job.terminates and job not in pjobs:
						hws.append(job)
						break
				assert self._workers and hws, 'Non-terminated worker processes must exist here'
				for job in self._workers:
					# Note: use some treshold for vmem evaluation and consider starting time on scheduling
					# to terminate first the least worked processes (for approximately the same memory consumption)
					dr = 0.1  # Threshold parameter ratio, recommended value: 0.05 - 0.15; 0.1 means delta of 10%
					if not job.terminates and (job.vmem * (1 - dr) >= hws[-1].vmem or
					(job.vmem * (1 + dr/2) >= hws[-1].vmem and job.tstart > hws[-1].tstart)) and job not in pjobs:
						hws.append(job)
				# Move the largest jobs to postponed until memov is negative
				while memov >= 0 and hws:
					job = hws.pop()
					pjobs.add(job)
					memov -= job.vmem
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
			# Update amount of estimated vmtotal
			vmtotal = self._vmlimit + memov  # Note: memov is negative here

		# Process completed (and terminated) jobs: execute callbacks and remove the workers
		wsreduced = False  # Workers limit is reduced on this revise: only a single reduction per each revision is allowed
		for job in completed:
			self.__clearAffinity(job)  # Note: the affinity must be updated before the job restart or on completion
			job.complete(not job.terminates and not job.proc.returncode)  # The completion is graceful only if the termination requests were not received
			exectime = job.tstop - job.tstart
			# Restart the job if it was terminated and should be restarted
			if not job.terminates:
				continue
			print('WARNING, "{}" #{} is terminated because of the {} violation'
				' consuming {:.4f} / {:.4f} Gb with timeout of {:.4f} sec, executed: {:.4f} sec ({} h {} m {:.4f} s)'
				.format(job.name, job.proc.pid
				, 'timeout' if job.timeout and exectime >= job.timeout else (
					('' if job.vmem >= self._vmlimit else 'group ') + 'memory limit')
				, 0 if not self._vmlimit else job.vmem, self._vmlimit
				, job.timeout, exectime, *secondsToHms(exectime)), file=sys.stderr)
			# Skip memory limit and timeout violating jobs that do not require autorestart (applicable only for the timeout)
			if (self._vmlimit and job.vmem >= self._vmlimit) or (exectime >= job.timeout and not job.ontimeout):
				continue
			# Reschedule job having the group violation of the memory limit
			# if timeout is not violated or restart on timeout is requested
			if self._vmlimit and vmtotal + job.vmem >= self._vmlimit and (
			not job.timeout or exectime < job.timeout or job.ontimeout):
				wsreduced = self.__postpone(job, wsreduced)
			# Restart the job on timeout if requested
			elif exectime >= job.timeout and job.ontimeout:  # ATTENTION: restart on timeout only and if required
				# Note: if the job was terminated by timeout then memory limit was not met
				# Note: earlier executed job might not fit into the RAM now because of the inreasing vmem consumption by the workers
				#if _DEBUG_TRACE == 3:
				#	print('  "{}" is being rescheduled, workers: {} / {}, estimated vmem: {:.4f} / {:.4f} Gb'
				#		.format(job.name, len(self._workers), self._wkslim, vmtotal + job.vmem, self._vmlimit)
				#		, file=sys.stderr)
				assert len(self._workers) < self._wkslim, 'Completed job formed from the reduced workers'
				assert not self._vmlimit or vmtotal + job.vmem < self._vmlimit, (
					'Group exceeding of the memory limit should be already processed')
				if not self.__start(job) and self._vmlimit:
					vmtotal += job.vmem  # Reuse .vmem from the previous run if exists
				# Note: do not call complete() on failed restart
			else:
				assert exectime < job.timeout, 'Timeout violating jobs should be already skipped'
				# The job was terminated by group violation of memory limit,
				# but now can be started successfully
				wsreduced = self.__postpone(job, wsreduced, True)
		# Note: actually the number of workers is not reduced to less than 1, however this case still can be handled
		#if not self._wkslim:
		#	assert wsreduced and _LIMIT_WORKERS_RAM and _CHAINED_CONSTRAINTS, '_wkslim can become zero only afer the reduction'
		#	print('WARNING, the number of workers is reduced to zero, the whole execution pool should be terminated', file=sys.stderr)
		#	return False

		# Start subsequent job if it is required
		if _DEBUG_TRACE == 2:
			print('  Non-scheduled jobs: ', ', '.join(['{} ({})'.format(job.name, job.wkslim) for pj in self._jobs]))
		while self._jobs and len(self._workers) < self._wkslim:
			#if _DEBUG_TRACE == 3:
			#	print('  "{}" (expected totvmem: {:.4f} / {:.4f} Gb) is being resheduled, {} nonstarted jobs: {}'
			#		.format(self._jobs[0].name, 0 if not self._vmlimit else vmtotal + job.vmem, self._vmlimit
			#		, len(self._jobs), ', '.join([j.name for j in self._jobs])), file=sys.stderr)
			job = self._jobs.popleft()
			# Jobs should use less memory than the limit, a worker process violating (time/memory) constaints are already filtered out
			if self._vmlimit and vmtotal + job.vmem >= self._vmlimit:
				# Note: only restarted jobs have defined vmem
				# Postpone the job updating it's workers limit
				assert job.vmem < self._vmlimit, 'The workers exceeding memory constraints were already filtered out'
				wsreduced = self.__postpone(job, wsreduced)
				break
			elif not self.__start(job) and self._vmlimit:
				vmtotal += job.vmem  # Reuse .vmem from the previous run if exists
		assert (self._workers or not self._jobs) and self._wkslim, 'Worker processes should always exist if nonstarted jobs are remained'


	def execute(self, job, async=True):
		"""Schecule the job for the execution

		job  - the job to be executed, instance of Job
		async  - async execution or wait until execution completed
		  NOTE: sync tasks are started at once
		return  - 0 on successful execution, process return code otherwise
		"""
		assert isinstance(job, Job), 'job type is invalid'
		assert len(self._workers) <= self._wkslim, 'Number of workers exceeds the limit'
		assert job.name, 'Job parameters must be defined'  #  and job.workdir and job.args

		if _DEBUG_TRACE:
			print('Scheduling the job "{}" with timeout {}'.format(job.name, job.timeout))
		# Consider the case when the number of workers processes is reduced to 0 and then new jobs are added
		if not self._wkslim:
			assert _LIMIT_WORKERS_RAM, 'The number of workers could be reduced only on _LIMIT_WORKERS_RAM'
			# Note: it is not possible to estimate whether the new job can be executed because all former jobs with
			# their categories and sizes are removed
			print('WARNING, "{}" (vmem: {:.4f} / {:.4f} Gb, size: {}, timeout: {}) is cancelled because the executors do not exist'
				.format(job.name, job.vmem, self._vmlimit, job.size, job.timeout), file=sys.stderr)
			return 1
		errcode = 0
		# Initialize the [latest] value of job workers limit
		if _LIMIT_WORKERS_RAM:
			job.wkslim = self._wkslim
		if async:
			# Start the execution timer
			if self._tstart is None:
				self._tstart = time.time()
			# Schedule the job, postpone it if already nonstarted jobs exist or there are no any free workers
			if self._jobs or len(self._workers) >= self._wkslim:
				self._jobs.append(job)
				#self.__reviseWorkers()  # Anyway the workers are revised if exist in the working cycle
			else:
				errcode = self.__start(job)
		else:
			errcode = self.__start(job, False)
			# Note: sync job is completed automatically on fails
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
		while self._jobs or self._workers:
			if timeout and time.time() - self._tstart > timeout:
				print('WARNING, the execution pool is terminated on timeout', file=sys.stderr)
				self.__terminate()
				return False
			time.sleep(self._latency)
			self.__reviseWorkers()
		self._tstart = None  # Be ready for the following execution

		assert not self._jobs and not self._workers, 'All jobs should be finalized'
		return True


# Unit Tests -------------------------------------------------------------------
import unittest
from sys import executable as PYEXEC  # Full path to the current Python interpreter
try:
	from unittest import mock
except ImportError:
	import mock


# Accessory Funcitons
def allocDelayProg(size, duration):
	"""Python program as str that allocates object of the specified size
	in Gigabytes and then waits for the specified time

	size  - allocation size, bytes; None to skip allocation
	duration  - execution duration, sec; None to skip the sleep

	return  - Python program as str
	"""
	return """from __future__ import print_function, division  # Required for stderr output, must be the first import
import sys
import time
import array

if {size} is not None:
	a = array.array('b', [0])
	asize = sys.getsizeof(a)
	# Note: allocate at least empty size, i.e. empty list
	buffer = a * int(max({size} - asize + 1, 0))
	#if _DEBUG_TRACE:
	# print(''.join(('  allocDelayProg(), allocated ', str(sys.getsizeof(buffer))
	# 	, ' bytes for ', str({duration}),' sec')), file=sys.stderr)
if {duration} is not None:
	time.sleep({duration})
""".format(size=size, duration=duration)


class TestExecPool(unittest.TestCase):
	#global _DEBUG_TRACE
	#_DEBUG_TRACE = True

	_WPROCSMAX = max(cpu_count() - 1, 1)  # Maximal number of the worker processes, should be >= 1
	_AFNSTEP = cpucorethreads()  # Affinity
	# Note: 0.1 is specified just for the faster tests execution on the non-first run,
	# generally at least 0.2 should be used
	_latency = 0.1  # Approximate minimal latency of ExecPool in seconds
	#_execpool = None


	@classmethod
	def terminationHandler(cls, signal=None, frame=None, terminate=True):
		"""Signal termination handler

		signal  - raised signal
		frame  - origin stack frame
		terminate  - whether to terminate the application
		"""
		#if signal == signal.SIGABRT:
		#	os.killpg(os.getpgrp(), signal)
		#	os.kill(os.getpid(), signal)

		if cls._execpool:
			del cls._execpool  # Destructors are caled later
			# Define _execpool to avoid unnessary trash in the error log, which might
			# be caused by the attempt of subsequent deletion on destruction
			cls._execpool = None  # Note: otherwise _execpool becomes undefined
		if terminate:
			sys.exit()  # exit(0), 0 is the default exit code.


	@classmethod
	def setUpClass(cls):
		cls._execpool = ExecPool(TestExecPool._WPROCSMAX, latency=TestExecPool._latency)  # , _AFNSTEP, vmlimit


	@classmethod
	def tearDownClass(cls):
		if cls._execpool:
			del cls._execpool  # Destructors are caled later
			# Define _execpool to avoid unnessary trash in the error log, which might
			# be caused by the attempt of subsequent deletion on destruction
			cls._execpool = None  # Note: otherwise _execpool becomes undefined


	def setUp(self):
		assert not self._execpool._workers, 'Worker processes should be empty for each new test case'


	def tearDown(self):
		assert not self._execpool._workers and not self._execpool._jobs, (
			'All jobs should be completed in the end of each testcase (workers: "{}"; jobs: "{}")'
			.format(', '.join([job.name for job in self._execpool._workers])
			, ', '.join([job.name for job in self._execpool._jobs])))


	def test_jobTimeoutSimple(self):
		"""Verify termination of a single job by timeout and completion of independent another job"""
		timeout = TestExecPool._latency * 4  # Note: should be larger than 3*_latency
		worktime = max(1, TestExecPool._latency) + (timeout * 2) // 1  # Job work time
		assert TestExecPool._latency * 3 < timeout < worktime, 'Testcase parameters validation failed'

		tstart = time.time()
		jterm = Job('j_timeout', args=('sleep', str(worktime)), timeout=timeout)
		self._execpool.execute(jterm)
		jcompl = Job('j_complete', args=('sleep', '0'), timeout=timeout)
		self._execpool.execute(jcompl)
		# Verify successful completion of the execution pool
		self.assertTrue(self._execpool.join())
		etime = time.time() - tstart  # Execution time
		self.assertFalse(self._execpool._workers)  # Workers shuold be empty
		# Verify termination time
		self.assertLess(etime, worktime)
		self.assertGreaterEqual(etime, timeout)
		# Verify jobs timings
		self.assertTrue(tstart < jterm.tstart <= jcompl.tstart <= jcompl.tstop < jterm.tstop)
		self.assertLessEqual(jterm.tstop - jterm.tstart, etime)
		self.assertGreaterEqual(jterm.tstop - jterm.tstart, timeout)


	def test_epoolTimeoutSimple(self):
		"""Validate:
			1. Termination of a single job by timeout of the execution pool
			2. Restart of the job by request on timeout and execution of the onstart()
		"""
		# Execution pool timeout
		etimeout = TestExecPool._latency * 4  # Note: should be larger than 3*_latency
		timeout = etimeout * 2  # Job timeout
		worktime = max(1, TestExecPool._latency) + (timeout * 2) // 1  # Job work time
		assert TestExecPool._latency * 3 < timeout < worktime, 'Testcase parameters validation failed'

		tstart = time.time()
		self._execpool.execute(Job('ep_timeout', args=('sleep', str(worktime)), timeout=timeout))

		runsCount={'count': 0}
		def updateruns(job):
			job.params['count'] += 1

		jrx = Job('ep_timeout_jrx', args=('sleep', str(worktime)), timeout=etimeout / 2, ontimeout=True
			, params=runsCount, onstart=updateruns)  # Reexecuting job
		self._execpool.execute(jrx)
		# Verify termination of the execution pool
		self.assertFalse(self._execpool.join(etimeout))
		etime = time.time() - tstart  # Execution time
		self.assertFalse(self._execpool._workers)  # Workers should be empty
		# Verify termination time
		self.assertLess(etime, worktime)
		self.assertLess(etime, timeout)
		self.assertGreaterEqual(etime, etimeout)
		# Verify jrx runsCount and onstart execution
		self.assertGreaterEqual(runsCount['count'], 2)


	@unittest.skipUnless(_CHAINED_CONSTRAINTS, 'Requires _CHAINED_CONSTRAINTS')
	def test_jobTimeoutChained(self):
		"""Verify chained termination by timeout:
			1. Termination of the related non-smaller job on termination of the main job
			2. Not affecting smaller related jobs on termination of the main job
			3. Not affecting non-related jobs (with another category)
			4. Execution of the ondone() only on the graceful termination
		"""
		timeout = TestExecPool._latency * 5  # Note: should be larger than 3*_latency
		worktime = max(1, TestExecPool._latency) + (timeout * 2) // 1  # Job work time
		assert TestExecPool._latency * 3 < timeout < worktime, 'Testcase parameters validation failed'

		tstart = time.time()
		jm = Job('jmaster_timeout', args=('sleep', str(worktime))
			, category='cat1', size=2, timeout=timeout)
		self._execpool.execute(jm)
		jss = Job('jslave_smaller', args=('sleep', str(worktime))
			, category='cat1', size=1, ondone=mock.MagicMock())  # ondone() should be called for the completed job
		self._execpool.execute(jss)
		jsl = Job('jslave_larger', args=('sleep', str(worktime))
			, category='cat1', size=3, ondone=mock.MagicMock())  # ondone() should be skipped for the terminated job
		self._execpool.execute(jsl)
		jso = Job('job_other', args=('sleep', str(worktime)), category='cat_other'
			, ondone=mock.MagicMock())  # ondone() should be called for the completed job
		self._execpool.execute(jso)
		jsf = Job('job_failed', args=('sleep'), category='cat_f'
			, ondone=mock.MagicMock())  # ondone() should be called for the completed job
		self._execpool.execute(jsf)

		# Execution pool timeout
		etimeout = max(1, TestExecPool._latency) + worktime * 3 * (1 +
			(len(self._execpool._workers) + len(self._execpool._jobs)) // self._execpool._wkslim)
		assert etimeout > worktime, 'Additional testcase parameters are invalid'
		print('jobTimeoutChained() started wth worktime: {}, etimeout: {}'.format(worktime, etimeout))

		# Verify exec pool completion
		self.assertTrue(self._execpool.join(etimeout))
		etime = time.time() - tstart  # Execution time
		# Verify timings
		self.assertTrue(worktime <= etime < etimeout)
		self.assertLess(jm.tstop - jm.tstart, worktime)
		self.assertLess(jsl.tstop - jsl.tstart, worktime)
		self.assertGreaterEqual(jsl.tstop - jm.tstart, timeout)  # Note: measure time from the master start
		self.assertGreaterEqual(jss.tstop - jss.tstart, worktime)
		self.assertLess(jsl.tstop - jsl.tstart, worktime)
		self.assertGreaterEqual(jso.tstop - jso.tstart, worktime)
		self.assertLess(jsf.tstop - jsl.tstart, worktime)
		# Verify ondone() calls
		jss.ondone.assert_called_once_with(jss)
		jsl.ondone.assert_not_called()
		jso.ondone.assert_called_once_with(jso)
		jsf.ondone.assert_not_called()


	@unittest.skipUnless(_LIMIT_WORKERS_RAM, 'Requires _LIMIT_WORKERS_RAM')
	def test_jobMemlimSimple(self):
		"""Verify memory violations caused by the single worker:
		1. Absence of side effects on the remained jobs after bad_alloc
			(exception of the external app) caused termination of the worker process
		2. Termination of the worker process that exceeds limit of the dedicated virtual memory
	 	3. Termination of the worker process that exceeds limit of the dedicated virtual memory
	 		or had bad_alloc and termination of all related non-smaller jobs
		"""
		worktime = TestExecPool._latency * 5  # Note: should be larger than 3*_latency; 400 ms can be insufficient for the Python 3
		timeout = worktime * 2  # Note: should be larger than 3*_latency
		#etimeout = max(1, TestExecPool._latency) + (worktime * 2) // 1  # Job work time
		etimeout = (max(1, TestExecPool._latency) + timeout) * 3  # Execution pool timeout; Note: *3 because nonstarted jobs exist here
		assert TestExecPool._latency * 3 < worktime < timeout and timeout < etimeout, 'Testcase parameters validation failed'

		# Note: we need another execution pool to set vmlimit (10 Mb) there
		epoolVmem = 0.2  # Execution pool vmem limit, Gb
		msmall = 256  # Small amount of memory for a job, bytes
		# Start not more than 3 simultaneous workers
		with ExecPool(max(TestExecPool._WPROCSMAX, 3), latency=TestExecPool._latency, vmlimit=epoolVmem) as xpool:  # , _AFNSTEP, vmlimit
			tstart = time.time()

			jmsDb = Job('jmem_small_ba', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, category='cat1', size=9, timeout=timeout)
			jmb = Job('jmem_badalloc', args=(PYEXEC, '-c', allocDelayProg(inBytes(_RAM_SIZE * 2), worktime))
				, category='cat1', size=9, timeout=timeout)

			jmvsize = 5  # Size of the task violating memory contraints
			jmv = Job('jmem_violate', args=(PYEXEC, '-c', allocDelayProg(inBytes(epoolVmem * 2), worktime))
				, category='cat2', size=jmvsize, timeout=timeout)
			jmsDvs = Job('jmem_small_v1', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, category='cat2', size=jmvsize-1, timeout=timeout)
			jms1 = Job('jmem_small_1', args=(PYEXEC, '-c', allocDelayProg(None, worktime))
				, category='cat3', size=7, timeout=timeout)
			jmsDvl1 = Job('jmem_large_v', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, category='cat2', size=jmvsize, timeout=timeout)
			jms2 = Job('jmem_small_2', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, size=7, timeout=timeout)
			jmsDvl2 = Job('jmem_small_v1', args=(PYEXEC, '-c', allocDelayProg(None, worktime))
				, category='cat2', size=jmvsize*2, timeout=timeout)

			xpool.execute(jmsDb)
			xpool.execute(jmb)

			xpool.execute(jmv)
			xpool.execute(jmsDvs)
			xpool.execute(jms1)
			xpool.execute(jmsDvl1)
			xpool.execute(jms2)
			xpool.execute(jmsDvl2)

			# Verify exec pool completion before the timeout
			self.assertTrue(xpool.join(etimeout))
			etime = time.time() - tstart  # Execution time

			# Verify timings
			self.assertLess(etime, etimeout)
			self.assertGreaterEqual(jmsDb.tstop - jmsDb.tstart, worktime)  # Note: internal errors in the external processes should not effect related jobs
			self.assertTrue(jmb.proc.returncode)  # bad_alloc causes non zero termintion code
			self.assertLess(jmb.tstop - jmb.tstart, worktime)  # Early termination cased by the bad_alloc (internal error in the external process)

			self.assertLess(jmv.tstop - jmv.tstart, worktime)  # Early termination by the memory constraints violation
			self.assertGreaterEqual(jmsDvs.tstop - jmsDvs.tstart, worktime)  # Smaller size of the ralted chained job to the vioated origin should not cause termination
			self.assertGreaterEqual(jms1.tstop - jms1.tstart, worktime)  # Independent job should have graceful completion
			self.assertFalse(jms1.proc.returncode)  # Errcode code is 0 on the gracefull completion
			if _CHAINED_CONSTRAINTS:
				self.assertIsNone(jmsDvl1.tstart)  # Postponed job should be terminated before being started by the chained relation on the memory-violating origin
				self.assertIsNone(jmsDvl2.tstart)  # Postponed job should be terminated before being started by the chained relation on the memory-violating origin
			#self.assertLess(jmsDvl1.tstop - jmsDvl1.tstart, worktime)  # Early termination by the chained retalion to the mem violated origin
			self.assertGreaterEqual(jms2.tstop - jms2.tstart, worktime)  # Independent job should have graceful completion


	@unittest.skipUnless(_LIMIT_WORKERS_RAM, 'Requires _LIMIT_WORKERS_RAM')
	def test_jobMemlimGroupSimple(self):
		"""Verify memory violations caused by group of workers but without chained jobs

		Reduction of the number of worker processes when their total memory consumption
		exceeds the dedicated limit and there are
			1) either no any nonstarted jobs
			2) or the nonstarted jobs were already rescheduled by the related worker (absence of chained constraints)
		"""
		worktime = TestExecPool._latency * 6  # Note: should be larger than 3*_latency
		timeout = worktime * 2  # Note: should be larger than 3*_latency
		#etimeout = max(1, TestExecPool._latency) + (worktime * 2) // 1  # Job work time
		etimeout = (max(1, TestExecPool._latency) + timeout) * 3  # Execution pool timeout; Note: *3 because nonstarted jobs exist here nad postponed twice
		assert TestExecPool._latency * 3 < worktime < timeout and timeout < etimeout, 'Testcase parameters validation failed'

		# Note: we need another execution pool to set vmlimit (10 Mb) there
		epoolVmem = 0.15  # Execution pool vmem limit, Gb
		msmall = inBytes(0.025)  # Small amount of memory for a job; Note: actual Python app consumes ~51 Mb for the allocated ~25 Mb
		# Start not more than 3 simultaneous workers
		with ExecPool(max(TestExecPool._WPROCSMAX, 3), latency=TestExecPool._latency, vmlimit=epoolVmem) as xpool:  # , _AFNSTEP, vmlimit
			tstart = time.time()
			jgms1 = Job('jgroup_mem_small_1', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, size=9, timeout=timeout, onstart=mock.MagicMock())
			jgms2 = Job('jgroup_mem_small_2', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, size=9, timeout=timeout)
			jgms3 = Job('jgroup_mem_small_3', args=(PYEXEC, '-c', allocDelayProg(msmall*1.25, worktime))
				, size=5, timeout=timeout, onstart=mock.MagicMock(), ondone=mock.MagicMock())
			jgmsp1 = Job('jgroup_mem_small_postponed_1', args=(PYEXEC, '-c', allocDelayProg(msmall*0.85, worktime))
				, size=4, timeout=timeout, onstart=mock.MagicMock())
			jgmsp2 = Job('jgroup_mem_small_postponed_2_to', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, timeout=worktime/2, ondone=mock.MagicMock())

			xpool.execute(jgms1)
			xpool.execute(jgms2)
			xpool.execute(jgms3)
			xpool.execute(jgmsp1)
			xpool.execute(jgmsp2)

			# Verify exec pool completion before the timeout
			self.assertTrue(xpool.join(etimeout))
			# All jobs should be completed
			etime = time.time() - tstart  # Execution time

			# Verify timings, gracefull copletion of all jobs except the last one
			self.assertLess(etime, etimeout)
			self.assertGreaterEqual(jgms1.tstop - jgms1.tstart, worktime)
			self.assertFalse(jgms1.proc.returncode)
			self.assertGreaterEqual(jgms2.tstop - jgms2.tstart, worktime)
			self.assertFalse(jgms2.proc.returncode)
			self.assertGreaterEqual(jgms3.tstop - jgms3.tstart, worktime)
			self.assertFalse(jgms3.proc.returncode)
			self.assertGreaterEqual(jgmsp1.tstop - jgmsp1.tstart, worktime)
			self.assertFalse(jgmsp1.proc.returncode)
			self.assertLess(jgmsp2.tstop - jgmsp2.tstart, worktime)
			self.assertTrue(jgmsp2.proc.returncode)
			# Check the last comleted job
			self.assertTrue(jgms3.tstop <= tstart + etime)

			# Verify handlers calls
			jgms1.onstart.assert_called_once_with(jgms1)
			jgms3.onstart.assert_called_with(jgms3)
			self.assertTrue(2 <= jgms3.onstart.call_count <= 3)
			jgms3.ondone.assert_called_once_with(jgms3)
			jgmsp1.onstart.assert_called_with(jgmsp1)
			self.assertTrue(1 <= jgmsp1.onstart.call_count <= 2)
			jgmsp2.ondone.assert_not_called()


	@unittest.skipUnless(_LIMIT_WORKERS_RAM and _CHAINED_CONSTRAINTS, 'Requires _LIMIT_WORKERS_RAM, _CHAINED_CONSTRAINTS')
	def test_jobMemlimGroupChained(self):
		"""Verify memory violations caused by group of workers having chained jobs
		Rescheduling of the worker processes when their total memory consumption
		exceeds the dedicated limit and there are some nonstarted jobs of smaller
		size and the same category that
			1) were not rescheduled by the non-heavier worker.
			2) were rescheduled by the non-heavier worker.
		"""
		# Note: for one of the tests timeout=worktime/2 is used
		worktime = TestExecPool._latency * 7  # Note: should be larger than 3*_latency
		timeout = worktime * 2  # Note: should be larger than 3*_latency
		#etimeout = max(1, TestExecPool._latency) + (worktime * 2) // 1  # Job work time
		etimeout = (max(1, TestExecPool._latency) + timeout) * 4  # Execution pool timeout; Note: *3 because nonstarted jobs exist here nad postponed twice
		assert TestExecPool._latency * 3 < worktime/2 and worktime < timeout and timeout < etimeout, 'Testcase parameters validation failed'

		# Note: we need another execution pool to set vmlimit (10 Mb) there
		epoolVmem = 0.15  # Execution pool vmem limit, Gb
		msmall = inBytes(0.025)  # Small amount of memory for a job; Note: actual Python app consumes ~51 Mb for the allocated ~25 Mb
		# Start not more than 3 simultaneous workers
		with ExecPool(max(TestExecPool._WPROCSMAX, 4), latency=TestExecPool._latency, vmlimit=epoolVmem) as xpool:  # , _AFNSTEP, vmlimit
			tstart = time.time()

			jgms1 = Job('jgroup_mem_small_1', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, size=5, timeout=timeout)
			jgms2 = Job('jgroup_mem_small_2s', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime/2))
				, size=5, timeout=timeout, onstart=mock.MagicMock())
			jgms3 = Job('jgroup_mem_small_3g', args=(PYEXEC, '-c', allocDelayProg(msmall*1.5, worktime))
				, category="cat_sa", size=5, timeout=timeout, onstart=mock.MagicMock(), ondone=mock.MagicMock())
			jgmsp1 = Job('jgroup_mem_small_postponed_1m', args=(PYEXEC, '-c', allocDelayProg(msmall*1.2, worktime*1.2))
				, category="cat_toch", size=6, timeout=timeout, onstart=mock.MagicMock())
			jgmsp2 = Job('jgroup_mem_small_postponed_2_to', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, category="cat_toch", size=4, timeout=worktime/2, ondone=mock.MagicMock())
			jgmsp3 = Job('jgroup_mem_small_postponed_3', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, size=9, timeout=worktime, onstart=mock.MagicMock())

			xpool.execute(jgms1)
			xpool.execute(jgms2)
			xpool.execute(jgms3)
			xpool.execute(jgmsp1)
			xpool.execute(jgmsp2)
			xpool.execute(jgmsp3)

			# Verify exec pool completion before the timeout
			self.assertTrue(xpool.join(etimeout))
			etime = time.time() - tstart  # Execution time

			# Verify timings, gracefull copletion of all jobs except the last one
			self.assertLess(etime, etimeout)
			self.assertGreaterEqual(jgms1.tstop - jgms1.tstart, worktime)
			self.assertFalse(jgms1.proc.returncode)
			self.assertGreaterEqual(jgms2.tstop - jgms2.tstart, worktime/2)
			self.assertFalse(jgms2.proc.returncode)
			self.assertGreaterEqual(jgms3.tstop - jgms3.tstart, worktime)
			self.assertFalse(jgms3.proc.returncode)
			self.assertLess(jgmsp1.tstop - jgmsp1.tstart, worktime*1.2)  # Canceled by chained timeout
			self.assertTrue(jgmsp1.proc.returncode)
			self.assertLessEqual(jgmsp2.tstop - jgmsp2.tstart, worktime)
			self.assertTrue(jgmsp2.proc.returncode)
			self.assertGreaterEqual(jgmsp3.tstop - jgmsp3.tstart, worktime)  # Execution time a bit exceeds te timeout
			# Note: jgmsp3 may complete gracefully or may be terminated by timeout depending on the wrkers revision time.
			# Most likely the completion is graceful
			## Check the last comleted job
			#self.assertTrue(jgms3.tstop < jgmsp1.tstop < tstart + etime)  # Note: heavier job is rescheduled after the more lightweight one

			# Verify handlers calls
			jgms2.onstart.assert_called_once_with(jgms2)
			jgms3.onstart.assert_called_with(jgms3)
			self.assertEqual(jgms3.onstart.call_count, 2)
			jgms3.ondone.assert_called_once_with(jgms3)
			jgmsp1.onstart.assert_called_once_with(jgmsp1)
			jgmsp2.ondone.assert_not_called()
			jgmsp3.onstart.assert_called_with(jgmsp3)
			self.assertTrue(1 <= jgmsp3.onstart.call_count <= 2)


class TestProcMemTree(unittest.TestCase):
	"""Process tree memory evaluation tests"""

	@staticmethod
	def allocAndSpawnProg(mprog, cprog):
		"""Python program as str that allocates object of the specified size
		in Gigabytes and then waits for the specified time

		mprog  - Python program text (action) for the main process
		cprog  - Python program text for the child process

		return  - Python program as str
		"""
		return """{mprog}
import subprocess
import os
from sys import executable as PYEXEC

fnull = open(os.devnull, 'w')
subprocess.call(args=('time', PYEXEC, '-c', '''{cprog}'''), stderr=fnull)
""".format(mprog=mprog, cprog=cprog)
#subprocess.call(args=('./exectime', PYEXEC, '-c', '''{cprog}'''))


	@unittest.skip('Used to understand what is included into the reported process memory by "psutil"')
	def test_psutilPTMem(self):
		"""Test psutil process tree memory consumpotion"""
		amem = 0.02  # Direct allocating memory in the process
		camem = 0.07  # Allocatinf memory in the child process
		duration = 0.2  # Duration in sec
		proc = subprocess.Popen(args=(PYEXEC, '-c', TestProcMemTree.allocAndSpawnProg(
			allocDelayProg(inBytes(amem), duration), allocDelayProg(inBytes(camem), duration))))
		time.sleep(duration*2)
		#proc.wait()  # Wait for the process termination
		try:
			up = psutil.Process(proc.pid)
		except psutil.Error as err:
			print('WARNING, psutil.Process() failed: ', err, file=sys.stderr)
			return
		vmem = inGigabytes(up.memory_info().vms) * 1000  # Mb; Virtual Memory Size
		rmem = inGigabytes(up.memory_info().rss) * 1000  # Mb; Resident Set Size
		uvmem = inGigabytes(up.memory_full_info().vms) * 1000  # Mb; Unique Set Size
		urmem = inGigabytes(up.memory_full_info().rss) * 1000  # Mb; Unique Set Size

		avmem = vmem
		armem = rmem
		auvmem = uvmem
		aurmem = urmem
		cxvmem = 0
		cxrmem = 0
		cxuvmem = 0
		cxurmem = 0
		cxpid = None
		cnum = 0  # The number of child processes
		for ucp in up.children(recursive=True):
			cnum += 1
			cvmem = inGigabytes(ucp.memory_info().vms) * 1000  # Mb; Virtual Memory Size
			crmem = inGigabytes(ucp.memory_info().rss) * 1000  # Mb; Resident Set Size
			cuvmem = inGigabytes(ucp.memory_full_info().vms) * 1000  # Mb; Unique Set Size
			curmem = inGigabytes(ucp.memory_full_info().rss) * 1000  # Mb; Unique Set Size
			print('Memory in Mb of "{pname}" #{pid}: (vmem: {vmem:.2f}, rmem: {rmem:.2f}, uvmem: {uvmem:.2f}, urmem: {urmem:.2f})'
				.format(pname=ucp.name(), pid=ucp.pid, vmem=cvmem, rmem=crmem, uvmem=cuvmem, urmem=curmem))
			# Identify consumption by the heaviest child (by absolute vmem)
			if cxvmem < cvmem:
				cxvmem = cvmem
				cxrmem = crmem
				cxuvmem = cuvmem
				cxurmem = curmem
				cxpid = ucp.pid
			avmem += cvmem
			armem += crmem
			auvmem += cuvmem
			aurmem += curmem

		amem *= 1000  # Mb
		camem *= 1000  # Mb
		proc.wait()  # Wait for the process termination

		print('Memory in Mb:\n  allocated for the proc #{pid}: {amem}, child: {camem}, total: {tamem}'
			'\n  psutil proc #{pid} (vmem: {vmem:.2f}, rmem: {rmem:.2f}, uvmem: {uvmem:.2f}, urmem: {urmem:.2f})'
			'\n  psutil proc #{pid} tree ({cnum} subprocs) heaviest child #{cxpid}'
			' (vmem: {cxvmem:.2f}, rmem: {cxrmem:.2f}, uvmem: {cxuvmem:.2f}, urmem: {cxurmem:.2f})'
			'\n  psutil proc #{pid} tree (vmem: {avmem:.2f}, rmem: {armem:.2f}, uvmem: {auvmem:.2f}, urmem: {aurmem:.2f})'
			''.format(pid=proc.pid, amem=amem, camem=camem, tamem=amem+camem
			, vmem=vmem, rmem=rmem, uvmem=uvmem, urmem=urmem
			, cnum=cnum, cxpid=cxpid, cxvmem=cxvmem, cxrmem=cxrmem, cxuvmem=cxuvmem, cxurmem=cxurmem
			, avmem=avmem, armem=armem, auvmem=auvmem, aurmem=aurmem))


	def test_jobVmem(self):
		"""Test job virual memory evaluation
		"""
		worktime = TestExecPool._latency * 6  # Note: should be larger than 3*_latency
		timeout = worktime * 2  # Note: should be larger than 3*_latency
		#etimeout = max(1, TestExecPool._latency) + (worktime * 2) // 1  # Job work time
		etimeout = (max(1, TestExecPool._latency) + timeout) * 3  # Execution pool timeout; Note: *3 because nonstarted jobs exist here nad postponed twice
		assert TestExecPool._latency * 3 < worktime < timeout and timeout < etimeout, 'Testcase parameters validation failed'

		# Start not more than 3 simultaneous workers
		with ExecPool(max(TestExecPool._WPROCSMAX, 3), latency=TestExecPool._latency) as xpool:  # , _AFNSTEP, vmlimit
			amem = 0.02  # Direct allocating memory in the process
			camem = 0.07  # Allocatinf memory in the child process
			duration = worktime / 3  # Duration in sec
			job =  Job('jvmem_proc', args=(PYEXEC, '-c', TestProcMemTree.allocAndSpawnProg(
				allocDelayProg(inBytes(amem), duration), allocDelayProg(inBytes(camem), duration)))
				, timeout=timeout, ondone=mock.MagicMock())
			jobtr =  Job('jvmem_tree', args=(PYEXEC, '-c', TestProcMemTree.allocAndSpawnProg(
				allocDelayProg(inBytes(amem), duration), allocDelayProg(inBytes(camem), duration)))
				, timeout=timeout, ondone=mock.MagicMock())
			jobx =  Job('jvmem_max-subproc', args=(PYEXEC, '-c', TestProcMemTree.allocAndSpawnProg(
				allocDelayProg(inBytes(amem), duration), allocDelayProg(inBytes(camem), duration)))
				, timeout=timeout, ondone=mock.MagicMock())

			# Verify that non-started job raises exception on memory update request
			self.assertRaises(AttributeError, job._updateVmem)

			tstart = time.time()
			xpool.execute(job)
			xpool.execute(jobtr)
			xpool.execute(jobx)
			time.sleep(duration*2)
			pvmem = job._updateVmem()
			tvmem = jobtr._updateVmem(True)
			xvmem = jobx._updateVmem(True, True)
			# Verify memory consumption
			self.assertTrue(pvmem < xvmem < tvmem)
			print('Memory consumption in Mb,  proc_vmem: {pvmem:.3g}, max_procInTree_vmem: {xvmem:.3g}, procTree_vmem: {tvmem:.3g}'
				.format(pvmem=pvmem*1000, xvmem=xvmem*1000, tvmem=tvmem*1000))
			# Verify exec pool completion before the timeout
			self.assertTrue(xpool.join(etimeout))
			etime = time.time() - tstart  # Execution time
			# Verify jobs execution time
			self.assertLessEqual(jobtr.tstop - jobtr.tstart, etime)


if __name__ == '__main__':
	"""Doc tests execution"""
	import doctest
	#doctest.testmod()  # Detailed tests output
	flags = doctest.REPORT_NDIFF | doctest.REPORT_ONLY_FIRST_FAILURE
	failed, total = doctest.testmod(optionflags=flags)
	if failed:
		print("Doctest FAILED: {} failures out of {} tests".format(failed, total), file=sys.stderr)
	else:
		print('Doctest PASSED')
	# Note: to check specific testcase use:
	# $ python -m unittest mpepool.TestExecPool.test_jobTimeoutChained
	if unittest.main().result:  # verbosity=2
		print('Try to reexecute the tests using x2-3 larger TestExecPool._latency (especially for the first run)')
