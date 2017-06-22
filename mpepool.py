#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
\descr:  Multi-Process Execution Pool to schedule Jobs execution with per-Job timeout,
	optionally grouping them into Tasks and specifying execution paremeters:
	- timeout per each Job (it was the main motivation to implemtent this module)
	- onstart/ondone callbacks, ondone is called only on successful completion (not termination)
	- stdout/err output, which can be redireted to any custom file or PIPE
	- custom parameters for each job and task besides the name/id

	Flexible API provides optional automatic restart of jobs on timeout, access to job's process,
	parent task, start and stop execution time and much more...

	Global functionality parameters:
	_LIMIT_WORKERS_RAM  - limit the amount of virtual memory (<= RAM) used by worker processes,
		requires psutil import
	_CHAINED_CONSTRAINTS  - terminate related jobs on terminating any job by the execution
		constraints (timeout or RAM limit)

\author: (c) Artem Lutov <artem@exascale.info>
\organizations: eXascale Infolab <http://exascale.info/>, Lumais <http://www.lumais.com/>, ScienceWise <http://sciencewise.info/>
\date: 2015-07
"""

from __future__ import print_function, division  # Required for stderr output, must be the first import
import sys
import time
import collections
import os
import ctypes  # Required for the multiprocessing Value definition
import types  # Required for instance methods definition
import traceback  # Stacktrace
import subprocess

from multiprocessing import cpu_count
from multiprocessing import Value

try:
	# Required to efficiently traverse items of dictionaries in both Python 2 and 3
	from future.utils import viewitems, viewkeys, viewvalues
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

	viewitems = lambda dct: viewMethod(dct, 'items')()
	viewkeys = lambda dct: viewMethod(dct, 'keys')()
	viewvalues = lambda dct: viewMethod(dct, 'values')()


# Limit the amount of virtual memory (<= RAM) used by worker processes
# NOTE: requires import of psutils
_LIMIT_WORKERS_RAM = False
if _LIMIT_WORKERS_RAM:
	try:
		import psutil
	except ImportError:
		_LIMIT_WORKERS_RAM = False

# Use chained constraints (timeout and memory limitation) in jobs to terminate
# also dependent worker processes and/or reschedule jobs, which have the same
# category and heavier than the origin violating the constraints
_CHAINED_CONSTRAINTS = False


_AFFINITYBIN = 'taskset'  # System app to set CPU affinity if required, should be preliminarry installed (taskset is present by default on NIX systems)
_DEBUG_TRACE = False  # Trace start / stop and other events to stderr


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
		tstop  - termination / completion time after ondone
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
	# rtm = 0.85  # Memory retention ratio, used to not drop the memory info fast on temporal releases, E [0, 1)
	# assert 0 <= rtm < 1, 'Memory retention ratio should E [0, 1)'

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
		category  - classification category, typically context or part of the name
		size  - size of the processing data, >= 0
			0 means undefined size and prevents jobs chaining on constraints violation
		slowdown  - execution slowdown ratio (inversely to the [estimated] execution speed), E (0, inf)

		# Execution parameters, initialized automatically on execution
		tstart  - start time is filled automatically on the execution start (before onstart). Default: None
		tstop  - termination / completion time after ondone
		proc  - process of the job, can be used in the ondone() to read it's PIPE
		vmem  - consuming virtual memory (smooth max, not just the current value) or the least expected value
			inherited from the jobs of the same category having non-smaller size
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
		self.terminates = 0  # The number of received termination requirests
		# Process-related file descriptors to be closed
		self._fstdout = None
		self._fstderr = None
		# Omit scheduler affinity policy (actual when some process is computed on all treads, etc.)
		self._omitafn = omitafn
		if _CHAINED_CONSTRAINTS:
			self.category = category  # Job name
			self.size = size  # Size of the processing data
			self.slowdown = slowdown  # Execution slowdown ratio, ~ 1 / exec_speed
		if _LIMIT_WORKERS_RAM:
			# Consumed VM on execution in gigabytes or the least expected (inherited from the
			# related jobs having the same category and non-smaller size)
			self.vmem = 0.


	# def updateVmem(self, curvmem):
	# 	"""Update virtual memory consumption using smooth max
	#
	# 	curvmem  - current consumption of virtual memory (vms) by the job
	#
	# 	return  - smooth max of job vmem
	# 	"""
	# 	self.vmem = max(curvmem, self.vmem * Job.rtm + curvmem * (1-Job.rtm))
	# 	return self.vmem


	def complete(self, graceful=True):
		"""Completion function
		ATTENTION: This function is called after the destruction of the job-associated process
		to perform cleanup in the context of the caller (main thread).

		graceful  - the job is successfully completed or it was terminated
		"""
		# Close process-related file descriptors
		for fd in (self._fstdout, self._fstderr):
			if fd and hasattr(fd, 'close'):
				fd.close()
		self._fstdout = None
		self._fstderr = None

		# Job-related post execution
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
			if _DEBUG_TRACE:
				print('"{}" #{} is completed'.format(self.name, self.proc.pid if self.proc else -1), file=sys.stderr)
		# Check whether the job is associated with any task
		if self.task:
			self.task = self.task.delJob(graceful)
		# Updated execution status
		self.tstop = time.time()


def ramfracs(fracsize):
	"""Evaluate the minimal number of RAM fractions of the specified size in GB

	Used to estimate the reasonable number of processes with the specified minimal
	dedicated RAM.

	fracsize  - minimal size of each fraction in GB, can be a fractional number
	return the minimal number of RAM fractions having the specified size in GB
	"""
	return int(os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / float(1024**3) / fracsize)


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


def gigabytes(vbytes):
	return vbytes / (1024. ** 3)


class ExecPool(object):
	'''Execution Pool of workers for jobs

	A worker in the pool executes only the one job, a new worker is created for
	each subsequent job.
	'''

	def __init__(self, workers=cpu_count(), afnstep=None, vmlimit=0., latency=0.):
		"""Execution Pool constructor

		workers  - number of resident worker processes, >=1. The reasonable value is
			<= NUMA nodes * node CPUs, which is typically returned by cpu_count(),
			where node CPUs = CPU cores * HW treads per core.
			To guarantee minimal number of RAM per a process, for example 2.5 GB:
				workers = min(cpu_count(), max(ramfracs(2.5), 1))
		afnstep  - affinity step, integer if applied. Used to bind worker to the
			processing units to have warm cache for single thread workers.
			Typical values:
				None  - do not use affinity at all (recommended for multi-threaded workers),
				1  - maximize parallelization (the number of worker processes = CPU units),
				cpucorethreads()  - maximize the dedicated CPU cache (the number of
					worker processes = CPU cores = CPU units / hardware treads per CPU core).
			NOTE: specification of the afnstep might cause reduction of the workers number.
		vmlimit  - limit total amount of VM (automatically constrained by the available RAM)
			in gigabytes that can be used by worker processes to provide in-RAM computations.
			Dynamically reduce the number of workers to consume total virtual memory
			not more than specified. The workers are rescheduled starting from the
			most memory-heavy processes.
			NOTE:
				- applicable only if _LIMIT_WORKERS_RAM
				- 0 means unlimited
		latency  - approximate minimal latency of the workers monitoring in sec, float >= 0.
			0 means automatically defined value (recommended, typically 2-3 sec).
		"""
		assert workers >= 1 and afnstep <= cpu_count() and vmlimit >= 0 and latency >= 0, 'Input parameters are invalid'

		# Verify and update workers and afnstep if required
		if afnstep:
			# Check whether _AFFINITYBIN exists in the system
			try:
				subprocess.call([_AFFINITYBIN, '-V'])
				if afnstep > cpu_count() / workers:
					print('WARNING: the number of worker processes is reduced'
						' ({wlim0} -> {wlim} to satisfy the affinity step'
						.format(wlim0=workers, wlim=cpu_count() // afnstep))
					workers = cpu_count() // afnstep
			except OSError as err:
				afnstep = None
				print('WARNING: {afnbin} does not exists in the system to fix affinity: {err}'
					.format(afnbin=_AFFINITYBIN, err=err))
		self._workersLim = workers  # Max number of resident workers
		self._workers = {}  # Scheduled jobs that were started, i.e. worker processes:  {process: executing_job}
		self._jobs = collections.deque()  # Scheduled jobs that have not been started yet:  deque(job)
		self._tstart = None  # Start time of the execution of the first task
		# Affinity scheduling attributes
		self._afnstep = afnstep  # Affinity step for each worker process
		self._affinity = None if not self._afnstep else [None]*self._workersLim
		assert self._workersLim * (self._afnstep if self._afnstep else 1) <= cpu_count(), (
			'_workersLim or _afnstep is too large')
		self._numanodes = cpunodes()  # Defines sequence of the CPU ids on affinity table mapping for the crossnodes enumeration
		# Virtual memory tracing attributes
		self._vmlimit =  0. if not _LIMIT_WORKERS_RAM else max(0, min(vmlimit
			, gigabytes(psutil.virtual_memory().total) - 0.25))  # in GB; Dedicate 256 Mb for OS
		#self.vmtotal = 0.  # Virtual memory used by all workers in gigabytes
		# Execution rescheduling attributes
		self._latency = latency if latency else 2 + (not not self._vmlimit)  # Seconds of sleep on pooling
		# Predefined private attributes
		self._killCount = 3  # 3 cycles of self._latency, termination wait time


	def __clearAffinity(self, job):
		"""Clear job affinity

		job  - the job to be processed
		"""
		if self._affinity and not job._omitafn and job.proc is not None:
			try:
				self._affinity[self._affinity.index(job.proc.pid)] = None
			except ValueError:
				print('WARNING: affinity clearup is requested to the job "{}" without the activated affinity'
					.format(job.name))
				pass  # Do nothing if the affinity was not set for this process


	def __del__(self):
		self.__terminate()


	def __finalize__(self):
		self.__del__()


	def __terminate(self):
		"""Force termination of the pool"""
		if not self._jobs and not self._workers:
			return

		print('WARNING: terminating the execution pool with {} non-scheduled jobs and {} workers...'
			.format(len(self._jobs), len(self._workers)))
		for job in self._jobs:
			job.complete(False)
			# Note: only executing jobs, i.e. workers might have activated affinity
			print('  Scheduled "{}" is removed'.format(job.name))
		self._jobs.clear()
		while self._workers:
			procs = viewkeys(self._workers)
			for proc in procs:
				print('  Terminating "{}" #{} ...'.format(self._workers[proc].name, proc.pid), file=sys.stderr)
				proc.terminate()
			# Wait a few sec for the successful process termitaion before killing it
			i = 0
			active = True
			while active and i < self._killCount:
				active = False
				for proc in procs:
					if proc.poll() is None:
						active = True
						break
				time.sleep(self._latency)
			# Kill nonterminated processes
			if active:
				for proc in procs:
					if proc.poll() is None:
						print('  Killing the worker #{} ...'.format(proc.pid), file=sys.stderr)
						proc.kill()
			# Tidy jobs
			for job in viewvalues(self._workers):
				job.complete(False)
				self.__clearAffinity(job)
			self._workers.clear()


	def __startJob(self, job, async=True):
		"""Start the specified job by one of workers

		job  - the job to be executed, instance of Job
		async  - async execution or wait intill execution completed
		return  - 0 on successful execution, proc.returncode otherwise
		"""
		assert isinstance(job, Job), 'Job type is invalid'
		assert job.tstop is None, 'Only non-completed jobs should be started'
		if async and len(self._workers) > self._workersLim:
			raise AssertionError('Free workers must be available ({} busy workers of {})'
				.format(len(self._workers), self._workersLim))

		if _DEBUG_TRACE:
			print('Starting "{}"{}...'.format(job.name, '' if async else ' in sync mode'), file=sys.stderr)
		job.terminates = 0  # Reset termination requests counter
		job.tstart = time.time()
		if job.onstart:
			#print('Starting onstart() for job {}: {}'.format(job.name), file=sys.stderr)
			try:
				job.onstart()
			except Exception as err:
				print('ERROR in onstart() callback of "{}": {}. {}'.format(
					job.name, err, traceback.format_exc()), file=sys.stderr)
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
							raise ValueError('Ivalid output stream value: ' + joutp)
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
					, str(job.stdout), str(job.stderr)))
			if(job.args):
				# Consider CPU affinity
				iafn = -1 if not self._affinity or job._omitafn else self._affinity.index(None)  # Index in the affinity table to bind process to the CPU/core
				if iafn >= 0:
					job.args = [_AFFINITYBIN, '-c', str(afnicpu(iafn, self._afnstep, self._numanodes))] + list(job.args)
				#print('Opening proc with:\n\tjob.args: {},\n\tcwd: {}'.format(' '.join(job.args), job.workdir), file=sys.stderr)
				job.proc = subprocess.Popen(job.args, bufsize=-1, cwd=job.workdir, stdout=fstdout, stderr=fstderr)  # bufsize=-1 - use system default IO buffer size
				if iafn >= 0:
					self._affinity[iafn] = job.proc.pid
					print('Affinity {afn} (CPU #{icpu}) of job "{jname}" proc {pid}'
						.format(afn=iafn, icpu=afnicpu(iafn, self._afnstep, self._numanodes), jname=job.name, pid=job.proc.pid))
				# Wait a little bit to start the process besides it's scheduling
				if job.startdelay > 0:
					time.sleep(job.startdelay)
		except Exception as err:  # Should not occur: subprocess.CalledProcessError
			print('ERROR on "{}" execution occurred: {}, skipping the job. {}'.format(
				job.name, err, traceback.format_exc()), file=sys.stderr)
			# Note: process-associated file descriptors are closed in complete()
			job.complete(False)
			if job.proc is not None:  # Note: this is an extra rare, but possible case
				self.__clearAffinity(job)  # Note: process can both exists here and does not exist, i.e. the process state is undefined
		else:
			if async:
				self._workers[job.proc] = job
			else:
				job.proc.wait()
				job.complete()
				self.__clearAffinity(job)
				return job.proc.returncode
		return 0


	def __reviseWorkers(self):
		"""Rewise the workers

		Check for the comleted jobs and their timeous, update corresponding
		workers and start the jobs if possible
		"""
		# Process completed jobs, check timeouts and memory constraints matching
		completed = []  # Completed workers:  (proc, job)
		vmtotal = 0.  # Consuming virtual memory by workers
		jtorigs = []  # Timeout caused terminating origins (jobs) for the chained termination
		for proc, job in viewitems(self._workers):  # .items()  Note: the number of _workers is small
			if proc.poll() is not None:  # Not None means the process has been terminated / completed
				completed.append((proc, job))
				continue
			# Update memory consumption statistics if applicable
			if self._vmlimit:
				#pminf = gigabytes(psutil.Process(proc.pid).memory_info())
				#vmem += job.updateVmem(pminf.vms)
				job.vmem = gigabytes(psutil.Process(proc.pid).memory_info().vms)
				vmtotal += job.vmem

			exectime = time.time() - job.tstart
			if not job.timeout or exectime < job.timeout:
				continue

			# Terminate the worker
			job.terminates += 1
			# Check related heavier workers and jobs, terminate them by timeout
			if _CHAINED_CONSTRAINTS and job.category and job.size:
				jtorigs.append(job)
				raise NotImplementedError('Chained termination is not implemented yet')

			# Force killling when termination does not work
			if job.terminates >= self._killCount:
				proc.kill()
				del self._workers[proc]
				if self._vmlimit:
					vmtotal -= job.vmem
				print('WARNING, "{}" #{} is killed by the timeout ({:.4f} sec): {:.4f} sec ({} h {} m {:.4f} s)'
					.format(job.name, proc.pid, job.timeout, exectime, *secondsToHms(exectime)), file=sys.stderr)
			else:
				proc.terminate()
		#self.vmtotal = vmtotal

		# Terminated torigs-depended workers and jobs
		# for job in jtorigs:
		# 	for proc, job in viewitems(self._workers):
		# 		if

		# Process completed (and terminated) jobs: execute callbacks and remove the workers
		for proc, job in completed:
			if job.terminates:
				exectime = time.time() - job.tstart
				print('WARNING, "{}" #{} is terminated by the timeout ({:.4f} sec): {:.4f} sec ({} h {} m {:.4f} s)'
					.format(job.name, proc.pid, job.timeout, exectime, *secondsToHms(exectime)), file=sys.stderr)
			# Restart the job if it was terminated and should be restarted
			if job.terminates and job.ontimeout:
				self.__clearAffinity(job)  # Note: the affinity must be updated before the job restart
				self.__startJob(job)
				if _DEBUG_TRACE:
					print('"  {}" #{} is restarted with timeout {:.4f} sec'
						.format(job.name, proc.pid, job.timeout), file=sys.stderr)
				if self._vmlimit:
					vmtotal += job.vmem
			else:
				del self._workers[proc]
				job.complete(not job.terminates)  # The completion is graceful only if the termination requests were not recuived
				self.__clearAffinity(job)

		# Start subsequent job if it is required
		while self._jobs and len(self._workers) <  self._workersLim:
			job = self._jobs.popleft()
			self.__startJob(job)
			if self._vmlimit:
				vmtotal += job.vmem

		# Check memory limitation fulfilling
		if self._vmlimit and vmtotal >= self._vmlimit:
			# Terminate some workers and reschedule jobs or reduce workers number
			raise NotImplementedError('Memory management is not implemented yet')


	def execute(self, job, async=True):
		"""Schecule the job for the execution

		job  - the job to be executed, instance of Job
		async  - async execution or wait until execution completed
		  NOTE: sync tasks are started at once
		return  - 0 on successful execution, process return code otherwise
		"""
		assert isinstance(job, Job), 'job type is invalid'
		assert len(self._workers) <= self._workersLim, 'Number of workers exceeds the limit'
		assert job.name, 'Job parameters must be defined'  #  and job.workdir and job.args

		if _DEBUG_TRACE:
			print('Scheduling the job "{}" with timeout {}'.format(job.name, job.timeout))
		if async:
			# Start the execution timer
			if self._tstart is None:
				self._tstart = time.time()
			# Schedule the job, postpone it if already postponed jobs exist or no any free workers
			if self._jobs or len(self._workers) >= self._workersLim:
				self._jobs.append(job)
				#self.__reviseWorkers()  # Anyway the workers are revised if exist in the working cycle
			else:
				self.__startJob(job)
		else:
			return self.__startJob(job, False)
		return  0



	def join(self, timeout=0.):
		"""Execution cycle

		timeout  - execution timeout in seconds before the workers termination, >= 0.
			0 means unlimited time. The time is measured SINCE the first job
			was scheduled UNTIL the completion of all scheduled jobs.
		return  - True on graceful completion, Flase on termination by the specified timeout
		"""
		assert timeout >= 0., 'timeout valiadtion failed'
		if self._tstart is None:
			assert not self._jobs and not self._workers, \
				'Start time should be defined for the present jobs'
			return

		self.__reviseWorkers()
		while self._jobs or self._workers:
			if timeout and time.time() - self._tstart > timeout:
				self.__terminate()
				return False
			time.sleep(self._latency)
			self.__reviseWorkers()
		self._tstart = None  # Be ready for the following execution
		return True


# Unit Tests -------------------------------------------------------------------
import unittest
#import math

class TestExecPool(unittest.TestCase):
	from math import ceil

	#global _DEBUG_TRACE
	#_DEBUG_TRACE = True

	_WPROCSMAX = max(cpu_count() - 1, 1)  # Maximal number of the worker processes, should be >= 1
	_AFNSTEP = cpucorethreads()  # Affinity
	_latency = 0.1  # Approximate minimal latency of ExecPool in seconds
	#_execpool = None


	#@staticmethod
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


	def test_jobTimeoutSimple(self):
		"""Validate termination of a single job by timeout and completion of another job"""
		timeout = TestExecPool._latency * 4  # Note: should be larger than 3*_latency
		worktime = max(1, TestExecPool._latency) + (timeout * 2) // 1  # Job work time
		assert TestExecPool._latency * 3 < timeout < worktime, 'Testcase parameters validation failed'

		tstart = time.time()
		jterm = Job('j_timeout', args=('sleep', str(worktime)), timeout=timeout)
		self._execpool.execute(jterm)
		jcompl = Job('j_complete', args=('sleep', '0'), timeout=timeout)
		self._execpool.execute(jcompl)
		# Validate successful completion of the execution pool
		self.assertTrue(self._execpool.join())
		etime = time.time() - tstart  # Execution time
		self.assertFalse(self._execpool._workers)  # Workers shuold be empty
		# Validate termination time
		self.assertLess(etime, worktime)
		self.assertGreaterEqual(etime, timeout)
		# Validate jobs timings
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
		# Validate termination of the execution pool
		self.assertFalse(self._execpool.join(etimeout))
		etime = time.time() - tstart  # Execution time
		self.assertFalse(self._execpool._workers)  # Workers shuold be empty
		# Validate termination time
		self.assertLess(etime, worktime)
		self.assertLess(etime, timeout)
		self.assertGreaterEqual(etime, etimeout)
		# Validate jrx runsCount and onstart execution
		self.assertGreaterEqual(runsCount['count'], 2)


	@unittest.skipUnless(_CHAINED_CONSTRAINTS, 'Requires _CHAINED_CONSTRAINTS')
	def test_jobTimeoutChained(self):
		"""Validate:
			1. Termination of the dependent larger job on termination of the main job
			2. Not affecting smaller dependent jobs on termination of the main job
			3. Not affecting independent jobs (with another category)
			4. Execution of the ondone() only on the graceful termination
		"""
		self.fail('Under development')

		timeout = TestExecPool._latency * 4  # Note: should be larger than 3*_latency
		worktime = max(1, TestExecPool._latency) + (timeout * 2) // 1  # Job work time
		assert TestExecPool._latency * 3 < timeout < worktime, 'Testcase parameters validation failed'

		tstart = time.time()
		jm = Job('jmaster_timeout', args=('sleep', str(worktime))
			, category='cat1', size=2, timeout=timeout)
		self._execpool.execute(jm)
		jss = Job('jslave_smaller', args=('sleep', str(worktime))
			, category='cat1', size=1, ondone=unittest.mock.MagicMock())
		self._execpool.execute(jss)  # ondone() should be called for the completed job
		jsl = Job('jslave_larger', args=('sleep', str(worktime))
			, category='cat1', size=3, ondone=unittest.mock.MagicMock())
		self._execpool.execute(jsl)  # ondone() should be skipped for the terminated job
		jso = Job('job_other', args=('sleep', str(worktime)), category='cat_other'
			, ondone=unittest.mock.MagicMock())
		self._execpool.execute(jso)  # ondone() should be called for the completed job

		# Execution pool timeout
		etimeout = max(1, TestExecPool._latency) + worktime * (1 + len(self._execpool._workers) // self._execpool._workersLim)
		assert etimeout > worktime, 'Additional testcase parameters are invalid'

		# Validate exec pool completion
		self.assertTrue(self._execpool.join(etimeout))
		etime = time.time() - tstart  # Execution time
		# Validate timings
		self.assertTrue(worktime <= etime < etimeout)
		self.assertLess(jm.tstop - jm.tstart, worktime)
		self.assertLess(jsl.tstop - jsl.tstart, worktime)
		self.assertGreaterEqual(jsl.tstop - jsl.tstart, jm.tstop - jm.tstart)
		self.assertGreaterEqual(jss.tstop - jss.tstart, worktime)
		self.assertLess(jsl.tstop - jsl.tstart, worktime)
		self.assertGreaterEqual(jso.tstop - jso.tstart, worktime)
		# Validate ondone() calls
		jss.ondone.assert_called_once_with(jss)  # Note: fails, becase equality is not defined for the Job
		self.assertTrue(len(jss.ondone.call_args) == 1 and jss.ondone.call_args[0] is jss)
		jss.ondone.assert_not_called()


if __name__ == '__main__':
	"""Doc tests execution"""
	import doctest
	#doctest.testmod()  # Detailed tests output
	flags = doctest.REPORT_NDIFF | doctest.REPORT_ONLY_FIRST_FAILURE
	failed, total = doctest.testmod(optionflags=flags)
	if failed:
		print("Doctest FAILED: {} failures out of {} tests".format(failed, total))
	else:
		print('Doctest PASSED')
	unittest.main()  # verbosity=2
