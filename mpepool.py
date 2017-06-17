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
	from future.utils import viewitems  # Required to efficiently traverse items of dictionaries in both Python 2 and 3
except ImportError:
	def hasMethod(obj, method):
		"""Whether the object has the specified method
		Note: no exceptions are raised

		obj  - the object to be checked for the method
		method  - name of the target method

		return  whether obj.method is callable
		"""
		try:
			ometh = getattr(obj, method, None)
			return callable(ometh)
		except Exception:
			pass
		return False

	viewitems = lambda dct: dct.items() if not hasMethod(dct, viewitems) else dct.viewitems()


_ADJUST_WORKERS_BY_RAM = False  # Adjust the number of worker processing depending on the remained free RAM, which is actual when computations should avoid swap usage
_ADJUST_RAM_MIN = 0.02  # Relative minimal amount of the remained free RAM to readjust workers count or terminate the benchmark, typically 5 .. 1 %
if _ADJUST_WORKERS_BY_RAM:
	raise NotImplementedError('Dynamic adjustment of the workers queue for in-RAM processing is not implemented')
	#try:
	#	import psutil
	#except ImportError:
	#	_ADJUST_WORKERS_BY_RAM = False

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
	""" Container of Jobs"""
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
			self.tstart = time.time()
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
	# Note: the same job can be executed as Popen or Process object, but ExecPool
	# should use some wrapper in the latter case to manage it
	"""Job is executed in a separate process via Popen or Process object and is
	managed by the Process Pool Executor
	"""
	# NOTE: keyword-only arguments are specified after the *, supported only since Python 3
	def __init__(self, name, workdir=None, args=(), timeout=0, ontimeout=False, task=None #,*
	, startdelay=0, onstart=None, ondone=None, params=None, stdout=sys.stdout, stderr=sys.stderr, omitafn=False):
		"""Initialize job to be executed

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
		omitafn  - Omit affinity policy of the scheduler, which is actual when the affinity is enabled
			and the process has multiple treads

		tstart  - start time is filled automatically on the execution start (before onstart). Default: None
		tstop  - termination / completion time after ondone
		proc  - process of the job, can be used in the ondone() to read it's PIPE
		"""
		assert isinstance(name, str) and timeout >= 0 and (task is None or isinstance(task, Task)), 'Parameters validaiton failed'
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
		# Private attributes
		self.proc = None  # Process of the job, can be used in the ondone() to read it's PIPE
		# Process-related file descriptors to be closed
		self._fstdout = None
		self._fstderr = None
		# Omit scheduler affinity policy (actual when some process is computed on all treads, etc.)
		self._omitafn = omitafn
		# Rescheduling data
		#self.category  # Job name + params
		#self.weight  # Network size
		#self.vmem  # Consumed VM on rescheduling,inherited in the dependent heviar tasks of the same category


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


class ExecPool(object):
	'''Execution Pool of workers for jobs

	A worker in the pool executes only the one job, a new worker is created for
	each subsequent job.
	'''

	def __init__(self, workers=cpu_count(), afnstep=None, inramproc=False):
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
			NOTE: specification of the afnstep might cause reduction of the workers.
		inramproc  - try to perform in-RAM processing adjusting the number of worker
			processes in case of lack of the free RAM up to them whole pool termination
			starting from the most memory-heavy processes.
			NOTE: applicable only if _ADJUST_WORKERS_BY_RAM
		"""
		assert workers >= 1, 'At least one worker should be managed by the pool'

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
		self._workersLim = workers  # Max number of workers
		self._workers = {}  # Current workers (processes, executing jobs): 'jname': <proc>; <proc>: timeout
		self._jobs = collections.deque()  # Scheduled jobs: 'jname': **args
		self._tstart = None  # Start time of the execution of the first task
		# Predefined privte attributes
		self._killCount = 3  # 3 cycles of self._latency, termination wait time
		self._afnstep = afnstep  # Affinity step for each worker process
		self._affinity = None if not self._afnstep else [None]*self._workersLim
		assert self._workersLim * (self._afnstep if self._afnstep else 1) <= cpu_count(), (
			'_workersLim or _afnstep is too large')
		self._numanodes = cpunodes()  # Defines secuence of the CPU ids on affinity table mapping for the crossnodes enumeration
		self._inramproc = _ADJUST_WORKERS_BY_RAM and inramproc
		self._latency = 2 + self._inramproc  # Seconds of sleep on pooling


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
			procs = self._workers.keys()
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
			for job in self._workers.values():
				job.complete(False)
				self.__clearAffinity(job)
			self._workers.clear()


	def __startJob(self, job, async=True):
		"""Start the specified job by one of workers

		job  - the job to be executed, instance of Job
		async  - async execution or wait intill execution completed
		return  - 0 on successful execution, proc.returncode otherwise
		"""
		assert isinstance(job, Job), 'job type is invalid'
		assert job.tstop is None, 'Only non-completed jobs should be started'
		if async and len(self._workers) > self._workersLim:
			raise AssertionError('Free workers must be available ({} busy workers of {})'
				.format(len(self._workers), self._workersLim))

		if _DEBUG_TRACE:
			print('Starting "{}"{}...'.format(job.name, '' if async else ' in sync mode'), file=sys.stderr)
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
		## Adjust workers depending on the memory consumption if required
		#if self._inramproc:
		#	# Check for the low free memory condition
		#	#assert _ADJUST_WORKERS_BY_RAM, '_inramproc verification failed'
		#	vmem = psutil.virtual_memory()
		#	if vmem.free < _ADJUST_RAM_MIN * vmem.total:
		#		# Find the heaviest worker process to be terminated
		#		xm = 0  # Largest process memory (VMS - all process memory, not just RSS because we are trying to fit it into the RAM)
		#		xp = None
		#		for proc in self._workers:
		#			pmem = psutil.Process(proc.pid).memory_info().vms
		#			if pmem > xm:
		#				xm = pmem
		#				xp = proc
		#		# Terminate the process if it has not been completed yet
		#		xj = self._workers[xp]  # Job of the correespondinf process
		#		if xp.poll() is None:
		#			xp.kill()
		#			#print('WARNING, "{}" #{} is terminated by the timeout ({:.4f} sec): {:.4f} sec ({} h {} m {:.4f} s)'
		#			#.format(jx.name, px.pid, jx.timeout, exectime, *secondsToHms(exectime)), file=sys.stderr)
		#			# Reschedule for the future
		#			self._jobs.append(xj)
		#		else:
		#			xj.complete()
		#		self.__clearAffinity(xj)
		#		del self._workers[xp]
		#		# Reduce the workers limit
		#		if self._workersLim > len(self._workers):
		#			self._workersLim = self._workers
		#		else:
		#			self._workersLim -= 1
		#			assert self._workersLim == len(self._workers), '_workersLim verification failed'
		#
		#		if not self._workersLim:
		#			self._terminate()
		#			return

		# Process completed jobs and check timeouts
		completed = []  # Completed workers
		for proc, job in viewitems(self._workers):  # .items()  Note: the number of _workers is small
			if proc.poll() is not None:
				completed.append((proc, job))
				continue
			exectime = time.time() - job.tstart
			if not job.timeout or exectime < job.timeout:
				continue
			# Terminate the worker
			proc.terminate()
			# Wait a few sec for the successful process termitaion before killing it
			i = 0
			while proc.poll() is None and i < self._killCount:
				i += 1
				time.sleep(self._latency)
			if proc.poll() is None:
				proc.kill()
			del self._workers[proc]
			print('WARNING, "{}" #{} is terminated by the timeout ({:.4f} sec): {:.4f} sec ({} h {} m {:.4f} s)'
				.format(job.name, proc.pid, job.timeout, exectime, *secondsToHms(exectime)), file=sys.stderr)
			# Restart the job if required
			self.__clearAffinity(job)  # Note: the affinity must be updated before the job restart
			if job.ontimeout:
				self.__startJob(job)
			else:
				job.complete(False)

		# Process completed jobs: execute callbacks and remove the workers
		for proc, job in completed:
			del self._workers[proc]
			job.complete()
			self.__clearAffinity(job)

		# Start subsequent job if it is required
		while self._jobs and len(self._workers) <  self._workersLim:
			self.__startJob(self._jobs.popleft())


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



	def join(self, timeout=0):
		"""Execution cycle

		timeout  - execution timeout in seconds before the workers termination, >= 0.
			0 means unlimited time. The time is measured SINCE the first job
			was scheduled UNTIL the completion of all scheduled jobs.
		return  - True on graceful completion, Flase on termination by the specified timeout
		"""
		assert timeout >= 0, 'timeout valiadtion failed'
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
		self._tstart = None
		return True


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
