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

from __future__ import print_function  # Required for stderr output, must be the first import
import sys
import time
import subprocess
import collections
import os
import ctypes  # Required for the multiprocessing Value definition
import types  # Required for instance methods definition
import traceback  # Stacktrace

from multiprocessing import cpu_count
from multiprocessing import Value
from subprocess import PIPE
from subprocess import STDOUT


DEBUG_TRACE = False  # Trace start / stop and other events to stderr


def secondsToHms(seconds):
	"""Convert seconds to hours, mins, secs

	seconds  - seconds to be converted, >= 0

	return hours, mins, secs
	"""
	assert seconds >= 0, 'seconds validation failed'
	hours = int(seconds / 3600)
	mins = int((seconds - hours * 3600) / 60)
	secs = seconds - hours * 3600 - mins * 60
	return hours, mins, secs


class Task(object):
	""" Container of Jobs"""
	#TODO: Implement timeout support in add/delJob
	def __init__(self, name, timeout=0, onstart=None, ondone=None, params=None, stdout=sys.stdout, stderr=sys.stderr):
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
		assert isinstance(name, str) and timeout >= 0, 'Parameters validaiton failed'
		self.name = name
		self.timeout = timeout
		self.params = params
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

		graceful  - the job is successfully completed or it was terminated
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
				except StandardError as err:
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
			if DEBUG_TRACE:
				print('"{}" #{} is completed'.format(self.name, self.proc.pid if self.proc else -1), file=sys.stderr)
		# Check whether the job is associated with any task
		if self.task:
			self.task = self.task.delJob(graceful)
		# Updated execution status
		self.tstop = time.time()


class ExecPool(object):
	'''Execution Pool of workers for jobs

	A worker in the pool executes only the one job, a new worker is created for
	each subsequent job.
	'''

	def __init__(self, workers=cpu_count()):
		"""Execution Pool constructor

		workers  - number of resident worker processes
		"""
		assert workers >= 1, 'At least one worker should be managed by the pool'

		self._workersLim = workers  # Max number of workers
		self._workers = {}  # Current workers: 'jname': <proc>; <proc>: timeout
		self._jobs = collections.deque()  # Scheduled jobs: 'jname': **args
		self._tstart = None  # Start time of the execution of the first task
		# Predefined privte attributes
		self._latency = 1  # 1 sec of sleep on pooling
		self._killCount = 3  # 3 cycles of self._latency, termination wait time


	def __del__(self):
		self.__terminate()


	def __finalize__(self):
		self.__del__()


	def __terminate(self):
		"""Force termination of the pool"""
		if not self._jobs and not self._workers:
			return

		print('WARNING: terminating the workers pool ...')
		for job in self._jobs:
			job.complete(False)
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

		if DEBUG_TRACE:
			print('Starting "{}"{}...'.format(job.name, '' if async else ' in sync mode'), file=sys.stderr)
		job.tstart = time.time()
		if job.onstart:
			#print('Starting onstart() for job {}: {}'.format(job.name), file=sys.stderr)
			try:
				job.onstart()
			except StandardError as err:
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

			if DEBUG_TRACE and (fstdout or fstderr):
				print('"{}" output channels:\n\tstdout: {}\n\tstderr: {}'.format(job.name
					, str(job.stdout), str(job.stderr)))
			if(job.args):
				#print('Opening proc with:\n\tjob.args: {},\n\tcwd: {}'.format(' '.join(job.args), job.workdir), file=sys.stderr)
				job.proc = subprocess.Popen(job.args, bufsize=-1, cwd=job.workdir, stdout=fstdout, stderr=fstderr)  # bufsize=-1 - use system default IO buffer size
				# Wait a little bit to start the process besides it's scheduling
				if job.startdelay > 0:
					time.sleep(job.startdelay)
		except StandardError as err:  # Should not occur: subprocess.CalledProcessError
			print('ERROR on "{}" execution occurred: {}, skipping the job. {}'.format(
				job.name, err, traceback.format_exc()), file=sys.stderr)
			# Note: process-associated file descriptors are closed in complete()
			job.complete(False)
		else:
			if async:
				self._workers[job.proc] = job
			else:
				job.proc.wait()
				job.complete()
				return job.proc.returncode
		return 0


	def __reviseWorkers(self):
		"""Rewise the workers

		Check for the comleted jobs and their timeous, update corresponding
		workers and start the jobs if possible
		"""
		completed = []  # Completed workers
		for proc, job in self._workers.items():
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
			if job.ontimeout:
				self.__startJob(job)
			else:
				job.complete(False)

		# Process completed jobs: execute callbacks and remove the workers
		for proc, job in completed:
			del self._workers[proc]
			job.complete()

		# Start subsequent job if it is required
		while self._jobs and len(self._workers) <  self._workersLim:
			self.__startJob(self._jobs.popleft())


	def execute(self, job, async=True):
		"""Schecule the job for the execution

		job  - the job to be executed, instance of Job
		async  - async execution or wait until execution completed
		  NOTE: sync tasks are started at once
		return  - 0 on successful execution, proc. returncode otherwise
		"""
		assert isinstance(job, Job), 'job type is invalid'
		assert len(self._workers) <= self._workersLim, 'Number of workers exceeds the limit'
		assert job.name, "Job parameters must be defined"  #  and job.workdir and job.args

		if DEBUG_TRACE:
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
			0 means absebse of the timeout. The time is measured SINCE the first job
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
