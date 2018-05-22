#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Description:  Unit tests for the Multi-Process Execution Pool

:Authors: (c) Artem Lutov <artem@exascale.info>
:Organizations: eXascale Infolab <http://exascale.info/>, Lumais <http://www.lumais.com/>,
	ScienceWise <http://sciencewise.info/>
:Date: 2017-06 v2
"""

from __future__ import print_function, division  # Required for stderr output, must be the first import
import time, subprocess, unittest
import sys
from sys import executable as PYEXEC  # Full path to the current Python interpreter
# import signal  # Intercept kill signals
# import atexit  # At exit termination handling
# from functools import partial  # Partially predefined functions (callbacks)
try:
	from unittest import mock  #pylint: disable=C0412
except ImportError:
	try:
		# Note: mock is not available under default Python2 / pypy installation
		import mock
	except ImportError:
		mock = None  # Skip unit tests operating with the mock if it is not installed
from mpepool import (AffinityMask, ExecPool, Job, Task, _CHAINED_CONSTRAINTS
	, _LIMIT_WORKERS_RAM, _RAM_SIZE, inBytes, inGigabytes)
# Consider compatibility with Python before v3.3
if not hasattr(time, 'perf_counter'):
	time.perf_counter = time.time
try:
	import psutil
except ImportError as err:
	print('ERROR, psutil import failed: ', err, file=sys.stderr)


# Accessory Functions
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


_WPROCSMAX = max(AffinityMask.CPUS-1, 1)  # Maximal number of the worker processes, should be >= 1
# Note: 0.1 is specified just for the faster tests execution on the non-first run,
# generally at least 0.2 should be used
_TEST_LATENCY = 0.2  # Approximate minimal latency of ExecPool in seconds

class TestExecPool(unittest.TestCase):
	"""Basic tests for the ExecPool"""
	#global _DEBUG_TRACE
	#_DEBUG_TRACE = True

	@classmethod
	def terminationHandler(cls, signal=None, frame=None, terminate=True):  #pylint: disable=W0613
		"""Signal termination handler

		signal  - raised signal
		frame  - origin stack frame
		terminate  - whether to terminate the application
		"""
		#if signal == signal.SIGABRT:
		#	os.killpg(os.getpgrp(), signal)
		#	os.kill(os.getpid(), signal)

		if cls._execpool:
			# print('WARNING{}, execpool is terminating by the signal {} ({})'
			# 	.format('' if not cls._execpool.name else ' ' + cls._execpool.name, signal
			# 	, cls._signals.get(signal, '-')))  # Note: this is a trace log record
			del cls._execpool  # Destructors are called later
			# Define _execpool to avoid unnecessary trash in the error log, which might
			# be caused by the attempt of subsequent deletion on destruction
			cls._execpool = None  # Note: otherwise _execpool becomes undefined
		if terminate:
			sys.exit()  # exit(0), 0 is the default exit code.


	@classmethod
	def setUpClass(cls):
		cls._execpool = ExecPool(_WPROCSMAX, latency=_TEST_LATENCY)

		# Note: signals are redundant since the timeout are tiny
		# # Fill signals mapping {value: name}
		# cls._signals = {sv: sn for sn, sv in signal.__dict__.items()
		# 	if sn.startswith('SIG') and not sn.startswith('SIG_')}

		# # Substitute class parameter to terminationHandler
		# termHandler = partial(cls.terminationHandler, cls)
		# # Set handlers of external signals
		# signal.signal(signal.SIGTERM, termHandler)
		# signal.signal(signal.SIGHUP, termHandler)
		# signal.signal(signal.SIGINT, termHandler)
		# signal.signal(signal.SIGQUIT, termHandler)
		# signal.signal(signal.SIGABRT, termHandler)

		# # Set termination handler for the internal termination
		# atexit.register(termHandler, terminate=False)


	@classmethod
	def tearDownClass(cls):
		if cls._execpool:
			del cls._execpool  # Destructors are called later
			# Define _execpool to avoid unnecessary trash in the error log, which might
			# be caused by the attempt of subsequent deletion on destruction
			cls._execpool = None  # Note: otherwise _execpool becomes undefined


	def setUp(self):
		assert not self._execpool._workers, 'Worker processes should be empty for each new test case'
		self._execpool.alive = True  # Set alive flag after the termination


	def tearDown(self):
		assert not self._execpool._workers and not self._execpool._jobs, (
			'All jobs should be completed in the end of each testcase (workers: "{}"; jobs: "{}")'
			.format(', '.join([job.name for job in self._execpool._workers])
			, ', '.join([job.name for job in self._execpool._jobs])))


	def test_jobTimeoutSimple(self):
		"""Verify termination of a single job by timeout and completion of independent another job"""
		timeout = _TEST_LATENCY * 4  # Note: should be larger than 3*latency
		worktime = max(1, _TEST_LATENCY) + (timeout * 2) // 1  # Job work time
		assert _TEST_LATENCY * ExecPool._KILLDELAY < timeout < worktime, 'Testcase parameters validation failed'

		tstart = time.perf_counter()
		jterm = Job('j_timeout', args=('sleep', str(worktime)), timeout=timeout)
		self._execpool.execute(jterm)
		jcompl = Job('j_complete', args=('sleep', '0'), timeout=timeout)
		self._execpool.execute(jcompl)
		# Verify successful completion of the execution pool
		self.assertTrue(self._execpool.join())
		etime = time.perf_counter() - tstart  # Execution time
		self.assertFalse(self._execpool._workers)  # Workers should be empty
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
		etimeout = _TEST_LATENCY * 4  # Note: should be larger than 3*latency
		timeout = etimeout * 2  # Job timeout
		worktime = max(1, _TEST_LATENCY) + (timeout * 2) // 1  # Job work time
		assert _TEST_LATENCY * ExecPool._KILLDELAY < timeout < worktime, 'Testcase parameters validation failed'

		tstart = time.perf_counter()
		self._execpool.execute(Job('ep_timeout', args=('sleep', str(worktime)), timeout=timeout))

		runsCount = {'count': 0}
		def updateruns(job):
			"""Onstart callback"""
			job.params['count'] += 1

		jrx = Job('ep_timeout_jrx', args=('sleep', str(worktime)), timeout=etimeout / 2, rsrtonto=True
			, params=runsCount, onstart=updateruns)  # Re-executing job
		self._execpool.execute(jrx)
		# Verify termination of the execution pool
		self.assertFalse(self._execpool.join(etimeout))
		etime = time.perf_counter() - tstart  # Execution time
		self.assertFalse(self._execpool._workers)  # Workers should be empty
		# Verify termination time
		self.assertLess(etime, worktime)
		self.assertLess(etime, timeout)
		self.assertGreaterEqual(etime, etimeout)
		# Verify jrx runsCount and onstart execution
		self.assertGreaterEqual(runsCount['count'], 2)


	@unittest.skipUnless(_CHAINED_CONSTRAINTS and mock is not None, 'Requires _CHAINED_CONSTRAINTS and defined mock')
	def test_jobTimeoutChained(self):
		"""Verify chained termination by timeout:
			1. Termination of the related non-smaller job on termination of the main job
			2. Not affecting smaller related jobs on termination of the main job
			3. Not affecting non-related jobs (with another category)
			4. Execution of the ondone() only on the graceful termination
		"""
		timeout = _TEST_LATENCY * 5  # Note: should be larger than 3*latency
		worktime = max(1, _TEST_LATENCY) + (timeout * 2) // 1  # Job work time
		assert _TEST_LATENCY * ExecPool._KILLDELAY < timeout < worktime, 'Testcase parameters validation failed'

		tstart = time.perf_counter()
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
		etimeout = ExecPool._KILLDELAY * (max(1, _TEST_LATENCY) + worktime * (1 +
			(len(self._execpool._workers) + len(self._execpool._jobs)) // self._execpool._wkslim))
		assert etimeout > worktime, 'Additional testcase parameters are invalid'
		print('jobTimeoutChained() started wth worktime: {}, etimeout: {}'.format(worktime, etimeout))

		# Verify exec pool completion
		self.assertTrue(self._execpool.join(etimeout))
		etime = time.perf_counter() - tstart  # Execution time
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
		2. Termination of the worker process that exceeds limit of the dedicated memory
		3. Termination of the worker process that exceeds limit of the dedicated memory
	 		or had bad_alloc and termination of all related non-smaller jobs
		"""
		# Note: should be larger than 3*latency; 400 ms can be insufficient for the Python3
		worktime = _TEST_LATENCY * 5
		timeout = worktime * 2  # Note: should be larger than 3*latency
		#etimeout = max(1, _TEST_LATENCY) + (worktime * 2) // 1  # Job work time
		# Execution pool timeout; Note: *ExecPool._KILLDELAY because non-started jobs exist here
		etimeout = (max(1, _TEST_LATENCY) + timeout) * ExecPool._KILLDELAY
		assert _TEST_LATENCY * ExecPool._KILLDELAY < worktime < timeout and timeout < etimeout, (
			'Testcase parameters validation failed')

		# Note: we need another execution pool to set memlimit (10 MB) there
		epoolMem = 0.2  # Execution pool mem limit, GB
		msmall = 256  # Small amount of memory for a job, bytes
		# Start not more than 3 simultaneous workers
		with ExecPool(max(_WPROCSMAX, 3), latency=_TEST_LATENCY, memlimit=epoolMem) as xpool:
			tstart = time.perf_counter()

			jmsDb = Job('jmem_small_ba', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, category='cat1', size=9, timeout=timeout)
			jmb = Job('jmem_badalloc', args=(PYEXEC, '-c', allocDelayProg(inBytes(_RAM_SIZE * 2), worktime))
				, category='cat1', size=9, timeout=timeout)

			jmvsize = 5  # Size of the task violating memory constraints
			jmv = Job('jmem_violate', args=(PYEXEC, '-c', allocDelayProg(inBytes(epoolMem * 2), worktime))
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

			time.sleep(worktime / 3)  # Wait for the Job starting and memory allocation
			# Verify exec pool completion before the timeout
			self.assertTrue(xpool.join(etimeout))
			etime = time.perf_counter() - tstart  # Execution time

			# Verify timings
			self.assertLess(etime, etimeout)
			# Note: internal errors in the external processes should not effect related jobs
			self.assertGreaterEqual(jmsDb.tstop - jmsDb.tstart, worktime)
			self.assertTrue(jmb.proc.returncode)  # bad_alloc causes non zero termination code
			# Early termination cased by the bad_alloc (internal error in the external process)
			self.assertLess(jmb.tstop - jmb.tstart, worktime)

			self.assertLess(jmv.tstop - jmv.tstart, worktime)  # Early termination by the memory constraints violation
			# Smaller size of the related chained job to the violated origin should not cause termination
			self.assertGreaterEqual(jmsDvs.tstop - jmsDvs.tstart, worktime)
			# Independent job should have graceful completion
			self.assertGreaterEqual(jms1.tstop - jms1.tstart, worktime)
			self.assertFalse(jms1.proc.returncode)  # Errcode code is 0 on the graceful completion
			if _CHAINED_CONSTRAINTS:
				# Postponed job should be terminated before being started by the chained
				# relation on the memory-violating origin
				self.assertIsNone(jmsDvl1.tstart)
				self.assertIsNone(jmsDvl2.tstart)
			# Early termination by the chained relation to the mem violated origin
			#self.assertLess(jmsDvl1.tstop - jmsDvl1.tstart, worktime)
			# Independent job should have graceful completion
			self.assertGreaterEqual(jms2.tstop - jms2.tstart, worktime)


	@unittest.skipUnless(_LIMIT_WORKERS_RAM and mock is not None, 'Requires _LIMIT_WORKERS_RAM and defined mock')
	def test_jobMemlimGroupSimple(self):
		"""Verify memory violations caused by group of workers but without chained jobs

		Reduction of the number of worker processes when their total memory consumption
		exceeds the dedicated limit and there are
			1) either no any non-started jobs
			2) or the non-started jobs were already rescheduled by the related worker (absence of chained constraints)
		"""
		worktime = _TEST_LATENCY * 10  # Note: should be larger than 3*latency
		timeout = worktime * 2  # Note: should be larger than 3*latency
		#etimeout = max(1, _TEST_LATENCY) + (worktime * 2) // 1  # Job work time
		# Execution pool timeout; Note: * ExecPool._KILLDELAY because non-started jobs exist here and postponed twice
		etimeout = (max(1, _TEST_LATENCY) + timeout) * ExecPool._KILLDELAY
		assert _TEST_LATENCY * ExecPool._KILLDELAY < worktime < timeout and timeout < etimeout, (
			'Testcase parameters validation failed')

		# Note: we need another execution pool to set memlimit (10 MB) there
		epoolMem = 0.15  # Execution pool mem limit, GB
		# Small amount of memory for a job; Note: actual Python app consumes ~51 MB for the allocated ~25 MB
		msmall = inBytes(0.025)
		# Start not more than 3 simultaneous workers
		with ExecPool(max(_WPROCSMAX, 3), latency=_TEST_LATENCY, memlimit=epoolMem) as xpool:
			tstart = time.perf_counter()
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

			time.sleep(worktime / 3)  # Wait for the Job starting and memory allocation
			# Verify exec pool completion before the timeout
			self.assertTrue(xpool.join(etimeout))
			# All jobs should be completed
			etime = time.perf_counter() - tstart  # Execution time

			# Verify timings, graceful completion of all jobs except the last one
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
			# Check the last completed job
			self.assertTrue(jgms3.tstop <= tstart + etime)

			# Verify handlers calls
			jgms1.onstart.assert_called_once_with(jgms1)
			jgms3.onstart.assert_called_once_with(jgms3)
			jgms3.ondone.assert_called_once_with(jgms3)
			jgmsp1.onstart.assert_called_with(jgmsp1)
			self.assertTrue(1 <= jgmsp1.onstart.call_count <= 2)
			jgmsp2.ondone.assert_not_called()


	@unittest.skipUnless(_LIMIT_WORKERS_RAM and _CHAINED_CONSTRAINTS
		, 'Requires _LIMIT_WORKERS_RAM, _CHAINED_CONSTRAINTS')
	def test_jobMemlimGroupChained(self):
		"""Verify memory violations caused by group of workers having chained jobs
		Rescheduling of the worker processes when their total memory consumption
		exceeds the dedicated limit and there are some non-started jobs of smaller
		size and the same category that
			1) were not rescheduled by the non-heavier worker.
			2) were rescheduled by the non-heavier worker.
		"""
		# Note: for one of the tests timeout=worktime/2 is used, so use multiplier of at least *3*2 = 6
		worktime = _TEST_LATENCY * 10  # Note: should be larger than 3*latency
		timeout = worktime * 2  # Note: should be larger than 3*latency
		#etimeout = max(1, _TEST_LATENCY) + (worktime * 2) // 1  # Job work time
		# Execution pool timeout; Note: * ExecPool._KILLDELAY because non-started jobs exist here and postponed twice
		etimeout = (max(1, _TEST_LATENCY) + timeout) * (1 + ExecPool._KILLDELAY)
		assert _TEST_LATENCY * ExecPool._KILLDELAY < worktime/2 and worktime < timeout and (
			timeout < etimeout), 'Testcase parameters validation failed'

		# Note: we need another execution pool to set memlimit (10 MB) there
		epoolMem = 0.15  # Execution pool mem limit, GB
		# Small amount of memory for a job; Note: actual Python app consumes ~51 MB for the allocated ~25 MB
		msmall = inBytes(0.025)
		# Start not more than 3 simultaneous workers
		with ExecPool(max(_WPROCSMAX, 4), latency=_TEST_LATENCY, memlimit=epoolMem) as xpool:
			tstart = time.perf_counter()

			jgms1 = Job('jcgroup_mem_small_1', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, size=5, timeout=timeout)
			tjms2 = worktime/3
			jgms2 = Job('jcgroup_mem_small_2s', args=(PYEXEC, '-c', allocDelayProg(msmall, tjms2))
				, size=5, timeout=timeout, onstart=mock.MagicMock())
			jgms3 = Job('jcgroup_mem_small_3g', args=(PYEXEC, '-c', allocDelayProg(msmall*1.5, worktime))
				, category="cat_sa", size=5, timeout=timeout, onstart=mock.MagicMock(), ondone=mock.MagicMock())
			jgmsp1 = Job('jcgroup_mem_small_postponed_1m', args=(PYEXEC, '-c', allocDelayProg(msmall*1.2, worktime*1.25))
				, category="cat_toch", size=6, timeout=timeout, onstart=mock.MagicMock())
			jgmsp2 = Job('jcgroup_mem_small_postponed_2_to', args=(PYEXEC, '-c', allocDelayProg(msmall*0.8, worktime))
				, category="cat_toch", size=4, timeout=worktime/2, ondone=mock.MagicMock())
			jgmsp3 = Job('jcgroup_mem_small_postponed_3', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, size=9, timeout=worktime, onstart=mock.MagicMock())

			xpool.execute(jgms1)
			xpool.execute(jgms2)
			xpool.execute(jgms3)
			xpool.execute(jgmsp1)
			xpool.execute(jgmsp2)
			xpool.execute(jgmsp3)

			time.sleep(worktime / 4)  # Wait for the Job starting and memory allocation
			# Verify exec pool completion before the timeout
			self.assertTrue(xpool.join(etimeout))
			etime = time.perf_counter() - tstart  # Execution time

			# Verify timings, graceful completion of all jobs except the last one
			self.assertLess(etime, etimeout)
			self.assertGreaterEqual(jgms1.tstop - jgms1.tstart, worktime)
			self.assertFalse(jgms1.proc.returncode)
			self.assertGreaterEqual(jgms2.tstop - jgms2.tstart, tjms2)
			self.assertFalse(jgms2.proc.returncode)
			self.assertGreaterEqual(jgms3.tstop - jgms3.tstart, worktime)
			self.assertFalse(jgms3.proc.returncode)
			if jgmsp1.tstop > jgmsp2.tstop + _TEST_LATENCY:
				# Canceled by chained timeout
				self.assertLessEqual(jgmsp1.tstop - jgmsp1.tstart
					, worktime*1.25 + _TEST_LATENCY * ExecPool._KILLDELAY)
				self.assertTrue(jgmsp1.proc.returncode)
			self.assertLessEqual(jgmsp2.tstop - jgmsp2.tstart, worktime)
			self.assertTrue(jgmsp2.proc.returncode)
			# Execution time a bit exceeds te timeout
			self.assertGreaterEqual(jgmsp3.tstop - jgmsp3.tstart, worktime)
			# Note: jgmsp3 may complete gracefully or may be terminated by timeout
			# depending on the workers revision time.
			# Most likely the completion is graceful
			## Check the last completed job
			# Note: heavier job is rescheduled after the more lightweight one
			#self.assertTrue(jgms3.tstop < jgmsp1.tstop < tstart + etime)

			# Verify handlers calls
			jgms2.onstart.assert_called_with(jgms2)
			jgms3.onstart.assert_called_with(jgms3)
			self.assertTrue(2 <= jgms3.onstart.call_count <= 3)
			jgms3.ondone.assert_called_once_with(jgms3)
			jgmsp1.onstart.assert_called_with(jgmsp1)
			self.assertTrue(1 <= jgmsp1.onstart.call_count <= 2)
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

fnull = open(os.devnull, 'wb')
subprocess.call(args=('time', PYEXEC, '-c', '''{cprog}'''), stderr=fnull)
""".format(mprog=mprog, cprog=cprog)
#subprocess.call(args=('./exectime', PYEXEC, '-c', '''{cprog}'''))


	@unittest.skip('Used to understand what is included into the reported process memory by "psutil"')
	def test_psutilPTMem(self):
		"""Test psutil process tree memory consumption"""
		amem = 0.02  # Direct allocating memory in the process
		camem = 0.07  # Allocating memory in the child process
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
		mem = inGigabytes(up.memory_info().vms) * 1000  # MB; Virtual Memory Size
		rmem = inGigabytes(up.memory_info().rss) * 1000  # MB; Resident Set Size
		umem = inGigabytes(up.memory_full_info().vms) * 1000  # MB; Unique Set Size
		urmem = inGigabytes(up.memory_full_info().rss) * 1000  # MB; Unique Set Size

		acmem = mem
		armem = rmem
		aumem = umem
		aurmem = urmem
		cxmem = 0
		cxrmem = 0
		cxumem = 0
		cxurmem = 0
		cxpid = None
		cnum = 0  # The number of child processes
		for ucp in up.children(recursive=True):
			cnum += 1
			cmem = inGigabytes(ucp.memory_info().vms) * 1000  # MB; Virtual Memory Size
			crmem = inGigabytes(ucp.memory_info().rss) * 1000  # MB; Resident Set Size
			cumem = inGigabytes(ucp.memory_full_info().vms) * 1000  # MB; Unique Set Size
			curmem = inGigabytes(ucp.memory_full_info().rss) * 1000  # MB; Unique Set Size
			print('Memory in MB of "{pname}" #{pid}: (mem: {mem:.2f}, rmem: {rmem:.2f}, umem: {umem:.2f}, urmem: {urmem:.2f})'
				.format(pname=ucp.name(), pid=ucp.pid, mem=cmem, rmem=crmem, umem=cumem, urmem=curmem))
			# Identify consumption by the heaviest child (by absolute mem)
			if cxmem < cmem:
				cxmem = cmem
				cxrmem = crmem
				cxumem = cumem
				cxurmem = curmem
				cxpid = ucp.pid
			acmem += cmem
			armem += crmem
			aumem += cumem
			aurmem += curmem

		amem *= 1000  # MB
		camem *= 1000  # MB
		proc.wait()  # Wait for the process termination

		print('Memory in MB:\n  allocated for the proc #{pid}: {amem}, child: {camem}, total: {tamem}'
			'\n  psutil proc #{pid} (mem: {mem:.2f}, rmem: {rmem:.2f}, umem: {umem:.2f}, urmem: {urmem:.2f})'
			'\n  psutil proc #{pid} tree ({cnum} subprocs) heaviest child #{cxpid}'
			' (mem: {cxmem:.2f}, rmem: {cxrmem:.2f}, umem: {cxumem:.2f}, urmem: {cxurmem:.2f})'
			'\n  psutil proc #{pid} tree (mem: {acmem:.2f}, rmem: {armem:.2f}, umem: {aumem:.2f}, urmem: {aurmem:.2f})'
			''.format(pid=proc.pid, amem=amem, camem=camem, tamem=amem+camem
			, mem=mem, rmem=rmem, umem=umem, urmem=urmem
			, cnum=cnum, cxpid=cxpid, cxmem=cxmem, cxrmem=cxrmem, cxumem=cxumem, cxurmem=cxurmem
			, acmem=acmem, armem=armem, aumem=aumem, aurmem=aurmem))


	@unittest.skipUnless(_LIMIT_WORKERS_RAM and mock is not None, 'Requires _LIMIT_WORKERS_RAM and defined mock')
	def test_jobMem(self):
		"""Test job virual memory evaluation
		"""
		worktime = _TEST_LATENCY * 6  # Note: should be larger than 3*latency
		timeout = worktime * 2  # Note: should be larger than 3*latency
		#etimeout = max(1, _TEST_LATENCY) + (worktime * 2) // 1  # Job work time
		# Execution pool timeout; Note: * ExecPool._KILLDELAY because non-started jobs exist here and postponed twice
		etimeout = (max(1, _TEST_LATENCY) + timeout) * ExecPool._KILLDELAY
		assert _TEST_LATENCY * ExecPool._KILLDELAY < worktime < timeout and timeout < etimeout, (
			'Testcase parameters validation failed')

		# Start not more than 3 simultaneous workers
		with ExecPool(max(_WPROCSMAX, 3), latency=_TEST_LATENCY) as xpool:
			amem = 0.02  # Direct allocating memory in the process
			camem = 0.07  # Allocating memory in the child process
			duration = worktime / 3  # Duration in sec
			job = Job('jmem_proc', args=(PYEXEC, '-c', TestProcMemTree.allocAndSpawnProg(
				allocDelayProg(inBytes(amem), duration), allocDelayProg(inBytes(camem), duration)))
				, timeout=timeout, memkind=0, ondone=mock.MagicMock())
			jobx = Job('jmem_max-subproc', args=(PYEXEC, '-c', TestProcMemTree.allocAndSpawnProg(
				allocDelayProg(inBytes(amem), duration), allocDelayProg(inBytes(camem), duration)))
				, timeout=timeout, memkind=1, ondone=mock.MagicMock())
			jobtr = Job('jmem_tree', args=(PYEXEC, '-c', TestProcMemTree.allocAndSpawnProg(
				allocDelayProg(inBytes(amem), duration), allocDelayProg(inBytes(camem), duration)))
				, timeout=timeout, memkind=2, ondone=mock.MagicMock())

			# Verify that non-started job raises exception on memory update request
			if _LIMIT_WORKERS_RAM:
				self.assertRaises(AttributeError, job._updateMem)
			else:
				self.assertRaises(NameError, job._updateMem)

			tstart = time.perf_counter()
			xpool.execute(job)
			xpool.execute(jobx)
			xpool.execute(jobtr)
			time.sleep(duration*1.9)
			pmem = job._updateMem()
			xmem = jobx._updateMem()
			tmem = jobtr._updateMem()
			# Verify memory consumption
			print('Memory consumption in MB,  proc_mem: {pmem:.3g}, max_procInTree_mem: {xmem:.3g}, procTree_mem: {tmem:.3g}'
				.format(pmem=pmem*1000, xmem=xmem*1000, tmem=tmem*1000))
			self.assertTrue(pmem < xmem < tmem)
			# Verify exec pool completion before the timeout
			time.sleep(worktime / 3)  # Wait for the Job starting and memory allocation
			self.assertTrue(xpool.join(etimeout))
			etime = time.perf_counter() - tstart  # Execution time
			# Verify jobs execution time
			self.assertLessEqual(jobtr.tstop - jobtr.tstart, etime)


class TestTasks(unittest.TestCase):
	"""Process tasks evaluation tests"""
	@unittest.skipUnless(mock is not None, 'Requires defined mock')
	def test_taskDone(self):
		"""Test for the successful task execution"""
		with ExecPool(2, latency=_TEST_LATENCY) as xpool:
			# Prepare jobs task with the post-processing
			worktime = max(0.5, _TEST_LATENCY * ExecPool._KILLDELAY)
			task = Task('TimeoutTask', onstart=mock.MagicMock(), ondone=mock.MagicMock(), params=0)
			job1 = Job('j1', args=('sleep', str(worktime)), task=task, onstart=mock.MagicMock(), ondone=mock.MagicMock())
			job2 = Job('j2', args=('sleep', str(worktime)), task=task, ondone=mock.MagicMock(), timeout=worktime*2)

			xpool.execute(job1)
			xpool.execute(job2)
			xpool.join(worktime*2)

			job1.onstart.assert_called_once_with(job1)
			job1.ondone.assert_called_once_with(job1)
			job2.ondone.assert_called_once_with(job2)
			task.onstart.assert_called_once_with(task)
			task.ondone.assert_called_once_with(task)


	@unittest.skipUnless(mock is not None, 'Requires defined mock')
	def test_taskTimeout(self):
		"""Test for the task of jobs termination by timeout"""
		with ExecPool(2, latency=_TEST_LATENCY) as xpool:
			# Prepare jobs task with the post-processing
			worktime = max(0.5, _TEST_LATENCY * ExecPool._KILLDELAY)
			task = Task('TimeoutTask', onstart=mock.MagicMock(), ondone=mock.MagicMock(), onfinish=mock.MagicMock())
			timelong = (worktime + _TEST_LATENCY) * ExecPool._KILLDELAY  # Note: should be larger than 3*latency
			job1 = Job('j1', args=('sleep', str(worktime)), task=task, onstart=mock.MagicMock(), ondone=mock.MagicMock())
			job2 = Job('j2', args=('sleep', str(timelong)), task=task, ondone=mock.MagicMock(), timeout=worktime)

			xpool.execute(job1)
			xpool.execute(job2)
			xpool.join(worktime*2)

			job1.onstart.assert_called_once_with(job1)
			job1.ondone.assert_called_once_with(job1)
			job2.ondone.assert_not_called()
			task.onstart.assert_called_once_with(task)
			task.ondone.assert_not_called()
			task.onfinish.assert_called_once_with(task)
			self.assertEqual(task.numadded, 2)
			self.assertEqual(task.numdone, 1)
			self.assertEqual(task.numterm, 1)


	@unittest.skipUnless(mock is not None, 'Requires defined mock')
	def test_subtasksTimeout(self):
		"""Test for the termination of the hierarchy of tasks of jobs by the timeout"""
		# Note: it's OK if this test fails in case the computer has less than 3 CPU cores
		with ExecPool(3, latency=_TEST_LATENCY) as xpool:
			# Prepare jobs task with the post-processing
			worktime = max(0.5, _TEST_LATENCY * ExecPool._KILLDELAY)
			# Note: should be larger than ExecPool._KILLDELAY*latency
			timelong = (worktime + _TEST_LATENCY) * ExecPool._KILLDELAY
			t1 = Task('Task1', onstart=mock.MagicMock(), ondone=mock.MagicMock(), onfinish=mock.MagicMock())
			j1 = Job('j1', args=('sleep', str(worktime)), task=t1
				, onstart=mock.MagicMock(), ondone=mock.MagicMock())
			st1 = Task('Subtask1', onstart=mock.MagicMock(), task=t1
				, ondone=mock.MagicMock(), onfinish=mock.MagicMock())
			st1j1 = Job('st1j1', args=('sleep', str(timelong)), task=st1
				, onstart=mock.MagicMock(), ondone=mock.MagicMock(), timeout=worktime)
			st1j2 = Job('st1j2', args=('sleep', str(worktime)), task=st1, ondone=mock.MagicMock())

			xpool.execute(j1)
			xpool.execute(st1j1)
			xpool.execute(st1j2)
			xpool.join(timelong * 1.1)

			j1.onstart.assert_called_once_with(j1)
			st1j1.onstart.assert_called_once_with(st1j1)
			st1.onstart.assert_called_once_with(st1)
			t1.onstart.assert_called_once_with(t1)

			st1j2.ondone.assert_called_once_with(st1j2)
			st1j1.ondone.assert_not_called()
			st1.ondone.assert_not_called()
			st1.onfinish.assert_called_once_with(st1)
			self.assertEqual(st1.numadded, 2)
			self.assertEqual(st1.numdone, 1)
			self.assertEqual(st1.numterm, 1)
			t1.ondone.assert_not_called()
			t1.onfinish.assert_called_once_with(t1)
			self.assertEqual(t1.numadded, 2)
			self.assertEqual(t1.numdone, 1)
			self.assertEqual(t1.numterm, 1)


	@unittest.skipUnless(mock is not None, 'Requires defined mock')
	def test_subtasksTimeoutExt(self):
		"""Test for the termination of the extended hierarchy of tasks of jobs by the timeout"""
		# Note: it's OK if this test fails in case the computer has less than 3 CPU cores
		with ExecPool(3, latency=_TEST_LATENCY) as xpool:
			# Prepare jobs task with the post-processing
			worktime = max(0.5, _TEST_LATENCY * ExecPool._KILLDELAY)
			# Note: should be larger than ExecPool._KILLDELAY*latency
			timelong = worktime + _TEST_LATENCY * ExecPool._KILLDELAY
			t1 = Task('Task1', onstart=mock.MagicMock(), ondone=mock.MagicMock(), onfinish=mock.MagicMock())
			j1 = Job('j1', args=('sleep', str(timelong)), task=t1, timeout=worktime
				, onstart=mock.MagicMock(), ondone=mock.MagicMock())
			st1 = Task('Subtask1', onstart=mock.MagicMock(), task=t1
				, ondone=mock.MagicMock(), onfinish=mock.MagicMock())
			st1j1 = Job('st1j1', args=('sleep', str(timelong)), task=st1
				, onstart=mock.MagicMock(), ondone=mock.MagicMock(), timeout=worktime)
			st1j2 = Job('st1j2', args=('sleep', str(2*timelong)), task=st1, ondone=mock.MagicMock())

			xpool.execute(j1)
			xpool.execute(st1j1)
			xpool.execute(st1j2)
			xpool.join(timelong * 1.1)

			j1.onstart.assert_called_once_with(j1)
			st1j1.onstart.assert_called_once_with(st1j1)
			st1.onstart.assert_called_once_with(st1)
			t1.onstart.assert_called_once_with(t1)

			st1j2.ondone.assert_not_called()
			st1j1.ondone.assert_not_called()
			st1.ondone.assert_not_called()
			st1.onfinish.assert_called_once_with(st1)
			self.assertEqual(st1.numadded, 2)
			self.assertEqual(st1.numdone, 0)
			self.assertEqual(st1.numterm, 2)
			t1.ondone.assert_not_called()
			t1.onfinish.assert_called_once_with(t1)
			self.assertEqual(t1.numadded, 2)
			self.assertEqual(t1.numdone, 0)
			self.assertEqual(t1.numterm, 2)


################################################################################
# WebUI related tests
# import threading  # To request URL in parallel with execpool joining
import os
from mpewui import WebUiApp, UiCmdId, UiResOpt, UiResFmt  #pylint: disable=C0413;  UiResCol
try:
	# from urllib.parse import urlparse, urlencode
	from urllib.request import urlopen #, Request
	from urllib.error import HTTPError
except ImportError:
	# from urlparse import urlparse
	# from urllib import urlencode
	from urllib2 import urlopen, HTTPError  # , Request

_webuiapp = None  # Global WebUI application

# @unittest.skip('Under development, currently used manually')
@unittest.skipUnless(os.getenv('MANUAL'),
	'Only the manual WebUI testing is supported now, MANUAL evn var should be set')
class TestWebUI(unittest.TestCase):
	"""WebUI tests"""

	@classmethod
	def setUpClass(cls):
		cls.host = 'localhost'
		cls.port = 8080
		global _webuiapp  #pylint: disable=W0603
		if _webuiapp is None:
			_webuiapp = WebUiApp(host=cls.host, port=cls.port, name='MpeWebUI', daemon=True)
		cls._execpool = ExecPool(2, latency=_TEST_LATENCY, name='MpePoolWUI', webuiapp=_webuiapp)


	@classmethod
	def tearDownClass(cls):
		if cls._execpool:
			del cls._execpool  # Destructors are called later
			# Define _execpool to avoid unnecessary trash in the error log, which might
			# be caused by the attempt of subsequent deletion on destruction
			cls._execpool = None  # Note: otherwise _execpool becomes undefined


	def setUp(self):
		assert not self._execpool._workers, 'Worker processes should be empty for each new test case'
		self._execpool.alive = True  # Set alive flag after the termination


	def tearDown(self):
		assert not self._execpool._workers and not self._execpool._jobs, (
			'All jobs should be completed in the end of each testcase (workers: "{}"; jobs: "{}")'
			.format(', '.join([job.name for job in self._execpool._workers])
			, ', '.join([job.name for job in self._execpool._jobs])))


	# @unittest.skipUnless(mock is not None, 'Requires defined mock')
	def test_failures(self):
		"""Test WebUI listing of the ExecPool failures"""
		timeout = 100  # Note: should be larger than 3*latency
		# worktime = max(1, _TEST_LATENCY) + (timeout * 2) // 1  # Job work time
		worktime = 0.75 * timeout  # Job work time
		assert _TEST_LATENCY * ExecPool._KILLDELAY < timeout and _TEST_LATENCY * ExecPool._KILLDELAY < worktime, (
			'Testcase parameters validation failed')

		t1 = Task('Task1')  # onstart=mock.MagicMock(), ondone=mock.MagicMock(), onfinish=mock.MagicMock()
		st1 = Task('SubTask1.1', task=t1)  # onstart=mock.MagicMock(), ondone=mock.MagicMock(), onfinish=mock.MagicMock()
		j1 = Job('j1f', args=('sleep', str(worktime)), task=t1, timeout=_TEST_LATENCY)  # , onstart=mock.MagicMock()
		j2 = Job('j2', args=('sleep', str(timeout/10.)), task=st1, timeout=timeout)  # , onstart=mock.MagicMock()
		j3 = Job('j3', args=('sleep', str(worktime)), timeout=timeout)  # , onstart=mock.MagicMock()
		j4 = Job('j4', args=('sleep', str(timeout/5.)), task=t1, timeout=timeout)  # , onstart=mock.MagicMock()
		j5 = Job('j5', args=('sleep', str(worktime)), timeout=timeout)  # , onstart=mock.MagicMock()
		j6 = Job('j6f', args=('sleep', str(worktime)), timeout=_TEST_LATENCY)  # , onstart=mock.MagicMock()
		j7 = Job('j7f', args=('sleep', str(worktime)), task=st1, timeout=worktime/2)  # , onstart=mock.MagicMock()
		j8 = Job('j8f', args=('sleep', str(worktime)), task=st1, timeout=_TEST_LATENCY)  # , onstart=mock.MagicMock()
		# j7 = Job('j7f', args=('sleep', str(worktime)), task=st1, timeout=_TEST_LATENCY)  # , onstart=mock.MagicMock()
		time.sleep(_TEST_LATENCY)
		self._execpool.execute(j1)
		self._execpool.execute(j2)
		self._execpool.execute(j6)
		self._execpool.execute(j7)
		self._execpool.execute(j8)
		# self._execpool.execute(j7)
		self._execpool.execute(j3)
		time.sleep(_TEST_LATENCY)
		self._execpool.execute(j4)
		self._execpool.execute(j5)

		# Fetch data in the concurrent thread
		# global _webuiapp
		# _webuiapp.cmd.id = UiCmdId.FAILURES
		# http://localhost:8080/?fmt=json&fltStatus=work,defer
		# url = ''.join(('http://', self.host, str(self.port), '/?'
		# 	, UiResOpt.fmt.name	,'=', UiResFmt.json.name
		# 	, '&', UiResOpt.fltStatus.name, '=', UiResKind.work.name, ',', UiResKind.defer.name))
		# json = urlopen(url).read().decode('utf-8')
		# print(json)

		# Verify successful completion of the execution pool
		self.assertTrue(self._execpool.join(timeout*1.1))



if __name__ == '__main__':
	if unittest.main().result:  # verbosity=2
		print('Try to re-execute the tests (hot run) or set x2-3 larger TEST_LATENCY')
