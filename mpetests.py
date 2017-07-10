#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
\descr:  Unit tests for the Multi-Process Execution Pool

\author: (c) Artem Lutov <artem@exascale.info>
\organizations: eXascale Infolab <http://exascale.info/>, Lumais <http://www.lumais.com/>, ScienceWise <http://sciencewise.info/>
\date: 2017-06 v2
"""

from __future__ import print_function, division  # Required for stderr output, must be the first import
from sys import executable as PYEXEC  # Full path to the current Python interpreter
try:
	from unittest import mock
except ImportError:
	try:
		# Note: mock is not available under default Python2 / pypy installation
		import mock
	except ImportError:
		mock = None  # Skip the unittests if the mock module is not installed
from mpepool import AffinityMask, ExecPool, Job, _CHAINED_CONSTRAINTS, _LIMIT_WORKERS_RAM, _RAM_SIZE, inBytes, inGigabytes
import sys, time, subprocess, unittest
try:
	import psutil
except ImportError as err:
	print('ERROR, psutil import failed: ', err, file=sys.stderr)


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


_WPROCSMAX = max(AffinityMask.CPUS-1, 1)  # Maximal number of the worker processes, should be >= 1
# Note: 0.1 is specified just for the faster tests execution on the non-first run,
# generally at least 0.2 should be used
_TEST_LATENCY = 0.1  # Approximate minimal latency of ExecPool in seconds

class TestExecPool(unittest.TestCase):
	#global _DEBUG_TRACE
	#_DEBUG_TRACE = True

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
		cls._execpool = ExecPool(_WPROCSMAX, latency=_TEST_LATENCY)


	@classmethod
	def tearDownClass(cls):
		if cls._execpool:
			del cls._execpool  # Destructors are caled later
			# Define _execpool to avoid unnessary trash in the error log, which might
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
		assert _TEST_LATENCY * 3 < timeout < worktime, 'Testcase parameters validation failed'

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
		etimeout = _TEST_LATENCY * 4  # Note: should be larger than 3*latency
		timeout = etimeout * 2  # Job timeout
		worktime = max(1, _TEST_LATENCY) + (timeout * 2) // 1  # Job work time
		assert _TEST_LATENCY * 3 < timeout < worktime, 'Testcase parameters validation failed'

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
		timeout = _TEST_LATENCY * 5  # Note: should be larger than 3*latency
		worktime = max(1, _TEST_LATENCY) + (timeout * 2) // 1  # Job work time
		assert _TEST_LATENCY * 3 < timeout < worktime, 'Testcase parameters validation failed'

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
		etimeout = max(1, _TEST_LATENCY) + worktime * 3 * (1 +
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
		2. Termination of the worker process that exceeds limit of the dedicated memory
		3. Termination of the worker process that exceeds limit of the dedicated memory
	 		or had bad_alloc and termination of all related non-smaller jobs
		"""
		worktime = _TEST_LATENCY * 5  # Note: should be larger than 3*latency; 400 ms can be insufficient for the Python 3
		timeout = worktime * 2  # Note: should be larger than 3*latency
		#etimeout = max(1, _TEST_LATENCY) + (worktime * 2) // 1  # Job work time
		etimeout = (max(1, _TEST_LATENCY) + timeout) * 3  # Execution pool timeout; Note: *3 because nonstarted jobs exist here
		assert _TEST_LATENCY * 3 < worktime < timeout and timeout < etimeout, 'Testcase parameters validation failed'

		# Note: we need another execution pool to set memlimit (10 Mb) there
		epoolMem = 0.2  # Execution pool mem limit, Gb
		msmall = 256  # Small amount of memory for a job, bytes
		# Start not more than 3 simultaneous workers
		with ExecPool(max(_WPROCSMAX, 3), latency=_TEST_LATENCY, memlimit=epoolMem) as xpool:
			tstart = time.time()

			jmsDb = Job('jmem_small_ba', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, category='cat1', size=9, timeout=timeout)
			jmb = Job('jmem_badalloc', args=(PYEXEC, '-c', allocDelayProg(inBytes(_RAM_SIZE * 2), worktime))
				, category='cat1', size=9, timeout=timeout)

			jmvsize = 5  # Size of the task violating memory contraints
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
		worktime = _TEST_LATENCY * 6  # Note: should be larger than 3*latency
		timeout = worktime * 2  # Note: should be larger than 3*latency
		#etimeout = max(1, _TEST_LATENCY) + (worktime * 2) // 1  # Job work time
		etimeout = (max(1, _TEST_LATENCY) + timeout) * 3  # Execution pool timeout; Note: *3 because nonstarted jobs exist here nad postponed twice
		assert _TEST_LATENCY * 3 < worktime < timeout and timeout < etimeout, 'Testcase parameters validation failed'

		# Note: we need another execution pool to set memlimit (10 Mb) there
		epoolMem = 0.15  # Execution pool mem limit, Gb
		msmall = inBytes(0.025)  # Small amount of memory for a job; Note: actual Python app consumes ~51 Mb for the allocated ~25 Mb
		# Start not more than 3 simultaneous workers
		with ExecPool(max(_WPROCSMAX, 3), latency=_TEST_LATENCY, memlimit=epoolMem) as xpool:
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

			time.sleep(worktime / 3)  # Wait for the Job starting and memory allocation
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
			jgms3.onstart.assert_called_once_with(jgms3)
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
		# Note: for one of the tests timeout=worktime/2 is used, so use multiplier of at least *3*2 = 6
		worktime = _TEST_LATENCY * 8  # Note: should be larger than 3*latency
		timeout = worktime * 2  # Note: should be larger than 3*latency
		#etimeout = max(1, _TEST_LATENCY) + (worktime * 2) // 1  # Job work time
		etimeout = (max(1, _TEST_LATENCY) + timeout) * 4  # Execution pool timeout; Note: *3 because nonstarted jobs exist here nad postponed twice
		assert _TEST_LATENCY * 3 < worktime/2 and worktime < timeout and timeout < etimeout, 'Testcase parameters validation failed'

		# Note: we need another execution pool to set memlimit (10 Mb) there
		epoolMem = 0.15  # Execution pool mem limit, Gb
		msmall = inBytes(0.025)  # Small amount of memory for a job; Note: actual Python app consumes ~51 Mb for the allocated ~25 Mb
		# Start not more than 3 simultaneous workers
		with ExecPool(max(_WPROCSMAX, 4), latency=_TEST_LATENCY, memlimit=epoolMem) as xpool:
			tstart = time.time()

			jgms1 = Job('jgroup_mem_small_1', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
				, size=5, timeout=timeout)
			tjms2 = worktime/3
			jgms2 = Job('jgroup_mem_small_2s', args=(PYEXEC, '-c', allocDelayProg(msmall, tjms2))
				, size=5, timeout=timeout, onstart=mock.MagicMock())
			jgms3 = Job('jgroup_mem_small_3g', args=(PYEXEC, '-c', allocDelayProg(msmall*1.5, worktime))
				, category="cat_sa", size=5, timeout=timeout, onstart=mock.MagicMock(), ondone=mock.MagicMock())
			jgmsp1 = Job('jgroup_mem_small_postponed_1m', args=(PYEXEC, '-c', allocDelayProg(msmall*1.2, worktime*1.25))
				, category="cat_toch", size=6, timeout=timeout, onstart=mock.MagicMock())
			jgmsp2 = Job('jgroup_mem_small_postponed_2_to', args=(PYEXEC, '-c', allocDelayProg(msmall*0.8, worktime))
				, category="cat_toch", size=4, timeout=worktime/2, ondone=mock.MagicMock())
			jgmsp3 = Job('jgroup_mem_small_postponed_3', args=(PYEXEC, '-c', allocDelayProg(msmall, worktime))
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
			etime = time.time() - tstart  # Execution time

			# Verify timings, gracefull copletion of all jobs except the last one
			self.assertLess(etime, etimeout)
			self.assertGreaterEqual(jgms1.tstop - jgms1.tstart, worktime)
			self.assertFalse(jgms1.proc.returncode)
			self.assertGreaterEqual(jgms2.tstop - jgms2.tstart, tjms2)
			self.assertFalse(jgms2.proc.returncode)
			self.assertGreaterEqual(jgms3.tstop - jgms3.tstart, worktime)
			self.assertFalse(jgms3.proc.returncode)
			if jgmsp1.tstop > jgmsp2.tstop + _TEST_LATENCY:
				self.assertLessEqual(jgmsp1.tstop - jgmsp1.tstart, worktime*1.25 + _TEST_LATENCY * 3)  # Canceled by chained timeout
				self.assertTrue(jgmsp1.proc.returncode)
			self.assertLessEqual(jgmsp2.tstop - jgmsp2.tstart, worktime)
			self.assertTrue(jgmsp2.proc.returncode)
			self.assertGreaterEqual(jgmsp3.tstop - jgmsp3.tstart, worktime)  # Execution time a bit exceeds te timeout
			# Note: jgmsp3 may complete gracefully or may be terminated by timeout depending on the wrkers revision time.
			# Most likely the completion is graceful
			## Check the last comleted job
			#self.assertTrue(jgms3.tstop < jgmsp1.tstop < tstart + etime)  # Note: heavier job is rescheduled after the more lightweight one

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
		mem = inGigabytes(up.memory_info().vms) * 1000  # Mb; Virtual Memory Size
		rmem = inGigabytes(up.memory_info().rss) * 1000  # Mb; Resident Set Size
		umem = inGigabytes(up.memory_full_info().vms) * 1000  # Mb; Unique Set Size
		urmem = inGigabytes(up.memory_full_info().rss) * 1000  # Mb; Unique Set Size

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
			cmem = inGigabytes(ucp.memory_info().vms) * 1000  # Mb; Virtual Memory Size
			crmem = inGigabytes(ucp.memory_info().rss) * 1000  # Mb; Resident Set Size
			cumem = inGigabytes(ucp.memory_full_info().vms) * 1000  # Mb; Unique Set Size
			curmem = inGigabytes(ucp.memory_full_info().rss) * 1000  # Mb; Unique Set Size
			print('Memory in Mb of "{pname}" #{pid}: (mem: {mem:.2f}, rmem: {rmem:.2f}, umem: {umem:.2f}, urmem: {urmem:.2f})'
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

		amem *= 1000  # Mb
		camem *= 1000  # Mb
		proc.wait()  # Wait for the process termination

		print('Memory in Mb:\n  allocated for the proc #{pid}: {amem}, child: {camem}, total: {tamem}'
			'\n  psutil proc #{pid} (mem: {mem:.2f}, rmem: {rmem:.2f}, umem: {umem:.2f}, urmem: {urmem:.2f})'
			'\n  psutil proc #{pid} tree ({cnum} subprocs) heaviest child #{cxpid}'
			' (mem: {cxmem:.2f}, rmem: {cxrmem:.2f}, umem: {cxumem:.2f}, urmem: {cxurmem:.2f})'
			'\n  psutil proc #{pid} tree (mem: {acmem:.2f}, rmem: {armem:.2f}, umem: {aumem:.2f}, urmem: {aurmem:.2f})'
			''.format(pid=proc.pid, amem=amem, camem=camem, tamem=amem+camem
			, mem=mem, rmem=rmem, umem=umem, urmem=urmem
			, cnum=cnum, cxpid=cxpid, cxmem=cxmem, cxrmem=cxrmem, cxumem=cxumem, cxurmem=cxurmem
			, acmem=acmem, armem=armem, aumem=aumem, aurmem=aurmem))


	@unittest.skipUnless(_LIMIT_WORKERS_RAM, 'Requires _LIMIT_WORKERS_RAM')
	def test_jobMem(self):
		"""Test job virual memory evaluation
		"""
		worktime = _TEST_LATENCY * 6  # Note: should be larger than 3*latency
		timeout = worktime * 2  # Note: should be larger than 3*latency
		#etimeout = max(1, _TEST_LATENCY) + (worktime * 2) // 1  # Job work time
		etimeout = (max(1, _TEST_LATENCY) + timeout) * 3  # Execution pool timeout; Note: *3 because nonstarted jobs exist here nad postponed twice
		assert _TEST_LATENCY * 3 < worktime < timeout and timeout < etimeout, 'Testcase parameters validation failed'

		# Start not more than 3 simultaneous workers
		with ExecPool(max(_WPROCSMAX, 3), latency=_TEST_LATENCY) as xpool:
			amem = 0.02  # Direct allocating memory in the process
			camem = 0.07  # Allocatinf memory in the child process
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

			tstart = time.time()
			xpool.execute(job)
			xpool.execute(jobx)
			xpool.execute(jobtr)
			time.sleep(duration*1.9)
			pmem = job._updateMem()
			xmem = jobx._updateMem()
			tmem = jobtr._updateMem()
			# Verify memory consumption
			print('Memory consumption in Mb,  proc_mem: {pmem:.3g}, max_procInTree_mem: {xmem:.3g}, procTree_mem: {tmem:.3g}'
				.format(pmem=pmem*1000, xmem=xmem*1000, tmem=tmem*1000))
			self.assertTrue(pmem < xmem < tmem)
			# Verify exec pool completion before the timeout
			time.sleep(worktime / 3)  # Wait for the Job starting and memory allocation
			self.assertTrue(xpool.join(etimeout))
			etime = time.time() - tstart  # Execution time
			# Verify jobs execution time
			self.assertLessEqual(jobtr.tstop - jobtr.tstart, etime)


if __name__ == '__main__':
	if mock is not None:
		if unittest.main().result:  # verbosity=2
			print('Try to reexecute the tests (hot run) or set x2-3 larger TEST_LATENCY')
	else:
		print('WARNING, the unit tests are skipped because the mock module is not installed', file=sys.stderr)
