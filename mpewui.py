#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
:Description:  Minimalistic Web User Interface for the Multiprocess Execution Pool.
    The functionality includes only fetching of the specified attributes of the
    scheduled and executing Jobs in the specified format (HTML table / JSON / TXT)
    for the profiling and debugging.

:Authors: (c) Artem Lutov <artem@exascale.info>
:Organizations: eXascale Infolab <http://exascale.info/>, Lumais <http://www.lumais.com/>
:Date: 2018-04
"""
from __future__ import print_function, division  # Required for stderr output, must be the first import
# External API (exporting functions)
__all__ = ['WebUiApp', 'UiCmdId', 'UiResOpt', 'UiResFmt', 'UiResCol']

# import signal  # Required for the correct handling of KeyboardInterrupt: https://docs.python.org/2/library/thread.html
import threading  # Run bottle in the dedicated thread
from functools import partial  # Custom parameterized routes
from collections import namedtuple  # UiResFilterVal
# Internal format serialization to JSON/HTM/TXT
import re
import json
try:
	from html import escape
except ImportError:
	from cgi import escape  # Consider both both Python2/3
import bottle  # Web service
# from bottle import run as websrvrun

# Constants Definitions
try:
	from enum import IntEnum
except ImportError:
	# As a falback make a manual wrapper with the essential functionality
	def IntEnum(clsname, names, module=__name__, start=1):  #pylint: disable=W0613
		"""IntEnum-like class builder

		Args:
			clsname (str): class name
			names (str|list|dict): enum members, optionally enumerated
			module (str): class module
			start (int, optional): stating value for the default enumeration

		Returns:
			Clsname: IntEnum-like class Clsname

		>>> IntEnum('UiResFmt', 'json htm txt').htm.name
		'htm'

		>>> IntEnum('UiResFmt', 'json htm txt').txt.name
		'txt'
		"""

		def valobj(clsname, name, val):
			"""Build object with the specified class name, name and value attributes

			Args:
				clsname (str): name of the embracing class
				name (str): object name
				val: the value to be assigned

			Returns:
				required object of the type '<clsname>.<name>'
			"""
			# assert isinstance(clsname, str) and isinstance(name, str)
			return type('.'.join((clsname, name)), (object,), {'name': name, 'value': val})()

		dnames = {}  # Constructing members of the enum-like object
		if names:
			if isinstance(names, str):
				names = [name.rstrip(',') for name in names.split()]
			if not isinstance(names, dict) and (not isinstance(names, list) or isinstance(names[0], str)):
				# {'name': name, 'value': i}
				dnames = {name: valobj(clsname, name, i) for i, name in enumerate(names, start)}
			else:
				dnames = {name: valobj(clsname, name, i) for i, name in dict(names).items()}
		cls = type(clsname, (object,), dnames)
		# Consider some specific attributes of the enum
		cls.__members__ = dnames
		return cls


RE_INT = re.compile(r'[-+]?\d+$')  # Match int
RE_FLOAT = re.compile(r'[-+]?\d+(\.\d*)?([eE][-+]\d+)?$')  # Match float


def inferNumType(val):
	"""Infer a numeric type if possible or retain the variable as it is

	Args:
		val: str  - the value to be converted  to the numeric type

	Returns:
		int|float|<type=str>  - origin value with possible converted type
	"""
	if RE_FLOAT.match(val):
		if RE_INT.match(val):
			return int(val)
		else:
			return float(val)
	return val


UiCmdId = IntEnum('UiCmdId', 'FAILURES LIST_JOBS LIST_TASKS API_MANUAL')  # JOB_INFO, TASK_INFO
"""UI Command Identifier associated with the REST URL"""

UiResOpt = IntEnum('UiResOpt', 'fmt cols flt jlim refresh')  # fltStatus / kind
# Note: filter keys ending with '*' are optional (such columns allowed
# to be absent in the target item)
"""UI Command parameters
NOTE: None UI command parameters means that any non-None value of the respective property should be present

fmt: str(UiResFmt)  - required format of the result
cols: str(UiResCol)  - required columns in the result, all by default
flt: str(UiResFilterVal)  - filter target items per each column, notations:
* - optional column, absense of the value(:xxx) - the property should present with any non-None value:
	?flt=rcode*:-15|duration:1.5..3600|category*|...
jlim: uint  - limit of the listed number of the active jobs / tasks having this number of jobs, 0 means any;
	NOTE: jlim omission results in the ExecPool default value for the jlim, failures are always fully shown.
refresh: uint  - page refresh time, seconds >= 2
"""
# TODO:
# - add inverse notation the the item filter: ! - invert condition
# - add human-readable Duration type with inferType support: NdNhNmF, where N: uint, F: ufloat, for example:
# 	duration:5m1.15..2d3h
#	?flt=rcode*!:-15|...
# flt=rcode*:-15&flt=duration:5m1.15..2d3h&...
# kind: str(UiResKind)  - kind of the showing items

UiResFmt = IntEnum('UiResFmt', 'json htm txt')
"""UI Command: `fmt` parameter values"""

# UiResCol = IntEnum('UiResCol', 'pid state tstart tstop duration memory task category')
UiResCol = IntEnum('UiResCol',
	'category rcode duration memkind memsize name numadded numdone numterm pid task tstart tstop')
"""UI Command: `cols` parameter values for Jobs and Tasks listing"""

UiResFilterVal = namedtuple('UiResFilterVal', 'beg end opt')
"""Property value constraint of the item filter for the ObjInfo (Task/Job Info)

beg  - begin of the range bound
end  - end of the range bound
opt: bool  - target property of the filter is optional, i.e. omit the filter if the property is absent or None
"""

# Note: jobs by status can be filtered in more generalized way using the UiResFilterVal
# # # Note: 'exec' is a keyword in Python 2 and can't be used as an object attribute
# # work - working (executing) jobs (workers), defer - deferred (regular, postponed) jobs
# UiResKind = IntEnum('UiResKind', 'work defer')  #  task
# """UI Command: Result filtration by the job status: executed (worked job), deferred (scheduled obj)"""

class ResultOptions(object):
	"""Result options extractor from the URL GET query"""

	def __init__(self, qdict):
		"""Initialization of the result options

		Args:
			qdict: FormsDict(MultiDict)  - user query parsed to the multi dictionary

		Raises:
			KeyError  - parameter value does not correspond to the expected one
			ValueError  - parameter value has unexpected format
		"""
		self.fmt = UiResFmt.htm
		# Fetch the required result format
		# fmtKey = UiResOpt.fmt.name
		# return "query dict: {}, dict.keys: {}, dict[fmt]: {}".format(qdict.items(), qdict.keys(), qdict[fmtKey])
		# if fmtKey in qdict:  #pylint: disable=E1135
		# 	fmt = UiResFmt[qdict[fmtKey]]  #pylint: disable=E1136
		if UiResOpt.fmt.name in qdict:  #pylint: disable=E1135
			self.fmt = UiResFmt[qdict[UiResOpt.fmt.name]]  #pylint: disable=E1136
			# try:
			# 	fmt = UiResFmt[qdict[fmtKey]]  #pylint: disable=E1136
			# 	# del qdict[fmtKey]  #pylint: disable=E1138
			# except KeyError:
			# 	# 400  - Bad Request
			# 	# 415  - Unsupported Media Type
			# 	bottle.response.status = 415  # 400
			# 	return 'Invalid URL parameter value of "{}": {}'.format(fmtKey, qdict[fmtKey])  #pylint: disable=E1136
			# 	# raise HTTPResponse(body='Invalid URL parameter value of "fmt": ' + qdict['fmt'], status=400)
		# Fetch the required resulting columns (property filter)
		self.cols = qdict.get(UiResOpt.cols.name)  #pylint: disable=E1101
		# Fetch the required results filtering options (object filter)
		self.flt = qdict.get(UiResOpt.flt.name)  #pylint: disable=E1101
		if self.flt:
			# Parse Filter parameters to UiResFilterVal
			self.fltopts = {}
			for prm in self.flt.split('|'):
				pkv = prm.split(':')
				k = pkv[0]
				if len(pkv) > 1:
					v = pkv[1]
					# Consider range values and infer the numeric type if possible
					if v.find('..') != -1:
						v, ve = v.split('..', 1)
						ve = inferNumType(ve)
					else:
						ve = None
					v = inferNumType(v)
					# Consider optional filtering fields
					val = UiResFilterVal(beg=v, end=ve, opt=k[-1]=='*')  #pylint: disable=C0326
					if val.opt:
						k = k[:-1]
					self.fltopts[k] = val
				else:
					# Filter meaning: the option should present with any (non-None) value
					self.fltopts[k] = None
		# # Fetch the kind of items to be shown
		# self.kind = qdict.get(UiResOpt.kind.name)  #pylint: disable=E1101
		# if self.kind:
		# 	self.kind = self.kind.split(',')
		# 	# Validate kind values
		# 	for kind in self.kind:  #pylint: disable=W0104
		# 		UiResKind[kind]  # Note: KeyError is thrown on invalid value
		# Fetch the jobs limit value if any
		self.jlim = qdict.get(UiResOpt.jlim.name)  #pylint: disable=E1101
		# Fetch the refresh time if any
		self.refresh = qdict.get(UiResOpt.refresh.name)
		if self.refresh:
			self.refresh = int(self.refresh)  # Note: refresh content value is int



class UiCmd(object):
	"""UI Command (for the MVC controller)"""
	def __init__(self, cid, data={}):
		"""UI command

		Args:
			cid: UiCmdId  - Command identifier
			data: dict  - request (parameters) to the ExecPool on it's call and
				resulting (response) data (or empty dict) afterwards

		Internal attributes:
			cond: Condition  - synchronizing condition
		"""
		# params (dict)  - Command parameters, params={};   and isinstance(params, dict)
		#dshape (set(str), optional): Defaults to None. Expected data shape (columns of the returning table)
		# self.lock = threading.Lock()
		self.cond = threading.Condition()  # (self.lock)
		assert (cid is None or isinstance(cid, UiCmdId)) and isinstance(data, dict), (
			'Unexpected type of arguments, id: {}, data: {}'.format(type(id).__name__, type(data).__name__))
		self.id = cid
		# self.dshape = dshape
		# self.params = params
		self.data = data


class SummaryBrief(object):
	"""Brief summary of the ExecPool state"""

	__slots__ = ('workers', 'jobs', 'jobsDone', 'jobsFailed', 'tasks', 'tasksFailed', 'tasksRoot', 'tasksRootFailed')

	def __init__(self, workers=0, jobs=0, jobsDone=0, jobsFailed=0, tasksFailed=0
	, tasks=0, tasksRootFailed=0, tasksRoot=0):
		"""Brief summary initialization

		workers: int  - the number of workers (executors, executing jobs)
		jobs: int  - the number of (deferred/postponed/regular) non-finished jobs
		jobsDone: int  - the number of [successfully] completed jobs
		jobsFailed: int  - the number of failed jobs
		tasks: int  - total number of (flattened) tasks
		tasksFailed: int  - total number of tasks having at least one subtask/job failed
		tasksRoot: int  - total number of the root (major/super) tasks
		tasksRootFailed: int  - total number of root tasks having at least one subtask/job failed
		"""
		self.workers = workers
		self.jobs = jobs
		self.jobsFailed = jobsFailed
		self.jobsDone = jobsDone
		self.tasks = tasks
		self.tasksFailed = tasksFailed
		self.tasksRoot = tasksRoot
		self.tasksRootFailed = tasksRootFailed

	# @property
	# def __dict__(self):
	# 	return dict({p: self.__getattribute__(p) for p in self.__slots__})


class WebUiApp(threading.Thread):
	"""WebUI App starting in the dedicated thread and providing remote interface to inspect ExecPool"""
	RAM = None
	LCPUS = None  # Logical CPUs
	CPUCORES = None  # CPU Cores <= LCPUS
	CPUNODES = None  # NUMA nodes (typically, physical CPUs)
	WKSMAX = None

	def __init__(self, host='localhost', port=8080, name=None, daemon=None, group=None, args=(), kwargs={}):
		"""WebUI App constructor

		ATTENTION: Once constructed, the WebUI App lives in the dedicated thread until the main program exit.

		Args:
			uihost: str  - Web UI host
			uiport: uint16  - Web UI port
			name: str  - The thread name. By default, a unique name
				is constructed of the form “Thread-N” where N is a small decimal number.
			daemon: bool  - Start the thread in the daemon mode to
				be automatically terminated on the main app exit.
			group  - Reserved for future extension
				when a ThreadGroup class is implemented.
			args: tuple  - The argument tuple for the target invocation.
			kwargs: dict  - A dictionary of keyword arguments for the target invocation.

		Internal attributes:
			cmd: UiCmd  - UI command to be executed, which includes (reserved) attribute(s) for the invocation result.
		"""
		# target (callable, optional): Defaults to None. The callable object to be invoked by the run() method.
		self.cmd = UiCmd(None)  # Shared data between the UI WebApp and MpePool backend
        # Initialize web app before starting the thread
		webuiapp = bottle.default_app()  # The same as bottle.Bottle()
		# Define partial function with the substituted first parameter
		webuiapp.route('/', callback=partial(WebUiApp.root, self.cmd), name=UiCmdId.FAILURES.name)
		webuiapp.route('/jobs', callback=partial(WebUiApp.jobs, self.cmd), name=UiCmdId.LIST_JOBS.name)
		webuiapp.route('/tasks', callback=partial(WebUiApp.tasks, self.cmd), name=UiCmdId.LIST_TASKS.name)
		webuiapp.route('/apinfo', callback=WebUiApp.apinfo, name=UiCmdId.API_MANUAL.name)
		# TODO, add interfaces to inspect tasks and selected jobs/tasks:
		# webuiapp.route('/job/<name>', callback=mroot, name=UiCmdId.JOB_INFO.name)
		# webuiapp.route('/task/<name>', callback=mroot, name=UiCmdId.TASK_INFO.name)
		kwargs.update({'host': host, 'port': port})
		super(WebUiApp, self).__init__(group=group, target=bottle.run, name=name, args=args, kwargs=kwargs)
		self.daemon = daemon


	@bottle.get('/favicon.ico')
	@staticmethod
	def favicon():
		"""Favicon for browsers"""
		return bottle.static_file('favicon.ico', root='/images')


	@bottle.error(404)
	@staticmethod
	def error404(err, msg=''):
		"""Custom page not found"""
		if msg:
			return 'The URL is invalid: {}, {}'.format(msg, err)
		else:
			return 'Nothing here, sorry'


	@staticmethod
	def root(cmd):
		"""Default command of the UI (UiCmdId.FAILURES)

		Shows:
		- execpool name if any
		- the current number of workers and the total number of them
		- the amount of consuming memory by the workers and the specified memory constraint
		- duration of the execution
		- failed jobs, ordered in the historical order by the termination time (tstop):
				[name  - job name, always present]
				pid  - process id
				tstart  - start time of the job
				//tstop  - termination time of the job
				duration  - current execution time
				memory  - consuming memory as specified by Job
				//memkind  - kind of the reporting memory
				task  - task name if assigned
				category  - job category if specified


		URL parameters:
			fmt (UiResFmt values) = {json, htm, txt}  - format of the result
			cols (UiResCol values):
				[name  - job name, always present]
				pid  - process id
				# state = {'w', 's'}  - execution state (working, scheduled)
				duration  - current execution time
				memory  - consuming memory as specified by Job
				task  - task name if assigned
				category  - job category if specified

		Args:
			cmd (UiCmd): UI command to be executed

		Returns:
			Jobs listing for the specified detalization in the specified format
		"""
		# Parameters fetching options:
		# 1. The arguments extracted from the URL:
		# bottle.request.url_args		# Empty
		# 2. Raw URL params as str:
		# bottle.request.query_string	# OK
		# 3. URL params as MultiDict:
		# bottle.request.query

		# Parse URL parameters and form the UI command parameters
		try:
			resopts = ResultOptions(bottle.request.query)
		except (KeyError, ValueError) as err:
			# 400  - Bad Request
			# 415  - Unsupported Media Type
			bottle.response.status = 400
			return 'Invalid value of the URL parameter, {}'.format(err)
			# return 'Invalid URL parameter value of "{}": {}'.format(UiResOpt.fmt.name
			# 	, bottle.request.query[UiResOpt.fmt.name])  #pylint: disable=E1136

		with cmd.cond:
			cmd.id = UiCmdId.FAILURES
			# Prepare .data for the request parameters storage
			if cmd.data:
				cmd.data.clear()
			if resopts.cols:
				cmd.data[UiResOpt.cols] = resopts.cols.split(',')
				# cols = None
			if resopts.flt:
				cmd.data[UiResOpt.flt] = resopts.fltopts
				# flt = None
			if resopts.jlim is not None:
				cmd.data[UiResOpt.jlim] = resopts.jlim
			# fltStatus = qdict.get(UiResOpt.fltStatus.name)  #pylint: disable=E1101
			# if fltStatus:
			# 	cmd.data[UiResOpt.fltStatus] = fltStatus.split(',')

			# Wait for the command execution and notification
			# return "cmd.id: {}, cmd.data: {}, keys: {}, vals: {}".format(
			# 	cmd.id, cmd.data.items(), cmd.data.keys(), cmd.data.values())
			cmd.cond.wait()
			# Now .data contains the response results
			# Read the result and transfer to the client
			if not cmd.data:
				# 500 Internal Server Error
				bottle.response.status = 500
				return 'Service error occurred, the command has not been executed'

			cmderr = cmd.data.get('errmsg')  # Error message
			if len(cmd.data) == 1 and cmderr:
				# Note: occurs mostly if the execution pool is finished
				# 503  - Service Unavailable
				bottle.response.status = 503
				return cmderr

			# Expected format of data is a table: header, rows
			if resopts.fmt == UiResFmt.json:
				# TODO: implements JSON serializer
				return json.dumps(cmd.data)
			elif resopts.fmt == UiResFmt.txt:
				# 501  - Not Implemented
				bottle.response.status = 501
				return 'Not Implemented'
				# TOFIX
				# return '\n'.join(['\t'.join([str(v) for v in cols]) for cols in cmd.data])
			#elif fmt == UiResFmt.htm:
			else:  # HTML format by default
				smr = cmd.data.get('summary')
				# print('>>> jobsInfo: {}'.format(cmd.data.get('jobsInfo')))
				return bottle.template('webui', pageRefresh=resopts.refresh, title='Failures'
					, pageDescr='Information about the failed tasks and jobs.'
					, page='failures', errmsg=cmderr
					, summary=smr is not None, ramUsage=cmd.data.get('ramUsage', 'NA')
						, ramTotal=WebUiApp.RAM, cpuLoad=cmd.data.get('cpuLoad', 'NA')
						, lcpus=WebUiApp.LCPUS, cpuCores=WebUiApp.CPUCORES, cpuNodes=WebUiApp.CPUNODES
						, workers=smr.workers, wksmax=WebUiApp.WKSMAX
						, jobsFailed=smr.jobsFailed, jobs=smr.jobs, jobsDone=smr.jobsDone
						, tasksRootFailed=smr.tasksRootFailed, tasksRoot=smr.tasksRoot
						, tasksFailed=smr.tasksFailed, tasks=smr.tasks
					, jobsFailedInfo=cmd.data.get('jobsInfo'), tasksFailedInfo=cmd.data.get('tasksInfo')
					, tasksFailedInfoWide=cmd.data.get('tasksInfoWide'), jlim=cmd.data.get(UiResOpt.jlim)
					)


	@staticmethod
	def jobs(cmd):
		"""Jobs listing including workers (UiCmdId.LIST_JOBS)"""
		# # 501  - Not Implemented
		# bottle.response.status = 501
		# return 'Not Implemented'
		try:
			resopts = ResultOptions(bottle.request.query)
		except (KeyError, ValueError) as err:
			# 400  - Bad Request
			# 415  - Unsupported Media Type
			bottle.response.status = 400
			return 'Invalid value of the URL parameter, {}'.format(err)

		with cmd.cond:
			cmd.id = UiCmdId.LIST_JOBS
			# Prepare .data for the request parameters storage
			if cmd.data:
				cmd.data.clear()
			if resopts.cols:
				cmd.data[UiResOpt.cols] = resopts.cols.split(',')
				# print('>> cols:', cmd.data[UiResOpt.cols], cmd.data.get(UiResOpt.cols))
				# cols = None
			if resopts.flt:
				cmd.data[UiResOpt.flt] = resopts.fltopts
				# flt = None
			if resopts.jlim is not None:
				cmd.data[UiResOpt.jlim] = resopts.jlim

			cmd.cond.wait()
			# Now .data contains the response results
			# Read the result and transfer to the client
			if not cmd.data:
				# 500 Internal Server Error
				bottle.response.status = 500
				return 'Service error occurred, the command has not been executed'

			cmderr = cmd.data.get('errmsg')  # Error message
			if len(cmd.data) == 1 and cmderr:
				# Note: occurs mostly if the execution pool is finished
				# 503  - Service Unavailable
				bottle.response.status = 503
				return cmderr

			if resopts.fmt == UiResFmt.json:
				# TOFIX: implements JSON serializer
				return json.dumps(cmd.data)
			elif resopts.fmt == UiResFmt.txt:
				# 501  - Not Implemented
				bottle.response.status = 501
				return 'Not Implemented'
				# TOFIX
				# return '\n'.join(['\t'.join([str(v) for v in cols]) for cols in cmd.data])
			#elif fmt == UiResFmt.htm:
			else:  # HTML format by default
				smr = cmd.data.get('summary')
				return bottle.template('webui', pageRefresh=resopts.refresh, title='Jobs'
					, pageDescr='Information about the executing (workers) and deferred jobs (non-finished items only).'
					, page='jobs', errmsg=cmderr
					, summary=smr is not None, ramUsage=cmd.data.get('ramUsage', 'NA')
						, ramTotal=WebUiApp.RAM, cpuLoad=cmd.data.get('cpuLoad', 'NA')
						, lcpus=WebUiApp.LCPUS, cpuCores=WebUiApp.CPUCORES, cpuNodes=WebUiApp.CPUNODES
						, workers=smr.workers, wksmax=WebUiApp.WKSMAX
						, jobsFailed=smr.jobsFailed, jobs=smr.jobs, jobsDone=smr.jobsDone
						, tasksRootFailed=smr.tasksRootFailed, tasksRoot=smr.tasksRoot
						, tasksFailed=smr.tasksFailed, tasks=smr.tasks
					, workersInfo=cmd.data.get('workersInfo'), jobsInfo=cmd.data.get('jobsInfo')
					, jlim=cmd.data.get(UiResOpt.jlim)
					)


	@staticmethod
	def tasks(cmd):
		"""Listing of the hierarchical tasks with their subtasks and jobs"""
		try:
			resopts = ResultOptions(bottle.request.query)
		except (KeyError, ValueError) as err:
			# 400  - Bad Request
			# 415  - Unsupported Media Type
			bottle.response.status = 400
			return 'Invalid value of the URL parameter, {}'.format(err)

		with cmd.cond:
			cmd.id = UiCmdId.LIST_TASKS
			# Prepare .data for the request parameters storage
			if cmd.data:
				cmd.data.clear()
			if resopts.cols:
				cmd.data[UiResOpt.cols] = resopts.cols.split(',')
			if resopts.flt:
				cmd.data[UiResOpt.flt] = resopts.fltopts
			if resopts.jlim is not None:
				cmd.data[UiResOpt.jlim] = resopts.jlim

			cmd.cond.wait()
			# Now .data contains the response results
			# Read the result and transfer to the client
			if not cmd.data:
				# 500 Internal Server Error
				bottle.response.status = 500
				return 'Service error occurred, the command has not been executed'

			cmderr = cmd.data.get('errmsg')  # Error message
			if len(cmd.data) == 1 and cmderr:
				# Note: occurs mostly if the execution pool is finished
				# 503  - Service Unavailable
				bottle.response.status = 503
				return cmderr

			if resopts.fmt == UiResFmt.json:
				# TOFIX: implements JSON serializer
				return json.dumps(cmd.data)
			elif resopts.fmt == UiResFmt.txt:
				# 501  - Not Implemented
				bottle.response.status = 501
				return 'Not Implemented'
				# TOFIX
				# return '\n'.join(['\t'.join([str(v) for v in cols]) for cols in cmd.data])
			#elif fmt == UiResFmt.htm:
			else:  # HTML format by default
				smr = cmd.data.get('summary')
				return bottle.template('webui', pageRefresh=resopts.refresh, title='Tasks'
					, pageDescr='Information about the non-finished hierarchy of tasks with their jobs.'
					, page='tasks', errmsg=cmderr
					, summary=smr is not None, ramUsage=cmd.data.get('ramUsage', 'NA')
						, ramTotal=WebUiApp.RAM, cpuLoad=cmd.data.get('cpuLoad', 'NA')
						, lcpus=WebUiApp.LCPUS, cpuCores=WebUiApp.CPUCORES, cpuNodes=WebUiApp.CPUNODES
						, workers=smr.workers, wksmax=WebUiApp.WKSMAX
						, jobsFailed=smr.jobsFailed, jobs=smr.jobs, jobsDone=smr.jobsDone
						, tasksRootFailed=smr.tasksRootFailed, tasksRoot=smr.tasksRoot
						, tasksFailed=smr.tasksFailed, tasks=smr.tasks
					, tasksInfo=cmd.data.get('tasksInfo'), tasksInfoWide=cmd.data.get('tasksInfoWide')
					, jlim=cmd.data.get(UiResOpt.jlim)
					)


	@staticmethod
	def apinfo():
		"""API manual"""
		# 501  - Not Implemented
		# bottle.response.status = 501
		return bottle.template('webui', title='REST API Manual'
			, pageDescr='REST API Manual.', page='apinfo', restApi=True)


	# @staticmethod
	# def taskInfo(cmd, name):
	# 	pass


if __name__ == '__main__':
	# Doc tests execution
	import doctest
	import sys
	#doctest.testmod()  # Detailed tests output
	flags = doctest.REPORT_NDIFF | doctest.REPORT_ONLY_FIRST_FAILURE | doctest.IGNORE_EXCEPTION_DETAIL
	failed, total = doctest.testmod(optionflags=flags)
	if failed:
		print("Doctest FAILED: {} failures out of {} tests".format(failed, total), file=sys.stderr)
	else:
		print('Doctest PASSED')
