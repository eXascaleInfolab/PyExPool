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
__all__ = ['WebUiApp', 'UiCmdId', 'UiResOpt', 'UiResCol', 'UiResFltStatus']

# import signal  # Required for the correct handling of KeyboardInterrupt: https://docs.python.org/2/library/thread.html
import threading  # Run bottle in the dedicated thread
from functools import partial  # Custom parameterized routes
# Internal format serialization to JSON/HTM/TXT
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

		>>> IntEnum('UiResFltStatus', 'work defer').defer.name
		'defer'
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


UiCmdId = IntEnum('UiCmdId', 'SUMMARY LIST_JOBS')  # 'JOB_INFO', 'LIST_TASKS', 'TASK_INFO'
"""UI Command Identifier associated with the REST URL"""

UiResOpt = IntEnum('UiResOpt', 'fmt cols fltStatus')
"""UI Command parameters"""

UiResFmt = IntEnum('UiResFmt', 'json htm txt')
"""UI Command: Result format parameter values"""

UiResCol = IntEnum('UiResCol', 'pid state tstart tstop duration memory task category')
"""UI Command: Result columns parameter values for the Jobs listing"""

# Note: 'exec' is a keyword in Python 2 and can't be used as an object attribute
UiResFltStatus = IntEnum('UiResFltStatus', 'work defer')
"""UI Command: Result filtration by the job status: executing (worker job), deferred (scheduled obj)"""

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


class WebUiApp(threading.Thread):
	"""WebUI App starting in the dedicated thread and providing remote interface to inspect ExecPool"""
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
		mroot = partial(WebUiApp.root, self.cmd)  # Define partial function with the substituted first parameter
		webuiapp.route('/', callback=mroot, name=UiCmdId.SUMMARY.name)
		webuiapp.route('/jobs', callback=mroot, name=UiCmdId.LIST_JOBS.name)
		# TODO, add interfaces to inspect tasks and selected jobs/tasks:
		# webuiapp.route('/job/<name>', callback=mroot, name=UiCmdId.JOB_INFO.name)
		# webuiapp.route('/tasks', callback=mroot, name=UiCmdId.LIST_TASKS.name)
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
	def error404(error, msg=''):
		"""Custom page not found"""
		if msg:
			return 'The URL is invalid: ' + msg
		else:
			return 'Nothing here, sorry'


	@staticmethod
	def root(cmd):
		"""Default command of the UI (UiCmdId.SUMMARY)

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
		# return 'Root works'
		with cmd.cond:
			cmd.id = UiCmdId.SUMMARY
			# Parameters fetching options:
			# 1. The arguments extracted from the URL:
			# bottle.request.url_args		# Empty
			# 2. Raw URL params as str:
			# bottle.request.query_string	# OK
			# 3. URL params as MultiDict:
			# bottle.request.query

			# Parse URL parameters and form the UI command parameters
			fmt = UiResFmt.htm
			qdict = bottle.request.query
			fmtKey = UiResOpt.fmt.name
			# return "query dict: {}, dict.keys: {}, dict[fmt]: {}".format(qdict.items(), qdict.keys(), qdict[fmtKey])
			if fmtKey in qdict:  #pylint: disable=E1135
				try:
					fmt = UiResFmt[qdict[fmtKey]]  #pylint: disable=E1136
					del qdict[fmtKey]  #pylint: disable=E1138
				except KeyError:
					bottle.response.status = 400
					return 'Invalid URL parameter value of "{}": {}'.format(fmtKey, qdict[fmtKey])  #pylint: disable=E1136
					# raise HTTPResponse(body='Invalid URL parameter value of "fmt": ' + qdict['fmt'], status=400)
			# Prepare .data for the request parameters storage
			if cmd.data:
				cmd.data.clear()
			cols = qdict.get(UiResOpt.cols.name)  #pylint: disable=E1101
			if cols:
				cmd.data[UiResOpt.cols] = cols.split(',')
			fltStatus = qdict.get(UiResOpt.fltStatus.name)  #pylint: disable=E1101
			if fltStatus:
				cmd.data[UiResOpt.fltStatus] = fltStatus.split(',')
			# Wait for the command execution and notification
			# return "cmd.id: {}, cmd.data: {}, keys: {}, vals: {}".format(
			# 	cmd.id, cmd.data.items(), cmd.data.keys(), cmd.data.values())
			cmd.cond.wait()
			# Now .data contains the response results
			return "cmd.data: {}".format(cmd.data)
			# Read the result and transfer to the client
			if not cmd.data:
				return ''
			# Expected format of data is a table: header, rows
			if fmt == UiResFmt.json:
				return json.dumps(cmd.data)
			elif fmt == UiResFmt.txt:
				return '\n'.join(['\t'.join([str(v) for v in cols]) for cols in cmd.data])
			#elif fmt == UiResFmt.htm:
			else:
				return ''.join(('<table>',
					'\n'.join([''.join(('<tr>', ''.join(
						[''.join(('<td>', escape(str(v), True), '</td>')) for v in cols]
						), '</tr>')) for cols in cmd.data])
					, '</table>'))


	@staticmethod
	def jobs(cmd):
		"""Jobs listing including workers (UiCmdId.LIST_JOBS)"""
		with cmd.cond:
			cmd.id = UiCmdId.LIST_JOBS


	# @staticmethod
	# def taskInfo(cmd, name):
	# 	pass

# @webuiapp.route('/')
# def root():
# 	return 'Root'

# @route('/create', apply=[sqlite_plugin])
# def create(db):
#     db.execute('INSERT INTO ...')


if __name__ == '__main__':
	"""Doc tests execution"""
	import doctest
	import sys
	#doctest.testmod()  # Detailed tests output
	flags = doctest.REPORT_NDIFF | doctest.REPORT_ONLY_FIRST_FAILURE | doctest.IGNORE_EXCEPTION_DETAIL
	failed, total = doctest.testmod(optionflags=flags)
	if failed:
		print("Doctest FAILED: {} failures out of {} tests".format(failed, total), file=sys.stderr)
	else:
		print('Doctest PASSED')
