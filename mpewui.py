#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
:Description:  Minimalistic Web User Interface for the Multiprocess Execution Pool.
    The functionality includes only fetching of the specified atrifutes of the
    scheduled and executing Jobs in the specified format (HTML table / JSON / TXT)
    for the profiling and debugging.

:Authors: (c) Artem Lutov <artem@exascale.info>
:Organizations: eXascale Infolab <http://exascale.info/>, Lumais <http://www.lumais.com/>
:Date: 2018-04
"""
from __future__ import print_function, division  # Required for stderr output, must be the first import
# Extrenal API (exporting functions)
__all__ = ['WebUiApp', 'UiCmdId', 'UiResOpt', 'UiResCol', 'UiResFltStatus']


import bottle
# from bottle import run as websrvrun
import threading  # Run bottle in the dedicated thread
from functools import partial  # Customly parameterized routes
# Constants Definitions
try:
    from enum import IntEnum
except ImportError:
	# As a falback make a manual wrapper with the essential functionality
	def IntEnum(clsname, names, module=__name__, start=1):
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
			"""Build object with the specivied class name, name and value attributes
			
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
				#{'name': name, 'value': i}
				dnames = {name: valobj(clsname, name, i) for i, name in enumerate(names, start)}
			else:
				dnames = {name: valobj(clsname, name, i) for i, name in dict(names).items()}
		cls = type(clsname, (object,), dnames)
		# Consider some specific attributes of the enum
		cls.__members__ = dnames
		return cls

# Internal format serialization to JSON/HTM/TXT
import json
try:
	from html import escape
except ImportError:
	from cgi import escape  # Consider both both Python2/3

"""UI Command Identifier associated with the REST URL"""
UiCmdId = IntEnum('UiCmdId', 'LIST_JOBS')

"""UI Command parameters"""
UiResOpt = IntEnum('UiResOpt', 'fmt cols fltStatus')

"""UI Command: Result format parameter values"""
UiResFmt = IntEnum('UiResFmt', 'json htm txt')

"""UI Command: Result columns parameter values for the Jobs listing"""
UiResCol = IntEnum('UiResCol', 'pid state duration memory task category')

"""UI Command: Result filteration by the job status: executing (worker job), defered (scheduled ojb)"""
# Note: 'exec' is a keyword in Python 2 and can't be used as an obect attribute
UiResFltStatus = IntEnum('UiResFltStatus', 'work defer')

class UiCmd(object):
	"""UI Command (for the MVC controller)"""
	def __init__(self, cid, params=dict(), data=[]):
		"""UI command
		
		Args:
			cid (UiCmdId)  - Command identifier
			params (dict)  - Command parameters
			data (list)  - Resulting data

		Internal attributes:
			cond (Condition)  - synchronizing condition
		"""
		#dshape (set(str), optional): Defaults to None. Expected data shape (columns of the returning table)
		# self.lock = threading.Lock()
		self.cond = threading.Condition()  # (self.lock)
		# assert (params in None or isinstance(params, dict)) and (data in None or isinstance(data, set)), (
		# 	'Unexpected type of arguments, params: {}, data: {}'.format(type(params).__name__, type(data).__name__))
		assert (cid is None or isinstance(cid, UiCmdId)) and isinstance(params, dict) and isinstance(data, list), (
			'Unexpected type of arguments, id: {}, params: {}, data: {}'.format(
            type(id).__name__, type(params).__name__, type(data).__name__))
		self.id = cid
		# self.dshape = dshape
		self.params = params
		self.data = data


class WebUiApp(threading.Thread):
	"""WebUI App sarting in the dedicated thread and providing remote interface to inspect ExecPool"""
	def __init__(self, host='localhost', port=8080, name=None, daemon=None, group=None, args=(), kwargs={}):
		"""WebUI App constructor

		ATTENTION: Once constructed, the WebUI App lives in the dedicated thread until the main program exit.
		
		Args:
			uihost (str)  - Web UI host
			uiport (uint16)  - Web UI port
			name  - The thread name. By default, a unique name
				is constructed of the form “Thread-N” where N is a small decimal number.
			daemon (bool)  - Start the thread in the daemon mode to
				be automatcally terminated on the main app exit.
			group  - Reserved for future extension
				when a ThreadGroup class is implemented.
			args (tuple)  - The argument tuple for the target invocation.
			kwargs (dict)  - A dictionary of keyword arguments for the target invocation.

		Internal attributes:
			cmd (UiCmd)  - UI command to be executed, which includes (reserved) attribute(s) for the invocation result.
		"""
		# target (callable, optional): Defaults to None. The callable object to be invoked by the run() method.
		self.cmd = UiCmd(None)  # Shared data between the UI WebApp and MpePool backend
        # Initialize web app before starting the thread
		webuiapp = bottle.default_app()  # The same as bottle.Bottle()
		mroot = partial(WebUiApp.root, self.cmd)
		webuiapp.route('/', callback=mroot, name=UiCmdId.LIST_JOBS.name)
		webuiapp.route('/jobs', callback=mroot, name=UiCmdId.LIST_JOBS.name)
		# TODO, add interfaces to inspect tasks and selected jobs/tasks:
		# webuiapp.route('/job/<name>', callback=mroot, name=UiCmdId.JOB_INFO.name)
		# webuiapp.route('/tasks', callback=mroot, name=UiCmdId.LIST_TASKS.name)
		# webuiapp.route('/task/<name>', callback=mroot, name=UiCmdId.TASK_INFO.name)
		kwargs.update({'host': host, 'port': port})
		super(WebUiApp, self).__init__(group=group, target=bottle.run, name=name, args=args, kwargs=kwargs)
		self.daemon = daemon


	@staticmethod
	def root(cmd):
		"""Default command of the UI (UiCmdId.LIST_JOBS)

		URL parameters:
			fmt (UiResFmt values) = {json, htm, txt}  - format of the result
			cols (UiResCol values):
				[name]  - job name, always present
				pid  - process id
				state = {'w', 's'}  - execution state (working, shceduled)
				duration  - cuttent execution time
				memory  - consuming memory as specified by Job
				task  - task name if assigned
				category  - job category if specified
		
		Args:
			cmd (UiCmd): UI command to be executed
		
		Returns:
			Jobs listing for the specified detalizaion in the specified format
		"""
		with cmd.cond:
			cmd.cid = UiCmdId.LIST_JOBS
			# Parameters fetching options:
			# 1. The arguments extracted from the URL:
			# bottle.request.url_args
			# 2. Raw URL params as str:
			# bottle.request.query_string
			# 3. URL params as MultiDict:
			# bottle.request.query

			# Parse URL parametrs and form the UI command parameters
			fmt = UiResFmt.htm
			qdict = bottle.request.query
			fmtKey = UiResOpt.fmt.name
			if fmtKey in qdict:
				try:
					fmt = UiResFmt[qdict[fmtKey]]
					del qdict[fmtKey]
				except KeyError:
					bottle.response.status = 400
					return 'Invalid URL parameter value of "{}": {}'.format(fmtKey, qdict[fmtKey])
					# raise HTTPResponse(body='Invalid URL parameter value of "fmt": ' + qdict['fmt'], status=400)
			if cmd.params:
				cmd.params.clear()
			cols = qdict.get(UiResOpt.cols.name)
			if cols:
				cmd.params[UiResOpt.cols] = cols.split(',')
			fltStatus = qdict.get(UiResOpt.fltStatus.name)
			if fltStatus:
				cmd.params[UiResOpt.fltStatus] = fltStatus.split(',')
			# Wait for the command execution and notification
			cmd.cond.wait()
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
				return ''.join('<table>',
					'\n'.join([''.join('<tr>', ''.join(
						[''.join(('<td>', escape(str(v), True), '</td>')) for v in cols]
						), '</tr>') for cols in cmd.data])
					, '</table>')


	# @staticmethod
	# def taskInfo(cmd, name):
	# 	pass


	@bottle.error(404)
	@staticmethod
	def error404(error, msg=''):
		if msg:
			return 'The URL is invalid: ' + msg
		else:
			return 'Nothing here, sorry'

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
