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
    class Enum(object):
        def __init__(self, value, names, module=IntEnum.__module__, qualname='.'.join((IntEnum.__module__
                , IntEnum.__class__.__name__)), type=type(IntEnum), start=1):
            IntEnum.__name__ = value
            if isinstance(names, str):
                names = [name.rstrip(',') for name in names.split()]
            IntEnum.__members__ = {}
            if names and not isinstance(names, dict) and (
            not isinstance(names, list) or isinstance(names[0], str)):
                for i, name in enumerate(names, start):
                    IntEnum.__members__[name] = i
                    IntEnum.__members__[name]._name_ = name  # For the Enum attributes compatibility
            IntEnum.__dict__.update(IntEnum.__members__)
            IntEnum.__module__ = module
            # self.qualname = IntEnum.__class__.__name__
            # self.type = IntEnum
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
UiResFltStatus = IntEnum('UiResFmt', 'exec defer')

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
		webuiapp.route('/', callback=mroot, name=UiCmdId.LIST_JOBS._name_)
		webuiapp.route('/jobs', callback=mroot, name=UiCmdId.LIST_JOBS._name_)
		# TODO, add interfaces to inspect tasks and selected jobs/tasks:
		# webuiapp.route('/job/<name>', callback=mroot, name=UiCmdId.JOB_INFO._name_)
		# webuiapp.route('/tasks', callback=mroot, name=UiCmdId.LIST_TASKS._name_)
		# webuiapp.route('/task/<name>', callback=mroot, name=UiCmdId.TASK_INFO._name_)
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
			fmtKey = UiResOpt.fmt._name_
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
			cols = qdict.get(UiResOpt.cols._name_)
			if cols:
				cmd.params[UiResOpt.cols] = cols.split(',')
			fltStatus = qdict.get(UiResOpt.fltStatus._name_)
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
