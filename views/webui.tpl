<html>
<head>
	<meta charset="UTF-8" />
	<meta name="viewport" content="width=device-width, initial-scale=1.0" />
	<meta name="application-name" content="[PyExPool (Web UI)](https://github.com/eXascaleInfolab/PyExPool)" />
	<meta name="author" content="Artem Lutov <artem@exascale.info>" />
	<title>{{title}}</title>
% if get('pageDescr'):
	<meta name="description" content="{{pageDescr}}" />
% end
	<!-- <meta name="keywords" content="HTML, CSS, XML, JavaScript"> -->
% if get('pageRefresh') is not None:
	<meta http-equiv="refresh" content="{{max(2, pageRefresh)}}" />  <!-- Refresh this page every pageRefresh>=2 seconds -->
% end

	<style>
		body {
			background: #EEE;
			font-family: Garamond, Helvetica, Times, serif;
		}

		h1, h2, h3, h4, h5, h6 {
			font-family: Arial, Helvetica, sans-serif;
		}

		/* Errors */
		.err {
			color: red;
			font-size: larger;
		}

		/* Selected items (in the navigation bar) */
		a.sel {
			color: black;
			text-decoration: none;
		}

		.label {
			padding-right: 1em;
		}

		/* Summary layout --------------------------------------------------- */
		/* Using Flex Layout (HTML5) */
		.row {
   		display: flex;
		}
		
		.col2 {
   		flex: 50%;
			padding-right: 1em;
		}

		/* Consider small screens */
		@media screen and (max-width: 600px) {
			.col2 {
				flex: auto;
			}

			.row {
				flex-direction: column;
			}
		}

		.ta-center {
			text-align: center;
		}

		.ta-right {
			text-align: right;
		}

		/* Table style ------------------------------------------------------ */
		table.frame {
			border-collapse: collapse;
			border-top: 1px solid #888;
			border-bottom: 1px solid #888;
			width: 100%;
			text-align: center;
		}

		/* Hierarchy of tasks with their jobs */
		.noframe, .noframe th, .noframe td  {
			border: none;
			width: auto;
			padding-right: 1em;
		}

		/* .frame td { border: none } */
		.frame th {
			/* border-top: thin solid #888; */
			border-bottom: 1px solid #888;
		}

		/* Header cell for the hierarchy of tasks */
		td.hdr {
			border-top: 1px solid #888;
			font-weight: bold;
			text-align: center;
		}

		td.task { background: #CCC }

		/* Color even rows; .frame */
		.frame tr:nth-child(even) { background: #DDD }

		tr:hover {
			background: lightyellow;
		}
	</style>
</head>
<body>

<!-- Navigation bar -->
<nav class="ta-center">
	<a href="/" \\
% if get('page') in (None, 'failures'):
		class="sel" \\
% end
	>Failures</a> |

	<a href="/jobs" \\
% if get('page') == 'jobs':
		class="sel" \\
% end
	>Jobs</a> |

	<a href="/tasks" \\
% if get('page') == 'tasks':
		class="sel" \\
% end
	>Tasks</a> |

	<a href="/apinfo" \\
% if get('page') == 'apinfo':
		class="sel" \\
% end
	>API Manual</a>
</nav>

<!-- <h1>{{title}}</h1> -->

<!-- Show the error if occurred -->
% if get('errmsg'):
<pre class="err">{{errmsg}}</pre>
% end

% if get('summary'):
<h2>Summary</h2>
<div class="row">
	<div class="col2">
		<table class="noframe">
			<tr>
				<td class="ta-right">RSS RAM usage:</td>
				<td>{{'{:.2%}'.format(ramUsage / ramTotal)}} ({{round(ramUsage, 3)}} / {{round(ramTotal, 3)}} GB)</td>
			</tr>
			<tr>
				<td class="ta-right">CPU loading:</td>
				<td>{{'{:.2%}'.format(cpuLoad)}} ({{lcpus}} lcpus, {{cpuCores}} cores, {{cpuNodes}} nodes)</td>
			</tr>
			<tr>
				<td class="ta-right">Workers:</td>
				<td>{{workers}} / {{wksmax}}</td>
			</tr>
		</table>
		<!-- <div>RSS RAM usage: {{'{:.2%}'.format(ramUsage / ramTotal)}} ({{round(ramUsage, 3)}} / {{round(ramTotal, 3)}} GB)</div>
		<div>CPU loading: {{'{:.2%}'.format(cpuLoad)}} ({{lcpus}} lcpus, {{cpuCores}} cores, {{cpuNodes}} nodes)</div>
		<div>Workers: {{workers}} / {{wksmax}}</div> -->
	</div>
	<div class="col2">
		<table class="noframe">
			<tr>
				<td class="ta-right">Failed | Remained | Completed Jobs:</td>
				<td>{{jobsFailed}} | {{jobs}} | {{jobsDone}}</td>
			</tr>
			<tr>
				<td class="ta-right">Failed Root Tasks:</td>
				<td>{{tasksRootFailed}} / {{tasksRoot}}</td>
			</tr>
			<tr>
				<td class="ta-right">Failed Tasks:</td>
				<td>{{tasksFailed}} / {{tasks}}</td>
			</tr>
		</table>
		<!-- <div>Failed Jobs: {{jobsFailed}} / {{jobs}}</div>
		<div>Failed Root Tasks: {{tasksRootFailed}} / {{tasksRoot}}</div>
		<div>Failed Tasks: {{tasksFailed}} / {{tasks}}</div> -->
	</div>
</div>
% end

<!-- Failures page specific data ########################################### -->
% if get('jobsFailedInfo'):
<h2>Failed Jobs not Assigned to Tasks</h2>
<table class="frame">   <!-- style="width:100%" -->
	<!-- <caption>Failed Jobs not Assigned to Tasks</caption> -->
	<tr>
		% for jprop in jobsFailedInfo[0]:
		<th>{{jprop}}</th>
		% end
	</tr>
	% for jfi in jobsFailedInfo[1:]:
	<tr>
		% for jprop in jfi:
		<td>{{jprop if jprop is not None else ''}}</td>
		% end
	</tr>
	% end
</table>
% end  # jobsFailedInfo

% if get('tasksFailedInfo'):
<h2>Failed Tasks with Jobs</h2>
<!-- <hr /> -->
<table class="noframe">
	% tiw = get('tasksFailedInfoWide')
	%	for tfi in get('tasksFailedInfo'):
	<tr>
		% for idt in range(tfi.ident):  # Left align the output
		<td></td>
		% end
		% for tv in tfi.data:  # Output the data
			%	if tfi.compound is not None:
				%	if tfi.compound:
				<td class="hdr task">
				%	else:
				<td class="hdr">
				% end  # if
			%	else:
			<td>
			% end  # if
				{{tv}}
			</td>
		% end  # tv
		% for idt in range(tiw - tfi.ident - len(tfi.data)):  # Right align the output
		<td></td>
		% end
	</tr>
	% end  # tfi
</table>
<hr />
	% if get('jlim'):
<!-- Show jobs limit, which restricts the number of shown failed tasks -->
<p><span class="label">Jobs limit:</span> {{jlim}}</p>
	% end
% end  # tasksFailedInfo

<!-- Jobs page specific data ############################################### -->
% if get('workersInfo'):
<h2>Workers</h2>
<table class="frame">
	<!-- <caption>Executing jobs (workers)</caption> -->
	<tr>
		% for jprop in workersInfo[0]:
		<th>{{jprop}}</th>
		% end
	</tr>
	% for jfi in workersInfo[1:]:
	<tr>
		% for jprop in jfi:
		<td>{{jprop if jprop is not None else ''}}</td>
		% end
	</tr>
	% end
</table>
% end  # workersInfo

% if get('jobsInfo'):
<h2>Jobs</h2>
<table class="frame">
	<!-- <caption>Deferred jobs</caption> -->
	<tr>
		% for jprop in jobsInfo[0]:
		<th>{{jprop}}</th>
		% end
	</tr>
	% for jfi in jobsInfo[1:]:
	<tr>
		% for jprop in jfi:
		<td>{{jprop if jprop is not None else ''}}</td>
		% end
	</tr>
	% end
</table>
	<!-- Show jobs limit, which restricts the number of shown jobs -->
	% if get('jlim'):
<p><span class="label">Jobs limit:</span> {{jlim}}</p>
	% end
% end  # jobsInfo

<!-- Tasks page specific data ############################################## -->
% if get('tasksInfo'):
<h2>Remained Tasks with Jobs</h2>
<!-- <hr /> -->
<table class="noframe">
	% tiw = get('tasksInfoWide')
	%	for tfi in get('tasksInfo'):
	<tr>
		% for idt in range(tfi.ident):  # Left align the output
		<td></td>
		% end
		% for tv in tfi.data:  # Output the data
			%	if tfi.compound is not None:
				%	if tfi.compound:
				<td class="hdr task">
				%	else:
				<td class="hdr">
				% end  # if
			%	else:
			<td>
			% end  # if
				{{tv}}
			</td>
		% end  # tv
		% for idt in range(tiw - tfi.ident - len(tfi.data)):  # Right align the output
		<td></td>
		% end
	</tr>
	% end  # tfi
</table>
<hr />
	% if get('jlim'):
<!-- Show jobs limit, which restricts the number of shown failed tasks -->
<p><span class="label">Jobs limit:</span> {{jlim}}</p>
	% end
% end  # tasksInfo

<!-- API Manual page specific data ######################################### -->
% if get('restApi'):
<h2>REST API Manual</h2>
% include('restapi.htm')
% end  # restApi
<!-- ####################################################################### -->

</body>
</html>
