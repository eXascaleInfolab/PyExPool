<!-- Page parameters:
title  - page title
page  - base page URL path, '' for the root
...
-->
<html>
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>{{title}}</title>
% if get('pageDescr'):
	<meta name="description" content="{{pageDescr}}">
% end
	<!-- <meta name="keywords" content="HTML, CSS, XML, JavaScript"> -->
% if get('pageRefresh') is not None:
	<meta http-equiv="refresh" content="{{max(2, pageRefresh)}}">  <!-- Refresh this page every pageRefresh>=2 seconds -->
% end

	<style>
		/* Errors */
		.err {
			color: red;
			font-size: larger;
		}
		/* Selected items */
		a.sel {
			color: black;
			text-decoration: none;
		}

		/* Layout --------------------------------------------------------------- */
		/* Using Flex Layout (HTML5) */
		.row {
    		display: flex;
		}
		.col2 {
    		flex: 50%;
		}

		.center {
			text-align: center;
			/* margin: auto; */
			/* width: 50%; */
		}

		/* Using Float Layout */
		/* .col2 {
			float: left;
			width: 50%;
		} */
		/* Clear floats after the columns */
		/* .row:after {
				content: "";
				display: table;
				clear: both;
		} */
		/* ---------------------------------------------------------------------- */

		/* body {background-color: powderblue;} */
		table, th, td {
			border: 1px solid black;
			border-collapse: collapse;
		}
	</style>
</head>
<body>

<nav class="center">
	<a href="/" \\
% if not get('page'):
		class="sel" \\
% end
	>Fiailures</a> |

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

% if get('errmsg'):
<pre class="err">{{errmsg}}</pre>
% end

% if get('summary'):
<h2>Summary</h2>
<div class="row">
	<div class="col2">
		<div>RSS RAM usage: {{'{:.1%}'.format(ramUsage / ramTotal)}} ({{ramUsage}} / {{ramTotal}} GB)</div>
		<div>CPU loading: {{cpuLoad}} ({{lcpus}} lcpus, {{cpuCores}} cores, {{cpuNodes}} nodes)</div>
		<div>Workers: {{workers}} / {{wksmax}}</div>
	</div>
	<div class="col2">
		<div>Failed Jobs: {{jobsFailed}} / {{jobs}}</div>
		<div>Failed Root Tasks: {{tasksRootFailed}} / {{tasksRoot}}</div>
		<div>Failed Tasks: {{tasksFailed}} / {{tasks}}</div>
	</div>
	<!-- <span class="col2">Workers: {{workers}}</span>
	<span class="col2">Failed Root Tasks: {{tasksRootFailed}} / {{tasksRoot}}</span> -->
</div>
<!-- <div class="row">
	<div class="col2">Failed Tasks: {{tasksFailed}} / {{tasks}}</div>
	<!- - <span class="col2">Failed Jobs: {{jobsFailed}} / {{jobs}}</span>
	<span class="col2">Failed Tasks: {{tasksFailed}} / {{tasks}}</span> - ->
</div> -->
% end

<!-- Failures page specific data ########################################### -->
% if get('jobsFailedInfo'):
<h2>Failed Jobs not Assigned to Tasks</h2>
<table style="width:100%">
	<!-- <caption>Failed Jobs not Assigned to Tasks</caption> -->
	% for jfi in jobsFailedInfo:
	<tr>
		% for jprop in jfi:
		<th>{{jprop}}</th>
		% end
	</tr>
	% end
</table>
% end  # jobsFailedInfo

% if get('tasksFailedInfo'):
<h2>Failed Tasks with Jobs</h2>
	TODO: Complete
	% if get('jlim'):
<!-- Show jobs limit, which restricts the number of shown failed tasks -->
<p>Jobs limit: {{jlim}}</p>
	% end
% end  # tasksFailedInfo

<!-- Jobs page specific data ############################################### -->
% if get('workersInfo'):
<h2>Workers</h2>
<table style="width:100%">
	<!-- <caption>Failed Jobs not Assigned to Tasks</caption> -->
	% for jfi in workersInfo:
	<tr>
		% for jprop in jfi:
		<th>{{jprop}}</th>
		% end
	</tr>
	% end
</table>
% end  # workersInfo

% if get('jobsInfo'):
<h2>Jobs</h2>
<table style="width:100%">
	<!-- <caption>Failed Jobs not Assigned to Tasks</caption> -->
	% for jfi in jobsInfo:
	<tr>
		% for jprop in jfi:
		<th>{{jprop}}</th>
		% end
	</tr>
	% end
</table>
<!-- Show jobs limit, which restricts the number of shown jobs -->
	% if get('jlim'):
<p>Jobs limit: {{jlim}}</p>
	% end
% end  # jobsInfo

<!-- Tasks page specific data ############################################## -->

<!-- API Manual page specific data ######################################### -->

<!-- ####################################################################### -->

</body>
</html>
