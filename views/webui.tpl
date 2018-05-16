<!-- Page parameters:
title  - page title
page  - base page URL path, '' for the root
-->

<html>
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>{{title}}</title>
	<meta name="description" content="Free Web tutorials">
	<meta name="keywords" content="HTML, CSS, XML, JavaScript">
	<!-- <meta http-equiv="refresh" content="30">  <!- - Refresh this page every 30 seconds - -> -->

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

<nav>
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

<h1>{{title}}</h1>

% if defined('errmsg')
<p class="err">errmsg</p>
% end

% if defined('summary'):
<h2>Summary</h2>
<div class="row">
	<div class="col2">
		<div>Workers: {{workers}}</div>
		<div>Failed Jobs: {{jobsFailed}} / {{jobs}}</div>
	</div>
	<div class="col2">
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
% if defined('jobsFailedInfo'):
<h2>Failed Jobs not Assigned to Tasks</h2>
<table style="width:100%">
	<!-- <caption>Failed Jobs not Assigned to Tasks</caption> -->
	% for jfi in jobsFailedInfo:
	<tr>
		% for jprop in jfi:
		<th>{{jprop}}</th>
		%end
	</tr>
	%end
</table>
% end  # jobsFailedInfo

% if defined('tasksFailedInfo'):
<h2>Failed Tasks with Jobs</h2>
TODO: Complete
% end  # tasksFailedInfo
% if defined('jlim'):
<!-- Show jobs limit, which restricts the number of shown failed tasks -->
<p>Jobs limit: {{jlim}}</p>
%end

<!-- Jobs page specific data ############################################### -->
% if defined('workersInfo'):
<h2>Workers</h2>
<table style="width:100%">
	<!-- <caption>Failed Jobs not Assigned to Tasks</caption> -->
	% for jfi in jobsFailedInfo:
	<tr>
		% for jprop in jfi:
		<th>{{jprop}}</th>
		%end
	</tr>
	%end
</table>
% end  # workersInfo

% if defined('jobsInfo'):
<h2>Jobs</h2>
<table style="width:100%">
	<!-- <caption>Failed Jobs not Assigned to Tasks</caption> -->
	% for jfi in jobsFailedInfo:
	<tr>
		% for jprop in jfi:
		<th>{{jprop}}</th>
		%end
	</tr>
	%end
</table>
<!-- Show jobs limit, which restricts the number of shown jobs -->
% if defined('jlim'):
<p>Jobs limit: {{jlim}}</p>
%end
% end  # jobsInfo

<!-- Tasks page specific data ############################################## -->

<!-- API Manual page specific data ######################################### -->

<!-- ####################################################################### -->

</body>
</html>
