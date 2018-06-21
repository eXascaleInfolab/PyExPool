<h2>Rest API Manual</h2>

<p>The following <strong>endpoints</strong> are available:
<ul>
  <li><kbd>/</kbd> - list failures (failed jobs and tasks having at least one failed job).</li>
  <li><kbd>/jobs</kbd>  - list non-finished jobs.</li>
  <li><kbd>/tasks</kbd>  - list non-finished tasks.<br />
  <strong>Note</strong>: a task is registered in the <samp>ExecPool</samp> on start of the first descendant job, so only tasks related to the started jobs are shown.</li>
</ul>
</p>

<p>All endpoints have uniform URL <strong>parameters</strong>:
<ul>
  <li>
    <kbd id="fmt">fmt</kbd>  - required format of the result: <strong><samp>json htm txt</samp></strong>
  </li>
  <li>
    <kbd id="cols">cols</kbd>  - item (job/task) properties to be shown (all by default):
    <strong><samp>category rcode duration memkind memsize name numadded numdone numterm pid task tstart tstop</samp></strong>
  </li>
  <li>
    <kbd id="flt">flt</kbd>  - items filtering by the specified properties in the following format:
    <strong><samp>&lt;pname&gt;[*][:&lt;beg&gt;[..&lt;end&gt;]]</samp></strong>
    <ul>
      <li>
        <kbd>&lt;pname&gt;</kbd>  - property name, see <a href="#cols"><kbd>cols</kbd></a>.
      </li>
      <li>
        <kbd>*</kbd>  - optional property, the item is filtered out only if
        the property value is out of the specified range but not if
        this property is not present.
      </li>
      <li>
        <kbd>beg</kbd>  - begin of the property value range, inclusive.
        Exact match is expected if <kbd>end</kbd> is omitted.
      </li>
      <li>
        <kbd>end</kbd>  - end of the property value range, exclusive.
      </li>
    </ul>
    Fully omitted range requires any non-None property value. <br />
    For example, to show terminated jobs with return code <var>-15</var> and tasks having these jobs (<samp>rcode*</samp> is optional since tasks do not have this property), where the jobs have defined <samp>category*</samp> (optional since tasks do not have category) and where each job executed from <var>1.5 sec</var> up to <var>1 hour</var> (3600 sec):<br />
    <samp>?flt=rcode*:-15|duration:1.5..3600|category*</samp>
    <blockquote>
      Omitted low/high bound of the range is substituted with -/+inf: <samp>rcode:..</samp> is the same as <samp>rcode:-inf..inf</samp>.<br />
      Duration can be specified in the DHMS format: <samp>[&lt;days:int&gt;d][&lt;hours:int&gt;h][&lt;minutes:int&gt;m][&lt;seconds:float&gt;]</samp>, for example:  <samp>duration:2d5h12.8..</samp>.
    </blockquote>
  </li>
  <li>
    <kbd>lim</kbd>  - limit the number of the showing items up to this number of jobs / tasks, <var>50</var> by default.
  </li>
  <li>
    <kbd>refresh</kbd>  - page auto refresh time in seconds, &ge; 2, absent by default.
  </li>
</ul>
</p>

<p>Query <strong>examples</strong>:
<ul>
  <li>
    Show tasks with their jobs having <var>rcode=-15</var> if exists, <var>1.5 sec &le; duration &lt; 3600 sec</var> and <var>cat1 &le; category &lt; cat5 </var> if exists:
    <pre>
      http://localhost:8080/tasks?flt=rcode*:-15|duration:1.5..3600|category*=cat1..cat5
    </pre>
  </li>
  <li>
    Show jobs having <var>rcode &lt; 0</var> if exists, <var>1.5 sec &le; duration &lt; 10 days 2 hours 5.7 sec</var> and <var>task</var> attribute present with some defined value:
    <pre>
      http://localhost:8080/jobs?flt=rcode*:..0|duration:1.5..10d2h15.7|task
    </pre>
  </li>
</p>
