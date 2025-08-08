sample_respone: str = """<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">

<head>
<meta name="description" content="IXPE information">
<meta name="keywords" content="IXPE, X-ray Astrophysics, Polarization">
<meta name="author" content="Mitzi Adams">
<meta http-equiv="X-UA-Compatible" content="IE=edge">

<link rel="stylesheet" type="text/css" href="/styles/style.css">
<link rel="icon" type="image/png" href= "/images/ixpe_fav.png">

<title>IXPE: Long Term Plan</title>
</head>

<body>

<div id="wrap">
<div id="contentwrap">

<div id="header_top">
<a href="https://www.nasa.gov">
  <img src="/images/logo_nasa.gif" alt="NASA meatball"></a>
</div>

<div id="header">
<a href="index.html">
  <img src="/images/ixpe_banner_2_lr.jpg" alt="IXPE header image"></a>
</div>

<div id="divider2">
</div>

<div id="menu">
<ul>
<li><a href="https://ixpe.msfc.nasa.gov/index.html">Home</a></li>
  <li class="dropdown">
    <a href="/about/index.html" class="dropbtn">About</a>
  </li>
<!--
<li class="dropdown">
   <a href="/for_scientists/index.html" class="dropbtn">For Scientists</a>
       <div class="dropdown-content">
        <a href="/pimms/pimms_template.html">Web Pimms</a>
       </div>
</li>
-->
<!--
<li><a href="for_scientists/index.html"> For Scientists</a></li>
-->
   <li class="dropdown">
   <a href="/for_scientists/index.html" class="dropbtn">For Scientists</a>
       <div class="dropdown-content">
       <a href="/for_scientists/">Technical Information</a><br>
       <a href="/for_scientists/papers/">Papers</a><br>
       <a href="/for_scientists/presentations/">Presentations</a><br>
       <a href="/for_scientists/templates/">Templates</a><br>
       <a href="/for_scientists/ltp.html">Plan</a><br>
       <a href="/for_scientists/pimms/">WebPIMMS</a><br>
       <a href="/for_scientists/users_comm.html">Users' Committee</a><br>
</div>
   </li>
<li><a href="https://wwwastro.msfc.nasa.gov/index.html">MSFC X-ray Astronomy</a></li>
<li><a href="/links.html">Links</a></li>
<li><a href="/contact.html">Contact</a></li>
<li><a href="/partners.html">Partners</a></li>
</ul>
</div>


<div id="divider1">
</div>

<!-- <div id="contentwrap"> -->

<div id="content">
<!-- Put Your Content Here -->

<h2 style="text-align:center;">For Scientists: The Long-Term Plan</h2>
<hr/>
<P>This is the current Long-Term Plan (LTP) for IXPE.
The Science Advisory Team requested that transient targets be added when and if they become bright.
At such time, the LTP will be adjusted, attempting to minimize the
impact on the overall plan; however, nothing is guaranteed.
</P>

<P>
IXPE is telemetry limited for bright sources and this requires that
observations be handled slightly differently from the way most X-ray
missions operate.
If IXPE is observing a bright source that could potentially fill
on-board storage, we shall divide the observation into multiple
segments.
</P>

COLUMNS:
<br>P: can be A, C, L, D or T.  A and C targets were assigned by peer review.
T is a ToO and D is DDT time.  L denotes a large project.
If an A target is bumped due to a ToO, it will be replaced.
If a C target is bumped, it will likely be dropped from the plan.

<p>S: This is a segment number.
If segment is zero, then the observation has not been segmented.

<p>Pnum: This is the GO proposal number.
If the observation is a DDT or a calibration, then this number will be zero.

<p>Name: This is a common name for the target.

<p>RA: Target RA in decimal degrees.

<p>Dec: Target Dec in decimal degrees.

<p>Start: The Year-Month-Day The date the observation is planned to
begin.  The digits after the T is the start hour, in UT, truncated to a
6 hr interval.

<p>buffer: This is an estimate as to how much data will stll be on
the spacecraft when the observation ends in MB.
The on-board storage can hold just over 4500 MB and when planning
we keep the usage below 4000 MB.

<TABLE style="BORDER:0; margin:auto;">

<TR>
 <td style="width:26px">P S</td>
 <td style="width:40px">Pnum</td>
 <td style="width:120px">Name</td>
 <td style="width:64px">RA</td>
 <td style="width:56px">Dec</td>
 <td style="width:110px">Start</td>
 <td style="width:40px">buffer</td>
</TR>
<TR><td>A 0</td><td>2103  </td><td>KES 75      </td><td> 281.604</td><td>  -2.975</td><td>2025-04-08T06</td><td>     0.0</td></TR>
<TR><td>A 0</td><td>2022  </td><td>SS 433 WEST </td><td> 287.672</td><td>   5.032</td><td>2025-04-19T00</td><td>     0.0</td></TR>
<TR><td>A 0</td><td>2003  </td><td>MCG-5-23-16 </td><td> 146.917</td><td> -30.949</td><td>2025-04-24T12</td><td>     0.0</td></TR>
<TR><td>A 0</td><td>2184  </td><td>GRS 1915+105</td><td> 288.798</td><td>  10.946</td><td>2025-05-10T12</td><td>     0.0</td></TR>
<TR><td>C 0</td><td>2152  </td><td>NGC 3227    </td><td> 155.877</td><td>  19.865</td><td>2025-05-13T18</td><td>     0.0</td></TR>
<TR><td>D 0</td><td>0000  </td><td>Cyg X-2     </td><td> 326.171</td><td>  38.321</td><td>2025-05-28T12</td><td>  1669.8</td></TR>
<TR><td>A 0</td><td>2014  </td><td>LIGHTHOUSE N</td><td> 165.359</td><td> -61.015</td><td>2025-05-30T12</td><td>     0.0</td></TR>
<TR><td>C 0</td><td>2095  </td><td>1ES 1927+654</td><td> 291.831</td><td>  65.565</td><td>2025-06-17T12</td><td>     0.0</td></TR>
<TR><td>D 0</td><td>0000  </td><td>H 1417-624  </td><td> 215.300</td><td> -62.699</td><td>2025-06-27T18</td><td>     0.0</td></TR>
<TR><td>D 0</td><td>0000  </td><td>Cyg X-2     </td><td> 326.171</td><td>  38.321</td><td>2025-07-03T12</td><td>  2337.7</td></TR>
<TR><td>A 0</td><td>2067  </td><td>PG 1553+113 </td><td> 238.929</td><td>  11.190</td><td>2025-07-06T06</td><td>  1636.4</td></TR>
<TR><td>C 0</td><td>2093  </td><td>UW CRB      </td><td> 241.441</td><td>  25.863</td><td>2025-07-09T00</td><td>   167.6</td></TR>
<TR><td>A 1</td><td>2037  </td><td>NGC 5506    </td><td> 213.312</td><td>  -3.207</td><td>2025-07-14T06</td><td>     0.0</td></TR>
<TR><td>A 1</td><td>2111  </td><td>CIR X-1     </td><td> 230.170</td><td> -57.167</td><td>2025-07-24T06</td><td>     0.0</td></TR>
<TR><td>A 2</td><td>2037  </td><td>NGC 5506    </td><td> 213.312</td><td>  -3.207</td><td>2025-07-27T12</td><td>     0.0</td></TR>
<TR><td>A 0</td><td>2111  </td><td>CIR X-1     </td><td> 230.170</td><td> -57.167</td><td>2025-08-02T00</td><td>  1232.3</td></TR>
<TR><td>A 3</td><td>2037  </td><td>NGC 5506    </td><td> 213.312</td><td>  -3.207</td><td>2025-08-04T18</td><td>     0.0</td></TR>
<TR><td>A 2</td><td>2111  </td><td>CIR X-1     </td><td> 230.170</td><td> -57.167</td><td>2025-08-09T12</td><td>     0.0</td></TR>
<TR><td>A 4</td><td>2037  </td><td>NGC 5506    </td><td> 213.312</td><td>  -3.207</td><td>2025-08-12T18</td><td>     0.0</td></TR>
<TR><td>C 1</td><td>2114  </td><td>RX J1713.7-3</td><td> 257.963</td><td> -39.952</td><td>2025-08-14T12</td><td>     0.0</td></TR>
<TR><td>A 0</td><td>2067  </td><td>PG 1553+113 </td><td> 238.929</td><td>  11.190</td><td>2025-08-24T18</td><td>     0.0</td></TR>
<TR><td>C 2</td><td>2114  </td><td>RX J1713.7-3</td><td> 257.963</td><td> -39.952</td><td>2025-08-27T06</td><td>     0.0</td></TR>
<TR><td>C 1</td><td>2013  </td><td>CRAB PULSAR </td><td>  83.633</td><td>  22.014</td><td>2025-09-06T12</td><td>  2629.6</td></TR>
<TR><td>A 1</td><td>2081  </td><td>4U 1812-12  </td><td> 273.776</td><td> -12.096</td><td>2025-09-08T06</td><td>     0.0</td></TR>

</TABLE>

NOTES:
<br>
Plan covers time up to 2025 Sept.
<br>ToOs will cause planned times to shift around.
</div>

<!-- End Content -->
<div id="sidebar">

<h3>Polarization</h3>
<ul>
<!--   <li><a href="https://www2.hao.ucar.edu/sites/default/files/users/whawkins/Tutorial%201.pdf">The Physics of Polarization</a></li> -->
   <li><a href="/creation.html">Polarization - Creation</a></li>
   <li><a href="/detectors.html">Polarization - Detection</a></li>
</ul>

<h3>Useful Resources</h3>
<ul>
   <li><a href="/news.html">In the News</a></li>
   <li><a href="http://chandra.harvard.edu">Chandra X-ray Observatory</a></li>
   <li><a href="/multimedia/">Multimedia</a>
      <ul>
        <li><a href="/multimedia/podcasts/">Podcasts</a></li>
        <li><a href="/multimedia/vodcasts/">Vodcasts</a></li>
        <li><a href="/multimedia/images/">Images</a></li>
        <li><a href="/multimedia/blogposts/">Blog Posts</a></li>
      </ul>
   </li>
</ul>

</div>

<div style="clear: both;"> </div>

</div>

<div id="footer1">
   <a href="http://www.usa.gov/"></a>
    <a href="https://www.nasa.gov/about/highlights/HP_Privacy.html">
       + NASA Privacy, Security, Notices</a> |
         Last Updated:
         June 17, 2025 |
Author/Curator: Mitzi Adams, mitzi.adams @ nasa.gov
</div>

</div>

</body>
</html>
"""
