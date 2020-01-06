The SitesOnNetwork script generates a [riverscapes]() project that contains
a point ShapeFile with a separate feature for each [CHaMP]() visit. The attributes 
for each point contain the CHaMP metrics as well as basic site and visit information.

The original version of this script called the Sitka CHaMP API to obtain the metrics.
In January 2020, this script was updated to use the CHaMP Workbench SQLite database
as the source of the metrics.


``` bash
usage: SitesOnNetwork.py [-h] [--logfile LOGFILE] outdir database metricschema

positional arguments:
  outdir             output directory
  database           path to CHaMP Workbench SQLite database containing metrics
  metricschema       metric schema name

optional arguments:
  -h, --help         show this help message and exit
  --logfile LOGFILE  write the output of this script to a file
```