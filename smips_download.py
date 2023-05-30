#!/usr/bin/env python
#%pip install owslib shapely matplotlib

from owslib.wps import WebProcessingService, ComplexDataInput
from typing import Callable
import pandas as pd
import datetime, io, logging, os, sys

import smips_sites
from smips_common import *

_ENDPOINT="https://funcwps.ternlandscapes.org.au/wps/"
_TEMPORAL_DRILL = "temporalDrill"
_SLEEP = 5000 # Sleep interval in ms

_MIME_GEO_JSON = "application/vnd.geo+json"
_SCHEMA_BASE = "http://geojson.org/geojson-spec.html"
_SCHEMA_POINT = "Point"

# Number of milliseconds in a second.
_MS_PER_SEC = 1000

# Dataset used for downloading soil moisture data.
_DATASET_SMIPS = "smips"

# Dataset used for downloading aet data. Must be used with ET layer.
_DATASET_AET = "aet"

# Layer used to download soil moisture data. Must be used with smips dataset.
_LAYER_SW = "totalbucket"

# Layer used to download soil moisture index. Must be used with smips dataset.
_LAYER_SMINDEX = "SMindex"

# Layer used to download ET data. Must be used with AET dataset.
_LAYER_ET = "ETa"

# Asynchronous process execution mode.
_MODE_ASYNC = "async"

# Synchronouse process execution mode.
_MODE_SYNC = "sync"

# Date format used to pass dates to the PyWPS server
_API_DATE_FORMAT = r"%Y-%m-%d"

# Format of dates returned by the server.
_SERVER_DATE_FORMAT = r"%Y-%m-%dT%H:%M:%S%z"

# Format used when writing dates to output files.
_OUTPUT_DATE_FORMAT = r"%Y-%m-%d"

# Name of the date column in the data returned from the server.
_COL_DATE = "date"

# Output directory.
_OUT_DIR = "out-index"

# Set up owslib logging, to set relevant output
_logger = logging.getLogger("owslib")
_logger.setLevel(logging.WARNING)

_handler = logging.StreamHandler(sys.stdout)
_handler.setLevel(logging.WARNING)
_logger.addHandler(_handler)

# Define a connection to the server.
_server = WebProcessingService(_ENDPOINT, verbose=False)

def _describe_server(server: WebProcessingService):
	"""
	List all processes and operations supported by the srever. 
	"""
	print('WPS Server: type        = %s' % server.identification.type)
	print('WPS Server: title       = %s' % server.identification.title)
	print('WPS Server: abstract    = %s\n' % server.identification.abstract)

	for operation in server.operations:
		print('WPS Operation: name     = %s' % operation.name)

	for process in server.processes:
		print("\n", end = "")
		print('WPS Process: identifier = %s' % process.identifier)
		print("WPS Process: title      = %s" % process.title)
		print("WPS Process: abstract   = %s" % process.abstract)

def _describe_process(server: WebProcessingService, process_name: str):
	"""
	Describe the process on the server with the specified name.

	@param server: The server.
	@param process_name: Name of the process.
	"""
	process = server.describeprocess(process_name)

	print('WPS Process: identifier     = %s' % process.identifier)
	print('WPS Process: title          = %s' % process.title)
	print('WPS Process: abstract       = %s\n' % process.abstract)

	for input in process.dataInputs:
		print("\n", end = "")
		print("Process input: identifier   = %s" % input.identifier)
		print("Process input: data type    = %s" % input.dataType)
		print("Process input: minOccurs    = %d" % input.minOccurs)
		print("Process input: maxOccurs    = %d" % input.maxOccurs)

	for output in process.processOutputs:
		print("\n", end = "")
		print("Process output: identifier  = %s" % output.identifier)
		print("Process output: data type   = %s" % output.dataType)

def _get_schema(schema_type: str) -> str:
	"""
	Get a schema for the given schema type.

	@param schema_type: The schema type (typically one of the _SCHEMA_* constants).
	"""
	return "%s#%s" % (_SCHEMA_BASE, schema_type)

def _get_geo_json(lon: float, lat: float):
	"""
	Get a GeoJSON representation of the specified latitude and longitude.
	"""
	return '{ "type": "Point", "coordinates": [%.2f, %.2f] }' % (lon, lat)

def describe_server():
	"""
	List all processes and operations supported by the srever. 
	"""
	_describe_server(_server)

def describe_process(process_name: str):
	"""
	Describe the process on the server with the specified name.

	@param process_name: Name of the process.
	"""
	_describe_process(_server, process_name)

def download_timeseries(lon: float, lat: float, start: datetime, end: datetime
	, pcb: Callable[[float], None]):
	"""
	Download timeseries data for the specified point starting from the specified
	date.
	@param lon: Longitude of the point.
	@param lat: Latitude of the point.
	@param start: Desired start date of data.
	@param pcb: A progress-reporting function which accepts one argument (current progress as a percentage).
	"""

	schema = _get_schema(_SCHEMA_POINT)

	# Create request.
	polygon = _get_geo_json(lon, lat)
	point = ComplexDataInput(polygon, mimeType = _MIME_GEO_JSON, schema = schema)

	start_str = start.strftime(_API_DATE_FORMAT)
	end_str = end.strftime(_API_DATE_FORMAT)

	# Submit request (will execute asynchronously).
	resp = _server.execute(
		_TEMPORAL_DRILL,
		mode = _MODE_ASYNC,
		lineage = True,
		inputs=[("datasetId", f"{_DATASET_SMIPS}:{_LAYER_SMINDEX}"), ("startDate", start_str), ("endDate", end_str), ("point", point)],
		output=[("csv", False, "text/csv")]  # Get stats as embedded file in result
		# output = [("download_link", False, None)]
	)

	# Wait for request to complete.
	while not resp.isComplete():
		resp.checkStatus(sleepSecs = _SLEEP / _MS_PER_SEC)
		pcb(resp.percentCompleted)
	pcb(100.0)

	# Read response.
	csv_output = resp.processOutputs[0]
	text = "".join(d for d in csv_output.data)

	# Parse data into a dataframe.
	data = pd.read_csv(io.StringIO(text))

	# Order by date ascending.
	data = data.sort_values(_COL_DATE, ascending = True)

	# Change date format to yyyy-MM-dd.
	data[_COL_DATE] = data[_COL_DATE].apply(lambda x: datetime.datetime.strptime(x, _SERVER_DATE_FORMAT).strftime(_OUTPUT_DATE_FORMAT))
	return data

if __name__ == "__main__":
	report_progress = sys.stdout.isatty()
	if not os.path.exists(_OUT_DIR):
		os.mkdir(_OUT_DIR)
	for site in smips_sites.sites:
		if report_progress:
			pcb = lambda x: print("Downloading %s: %.2f%%" % (site.name, x), end = "\r")
		else:
			pcb = lambda _: ...
			print("Downloading %s..." % site.name)
		data = download_timeseries(site.lon, site.lat, site.start, site.end, pcb)
		print("\n", end = "")

		# Write csv to disk.
		data.to_csv("%s/%s.csv" % (_OUT_DIR, site.name), index = False)
