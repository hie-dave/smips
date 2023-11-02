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
_OUT_DIR_BASE = "out"

# Output directory for ET layer.
_OUT_DIR_ET = "et"

# Output directory for SW index layer.
_OUT_DIR_SWINDEX = "swindex"

# Output directory for SW layer.
_OUT_DIR_SW = "sw"

# Set up owslib logging, to set relevant output
_logger = logging.getLogger("owslib")
_logger.setLevel(logging.WARNING)

_handler = logging.StreamHandler(sys.stdout)
_handler.setLevel(logging.WARNING)
_logger.addHandler(_handler)

# Define a connection to the server.
_server = WebProcessingService(_ENDPOINT)

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
	, dataset: str, layer: str, pcb: Callable[[float], None]):
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
	process = _TEMPORAL_DRILL
	resp = _server.execute(
		process,
		mode = _MODE_ASYNC,
		lineage = True,
		inputs=[("datasetId", f"{dataset}:{layer}"), ("startDate", start_str), ("endDate", end_str), ("point", point)],
		output=[("csv", False, "text/csv")]  # Get stats as embedded file in result
		# output = [("download_link", False, None)]
	)

	# Wait for request to complete.
	while not resp.isComplete():
		resp.checkStatus(sleepSecs = _SLEEP / _MS_PER_SEC)
		pcb(resp.percentCompleted)
	pcb(100.0)

	# Read response.
	csv_outputs = [x for x in resp.processOutputs if x.identifier == "csv"]
	if len(csv_outputs) < 1:
		raise ValueError(f"{process} did not return a csv output")
	csv_output = csv_outputs[0]
	text = "".join(d for d in csv_output.data)

	# Parse data into a dataframe.
	data = pd.read_csv(io.StringIO(text))

	# Order by date ascending.
	data = data.sort_values(_COL_DATE, ascending = True)

	# Change date format to yyyy-MM-dd.
	data[_COL_DATE] = data[_COL_DATE].apply(lambda x: datetime.datetime.strptime(x, _SERVER_DATE_FORMAT).strftime(_OUTPUT_DATE_FORMAT))
	return data

def download_product(dataset: str, layer: str, out_dir: str, show_progress: bool):
	os.makedirs(out_dir, exist_ok = True)

	for site in smips_sites.sites:
		if show_progress:
			pcb = lambda x: print("Downloading %s %s:%s: %.2f%%" % (site.name, dataset, layer, x), end = "\r")
		else:
			pcb = lambda _: ...
			print(f"Downloading {site.name} {dataset}:{layer}...")
		data = download_timeseries(site.lon, site.lat, site.start, site.end
			, dataset, layer, pcb)
		print("\n", end = "")

		# Write csv to disk.
		out_file = os.path.join(out_dir, f"{site.name}.csv")
		data.to_csv(out_file, index = False)

def main():
	"""
	Main CLI entrypoint function.
	"""
	report_progress = sys.stdout.isatty()
	products = [
		(_DATASET_SMIPS, _LAYER_SMINDEX, _OUT_DIR_SWINDEX),
		(_DATASET_SMIPS, _LAYER_SW, _OUT_DIR_SW),
		(_DATASET_AET, _LAYER_ET, _OUT_DIR_ET)
	]
	for dataset, layer, out_dir_name in products:
		out_dir = os.path.join(_OUT_DIR_BASE, out_dir_name)
		download_product(dataset, layer, out_dir, report_progress)

if __name__ == "__main__":
	main()
