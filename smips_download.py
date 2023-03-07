#!/usr/bin/env python
#%pip install owslib shapely matplotlib

from owslib.wps import WebProcessingService, ComplexDataInput, monitorExecution
from owslib import wps
import matplotlib
from matplotlib import pyplot
from shapely.geometry import shape
import json
import pandas as pd
import rasterio
import logging
import sys
import io
from ipyleaflet import Map, GeoJSON, basemaps
import datetime
import smips_sites
from smips_common import *
from typing import Callable

_ENDPOINT="https://funcwps.ternlandscapes.org.au/wps/"
_TEMPORAL_DRILL = "temporalDrill"
_SLEEP = 100 # Sleep interval in ms

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

# Date format used to pass dates to the PyWPS server
_API_DATE_FORMAT = r"%Y-%m-%d"

# Format of dates returned by the server.
_SERVER_DATE_FORMAT = r"%Y-%m-%dT%H:%M:%S%z"

# Format used when writing dates to output files.
_OUTPUT_DATE_FORMAT = r"%Y-%m-%d"

# Name of the date column in the data returned from the server.
_COL_DATE = "date"

# Set up owslib logging, to set relevant output
_logger = logging.getLogger("owslib")
_logger.setLevel(logging.WARNING)

_handler = logging.StreamHandler(sys.stdout)
_handler.setLevel(logging.WARNING)
_logger.addHandler(_handler)

# Define a connection to the server.
_server = WebProcessingService(_ENDPOINT, verbose=False)

def describe_server(server: WebProcessingService):
	"""
	List all processes and operations supported by the srever. 
	"""
	print('WPS Server: type            = %s' % server.identification.type)
	print('WPS Server: title           = %s' % server.identification.title)
	print('WPS Server: abstract        = %s\n' % server.identification.abstract)

	for operation in server.operations:
		print('WPS Operation: name     = %s' % operation.name)
	print("\n")

	for process in server.processes:
		print('WPS Process: identifier = %s' % process.identifier)
		print("WPS Process: title      = %s" % process.title)
		print("WPS Process: abstract   = %s\n" % process.abstract)

def describe_process(server, process_name):
	"""
	Describe the process on the server with the specified name.
	"""
	process = server.describeprocess(server, process_name)

	print('WPS Process: identifier         = %s' % process.identifier)
	print('WPS Process: title              = %s' % process.title)
	print('WPS Process: abstract           = %s\n' % process.abstract)

	for input in process.dataInputs:
		print("Process input: identifier   = %s" % input.identifier)
		print("Process input: data type    = %s" % input.dataType)
		print("Process input: minOccurs    = %d" % input.minOccurs)
		print("Process input: maxOccurs    = %d\n" % input.maxOccurs)

	for output in process.processOutputs:
		print("Process output: identifier  = %s" % output.identifier)
		print("Process output: data type   = %s\n" % output.dataType)

# Optional - visualise shapes.
# m = Map(basemap=basemaps.OpenStreetMap.Mapnik, center=[-25.128, 135.588], zoom=4)
# m.add_layer(GeoJSON(data=json.loads(MY_POLYGON)))
# m

def get_schema(schema_type: str) -> str:
	"""
	Get a schema for the given schema type.

	@param schema_type: The schema type (typically one of the _SCHEMA_* constants).
	"""
	return "%s#%s" % (_SCHEMA_BASE, schema_type)

def get_geo_json(lon: float, lat: float):
	"""
	Get a GeoJSON representation of the specified latitude and longitude.
	"""
	return '{ "type": "Point", "coordinates": [%.2f, %.2f] }' % (lon, lat)

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

	schema = get_schema(_SCHEMA_POINT)

	# Create request.
	polygon = get_geo_json(lon, lat)
	point = ComplexDataInput(polygon, mimeType = _MIME_GEO_JSON, schema = schema)

	start_str = start.strftime(_API_DATE_FORMAT)
	end_str = end.strftime(_API_DATE_FORMAT)

	# Submit request (will execute asynchronously).
	resp = _server.execute(
		_TEMPORAL_DRILL,
		mode="async",
		inputs=[("datasetId", f"{_DATASET_SMIPS}:{_LAYER_SW}"), ("point", point), ("startDate", start_str), ("endDate", end_str)],
		output=[("csv", False, "text/csv")]  # Get stats as embedded file in result
	)

	# Wait for request to complete.
	while not resp.isComplete():
		resp.checkStatus(sleepSecs = _SLEEP / _MS_PER_SEC)
		pcb(resp.percentCompleted)
	pcb(100.0)

	# Read response.
	csv_output = resp.processOutputs[0]

	data = "".join(d for d in csv_output.data)
	df = pd.read_csv(io.StringIO(data))
	df = df.sort_values(_COL_DATE, ascending = True)
	df[_COL_DATE] = df[_COL_DATE].apply(lambda x: datetime.datetime.strptime(x, _SERVER_DATE_FORMAT).strftime(_OUTPUT_DATE_FORMAT))
	return df

end_date = datetime.datetime.today().date()
for site in smips_sites.sites:
	eol = "\r" if sys.stdout.isatty() else "\n"
	pcb = lambda x: print("Downloading %s: %.2f%%" % (site.name, x), end = eol)

	data = download_timeseries(site.lon, site.lat, site.start, end_date, pcb)
	print("\n", end = "")

	# Write csv to disk.
	data.to_csv("out/%s.csv" % site.name, index = False)
