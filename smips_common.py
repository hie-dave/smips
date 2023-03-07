import datetime

# Date format used when when converting strings to dates.
_DATE_FORMAT = r"%Y-%m-%d"

class Site:
	def __init__(self, lon: float, lat: float, start: str, end: str, name: str):
		"""
		Create a new Site instance.

		@param lon: Longitude.
		@param lat: Latitude.
		@param start: Start date in "yyyy-MM-dd" format.
		@param end: End date in "yyyy-MM-dd" format.
		@param name: Name of the site (optional).
		"""
		self.lon = lon
		self.lat = lat
		self.start = datetime.datetime.strptime(start, _DATE_FORMAT)
		self.end = datetime.datetime.strptime(end, _DATE_FORMAT)
		self.name = name
