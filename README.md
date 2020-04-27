# NYC_RE_Sales

This repository includes a collection of scripts used to generate layers for the NYC Geocoded Real Estate Sales series. Annual sales records from the NYC Dept of Finance are collated from spreadsheets, geocoded to match street addresses or blocks and lots using the city's geocoding client, and exported out as a shapefile for a single year and as a spatial table in a Spatialite / SQLite database that contains sales for several years.

https://www.baruch.cuny.edu/confluence/display/geoportal/NYC+Geocoded+Real+Estate+Sales

The original script (RE_script_py2) was written in python 2 as the city's geocoding module nyc-geoclient was written in that version. A new version (RE_script_py3) was written in python 3 to utilize the city's newer python-geoclient. Because the new geoclient uses different procedures for handling non-matching records (an error is returned instead of an empty record), the geocoding function in the latest script had to be completely revised. 
