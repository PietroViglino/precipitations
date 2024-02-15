INFORMATIONS ABOUT THE CSV FILES:

File names are structured as follows:

    (station name)_(wmo id)_(latitude in decimal degrees)_(longitude in decimal degrees)_(elevation in meters).csv

if wmo id is missing it will be = null
if elevation is missing it will be = -999.9

The column names inside the csv files are the following:

    TIME, PRCP, TAVG, SNOW

The column TIME will be expressed in ISO format.

if:

PRCP (precipitation expressed in tenths of mm),
TAVG(average daily temperature),
SNOW(snowfall expressed in mm)

are missing they will have value = null