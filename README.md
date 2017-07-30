# NOAAData

[![Build Status](https://travis-ci.org/pazzo83/NOAAData.jl.svg?branch=master)](https://travis-ci.org/pazzo83/NOAAData.jl)

[![Coverage Status](https://coveralls.io/repos/pazzo83/NOAAData.jl/badge.svg?branch=master&service=github)](https://coveralls.io/github/pazzo83/NOAAData.jl?branch=master)

[![codecov.io](http://codecov.io/github/pazzo83/NOAAData.jl/coverage.svg?branch=master)](http://codecov.io/github/pazzo83/NOAAData.jl?branch=master)

This package is a wrapper for NOAA observation data.  Currently two data sets are supported:
* GHCND - Daily observations
* GSOM - Monthly observations

Note: This package only supports Julia v0.6

### Install
```julia
Pkg.add("NOAAData")
```

### Usage
Usage is fairly simple - an API key from NOAA is required: <https://www.ncdc.noaa.gov/cdo-web/token>

First, create the NOAA data object:
```julia
apikey = "<your api key here>"
noaa = NOAA(apikey)
```

Let's get some daily ob data from Central Park, NY (You must use the GHCND station id, which for Central Park is: GHCND:USW00094728)
```julia
stationid = "GHCND:USW00094728"
startdate = Date(2015, 1, 1)
enddate = Date(2015, 12, 31)
```
The main method to get data is: get_data_set:
```julia
results = get(GHCND(), noaa, startdate, enddate, stationid)
```

This returns a NOAADataResult object, which you can use to generated more meaningful data structures.
Currently this package supports DataTables and IndexedTables:
```julia
dt = DataTable(results)
it = IndexedTable(results)
```

See those packages for more info on working with data sets in those structures.

More info about the API and the data fields is available here: <https://www.ncdc.noaa.gov/cdo-web/webservices/v2>
