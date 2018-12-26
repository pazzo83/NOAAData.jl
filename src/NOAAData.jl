__precompile__(true)
module NOAAData

using HTTP
using DataFrames
using IndexedTables
using Dates
using JSON

import Base.string, Base.get
import DataFrames.DataFrame
import IndexedTables.NDSparse

export NOAA, GHCND, GSOM, get, DataFrame, NDSparse, getstations, getlocations

struct NOAA
  token::String
end

abstract type NOAADataSet end

struct GHCND <: NOAADataSet end
struct GSOM <: NOAADataSet end

_divide_10(x::Float64) = x / 10.0
_divide_100(x::Float64) = x / 100.0
const SCHEMAS = Dict{String, Tuple{Vector{Union{DataType, Union}}, Vector{Symbol}}}(
  "GHCND" => ([Date, Union{Float64, Nothing}, DateTime, DateTime, Union{Float64, Nothing}, Union{Float64, Nothing}, 
              Union{Float64, Nothing}, Union{Float64, Nothing}, Union{Float64, Nothing}, Union{Float64, Nothing}, 
              Union{Float64, Nothing}, Union{Float64, Nothing}, Union{Float64, Nothing}, Union{Float64, Nothing}, Union{String, Nothing}],
              [:DATE, :AWND, :FMTM, :PGTM, :PRCP, :SNOW, :SNWD, :TMAX, :TMIN, :TAVG, :WDF2, :WDF5, :WSF2, :WSF5, :WT]),
  "GSOM" => ([Date, Union{Float64, Nothing}, Union{Int, Nothing}, Union{Int, Nothing}, Union{Int, Nothing}, Union{Int, Nothing}, 
              Union{Int, Nothing}, Union{Int, Nothing}, Union{Int, Nothing}, Union{Int, Nothing}, Union{Int, Nothing}, 
              Union{Int, Nothing}, Union{Int, Nothing}, Union{Int, Nothing}, Union{Int, Nothing}, Union{Float64, Nothing}, 
              Union{Int, Nothing}, Union{Int, Nothing}, Union{Float64, Nothing}, Union{Float64, Nothing}, Union{Float64, Nothing},
              Union{Float64, Nothing}, Union{Float64, Nothing}, Union{Int, Nothing}, Union{Float64, Nothing}, Union{Float64, Nothing}, 
              Union{Float64, Nothing}, Union{Float64, Nothing}, Union{Int, Nothing}, Union{Int, Nothing}, Union{Float64, Nothing}, 
              Union{Int, Nothing}, Union{Float64, Nothing}, Union{Int, Nothing}, Union{Int, Nothing}, Union{Int, Nothing}, 
              Union{Float64, Nothing}, Union{Float64, Nothing}, Union{Float64, Nothing}],
              [:DATE, :AWND, :CDSD, :CLDD, :DP01, :DP05, :DP10, :DP1X, :DSND, :DSNW, :DT00, :DT32, :DX32, :DX70, :DX90, :EMNT, :EMSD, :EMSN, :EMXP,
              :EMXT, :HDSD, :HTDD, :PRCP, :SNOW, :TAVG, :TMAX, :TMIN, :PSUN, :TSUN, :WDFM, :WSFM, :WDFG, :WSFG, :WDF1, :WDF2, :WDF5, :WSF1, :WSF2, :WSF5])
)

const CONVERTER_GHCND = Dict{Symbol, Function}(
  :TMAX => _divide_10,
  :TMIN => _divide_10,
  :TAVG => _divide_10,
  # :SNOW => _divide_10,
  :PRCP => _divide_10
)

const CONVERTER_GSOM = Dict{Symbol, Function}()

_get_converter(::GHCND) = CONVERTER_GHCND
_get_converter(::GSOM) = CONVERTER_GSOM

const DATETIMEFORMAT = Dates.DateFormat("y-m-d HHMM")
const DATEFORMAT = Dates.DateFormat("y-m-d")

_flt_identity(v::Float64) = v
_flt_identity(::Nothing) = nothing
# _defaultval(::Type{Float64}, ::Date) = 0.0
# _defaultval(::Type{Int}, ::Date) = 0
# _defaultval(::Type{String}, ::Date) = ""
_defaultval(::Type, ::Date) = nothing
_defaultval(::Type{Date}, d::Date) = d
_defaultval(::Type{DateTime}, d::Date) = DateTime(d)

_conversion(::Type{Union{Nothing, String}}, ::NOAADataSet, v::Dict, ::Symbol) = v["datatype"]

_flt_conversion(v) = float(v)
_flt_conversion(v::Nothing) = nothing
function _conversion(::Type{Union{Nothing, Float64}}, ds::NOAADataSet, v::Dict, symb::Symbol)
  return get(_get_converter(ds), symb, _flt_identity)(_flt_conversion(v["value"]))
end

_int_conversion(v) = parse(Int, v)
_int_conversion(v::Int) = v
_int_conversion(v::Float64) = round(Int, v)
_int_conversion(v::Nothing) = nothing
_conversion(::Type{Union{Nothing, Int}}, ::NOAADataSet, v::Dict, ::Symbol) = _int_conversion(v["value"])

function _conversion(::Type{DateTime}, ::NOAADataSet, v::Dict, ::Symbol)
  tm = string(v["value"])
  if length(tm) == 3
    tm = "0" * tm
  end
  currdate = split(v["date"], "T")[1]
  return DateTime(currdate * " " * tm, DATETIMEFORMAT)
end

extendval(existingval::String, val::String) = existingval * " " * val

_get_schema(::GHCND) = SCHEMAS["GHCND"]
_get_schema(::GSOM) = SCHEMAS["GSOM"]

_check_date_range(::GHCND, d1::Date, d2::Date) = (d2 - d1).value < 366
_check_date_range(::GSOM, d1::Date, d2::Date) = (d2 - d1).value < 1830

function check_date_range(ds::NOAADataSet, d1::Date, d2::Date)
  if d1 > d2
    return false
  end
  return _check_date_range(ds, d1, d2)
end

string(::GHCND) = "GHCND"
string(::GSOM) = "GSOM"

struct NOAADataResult{D <: NOAADataSet}
  data::Vector
  count::Int
  dataset::D
  stationid::String
end

function getresults(baseurl::String, headers::Dict{String, String}, query::Dict{String, String})
  result = []
  offset = 1
  while true
    resp = HTTP.get(baseurl, headers; query=query)
    js = JSON.parse(String(resp.body))
    if length(js["results"]) == 1000
      # we probably maxed out, need to get a new query
      append!(result, js["results"])
      offset += 1000
      query["offset"] = string(offset)
    else
      append!(result, js["results"])
      break
    end
  end

  return result
end

function get(ds::NOAADataSet, noaa::NOAA, startdate::Date, enddate::Date, stationid::String)
  check_date_range(ds, startdate, enddate) || error("Invalid date range")
  baseurl = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
  query =  Dict{String, String}()
  query["datasetid"] = string(ds)
  query["stationid"] = stationid
  query["startdate"] = string(startdate)
  query["enddate"] = string(enddate)
  query["limit"] = "1000"
  headers = Dict{String, String}()
  headers["token"] = noaa.token

  result = getresults(baseurl, headers, query)

  return NOAADataResult(result, length(result), ds, stationid)
end

function _process_data(result::NOAADataResult)
  schema = _get_schema(result.dataset)
  data = result.data
  cols = NamedTuple{Tuple(schema[2])}(dt[] for dt =schema[1])
  i = 1
  rowiter = 1
  startingdate = Date(split(data[i]["date"], "T")[1], DATEFORMAT)
  while i <= result.count
    currdate = Date(split(data[i]["date"], "T")[1], DATEFORMAT)
    if currdate != startingdate
      # check for blanks
      for col = cols
        if length(col) != rowiter
          push!(col, _defaultval(eltype(col), startingdate))
        end
      end
      startingdate = currdate
      rowiter += 1
    end
    coliter = Symbol(data[i]["datatype"])
    coltoadd = get(cols, coliter, cols[end])
    val = _conversion(eltype(coltoadd), result.dataset, data[i], coliter)
    if length(coltoadd) == rowiter
      # value already exists
      coltoadd[rowiter] = extendval(coltoadd[rowiter], val)
    else
      push!(coltoadd, val)
    end
    i += 1
  end
  # final cleanup
  for col = cols
    if length(col) != rowiter
      push!(col, _defaultval(eltype(col), startingdate))
    end
  end
  return cols, schema
end

function NDSparse(result::NOAADataResult)
  cols, schema = _process_data(result)
  return NDSparse(Columns(cols[1]; names=schema[2][1:1]), Columns(cols[2:end]...; names=schema[2][2:end]))
end

function DataFrame(result::NOAADataResult)
  cols, schema = _process_data(result)
  return DataFrame(cols, schema[2])
end

## station queries

function getstations(
    noaa::NOAA; 
    dataset::Union{NOAADataSet, Nothing} = nothing, 
    locationid::Union{String, Nothing} = nothing, 
    extent::Union{Vector{Float64}, Nothing} = nothing,
    startdate::Union{Date, Nothing} = nothing,
    enddate::Union{Date, Nothing} = nothing,
    datacategoryid::Union{String, Nothing} = nothing
  )
  baseurl = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations"
  query = Dict{String, String}()
  if dataset != nothing
    query["datasetid"] = string(dataset)
  end

  if locationid != nothing
    query["locationid"] = locationid
  end

  if extent != nothing
    query["extent"] = join(extent, ",")
  end

  if startdate != nothing
    query["startdate"] = string(startdate)
  end

  if enddate != nothing
    query["enddate"] = string(enddate)
  end

  if datacategoryid != nothing
    query["datacategoryid"] = datacategoryid
  end

  query["limit"] = "1000"

  headers = Dict{String, String}()
  headers["token"] = noaa.token

  result = getresults(baseurl, headers, query)

  # build dataframe from list of dicts
  DataFrame(
    id = getindex.(result, "id"), 
    name = getindex.(result, "name"), 
    latitude = getindex.(result, "latitude"), 
    longitude = getindex.(result, "longitude"), 
    mindate = Date.(getindex.(result, "mindate")), 
    maxdate = Date.(getindex.(result, "maxdate")),
    elevation = getindex.(result, "elevation"),
    elevation_unit = getindex.(result, "elevationUnit"),
    datacoverage = getindex.(result, "datacoverage")
  )
end

## location queries

function getlocations(
    noaa::NOAA; 
    dataset::Union{NOAADataSet, Nothing} = nothing, 
    locationcategoryid::Union{String, Nothing} = nothing, 
    extent::Union{Vector{Float64}, Nothing} = nothing,
    startdate::Union{Date, Nothing} = nothing,
    enddate::Union{Date, Nothing} = nothing,
    datacategoryid::Union{String, Nothing} = nothing
  )
  baseurl = "https://www.ncdc.noaa.gov/cdo-web/api/v2/locations"
  query = Dict{String, String}()
  if dataset != nothing
    query["datasetid"] = string(dataset)
  end

  if locationcategoryid != nothing
    query["locationcategoryid"] = locationcategoryid
  end

  if extent != nothing
    query["extent"] = join(extent, ",")
  end

  if startdate != nothing
    query["startdate"] = string(startdate)
  end

  if enddate != nothing
    query["enddate"] = string(enddate)
  end

  if datacategoryid != nothing
    query["datacategoryid"] = datacategoryid
  end

  query["limit"] = "1000"

  headers = Dict{String, String}()
  headers["token"] = noaa.token

  result = getresults(baseurl, headers, query)

  # build dataframe from list of dicts
  DataFrame(
    id = getindex.(result, "id"), 
    name = getindex.(result, "name"), 
    mindate = Date.(getindex.(result, "mindate")), 
    maxdate = Date.(getindex.(result, "maxdate")),
    datacoverage = getindex.(result, "datacoverage")
  )
end

end # module
