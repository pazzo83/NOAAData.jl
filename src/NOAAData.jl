module NOAAData

using Requests
using DataFrames
using IndexedTables

import Base.string, Base.get
import DataFrames.DataFrame
import IndexedTables.NDSparse

export NOAA, GHCND, GSOM, get, DataFrame, NDSparse, selectdata, aggregate_vec

struct NOAA
  token::String
end

abstract type NOAADataSet end

struct GHCND <: NOAADataSet end
struct GSOM <: NOAADataSet end

_divide_10(x::Float64) = x / 10.0
_divide_100(x::Float64) = x / 100.0
const SCHEMAS = Dict{String, Tuple{Vector{DataType}, Vector{Symbol}}}(
  "GHCND" => ([Date, Float64, DateTime, DateTime, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, String],
              [:DATE, :AWND, :FMTM, :PGTM, :PRCP, :SNOW, :SNWD, :TMAX, :TMIN, :WDF2, :WDF5, :WSF2, :WSF5, :WT]),
  "GSOM" => ([Date, Float64, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Float64, Int, Int, Float64, Float64, Float64,
              Float64, Float64, Int, Float64, Float64, Float64, Float64, Int, Int, Float64, Int, Float64, Int, Int, Int, Float64, Float64, Float64],
              [:DATE, :AWND, :CDSD, :CLDD, :DP01, :DP05, :DP10, :DP1X, :DSND, :DSNW, :DT00, :DT32, :DX32, :DX70, :DX90, :EMNT, :EMSD, :EMSN, :EMXP,
              :EMXT, :HDSD, :HTDD, :PRCP, :SNOW, :TAVG, :TMAX, :TMIN, :PSUN, :TSUN, :WDFM, :WSFM, :WDFG, :WSFG, :WDF1, :WDF2, :WDF5, :WSF1, :WSF2, :WSF5])
)

const CONVERTER_GHCND = Dict{Symbol, Function}(
  :TMAX => _divide_10,
  :TMIN => _divide_10
  # :SNOW => _divide_10,
  # :PRCP => _divide_10
)

const CONVERTER_GSOM = Dict{Symbol, Function}()

_get_converter(::GHCND) = CONVERTER_GHCND
_get_converter(::GSOM) = CONVERTER_GSOM

const DATETIMEFORMAT = Dates.DateFormat("y-m-d HHMM")
const DATEFORMAT = Dates.DateFormat("y-m-d")

_flt_identity(v::Float64) = v
_defaultval(::Type{Float64}, ::Date) = 0.0
_defaultval(::Type{Int}, ::Date) = 0
_defaultval(::Type{String}, ::Date) = ""
_defaultval(::Type{Date}, d::Date) = d
_defaultval(::Type{DateTime}, d::Date) = DateTime(d)

_conversion(::Type{String}, ::NOAADataSet, v::Dict, ::Vector{Symbol}, ::Int) = v["datatype"]

function _conversion(::Type{Float64}, ds::NOAADataSet, v::Dict, symbarr::Vector{Symbol}, i::Int)
  return get(_get_converter(ds), symbarr[i], _flt_identity)(float(v["value"]))
end

_int_conversion(v) = parse(Int, v)
_int_conversion(v::Int) = v
_int_conversion(v::Float64) = round(Int, v)
_conversion(::Type{Int}, ::NOAADataSet, v::Dict, ::Vector{Symbol}, ::Int) = _int_conversion(v["value"])

function _conversion(::Type{DateTime}, ::NOAADataSet, v::Dict, ::Vector{Symbol}, ::Int)
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

  result = []
  while true
    js = Requests.json(get(baseurl, query=query, headers=headers))
    if length(js["results"]) == 1000
      # we probably maxed out, need to get a new query
      i = 1000
      latestdate = Date(split(js["results"][i]["date"], "T")[1], DATEFORMAT)
      currdate = latestdate
      while currdate == latestdate
        i -= 1
        currdate = Date(split(js["results"][i]["date"], "T")[1], DATEFORMAT)
      end
      append!(result, js["results"][1:i])
      query["startdate"] = string(latestdate)
      query["enddate"] = string(enddate)
    else
      append!(result, js["results"])
      break
    end
  end

  return NOAADataResult(result, length(result), ds, stationid)
end

function _process_data(result::NOAADataResult)
  schema = _get_schema(result.dataset)
  data = result.data
  cols = Vector[]
  indexlookup = Dict{String, Int}()
  numcols = length(schema[1])
  for i in eachindex(schema[1])
    dt = schema[1][i]
    push!(cols, dt[])
    indexlookup[string(schema[2][i])] = i
  end
  i = 1
  rowiter = 1
  startingdate = Date(split(data[i]["date"], "T")[1], DATEFORMAT)
  while i <= result.count
    currdate = Date(split(data[i]["date"], "T")[1], DATEFORMAT)
    if currdate != startingdate
      # check for blanks
      for col in cols
        if length(col) != rowiter
          push!(col, _defaultval(eltype(col), startingdate))
        end
      end
      startingdate = currdate
      rowiter += 1
    end
    coliter = get(indexlookup, data[i]["datatype"], numcols)
    val = _conversion(schema[1][coliter], result.dataset, data[i], schema[2], coliter)
    if length(cols[coliter]) == rowiter
      # value already exists
      cols[coliter][rowiter] = extendval(cols[coliter][rowiter], val)
    else
      push!(cols[coliter], val)
    end
    i += 1
  end
  # final cleanup
  for col in cols
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

# various indexed table f'ns for aggregating data
# function aggregate(f::Function, arr::IndexedTable, col::Symbol)
#   idxs, data = IndexedTables.aggregate_to(f, arr.index, arr.data.columns[col])
#   return IndexedTable(idxs, data, presorted=true, copy=false)
# end

# function aggregate_vec(f::Function, arr::IndexedTable, col::Symbol)
#   idxs, data = IndexedTables.aggregate_vec_to(f, arr.index, arr.data.columns[col])
#   return IndexedTable(idxs, data, presorted=true, copy=false)
# end

# function selectdata(it::IndexedTable, which::IndexedTables.DimName...)
#   IndexedTables.flush!(it)
#   IndexedTable(it.index, Columns(it.data.columns[[:TMAX]]))
# end

end # module
