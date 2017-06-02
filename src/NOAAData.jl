module NOAAData

using Requests
using DataTables
using IndexedTables
# https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&locationid=ZIP:10280&startdate=2010-05-01&enddate=2010-05-01&token=QGdQVDottDzkuRlaZbtAtRRRWyNrBZiE

_divide_10(x::Float64) = x / 10.0
_divide_100(x::Float64) = x / 100.0
const SCHEMAS = Dict{String, Tuple{Vector{DataType}, Vector{Symbol}}}(
  "GHCND" => ([Date, Float64, DateTime, DateTime, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, String],
              [:DATE, :AWND, :FMTM, :PGTM, :PRCP, :SNOW, :SNWD, :TMAX, :TMIN, :WDF2, :WDF5, :WSF2, :WSF5, :WT])
)

const CONVERTER = Dict{Symbol, Function}(
  :TMAX => _divide_10,
  :TMIN => _divide_10,
  :SNOW => _divide_10,
  :PRCP => _divide_10
)

const DATETIMEFORMAT = Dates.DateFormat("y-m-d HHMM")
const DATEFORMAT = Dates.DateFormat("y-m-d")

_flt_identity(v::Float64) = v
_defaultval(::Type{Float64}, ::Date) = 0.0
_defaultval(::Type{String}, ::Date) = ""
_defaultval(::Type{Date}, d::Date) = d
_defaultval(::Type{DateTime}, d::Date) = DateTime(d)

_conversion(::Type{String}, v::Dict, ::Vector{Symbol}, ::Int) = v["datatype"]

function _conversion(::Type{Float64}, v::Dict, symbarr::Vector{Symbol}, i::Int)
  return get(CONVERTER, symbarr[i], _flt_identity)(float(v["value"]))
end

function _conversion(::Type{DateTime}, v::Dict, ::Vector{Symbol}, ::Int)
  tm = string(v["value"])
  if length(tm) == 3
    tm = "0" * tm
  end
  currdate = split(v["date"], "T")[1]
  return DateTime(currdate * " " * tm, DATETIMEFORMAT)
end

extendval(existingval::String, val::String) = existingval * " " * val

struct NOAA
  token::String
end

abstract type NOAADataSet end

struct GHCND <: NOAADataSet end

_get_schema(::GHCND) = SCHEMAS["GHCND"]

struct NOAADataResult{D <: NOAADataSet}
  data::Vector
  count::Int
  dataset::D
  stationid::String
end

function get_data_set(::GHCND, noaa::NOAA, startdate::Date, enddate::Date, stationid::String)
  baseurl = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
  query =  Dict{String, String}()
  query["datasetid"] = "GHCND"
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

  return NOAADataResult(result, length(result), GHCND(), stationid)
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
    val = _conversion(schema[1][coliter], data[i], schema[2], coliter)
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

function result_to_indexed_table(result::NOAADataResult)
  cols, schema = _process_data(result)
  return IndexedTable(Columns(cols[1]; names=schema[2][1:1]), Columns(cols[2:end]...; names=schema[2][2:end]))
end

function result_to_datatable(result::NOAADataResult)
  cols, schema = _process_data(result)
  return DataTable(cols, schema[2])
end

end # module
