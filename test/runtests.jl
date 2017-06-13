using NOAAData
using Base.Test

# write your own tests here
TOKEN = "QGdQVDottDzkuRlaZbtAtRRRWyNrBZiE"
stationid = "GHCND:USW00094728"
noaa = NOAA(TOKEN)

startdate = Date(2015, 1, 1)
enddate = Date(2015, 12, 31)

results = get_data_set(GHCND(), noaa, startdate, enddate, stationid)
@test results.count == 3873

dt = result_to_datatable(results)
@test size(dt) == (365, 14)
@test typeof(dt) == DataTables.DataTable
@test dt[:TMAX][1] == 3.9

it = result_to_indexed_table(results)
@test length(it) == 365
@test typeof(it) == IndexedTables.IndexedTable{NamedTuples._NT_AWND_FMTM_PGTM_PRCP_SNOW_SNWD_TMAX_TMIN_WDF2_WDF5_WSF2_WSF5_WT{Float64,DateTime,DateTime,Float64,Float64,Float64,
                    Float64,Float64,Float64,Float64,Float64,Float64,String},Tuple{Date},NamedTuples._NT_DATE{Array{Date,1}},IndexedTables.Columns{
                    NamedTuples._NT_AWND_FMTM_PGTM_PRCP_SNOW_SNWD_TMAX_TMIN_WDF2_WDF5_WSF2_WSF5_WT{Float64,DateTime,DateTime,Float64,Float64,Float64,Float64,Float64,
                    Float64,Float64,Float64,Float64,String},NamedTuples._NT_AWND_FMTM_PGTM_PRCP_SNOW_SNWD_TMAX_TMIN_WDF2_WDF5_WSF2_WSF5_WT{Array{Float64,1},
                    Array{DateTime,1},Array{DateTime,1},Array{Float64,1},Array{Float64,1},Array{Float64,1},Array{Float64,1},Array{Float64,1},Array{Float64,1},
                    Array{Float64,1},Array{Float64,1},Array{Float64,1},Array{String,1}}}}
@test it.data.columns[:TMAX][1] == 3.9

# aggregation with indexed tables
# max temp
maxtemp = IndexedTables.convertdim(IndexedTables.columns(it, :TMAX), 1, Dates.month, agg=max)
@test length(maxtemp) == 12
@test maxtemp[2] == 6.1

# mean high temp
meantemp = IndexedTables.convertdim(IndexedTables.columns(it, :TMAX), 1, Dates.month, vecagg=mean)
@test length(meantemp) == 12
@test meantemp[5] == 25.761290322580642

# let's try a new data type
startdate = Date(2011, 1, 1)
results = get_data_set(GSOM(), noaa, startdate, enddate, stationid)

@test results.count == 1739

dt = result_to_datatable(results)
@test size(dt) == (60, 38)

it = result_to_indexed_table(results)
@test length(it.data) == 60

# Test errors
startdate = Date(1985, 1, 1)
@test_throws ErrorException get_data_set(GSOM(), noaa, startdate, enddate, stationid)
@test_throws ErrorException get_data_set(GHCND(), noaa, startdate, enddate, stationid)
