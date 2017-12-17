using NOAAData
using Base.Test

# write your own tests here
TOKEN = "QGdQVDottDzkuRlaZbtAtRRRWyNrBZiE"
stationid = "GHCND:USW00094728"
noaa = NOAA(TOKEN)

startdate = Date(2015, 1, 1)
enddate = Date(2015, 12, 31)

results = get(GHCND(), noaa, startdate, enddate, stationid)
@test results.count == 3873

df = DataFrame(results)
@test size(df) == (365, 14)
@test typeof(df) == DataFrames.DataFrame
@test df[:TMAX][1] == 3.9

it = NDSparse(results)
@test length(it) == 365
@test typeof(it) == IndexedTables.NDSparse{NamedTuples._NT_AWND_FMTM_PGTM_PRCP_SNOW_SNWD_TMAX_TMIN_WDF2_WDF5_WSF2_WSF5_WT{Float64,DateTime,DateTime,Float64,Float64,
                    Float64,Float64,Float64,Float64,Float64,Float64,Float64,String},Tuple{Date},IndexedTables.Columns{NamedTuples._NT_DATE{Date},
                    NamedTuples._NT_DATE{Array{Date,1}}},IndexedTables.Columns{NamedTuples._NT_AWND_FMTM_PGTM_PRCP_SNOW_SNWD_TMAX_TMIN_WDF2_WDF5_WSF2_WSF5_WT{
                        Float64,DateTime,DateTime,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,String},
                    NamedTuples._NT_AWND_FMTM_PGTM_PRCP_SNOW_SNWD_TMAX_TMIN_WDF2_WDF5_WSF2_WSF5_WT{Array{Float64,1},Array{DateTime,1},Array{DateTime,1},Array{Float64,1},
                    Array{Float64,1},Array{Float64,1},Array{Float64,1},Array{Float64,1},Array{Float64,1},Array{Float64,1},Array{Float64,1},Array{Float64,1},Array{String,1}}}}
@test it.data.columns[:TMAX][1] == 3.9

# aggregation with indexed tables
# max temp
maxtemp = IndexedTables.convertdim(map(IndexedTables.pick(:TMAX), it), 1, Dates.month, agg=max)
@test length(maxtemp) == 12
@test maxtemp[2] == 6.1

# mean high temp
meantemp = IndexedTables.convertdim(map(IndexedTables.pick(:TMAX), it), 1, Dates.month, vecagg=mean)
@test length(meantemp) == 12
@test meantemp[5] == 25.761290322580642

# let's try a new data type
startdate = Date(2011, 1, 1)
results = get(GSOM(), noaa, startdate, enddate, stationid)

@test results.count == 1739

df = DataFrame(results)
@test size(df) == (60, 39)

it = NDSparse(results)
@test length(it.data) == 60

# Test errors
startdate = Date(1985, 1, 1)
@test_throws ErrorException get(GSOM(), noaa, startdate, enddate, stationid)
@test_throws ErrorException get(GHCND(), noaa, startdate, enddate, stationid)
