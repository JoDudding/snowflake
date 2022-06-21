--------------------------------------------------------------------------------
-- Citibike Zero to Snowflake training
-- Jo Dudding
-- June 2022
-- https://quickstarts.snowflake.com/guide/getting_started_with_snowflake/index.html
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 4. Preparing to Load Data
--------------------------------------------------------------------------------

-- create the file layout
create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

# list the citibike files
list @citibike_trips;

--create file format
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';
  
--verify file format is created
show file formats in database citibike;  

--------------------------------------------------------------------------------
-- 5. Loading Data
--------------------------------------------------------------------------------

-- load data
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

-- clear the table as we try again with a larger warehouse
truncate table trips;

--verify table is clear
select * from trips limit 10;

--change warehouse size from small to large (4x)
alter warehouse compute_wh set warehouse_size='large';

--load data with large warehouse
show warehouses;

-- load data (but faster)
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

--------------------------------------------------------------------------------
-- 6. Working with Queries, the Results Cache, & Cloning
--------------------------------------------------------------------------------

-- see a sample of the data
select * from trips limit 20;

-- basic hourly statistics
select 
  date_trunc('hour', starttime) as "date",
  count(*) as "num trips",
  avg(tripduration)/60 as "avg duration (mins)",
  avg(haversine(start_station_latitude, start_station_longitude, 
    end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 
order by 1;

-- run acaing to check the result cache
select 
  date_trunc('hour', starttime) as "date",
  count(*) as "num trips",
  avg(tripduration)/60 as "avg duration (mins)",
  avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, 
    end_station_longitude)) as "avg distance (km)"
from trips
group by 1 
order by 1;

-- which month is the busiest
select
  monthname(starttime) as "month",
  count(*) as "num trips"
from trips
group by 1 
order by 2 desc;

-- create a cone of the table
create table trips_dev clone trips;

--------------------------------------------------------------------------------
-- 7. Working with Semi-Structured Data, Views, & Joins
--------------------------------------------------------------------------------

-- create a database for weather
create database weather;

-- set worksheet context
use role sysadmin;

use warehouse compute_wh;

use database weather;

use schema public;

-- create the table
create table json_weather_data (v variant);

-- create a stage
create stage nyc_weather
url = 's3://snowflake-workshop-lab/weather-nyc';

-- look at the stage
list @nyc_weather;

-- load data
copy into json_weather_data
from @nyc_weather
file_format = (type=json);

-- look at what was loaded
select * from json_weather_data limit 10;

-- create a view for new york city
create view json_weather_data_view as
select
v:time::timestamp as observation_time,
v:city.id::int as city_id,
v:city.name::string as city_name,
v:city.country::string as country,
v:city.coord.lat::float as city_lat,
v:city.coord.lon::float as city_lon,
v:clouds.all::int as clouds,
(v:main.temp::float)-273.15 as temp_avg,
(v:main.temp_min::float)-273.15 as temp_min,
(v:main.temp_max::float)-273.15 as temp_max,
v:weather[0].main::string as weather,
v:weather[0].description::string as weather_desc,
v:weather[0].icon::string as weather_icon,
v:wind.deg::float as wind_dir,
v:wind.speed::float as wind_speed
from json_weather_data
where city_id = 5128638;

-- run a query against the view
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

-- add the weather data to the trips data and summarise
select 
  weather as conditions,
  count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
  on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 
order by 2 desc;

--------------------------------------------------------------------------------
-- 8. Using Time Travel
--------------------------------------------------------------------------------

-- drop a table
drop table json_weather_data;

-- run query against the dropped table - does not exist
select * from json_weather_data limit 10;

-- undrop the table
undrop table json_weather_data;

--verify table is undropped
select * from json_weather_data_view limit 10;

-- switch to citibike data
use role sysadmin;
use warehouse compute_wh;
use database citibike;
use schema public;

-- replace station name with 'oops'
update trips set start_station_name = 'oops';

-- run a query for top stations
select
  start_station_name as "station",
  count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

-- find the query_id of the last update
-- 01a51623-3200-f6c6-0000-0000c7e52105
set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time limit 1);

-- time travel to recreate table
create or replace table trips as
(select * from trips before (statement => $query_id));

-- run a query for top stations to check it has been restored
select
  start_station_name as "station",
  count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

--------------------------------------------------------------------------------
-- 9. Working with Roles, Account Admin, & Account Usage
--------------------------------------------------------------------------------

-- select the role to use
use role accountadmin;

-- create a new role as grant to me
create role junior_dba;
grant role junior_dba to user JODUZ;

-- switch to this role
use role junior_dba;

-- switch to account admin to grant usage access to the databases
use role accountadmin;
grant usage on database citibike to role junior_dba;
grant usage on database weather to role junior_dba;

-- switch to the new role to check databases are available
use role junior_dba;

--------------------------------------------------------------------------------
-- 11. Resetting Your Snowflake Environment
--------------------------------------------------------------------------------

-- switch to the right role
use role accountadmin;

-- drop objects created in the lab
drop share if exists zero_to_snowflake_shared_data;
drop database if exists citibike;
drop database if exists weather;
drop warehouse if exists analytics_wh;
drop role if exists junior_dba;
