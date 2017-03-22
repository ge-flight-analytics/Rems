# Rems
Rems is a R wrapper of EMS database API. If you are also interested in a Python wrapper for EMS API, visit <https://github.com/ge-flight-analytics/emspy>. 

With Rems package, you will be able to retrieve the EMS data from R without low-level knowledge of the EMS RESTful API. The project is still in very early alpha stage, so is not guarranteed working reliably nor well documented. I'll beef up the documentation as soon as possible. 

Any contribution is welcome!

Dependency: httr, jsonlite, RSQLite, DBI 

## v0.2 Major Changes
* User can select an API server.
* User can select an arbitrary database such as Flight, Operational, and Event databases. Only the EMS Flight database was accessible in v0.1.
* Meta-data (EMS data/database tree structures) is stored in SQLite format instead of R's own RDS format. This means better reusability of the meta-data. You can use the same meta-data file from emsPy, the Python wrapper version without generating one separately for emsPy. 


## Installation
```r
install.packages("devtools")
devtools::install_git("https://github.com/ge-flight-analytics/Rems.git", branch = "v0.2")
```
## Connect to EMS
<!-- <http://rmarkdown.rstudio.com>**Knit**`echo = FALSE` -->

The first step is authenticate for EMS connection with your EFOQA credentials. 

```r
library(Rems)
conn <- connect({efoqa_usr}, {efoqa_pwd})
```

In case you are behind the proxy, provide the proxy details with optional `proxies` argument.

```r
conn <- connect({efoqa_usr},
                {efoqa_pwd},
                proxies = list(url  = {proxy_address},
                               port = {port},
                               usr  = {proxy_usr},
                               pwd  = {proxy_pwd}),
                server = "prod")
```
With optional `server` argument, you can select one of the currently available EMS API servers, which are:
* "prod" (default)
* "cluster" (clustered production version)
* "stable" (stable test version)
* "beta" 

* "nightly"
## Flight Querying

### Instantiate Query

The following example instantiates a flight-specific query object that will send queries to the EMS 9 system. 

```r
qry <- flt_query(conn, "ems9", data_file = 'metadata.db')
```
where optional `data_file` input specifies the SQLite file that will be used to read/write the meta data in the local machine. If there is no file with a specified file name, a new db file will be created. If no file name is passed, it will generate a db file in the default location (Rems/data).


### EMS Database Setup
The FDW Flights database is one of the frequently used databases. In order to select it as your database for querying, you can simply run the following line.

```r
qry <- set_database(qry, "fdw flights")
```

In EMS system, all databases & data fields are organized in hierarchical tree structures. In order to use a database that is not the FDW Flights, you need to tell the query object where in the EMS DB tree your database is at. The following example specifies the location of one of the Event databases in the DB tree and then set the Event database that you want to use:

```r
qry <- update_dbtree(qry, "fdw", "events", "standard", "p0")
qry <- set_database(qry, "p0: library flight safety events")
```

These code lines first send queries to find the database-groups path, **FDW &rarr; APM Events &rarr; Standard Library Profiles &rarr; P0: Library Flight Safety Events**, and then select the "P0: Library Flight Safety Events" database that is located at the specified path. 

### Data Fields
Similar to the databases, the EMS data fields are organized in a tree structure so the steps are almost identical except that you use `update_fieldtree(...)` method in order to march through the tree branches.

Before calling the `update_fieldtree(...)`, you can call `update_preset_fieldtree(...)` method to load a basic tree with fields belonging to the following field groups:
* Flight Information
* Aircraft Information
* Navigation Information

Let say you have selected the FDW Flights database. The following code lines will query for the meta-data of basic data fields, and then some of the data fields in the Profile 301 in EMS9. 

```r
# Let the query object load preset data fields that are frequently used
qry <- generate_preset_fieldtree(qry)

# Load other data fields that you want to use
qry <- update_fieldtree(qry, "profiles", "standard", "block-cost", "p301",
                             "measured", "ground operations (before takeoff)")
```
The `update_fieldtree(...)` above queries the meta-data of all measurements located at the path, **Profiles &rarr; Standard Library Profiles &rarr; Block-Cost Model &rarr; P301: Block-Cost Model Planned Fuel Setup and Tests &rarr; Measured Items &rarr;Ground Operations (before takeoff)** in EMS Explorer.

**Caution**: the process of adding a subtree usually requires a very large number of recursive RESTful API calls which take quite a long time. Please try to specify the subtree to as low level as possible to avoid a long processing time.

As you may noticed in the example codes, you can specify a data entity by the string fraction of its full name. The "key words" of the entity name follows this rule:
* Case insensitive
* Keyword can be a single word or multiple consecutive words that are found in the full name string
* Keyword should uniquely specify a single data entity among all children under their parent database group
* Regular expression is not supported


### Saving Meta-Data
Finally, you can save your the meta-data of the database/data trees for later uses. Once you save it, you can go directly call `set_database(...)` without querying the same meta-data for later executions. However, you will have to update trees again if any of the data entities are modified at the EMS-system side.

```r
# This will save the meta-data into demo.db file, in SQLite format
save_metadata(qry)
```

### Build Flight Query

#### Select
As a next step, you will start make an actual query. The `select(...)` method is used to select what will be the columns of the returned data for your query. Following is an example:

```r
qry <- select(qry, "flight date", 
                   "customer id", 
                   "takeoff valid", 
                   "takeoff airport iata code")
```

The passed data fields must be part of the data fields in your data tree. 

You need to make a separate select call if you want to add a field with aggregation applied.

```python
qry <- select(qry, "P301: duration from first indication of engines running to start", 
                   aggregate="avg")
```
Supported aggregation functions are:
* avg
* count
* max
* min
* stdev
* sum
* var

You may want to define grouping, which is described in the next section, when you want to apply an aggregation function.

`select(...)` method accepts the keywords too, and even a combination of keywords to specify the parent directories of the fields in the data tree. For example, the following keywords are all valid to select "Flight Date (Exact)" for query:
- Search by a consecutive substring. The method returns a match with the shortest field name if there are multiple match.
    - Ex) "flight date"
- Search by exact name. 
    - Ex) "flight date (exact)"
- Field name keyword along with multiple keywords for the names of upstream field groups (i.e., directories). 
    - Ex) ("flight info", "date (exact)")


#### Group by & Order by

Similarly, you can pass the grouping and ordering conditions:
```r
qry <- group_by(qry, "flight date", "customer id", "takeoff valid", "takeoff airport code")
```

```r
qry <- order_by(qry, "flight date")
# the ascending order is default. You can pass a descending order by optional input:
#     qry <- order_by(qry, "flight date", order="desc")
```

#### Additional Conditions

If you want to get unique rows only (which is already set on as default),
```r
qry <- distinct(qry) # identical with distinct(qry, TRUE)
# If you want to turn off "distinct", do qry <- distinct(qry, FALSE)
```
Optionally you can control the number of rows that will be returned. The following code will set the top 5000 rows as your returned data.
```r
qry <- get_top(qry, 5000)
```

#### Filtering

Currently the following conditional operators are supported with respect to the data field types:
* Number: "==", "!=", "<", "<=", ">", ">="
* Discrete: "==", "!=", "in", "not in" (Filtering condition made with value, not discrete integer key)
* Boolean: "==", "!="
* String: "==", "!=", "in", "not in"

Following is the example:
```r
qry <- filter(qry, "'flight date' >= '2016-1-1'")
qry <- filter(qry, "'takeoff valid' == TRUE")
qry <- filter(qry, "'customer id' in c('CQH', 'EVA')")
qry <- filter(qry, "'takeoff airport code' == 'TPE'")
```
The current filter method has the following limitation:
* Single filtering condition for each filter method call
* Each filtering condition is combined only by "AND" relationship
* The field keyword must be at left-hand side of a conditional expression
* No support of NULL value filtering, which is being worked on now
* The datetime condition should be only with the ISO8601 format

### Checking the Generated JSON Query

You can check what would the JSON query look like before sending the query.
```r
json_str(qry)
```
This will print the following JSON string:
```
## {
##   "select": [
##     {
##       "fieldId": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exact-date]]]",
##       "aggregate": "none"
##     },
##     {
##       "fieldId": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-fcs][base-field][fdw-flight-extra.customer]]]",
##       "aggregate": "none"
##     },
##     {
##       "fieldId": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exist-takeoff]]]",
##       "aggregate": "none"
##     },
##     {
##       "fieldId": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.airport-takeoff]]]",
##       "aggregate": "none"
##     },
##     {
##       "fieldId": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-apm][flight-field][msmt:profile-b1ff12e2a2ff4da68bfbadfbe8a14acc:msmt-2df6b3503603472abf4b52feba8bf128]]]",
##       "aggregate": "avg"
##     }
##   ],
##   "groupBy": [
##     {
##       "fieldId": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exact-date]]]"
##     },
##     {
##       "fieldId": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-fcs][base-field][fdw-flight-extra.customer]]]"
##     },
##     {
##       "fieldId": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exist-takeoff]]]"
##     },
##     {
##       "fieldId": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.airport-takeoff]]]"
##     }
##   ],
##   "orderBy": [
##     {
##       "fieldId": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exact-date]]]",
##       "order": "asc",
##       "aggregate": "none"
##     }
##   ],
##   "distinct": true,
##   "top": 5000,
##   "format": "none",
##   "filter": {
##     "operator": "and",
##     "args": [
##       {
##         "type": "filter",
##         "value": {
##           "operator": "dateTimeOnAfter",
##           "args": [
##             {
##               "type": "field",
##               "value": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exact-date]]]"
##             },
##             {
##               "type": "constant",
##               "value": "2016-1-1"
##             },
##             {
##               "type": "constant",
##               "value": "Utc"
##             }
##           ]
##         }
##       },
##       {
##         "type": "filter",
##         "value": {
##           "operator": "isTrue",
##           "args": [
##             {
##               "type": "field",
##               "value": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exist-takeoff]]]"
##             }
##           ]
##         }
##       },
##       {
##         "type": "filter",
##         "value": {
##           "operator": "in",
##           "args": [
##             {
##               "type": "field",
##               "value": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-fcs][base-field][fdw-flight-extra.customer]]]"
##             },
##             {
##               "type": "constant",
##               "value": 18
##             },
##             {
##               "type": "constant",
##               "value": 11
##             }
##           ]
##         }
##       },
##       {
##         "type": "filter",
##         "value": {
##           "operator": "equal",
##           "args": [
##             {
##               "type": "field",
##               "value": "[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.airport-takeoff]]]"
##             },
##             {
##               "type": "constant",
##               "value": 5256
##             }
##           ]
##         }
##       }
##     ]
##   }
## }
```
### Reset Flight Query
In case you want to start over for a fresh new query,
```r
qry <- reset(qry)
```
Which will erase all the previous query settings.

### Finally, Run the Flight Query

`run()` method will send the query and translate the response from EMS in R's dataframe.
```r
df <- run(qry)

# This will return your data in R's dataframe format.
```
EMS API supports two different query executions which are regular and async queries. The regular query has a data size limit for the output data, which is 25000 rows. On the other hand, the async query is able to handle large output data by letting you send repeated requests for smaller batches of the large output data.

The `run()` method takes care of the repeated async request for a query whose returning data is expected to be large.

The batch data size for the async request is set 25,000 rows as default (which is the maximum). If you want to change this size,
```r
# Set the batch size as 20,000 rows per request
df <- query.run(qry, n_row = 20000)
```

## Querying Time-Series Data
You can query data of time-series parameters with respect to individual flight records. Below is a simple example code that sends a flight query first in order to retrieve a set of flights and then sends of queries to get some of the time-series parameters for each of these flights.

```r
# You should instantiate an EMS connection first. It was already described above.

# Flight query with an APM profile. It will return data for 10 flights
fq <- flt_query(conn, "ems9", data_file = "demo.db")
fq <- set_database(fq, "fdw flights")
# If you reuse the meta-data, you don't need to update db/field trees.

fq <- select(fq,
             "customer id", "flight record", "airframe", "flight date (exact)",
             "takeoff airport code", "takeoff airport icao code", "takeoff runway id",
             "takeoff airport longitude", "takeoff airport latitude",
             "p185: processed date", "p185: oooi pushback hour gmt",
             "p185: oooi pushback hour solar local",
             "p185: total fuel burned from first indication of engines running to start of takeoff (kg)")
fq <- order_by(fq, "flight record", order = 'desc')
fq <- get_top(fq, 10)
fq <- filter(fq,
             "'p185: processing state' == 'Succeeded'")
flt <- run(fq)

# === Run time series query for each flight ===

# Instantiate a time-series query for the same EMS9
tsq <- tseries_query(conn, "ems9")

# Select 7 example time-series params that will be retrieved for each of the 10 flights
tsq <- select(tsq, "baro-corrected altitude", "airspeed (calibrated; 1 or only)", "ground speed (best avail)",
                  "egt (left inbd eng)", "egt (right inbd eng)", "N1 (left inbd eng)", "N1 (right inbd eng)")

# Run mult-flight repeated query
xdata <- run_multiflts(tsq, flt, start = rep(0, nrow(flt)), end = rep(15*60, nrow(flt)))
```
The inputs to function `run_mutiflts(...)` are:
* qry  : time-series query instance
* flt  : a vector of Flight Records or flight data in R dataframe format. The R dataframe should have a column of flight records with its column name "Flight Record"
* start: a vector defining the starting times (secs) of the timepoints for individual flights. The vector length must be the same as the number of flight records
* end  : a vector defining the end times (secs) of the timepoints for individual flights. The vector length must be the same as the number of flight records

The output will be R list object whose elements contain the following data:
* flt_data : R list. Copy of the flight data for each flight
* ts_data  : R dataframe. the time series data for each flight

In case you just want to query for a single flight, `run(...)` function will be better suited. Below is an example of time-series querying for a single flight.
```r
xdata <- run(tsq, 1901112, start = 0, end = 900)
```
This function will return an R dataframe that contains timepoints from 0 to 900 secs and corresponding values for selected parameters.



