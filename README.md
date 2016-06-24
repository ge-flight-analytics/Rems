# Rems
Rems is a R wrapper of EMS database API. With Rems package, you will be able to retrieve the EMS data from R without the low-level knowledge of the EMS RESTful API. The project is still in very early alpha stage, so is not guarranteed working reliably nor well documented. I'll beef up the documentation as soon as possible. 

Any contribution is welcome!

Dependency: httr, jsonlite

## Installation
```r
install.package("devtools")
devtools::install_git("https://github.build.ge.com/212401522/Rems")
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
                               pwd  = {proxy_pwd}))
```



## Instantiate Query



```r
qry <- query(conn, "ems9")
```
Current limitations:
* The current query object only support the FDW Flight data source, which seems to be reasonable for POC functionality.
* Right now a query can be instantiated only with a single EMS system connection. I think a query object with multi-EMS connection could be quite useful for data analysts who want to do study pseudo-global patterns.
    + Ex) query = Query(c, ems_name = c('ems9', 'ems10', 'ems11'))
* It does not support querying time-series raw parameters yet. Adding this capability may be the next major goal. 

## Datasource Setup

The EMS system handles with data fields based on a hierarchical tree structure. This field tree manages the names and field ID mappings as well as the field groups of fields. In order to send query via EMS API, the Rems package already contains a data file for the static and frequently used part of the field tree as default. This bare field tree includes fields of the following field groups:
* Flight Information (sub-field groups Processing and Profile 16 Extra Data were excluded)
* Aircraft Information 
* Flight Review
* Data Information
* Navigation Information
* Weather Information

 In case that you want to query with fields that are not included in this default, stripped-down data tree, you'll have to add the field group where your fields belongs to and update your data field tree. For example, if you want to add a field group branch such as Profiles --> Standard Library Profiles --> Block-Cost Model --> P301: Block-Cost Model Planned Fuel Setup and Tests --> Measured Items --> Ground Operations (before takeoff), the execution of the following method will add the fields and their related subtree structure to the basic tree structure. You can use either the full name or just a fraction of consequtive keywords of each field group. The keyword is case insenstive.
 
 **Caution**: the process of adding a subtree usually requires a very large number of recursive RESTful API calls which takes quite a long time. Please try to specify the subtree to as low level as possible to avoid a long processing time.
 
```r
qry <- update_datatree(qry, "profiles", "standard", "block-cost", "p301", "measured", "ground operations (before takeoff)")
```
As it runs successfully, the console will print the progress similar to below. 
```
## === Starting to add subtree from 'Ground Operations (before takeoff)' ===
## On Ground Operations (before takeoff) ...
## On Start and Push ...
## On Measurements ...
## On Fuel ...
## On Fuel Management (total) ...
##    .
##    .
## On English ...
## -- Added 5 fields
## On Metric ...
## -- Added 5 fields
##    . 
##    .
##    .

```

You can save and load the updated data tree for a later usage in R's Rds file format.
```r
save_datatree(qry, file_name = "my_datatree.rds")

qry <- load_datatree(qry, file_name = "my_datatree.rds")
```

## Build Query

### Select
Select the columns to include in your query. Again you can pass consequtive words of the full field names as keywords which are case insenstive.
```r
qry <- select(qry, "flight date", "customer id", "takeoff valid", "takeoff airport code")
```
You will have to make a seperate `select` function call if you want to aggregate a field.
```r
qry <- select(qry,
              "P301: duration from first indication of engines running to start",
              aggregate = "avg")
```

### Group by & Order by

Similarly, you can pass the grouping and ordering conditions:
```r
qry <- group_by(qry, "flight date", "customer id", "takeoff valid", "takeoff airport code")
```

```r
qry <- order_by(qry, "flight date")
```

### Additional Conditions

If you want to get unique rows only,
```r
qry <- distinct(qry) # identical with distinct(qry, TRUE)
# If you want to turn of distinct, do qry <- distinct(qry, FALSE)
```
Also you can control the number of rows that will be returned. The current EMS API is limited to return maximum of 5000 rows. Any greater number will be truncated to 5000 rows.
```r
qry <- get_top(qry, 5000)
```

### Filtering

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

## Checking the Generated JSON Query

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

## Finally, Run the Query

`run()` method will send the query and translate the response from EMS in R's dataframe.
```r
df <- run(qry)

# In case you want the raw response,
# df <- run(qry, output = "raw")
```

```
## Sending a query to EMS ...Done.
## Raw JSON output to R dataframe...Done.
```



