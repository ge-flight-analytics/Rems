library(Rems)
efoqa_usr <- readline(prompt="Enter username: ")
efoqa_pwd <- .rs.askForPassword("Enter password")

use_proxy <- TRUE
proxy_url <- ""
proxy_port <- 80
proxies <- list(url  = proxy_url, port = proxy_port)

ems_system = ''

# --------------------------------------------------------
# Field/Entity ID Setup.  This information comes from EMS.
# --------------------------------------------------------

entity_id = '[api-write-test][entity-type][api-write-test]'

# A tinyint is just an unsigned int between 0-256.
schema_map <- list(
  "float_field_1" = "[-hub-][field][[[api-write-test][entity-type][api-write-test]][[api-write-test][base-field][api-write-test.float-field-1]]]",
  "string_field_1" = "[-hub-][field][[[api-write-test][entity-type][api-write-test]][[api-write-test][base-field][api-write-test.string-field-1]]]",
  "tinyint_field_1" = "[-hub-][field][[[api-write-test][entity-type][api-write-test]][[api-write-test][base-field][api-write-test.tinyint-field-1]]]",
  "datetime_field_1" = "[-hub-][field][[[api-write-test][entity-type][api-write-test]][[api-write-test][base-field][api-write-test.datetime-field-1]]]"
)

# ---------------
# Dataframe Setup
# ---------------

utc_time <- as.POSIXlt(Sys.time(), tz = "GMT")
utc_time_str <- format(utc_time, "%Y-%m-%dT%H:%M:%SZ")

# These column names should all be in schema_map.
df <- data.frame('float_field_1' = c(1.0, 2.0, 4.0, -10.0),
                 'string_field_1' = c("a", "b", "c", "d"),
                 'tinyint_field_1' = c(1, 1, 1, 1),
                 'datetime_field_1' = c(utc_time_str, utc_time_str, utc_time_str, utc_time_str)

)

# ------------------
# Actual query stuff
# ------------------

# Create connection
if (use_proxy){
  conn <- connect({efoqa_usr}, {efoqa_pwd}, proxies=proxies)
} else {
  conn <- connect({efoqa_usr}, {efoqa_pwd})
}

# Create an insert_query object
i_query <- insert_query(conn, ems_system, db_id=entity_id)

# --------
# Insert Method 1
# --------

# Query with a dataframe and a schema_map list.
i_query <- insert_data_frame.InsertQuery(i_query, df, schema_map)
run(i_query)

# --------
# Insert Method 2
# --------
i_query <- reset(i_query)

# Query with a dataframe, where the column names are EMS schema.
df_renamed <- df
df_renamed['float_field_1'] <- df_renamed['float_field_1']*1.2 # Make some small differences
df_renamed['tinyint_field_1'] <- 2 # Set the int field to 2 for later querying.
# Rename dataframe
for (col in names(df_renamed)){
  names(df_renamed)[names(df_renamed) == col] <- schema_map[[col]]
}
cat(sprintf('The column names of our renamed datafame are: %s', paste(names(df_renamed), sep=',', collapse=", ")))

# Insert dataframe without schema_map (columns are schema names)
i_query <- insert_data_frame.InsertQuery(i_query, df_renamed)
run(i_query)

# --------
# Insert Method 3
# --------
i_query <- reset(i_query)

# Change the tinyint field to 3 for later querying.
df_renamed['[-hub-][field][[[api-write-test][entity-type][api-write-test]][[api-write-test][base-field][api-write-test.tinyint-field-1]]]'] <- 3
# Insert row by row.
for (i in 1:nrow(df_renamed)){
  row <- df_renamed[i,]
  df_list <- as.list(row)
  i_query <- insert_row.InsertQuery(i_query, df_list)
}

run(i_query)

# -----------------
# Confirm insertion
# -----------------
qry <- flt_query(conn, ems_system, data_file = 'metadata.db')
qry <- update_dbtree(qry, "Misc. Providers")
qry <- set_database(qry, "API Write Tests")

qry <- select(qry, 'Datetime Field 1', 'Float Field 1', 'String Field 1', 'TinyInt Field 1')
res <- run(qry)

# --------------
# Check matching
# --------------
# df + schema
print('with schema map')
print('original df')
print(df)
res_similar <- res[(abs(res[, "Datetime Field 1"] - utc_time) < 1.0) & (res["TinyInt Field 1"] == 1),]
print('matching query results')
print(res_similar)

# df_renamed
print('with renamed columns')
print('original df')
print(df_renamed)
res_similar <- res[(abs(res[, "Datetime Field 1"] - utc_time) < 1.0) & (res["TinyInt Field 1"] == 2),]
print('matching query results')
print(res_similar)

# df row inserted one by one
print('with one_by_one columns')
print('original df')
print(df_renamed)
res_similar <- res[(abs(res[, "Datetime Field 1"] - utc_time) < 1.0) & (res["TinyInt Field 1"] == 3),]
print('matching query results')
print(res_similar)
