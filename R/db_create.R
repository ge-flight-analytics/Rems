#' @export
insert_query <-
  function(conn, ems_name, db_id, data_file=NULL)
  {
    obj <- list()
    class(obj) <- 'InsertQuery'
    # Instantiating other objects
    obj$connection <- conn
    obj$ems        <- ems(conn)
    obj$ems_id     <- get_id(obj$ems, ems_name)
    obj$db_id      <- db_id

    # object data
    obj$create <- list(createColumns=list())
    obj <- reset(obj)

    return(obj)
  }

#' @export
insert_row.InsertQuery <-
  function(qry, row)
  {
    #
    # Inputs:
    #    row (list): A list of values to input, where the key is the fieldId and the value is the value to input.
    #        e.g. row <- list(fieldId1 = value1, fieldId2 = value2, ..., fieldIdN = valueN)
    curr_length <- length(qry$create$createColumns) # Get length of createColumns field
    next_entry <- curr_length + 1 # Find next entry, which is where we want to put our next row
    qry$create$createColumns[[next_entry]] <- list() # Add an empty list at next_entry
    i <- 1 # Begin to loop over all key:value pairs in row.
    for (field_id in names(row)){
      # Add all keys and values as {fieldId = key, value = value}
      qry$create$createColumns[[next_entry]][[i]] <- list(fieldId = field_id, value = row[[field_id]])
      i <- i + 1
    }
    qry
  }

#' @export
insert_data_frame.InsertQuery <-
  function(qry, df, schema_map=NULL)
  {
    #
    # Inputs:
    #    df (data.frame): A DataFrame of values to input, where the columns are the fieldIds and the entries are values to input.
    #    schema_map (list): A mapping of named dataframe columns to field ids, e.g. list('column1' = '[-hub][schema]')

    # if schema_map is not null, we want to translate column names to fields in EMS, so we need to make sure all of the dataframe
    # columns are also in schema_map, which maps df column names to schemas in EMS
    if (!is.null(schema_map)){
      for (col in colnames(df)){
        if (!(col %in% names(schema_map))){
          cat(sprintf("Column: '%s' found in df, but not in mapper.  Please only pass in columns which should be updated in the target table and for which a
                      schema mapping exists in the supplied mapper list.", col))
          stop("Not all columns in df were found in schema_map.")
        }
      }
    }

    print('inserting')
    i <- 1 # Begin to loop over all key:value pairs in row.
    for (i in 1:nrow(df)){
      row <- as.list(df[i,])
      curr_length <- length(qry$create$createColumns) # Get length of createColumns field
      next_entry <- curr_length + 1 # Find next entry, which is where we want to put our next row
      qry$create$createColumns[[next_entry]] <- list() # Add an empty list at next_entry
      j <- 1
      for (name in names(row)){
        # Add all keys and values as {fieldId = key, value = value}
        # Use schema_map to translate dataframe column names into a schema, if schema_map is not null
        if (!is.null(schema_map)){
          qry$create$createColumns[[next_entry]][[j]] <- list(fieldId = schema_map[[name]], value = row[[name]])
        } else { # if schema_map is not null, assume column names are schemas
          qry$create$createColumns[[next_entry]][[j]] <- list(fieldId = name, value = row[[name]])
        }
        j <- j + 1
      }
      i <- i + 1
    }
    qry
  }

#' @export
run.InsertQuery <-
  function(qry)
  {
    cat("Sending a regular query to EMS ...")
    r <- request(qry$connection, rtype = "POST",
                 uri_keys = c('database', 'create'),
                 uri_args = c(qry$ems_id, qry$db_id),
                 jsondata = qry$create)
    cat("Done.\n")
    if (httr::status_code(r) == 200){
      n_rows <- length(qry$create$createColumns)
      if (httr::content(r)$rowsAdded == n_rows){
        cat(print('Successfully added all rows.'))
        return(TRUE)
      }
    } else if (httr::status_code(r) %in% c(400, 401, 503)) {
      cat(print('Failed to add rows.'))
      content <- httr::content(r)
      cat(sprintf('message: %s', content$message))
      cat(sprintf('messageDetail: %s', content$messageDetail))
      cat(sprintf('unexpected: %s', content$unexpected))
      return(FALSE)
    } else {
      cat(print("An unknown error occured."))
      return(FALSE)
    }

  }

#' @export
reset.InsertQuery <-
  function(qry)
  {
    qry$create <- list(
      createColumns = list()
    )
    qry
  }


#' @export
json_str.InsertQuery <-
  function(qry)
  {
    jsonlite::toJSON(qry$create, auto_unbox = T, pretty = T)
  }
