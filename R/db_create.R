basic_ops <- list(
  "==" = 'equal', "!=" = 'notEqual', "<" = 'lessThan',
  "<=" = 'lessThanOrEqual', ">" = 'greaterThan', ">=" = 'greaterThanOrEqual'
)
sp_ops <- list(
  ' in '= 'in',
  ' not in '= 'notIn'
)

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
    obj$rows <- 0
    obj <- reset(obj)
    
    return(obj)
  }

#' @export
insert_row.InsertQuery <-
  function(qry, row)
  {
    #' 
    #' Inputs:
    #'    row (list): A list of values to input, where the key is the fieldId and the value is the value to input. 
    #'        e.g. row <- list(fieldId1 = value1, fieldId2 = value2, ..., fieldIdN = valueN)
    curr_length <- length(qry$create$createColumns) # Get length of createColumns field
    next_entry <- curr_length + 1 # Find next entry, which is where we want to put our next row
    qry$create$createColumns[[next_entry]] <- list() # Add an empty list at next_entry
    i <- 1 # Begin to loop over all key:value pairs in row.
    for (field_id in names(row)){
      # Add all keys and values as {fieldId = key, value = value}
      qry$create$createColumns[[next_entry]][[i]] <- list(fieldId = field_id, value = row[[field_id]])
      i <- i + 1
    }
    qry$rows <- qry$rows + 1
    qry
  }

#' @export
insert_data_frame.InsertQuery <-
  function(qry, df, schema_map)
  {
    #'
    #' Inputs:
    #'    df (data.frame): A DataFrame of values to input, where the columns are the fieldIds and the entries are values to input.
    #'    mapper (list): A mapping of named dataframe columns to field ids, e.g. list('column1' = '[-hub][schema]')
    
    # make sure that all columns in df are also in mapper, which maps df column names to schemas
    for (col in colnames(df)){
      if (!(col %in% names(schema_map))){
        cat(sprintf("Column: '%s' found in df, but not in mapper.  Please only pass in columns which should be updated in the target table and for which a
                    schema mapping exists in the supplied mapper list.", col))
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
        qry$create$createColumns[[next_entry]][[j]] <- list(fieldId = schema_map[[name]], value = row[[name]])
        j <- j + 1
      }
      i <- i + 1
    }
    qry$rows <- qry$rows + 1
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
    if (status_code(r) == 200){
      if (content(r)$rowsAdded == qry$rows){
        cat(print('Successfully added all rows.'))
        return(TRUE)
      }
    } else if (status_code(r) %in% c(400, 401, 503)) {
      cat(print('Failed to add rows.'))
      cat(sprintf('message: %s'))
      cat(sprintf('messageDetail: %s'))
      cat(sprintf('unexpected: %s'))
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
json_str <-
  function(qry)
  {
    jsonlite::toJSON(qry$create, auto_unbox = T, pretty = T)
  }


#' @export
run.FltQuery <-
  function(qry, n_row = 25000)
  {
    if (is.null(qry$queryset$top)) {
      Nout <- NULL
    } else {
      Nout <- qry$queryset$top
    }
    if ((!is.null(Nout)) && (Nout <= 25000)) {
      return(simple_run(qry, output = "dataframe"))
    }
    return(async_run(qry, n_row = n_row))
  }

## --------------------------------------------------------------------------
## Functions to interface with the flight object

#' @export
update_dbtree <-
  function(qry, ...)
  {
    path <- unlist(list(...))
    qry$flight <- update_tree(qry$flight, path, treetype='dbtree')
    qry
  }

#' @export
update_fieldtree <-
  function(qry, ...)
  {
    path <- unlist(list(...))
    qry$flight <- update_tree(qry$flight, path, treetype='fieldtree')
    return(qry)
  }

#' @export
generate_preset_fieldtree <-
  function(qry)
  {
    qry$flight <- make_default_tree(qry$flight)
    qry
  }

#' @export
save_metadata <-
  function(qry, file_name = NULL)
  {
    save_tree(qry$flight, file_name)
  }

#' @export
load_metadata <-
  function(qry, file_name = NULL)
  {
    qry$flight <- load_tree(qry$flight, file_name)
    return(qry)
  }
