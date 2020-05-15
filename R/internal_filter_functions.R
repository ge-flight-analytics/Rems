## ---------------------------------------------------------------------------
## Filter-related low-level functions
filter_fmt <-
  function(op, field_info, val_info = NULL)
  {
    fltr <- list(
      type = "filter",
      value = list(
        operator = op,
        args = lapply(c(list(field_info), val_info),
                      function(x) list(type = x$type, value = x$value))
      )
    )
    return(fltr)
  }


boolean_filter <-
  function(op, field_info, val_info)
  {
    val_info <- val_info[[1]]

    if ( typeof(val_info$value)!="logical" ) {
      stop(sprintf("%s: use a boolean value.", val_info$value))
    }
    if ( op == "==" ) {
      t_op = paste('is', ifelse(val_info$value, "True", "False"), sep = "")
    } else if (op == "!=") {
      t_op = paste('is', ifelse(!val_info$value, "True", "False"), sep = "")
    } else {
      stop(sprintf("%s is not supported for boolean fields.", op))
    }
    fltr <- filter_fmt(t_op, field_info)
    return(fltr)
  }


discrete_filter <-
  function(op, field_info, val_info, flt)
  {
    if ( op %in% names(basic_ops) ) {
      t_op     <- basic_ops[[op]]
    } else if ( op %in% c(" in ", " not in ") ) {
      t_op     <- sp_ops[[op]]
    } else {
      stop(sprintf("%s: Unsupported conditional operator for discrete fields.", op))
    }
    val_info <- lapply(val_info,
                       function(v) {
                         v$value=get_value_id(flt, v$value, field_id = field_info$value)
                         v
                       })

    fltr <- filter_fmt(t_op, field_info, val_info)
    return(fltr)
  }


number_filter <-
  function(op, field_info, val_info)
  {
    if ( op %in% names(basic_ops) ) {
      t_op <- basic_ops[[op]]
    } else {
      stop(sprintf("%s: Unsupported conditional operator for discrete fields.", op))
    }
    fltr <- filter_fmt(t_op, field_info, val_info)
    return(fltr)
  }


string_filter <-
  function(op, field_info, val_info)
  {
    if ( op %in% c("==", "!=") ) {
      t_op <- basic_ops[[op]]
    } else if ( op %in% c(" in ", " not in ") ) {
      t_op <- sp_ops[[op]]
    } else {
      stop(sprintf("%s: Unsupported conditional operator for discrete fields.", op))
    }
    fltr <- filter_fmt(t_op, field_info, val_info)
    return(fltr)
  }


datetime_filter <-
  function(op, field_info, val_info)
  {
    date_ops <- list(
      "<" = "dateTimeBefore",
      ">=" = "dateTimeOnAfter"
    )
    if ( op %in% names(date_ops) ) {
      t_op <- date_ops[[op]]
    } else {
      stop(sprintf("%s: Unsupported conditional operator for date time fields. Only < and >= are supported for date time fields.", op))
    }
    val_info <- c(val_info, list(list(type="constant", value="Utc")))
    fltr     <- filter_fmt(t_op, field_info, val_info)
    return(fltr)
  }
