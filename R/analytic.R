#'@export
analytic <-
  function(conn, ems_id, data_file = NULL)
  {
    obj <- list()
    class(obj) <- "Analytic"
    obj$ems_id        <- ems_id
    obj$connection    <- conn
    obj$metadata      <- NULL
    obj               <- load_paramtable(obj, data_file)

    return(obj)
  }


load_paramtable <-
  function(anal, data_file = NULL)
  {
    if (is.null(anal$metadata)) {
      anal$metadata <- localdata(data_file)
    } else {
      if ((!is.null(data_file)) && (file_loc(anal$metadata) != file.path(file_name))) {
        close.LocalData(anal$metadata)
        anal$metadata <- localdata(data_file)
      }
    }
    anal$param_table = get_data(anal$metadata, "params", sprintf("ems_id = %d", anal$ems_id))

    return(anal)
  }


save_paramtable <-
  function(anal, data_file = NULL)
  {
    if (nrow(anal$param_table) > 0) {
      delete_data(anal$metadata, 'params', sprintf("ems_id = %d", anal$ems_id))
      append_data(anal$metadata, 'params', anal$param_table)
    }
  }

#'@export
search_param <-
  function(anal, keyword)
  {
    cat(sprintf('Searching for params with keyword "%s" from EMS ...', keyword))
    # EMS API Call
    r <- request(anal$connection,
                 uri_keys = c('analytic','search'),
                 uri_args = anal$ems_id,
                 body = list(text = keyword))
    # Param set JSON to R list
    prm <- content(r)
    if ( length(prm)==0 ) {
      stop(sprintf("No parameter found with search keyword %s.", keyword))
    } else {
      # Add ems_id in the data
      for (i in 1:length(prm)) prm[[i]]$ems_id <- anal$ems_id
      # If the param set has more than one param, order than by length
      # of their names
      word_len <- sapply(prm, function(x) nchar(x$name))
      prm <- prm[order(word_len)]
    }
    cat("Done.\n")
    return(prm)
  }

#'@export
get_param <-
  function(anal, keyword, unique = T)
  {
    if ( nrow(anal$param_table)==0 ) {
      return(list(ems_id = "", id="", name="", description="", units=""))
    }
    # Make sure the special characters are treated as raw characters, not
    # POSIX meta-characters
    # sp_chr <- c("\\.", "\\^", "\\(", "\\)", "\\[", "\\]", "\\{", "\\}",
    #             "\\-", "\\+", "\\?", "\\!", "\\*", "\\$", "\\|", "\\&")
    # for ( x in sp_chr ) {
    #   keyword <- gsub(x, paste("\\", x, sep=""), keyword)
    # }
    keyword <- treat_spchar(keyword)

    df <- subset(anal$param_table, grepl(keyword, name, ignore.case = T))
    if ( nrow(df)==0 ) {
      return(list(ems_id = "", id="", name="", description="", units=""))
    }
    df <- df[order(nchar(df$name)), ]
    if ( unique ) {
      prm <- as.list(df[1, ])
      return(prm)
    }
    prm <- lapply(1:nrow(df), function(x) as.list(df[x,]))
    return(prm)
  }
