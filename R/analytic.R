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

#' Search Global Parameters
#'
#' Search for global parameters matching a substring.
#'
#' This function uses the analytic element of a time series query
#' to search for global parameters matching a substring. It will return
#' a list containing all the parameters that match the substring, as well
#' as their EMS GUIDs, names, descriptions, and units.
#'
#' @export
#' @param anal An analytic object (an element of list returned by tseries_query)
#' @param keyword A character string to search for in the parameter names.
#' @param category Kind of parameter to search. Defaults to 'full' (all available.)
#' @param flight_record Optional flight record to restrict search to.
#' @param group A group ID to restrict the search to. Defaults to all group IDs.
#' @param max_results Defaults to 200 - if set to 0, it will return all results.
#'
#' @return A list of parameters lists (list of lists) with the following fields:
#' \itemize{
#' \item id: EMS GUID of the parameter
#' \item name: Name of the parameter
#' \item description: Description of the parameter
#' \item units: the units of the parameter, if available
#' \item metadata: any associated metadata text, if available
#' \item ems_id: EMS ID of the analytic
#' }
#' @examples
#' \dontrun{
#' # Connect to EMS
#' conn <- connect("http://localhost:8080")
#' # Get a time series query object
#' qry <- tseries_query(conn, 1)
#' # Search for parameters with keyword "weather"
#' prm <- search_param(qry$analytic, "weather")
#' }
#'
#'

search_param <-
  function(anal,
           keyword,
           category = c("full" , "physical", "logical"),
           flight_record = NULL,
           group = NULL,
           max_results = NULL
)
  {
    category <- match.arg(category, c("full" , "physical", "logical"))

    if (category == "physical" & is.null(flight_record)) {
      stop("Physical Parameter searches must specify a flight record.")
    }

    if (!is.null(max_results)) {
      if (max_results < 0) stop("max_results must be a number > 0.")
      if (max_results == 0) warning("Setting max_results to 0 returns all results.")
    }

    if (!is.null(flight_record)) {
      if (!is.numeric(flight_record)) stop("Flight record must be a number.")
    }

    if (!is.null(flight_record)) {
      search_type_key <- "search_f"
    } else {
      search_type_key <- "search"
    }

    cat(sprintf('Searching for params with keyword "%s" from EMS ...', keyword))
    # EMS API Call
    r <- request(anal$connection,
                 uri_keys = c('analytic',search_type_key),
                 uri_args = c(anal$ems_id, flight_record),
                 body = list(text = keyword,
                             group = group,
                             maxResults = max_results,
                             category = category))
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
