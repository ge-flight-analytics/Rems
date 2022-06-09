#' @export
get_database <- function(qry, ...) UseMethod("get_database", qry)

#' @export
get_database.FltQuery <-
  function(qry)
  {
    return( get_database.Flight(qry$flight) )
  }
