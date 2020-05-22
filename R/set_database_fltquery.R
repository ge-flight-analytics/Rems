#' @export
set_database <- function(qry, ...) UseMethod("set_database", qry)

#' @export
set_database.FltQuery <-
  function(qry, name)
  {
    qry$flight <- set_database.Flight(qry$flight, name)
    qry
  }
