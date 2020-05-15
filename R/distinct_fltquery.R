#' @export
distinct <- function(qry, ...) UseMethod("distinct", qry)

#' @export
distinct.FltQuery <-
  function(qry, d = TRUE)
  {
    qry$queryset$distinct <- d
    return(qry)
  }
