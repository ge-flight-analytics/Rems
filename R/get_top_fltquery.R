#' @export
get_top <- function(qry, ...) UseMethod("get_top", qry)

#' @export
get_top.FltQuery <-
  function(qry, n)
  {
    qry$queryset$top <- n
    return(qry)
  }
