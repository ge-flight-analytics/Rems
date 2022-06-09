#' @export
json_str <- function(qry, ...) UseMethod("json_str", qry)

#' @export
json_str.FltQuery <-
  function(qry)
  {
    jsonlite::toJSON(qry$queryset, auto_unbox = T, pretty = T)
  }
