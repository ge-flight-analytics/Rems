
#' @export
group_by <- function(qry, ...) UseMethod("group_by", qry)

#' @export
group_by.FltQuery <-
  function(qry, ...)
  {
    for ( fld in search_fields(qry$flight, ...)) {
      n_gby <- length(qry$queryset$groupBy)
      qry$queryset$groupBy[[n_gby + 1]] <- list(fieldId = fld$id)
    }
    return(qry)
  }
