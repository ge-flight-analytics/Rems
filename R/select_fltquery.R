
#' @export
select <- function(qry, ...) UseMethod("select", qry)

#' @export
select.FltQuery <-
  function(qry, ..., aggregate = c('none','avg','count','max','min','stdev','sum','var'))
  {
    aggregate <- match.arg(aggregate)

    fields <- search_fields(qry$flight, ...)

    for ( fld in fields ) {

      d <- list(
        fieldId = fld$id,
        aggregate = aggregate
      )
      n_sel <- length(qry$queryset$select)
      qry$queryset$select[[n_sel + 1]] <- d
      qry$columns[[n_sel + 1]] <- fld
    }
    return(qry)
  }
