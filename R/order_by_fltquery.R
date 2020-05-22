#' @export
order_by.FltQuery <-
  function(qry, field, order = c('asc', 'desc'))
  {
    order <- match.arg(order)
    fld   <- search_fields(qry$flight, field)[[1]]
    n_oby <- length(qry$queryset$orderBy)
    qry$queryset$orderBy[[n_oby + 1]] <- list(fieldId = fld$id,
                                              order = order,
                                              aggregate = 'none')
    return(qry)
  }
