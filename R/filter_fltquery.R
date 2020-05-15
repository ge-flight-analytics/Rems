#' @export
filter <- function(qry, ...) UseMethod("filter", qry)

#' @export
filter.FltQuery <-
  function(qry, expr)
  {
    if ( is.null(qry$queryset$filter) ) {
      qry$queryset$filter = list(operator = 'and',
                                 args = list())
    }
    spl_expr <- split_expr(expr)
    a_fltr   <- translate_expr(qry$flight, spl_expr)
    n_fltr   <- length(qry$queryset$filter$args)
    qry$queryset$filter$args[[n_fltr + 1]] <- a_fltr

    # Update kvmaps (temp measure. Change the design (see list_allvalues))
    qry$flight$trees$kvmaps <- get_kvmaps(qry$flight)

    return(qry)
  }
