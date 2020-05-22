#' @export
reset <- function(qry, ...) UseMethod("reset", qry)

#' @export
reset.FltQuery <-
  function(qry)
  {
    qry$queryset <- list(
      select = list(),
      groupBy = list(),
      orderBy = list(),
      distinct = TRUE,
      format = "none"
    )
    qry$columns <- list()
    qry
  }
