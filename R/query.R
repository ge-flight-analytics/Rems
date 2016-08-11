#' @export
reset <- function(qry, ...) UseMethod("reset", qry)

#' @export
select <- function(qry, ...) UseMethod("select", qry)

#' @export
run <- function(qry, ...) UseMethod("run", qry)
