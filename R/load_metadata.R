#' @export
load_metadata <-
  function(qry, file_name = NULL)
  {
    qry$flight <- load_tree(qry$flight, file_name)
    return(qry)
  }
