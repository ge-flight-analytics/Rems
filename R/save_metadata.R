#' @export
save_metadata <-
  function(qry, file_name = NULL)
  {
    save_tree(qry$flight, file_name)
  }
