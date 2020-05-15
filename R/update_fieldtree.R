#' @export
update_fieldtree <-
  function(qry, ..., exclude_tree=c())
  {
    path <- unlist(list(...))
    qry$flight <- update_tree(qry$flight, path, treetype='fieldtree', exclude_tree=exclude_tree)
    return(qry)
  }
