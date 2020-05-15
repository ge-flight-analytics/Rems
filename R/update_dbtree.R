#' @export
update_dbtree <-
  function(qry, ..., exclude_tree=c())
  {
    path <- unlist(list(...))
    qry$flight <- update_tree(qry$flight, path, treetype='dbtree', exclude_tree=exclude_tree)
    qry
  }
