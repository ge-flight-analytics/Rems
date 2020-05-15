#' @export
generate_preset_fieldtree <-
  function(qry)
  {
    qry$flight <- make_default_tree(qry$flight)
    qry
  }
