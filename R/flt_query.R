

#' @export
flt_query <-
  function(conn, ems_name, data_file=NULL)
  {
    obj <- list()
    class(obj) <- 'FltQuery'
    # Instantiating other objects
    obj$connection <- conn
    obj$ems        <- ems(conn)
    obj$ems_id     <- get_id(obj$ems, ems_name)
    obj$flight     <- flight(conn, obj$ems_id, data_file)

    # object data
    obj$queryset <- list()
    obj$columns  <- list()
    obj <- reset(obj)

    return(obj)
  }






