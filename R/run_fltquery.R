#' @export
run <- function(qry, ...) UseMethod("run", qry)

#' @export
run.FltQuery <-
  function(qry, n_row = 25000)
  {
    if (is.null(qry$queryset$top)) {
      Nout <- NULL
    } else {
      Nout <- qry$queryset$top
    }
    if ((!is.null(Nout)) && (Nout <= 25000)) {
      return(simple_run(qry, output = "dataframe"))
    }
    return(async_run(qry, n_row = n_row))
  }

#' @export
simple_run <- function(qry, ...) UseMethod("simple_run", qry)

#' @export
simple_run.FltQuery <-
  function(qry, output = c("dataframe", "raw"))
  {

    output <- match.arg(output)
    cat("Sending a regular query to EMS ...")
    r <- request(qry$connection, rtype = "POST",
                 uri_keys = c('database', 'query'),
                 uri_args = c(qry$ems_id, qry$flight$db_id),
                 jsondata = qry$queryset)
    cat("Done.\n")
    if ( output == "raw" ) {
      return(httr::content(r))
    }
    else if ( output == "dataframe" ) {
      return(to_dataframe(qry, httr::content(r)))
    }
  }

#' @export
async_run <- function(qry, ...) UseMethod("async_run", qry)

#' @export
async_run.FltQuery <-
  function(qry, n_row = 25000)
  {
    open_async_query <- function() {
      cat('Sending and opening an async-query to EMS ...\n')
      r <- request(qry$connection, rtype = "POST",
                   uri_keys = c('database', 'open_asyncq'),
                   uri_args = c(qry$ems_id, qry$flight$db_id),
                   jsondata = qry$queryset)
      if (is.null(httr::content(r)$id)) {
        print(httr::headers(r))
        print(httr::content(r))
        stop("Opening Async query did not return the query Id.")
      }
      cat('Done.\n')
      httr::content(r)
    }

    # Open async-query
    async_q <- open_async_query()

    ctr <- 1
    df  <- data.frame(stringsAsFactors = F)
    req_error <- F
    while(T) {
      cat(sprintf("=== Async call: %d === \n", ctr))
      tryCatch({
        # Mini batch async call. Sometimes the issued query ID gets expired.
        # In that case, try up to three times until getting a new query ID.
        for(i in 1:3) {
          r <- request(qry$connection, rtype = "GET",
                       uri_keys = c('database', 'get_asyncq'),
                       uri_args = c(qry$ems_id,
                                    qry$flight$db_id,
                                    async_q$id,
                                    formatC(n_row*(ctr-1), format="d"),
                                    formatC(n_row*ctr-1, format="d")))
          if (is.null(httr::content(r)$rows)) {
            # Reopen the query if not returning data
            async_q <- open_async_query()
          } else {
            break
          }
        }
        resp <- httr::content(r)
        resp$header <- async_q$header
      }, error = function(e) {
        cat("Something's wrong. Returning what has been sent so far.\n")
        print(httr::headers(r))
        break
      })
      if (length(resp$rows) < 1) {
        break
      }
      dff <- to_dataframe(qry, resp)
      df <- rbind(df, dff)
      cat(sprintf("Received up to  %d rows.\n", nrow(df)))
      if (nrow(dff) < n_row) {
        break
      }
      ctr <- ctr + 1
    }
    tryCatch({
      r <- request(qry$connection, rtype = "DELETE",
                   uri_keys = c('database', 'close_asyncq'),
                   uri_args = c(qry$ems_id, qry$flight$db_id, async_q$id))
      cat(sprintf("Async query connection (query ID: %s) deleted.\n", async_q$id))
    }, error = function(e) {
      cat(sprintf("Couldn't delete the async query (query ID: %s). Probably it was already expired.\n", async_q$id))
    })

    cat("Done.\n")
    df
  }
