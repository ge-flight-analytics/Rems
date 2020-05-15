
#' Create EMS Connection Object
#'
#' \code{connect} Creates a connection object to an EMS server for use in FltQuery objects.
#'
#' This function creates a connection object, including an authentication token.
#' These can be passed into a \code{fltquery} or \code{tsquery} function to start the process of retrieving data.
#' These functions take unmasked usernames and passwords - it is recommended that you either store your credentials
#' securely, or wrap this function in something like \code{rstudioapi::askForPassword()} to avoid disclosing
#' your credentials.
#'
#' @param usr A string, FOQA username (like firstname.lastname)
#' @param pwd A string, FOQA password
#' @param proxies A list containing the system proxy information, with attributes "url", "port", "usr", "pwd"
#' @param server A string. Lets you choose an API server. Should be one of 'prod', 'cluster', 'stable', 'beta', 'nightly', or 'ctc'.
#' @param server_url An optional string. If this is passed, it will be used instead of the internal server definitions matched by the 'server' argument.
#' @return a Connection object to be used in FltQuery.
#'
#' @examples
#' # connect in a standard way, using no proxy and connecting to prod by default.
#' \dontrun{
#' con <- connect("joe.bloggs", "mypassword")
#' qry <- flt_query(conn = con, ems_name = "my-ems", data_file = "file.db")
#' }
#'# connect using the beta build, and a proxy
#'\dontrun{
#'con <- connect("joe.bloggs", "mypassword",
#'               proxies = list(url = "http://myproxy.com", port = 8080, usr = "joebloggs", pwd = "mypassword"),
#'               server = 'beta')
#'}
#'
#' @export
connect <-
  function(usr, pwd, proxies = NULL, server = c('prod', 'cluster', 'stable', 'beta', 'nightly', 'ctc'), server_url = NULL)
  {
    # Prevent from the Peer certificate error ("Error in curl::curl_fetch_memory(url, handle = handle) :
    # Peer certificate cannot be authenticated with given CA certificates")
    httr::set_config( config( ssl_verifypeer = 0 ) )

    header <- c("Content-Type" = "application/x-www-form-urlencoded", "User-Agent" = user_agent)
    body <- list(grant_type = "password",
                username   = usr,
                password   = pwd)

    server <- match.arg(server, c('prod', 'cluster', 'stable', 'beta', 'nightly', 'ctc'), several.ok = FALSE)

    sel_uri_root <- if (is.null(server_url)) uri_root[[server]] else server_url
    uri = paste(sel_uri_root, uris$sys$auth, sep="")

    if (is.null(proxies)) {
      r <- POST(uri,
               add_headers(.headers = header),
               body = body,
               encode = "form")
    } else {
      r <- POST(uri,
               use_proxy(proxies$url, port = proxies$port, username = proxies$usr, password = proxies$pwd),
               add_headers(.headers = header),
               body = body,
               encode = "form")
    }
    if ( !is.null(content(r)$message) ) {
      print(paste("Message:", content(r)$message))
    }

    if ( http_error(r)) {
      stop(paste("Message:", content(r)$error_description))
    }


    c <- list(
      foqa      = list(usr=usr, pwd=pwd),
      proxies   = proxies,
      uri_root  = sel_uri_root,
      token     = content(r)$access_token,
      token_type= content(r)$token_type
    )
    c
  }

#' @export
reconnect <-
  function(conn)
  {
    # server_name = names(uri_root[uri_root==conn$uri_root])
    return(connect(conn$foqa$usr, conn$foqa$pwd, proxies = conn$proxies, server_url = conn$uri_root))
  }


#' @export
request <-
  function(conn, rtype = "GET", uri_keys = NULL, uri_args = NULL,
           headers = NULL, body = NULL, jsondata = NULL,
           verbose = F)
  {
    # Default encoding is "application/x-www-form-urlencoded"
    encoding <- "form"

    # use proxy
    if (!is.null(conn$proxies)) {
      prxy <- use_proxy(conn$proxies$url,
                        port      = conn$proxies$port,
                        username  = conn$proxies$usr,
                        password  = conn$proxies$pwd)
    } else {
      prxy = NULL
    }

    if (is.null(headers)) {
      headers <- c(Authorization = paste(conn$token_type, conn$token),
                   'Accept-Encoding' = 'gzip',
                   'User-Agent' = user_agent)
    }

    if (!is.null(uri_keys)) {
      uri <- paste(conn$uri_root,
                   uris[[uri_keys[1]]][[uri_keys[2]]],
                   sep = "")
    }

    if (!is.null(uri_args)) {
      # percent encode the args
      uri_args <- sapply(uri_args, function(x) if (is.na(suppressWarnings(as.numeric(x)))) URLencode(x, reserved = T) else x)
      uri      <- do.call(sprintf, as.list(c(uri, uri_args)))
    }

    if (!is.null(jsondata)) {
      body <- jsondata
      encoding <- "json"
    }

    if (rtype=="GET") {
      tryCatch({
        r <- GET(uri, prxy, query = body, add_headers(.headers = headers), encode = encoding)
      }, error = function(err) {
        print(err)
        cat(sprintf("Http status code %s: %s", status_code(r), content(r)))
        cat("Trying to Reconnect EMS...")
        conn = reconnect(conn)
        r <- GET(uri, prxy, query = body, add_headers(.headers = headers), encode = encoding)
      }

      )

    } else if (rtype=="POST") {
      tryCatch({
        r <- POST(uri, prxy, body = body, add_headers(.headers = headers), encode = encoding)
      }, error = function(err) {
        print(err)
        cat(sprintf("Http status code %s: %s", status_code(r), content(r)))
        cat("Trying to Reconnect EMS...\n")
        conn = reconnect(conn)
        r <- POST(uri, prxy, body = body, add_headers(.headers = headers), encode = encoding)
      })

    } else if (rtype=="DELETE") {
      tryCatch({
        r <- DELETE(uri, prxy, body = body, add_headers(.headers = headers), encode = encoding)
      }, error = function(err) {
        print(err)
        cat(sprintf("Http status code %s: %s", status_code(r), content(r)))
        cat("Trying to Reconnect EMS...\n")
        conn = reconnect(conn)
        r <- DELETE(uri, prxy, body = body, add_headers(.headers = headers), encode = encoding)
      })
    } else {
      stop(sprintf("%s: Unsupported request type.", rtype))
    }
    r
  }


