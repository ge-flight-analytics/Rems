
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
#'               proxies = list(url = "http://myproxy.com", port = 8080,
#'               usr = "joebloggs", pwd = "mypassword"),
#'               server = 'beta')
#'}
#'
#' @export
connect <- function(usr, pwd, proxies = NULL, server = c('prod', 'cluster', 'stable', 'beta', 'nightly', 'ctc'), server_url = NULL)
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
      r <- httr::POST(uri,
               httr::add_headers(.headers = header),
               body = body,
               encode = "form")
    } else {
      r <- httr::POST(uri,
               use_proxy(proxies$url, port = proxies$port, username = proxies$usr, password = proxies$pwd),
               httr::add_headers(.headers = header),
               body = body,
               encode = "form")
    }
    if ( !is.null(httr::content(r)$message) ) {
      print(paste("Message:", httr::content(r)$message))
    }

    if ( http_error(r)) {
      stop(paste("Message:", httr::content(r)$error_description))
    }


    c <- list(
      foqa      = list(usr=usr, pwd=pwd),
      proxies   = proxies,
      uri_root  = sel_uri_root,
      token     = httr::content(r)$access_token,
      token_type= httr::content(r)$token_type
    )
    c
  }

#' Refresh Connection
#'
#' Refresh the connection object in case auth token expires.
#'
#' This function will re-use the stored connection credentials in the connection object to regenerate the authentication token.
#'
#' @param conn A connection object created by the \code{connect} function.
#' @return A refreshed connection object like the one returned by \code{connect}
#'
#' @examples
#' \dontrun{
#' con <- connect("joe.bloggs", "mypassword")
#' sys.Sleep(3600)
#' con <- reconnect(con)
#' }
#'
#' @export
reconnect <-
  function(conn)
  {
    # server_name = names(uri_root[uri_root==conn$uri_root])
    return(connect(conn$foqa$usr, conn$foqa$pwd, proxies = conn$proxies, server_url = conn$uri_root))
  }

#' Arbitrary Request
#'
#' Perform an arbitrary GET, POST or DELETE function using a connection  object.
#'
#' For advanced users only, this function allows you to perform an arbitrary GET, POST or DELETE
#' request on the low-level API. Allows for full configurability of the request, including
#' setting the keys, arguments, headers, and JSON payload.
#'
#' @param conn A connection object generated by \code{connect}
#' @param rtype A string indicating the kind of HTTP request to be performed - accepts GET, POST and DELETE
#' @param uri_keys A two-element string vector indicating which URI to use - see common.R
#' @param uri_args A list of arguments to be passed to the URI
#' @param headers A list of headers to be used by the GET/POST/DELETE request, per the \code{.headers} argument
#' @param body A string, indicating the body to be passed to the request. Do not pass this and jsondata simultaneously.
#' @param jsondata A string, indicating the JSON body to be passed. See above.
#'
#' @return Returns the raw GET/POST/DELETE output from the request.
#'
#' @export
request <-
  function(conn, rtype = c("GET","POST","DELETE"), uri_keys = NULL, uri_args = NULL,
           headers = NULL, body = NULL, jsondata = NULL #, verbose = FALSE
           )
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
      uri_args <- sapply(uri_args, function(x) if (is.na(suppressWarnings(as.numeric(x)))) URLencode(x, reserved = TRUE) else x)
      uri      <- do.call(sprintf, as.list(c(uri, uri_args)))
    }

    if (!is.null(jsondata)) {
      body <- jsondata
      encoding <- "json"
    }

    rtype <- match.arg(rtype, c("GET","POST","DELETE"), several.ok = FALSE)

    if (rtype=="GET") {
      tryCatch({
        r <- httr::GET(uri, prxy, query = body, httr::add_headers(.headers = headers), encode = encoding)
      }, error = function(err) {
        print(err)
        cat(sprintf("Http status code %s: %s", httr::status_code(r), httr::content(r)))
        cat("Trying to Reconnect EMS...")
        conn = reconnect(conn)
        r <- httr::GET(uri, prxy, query = body, httr::add_headers(.headers = headers), encode = encoding)
      }

      )

    } else if (rtype=="POST") {
      tryCatch({
        r <- httr::POST(uri, prxy, body = body, httr::add_headers(.headers = headers), encode = encoding)
      }, error = function(err) {
        print(err)
        cat(sprintf("Http status code %s: %s", httr::status_code(r), httr::content(r)))
        cat("Trying to Reconnect EMS...\n")
        conn = reconnect(conn)
        r <- httr::POST(uri, prxy, body = body, httr::add_headers(.headers = headers), encode = encoding)
      })

    } else if (rtype=="DELETE") {
      tryCatch({
        r <- DELETE(uri, prxy, body = body, httr::add_headers(.headers = headers), encode = encoding)
      }, error = function(err) {
        print(err)
        cat(sprintf("Http status code %s: %s", httr::status_code(r), httr::content(r)))
        cat("Trying to Reconnect EMS...\n")
        conn = reconnect(conn)
        r <- DELETE(uri, prxy, body = body, httr::add_headers(.headers = headers), encode = encoding)
      })
    } else {
      stop(sprintf("%s: Unsupported request type.", rtype))
    }
    r
  }


