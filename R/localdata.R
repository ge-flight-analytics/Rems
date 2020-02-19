localdata <-
  function(dbfile = NULL)
  {
    obj <- list()
    class(obj) <- 'LocalData'
    obj$table_info <- list(fieldtree = c('ems_id','db_id','id','nodetype','type','name','parent_id'),
                           dbtree    = c('ems_id','id','nodetype','name','parent_id'),
                           kvmaps    = c('ems_id','id','key','value'),
                           params    = c('ems_id','id','name','description','units'))

    if (is.null(dbfile)) {
      obj$dbfile <- file.path(path.package("Rems"),'data', 'emsMetaData.db')
    } else {
      obj$dbfile <- file.path(dbfile)
    }
    obj <- connect.LocalData(obj)
    obj
  }

connect.LocalData <-
  function(ldat)
  {
    ldat$conn <- dbConnect(SQLite(), dbname = ldat$dbfile)
    ldat
  }

check_colnames <-
  function(ldat, table_name, dat)
  {
    colnames <- ldat$table_info[[table_name]]
    missing  <- setdiff(colnames, names(dat))
    if (length(missing) > 0) {
      stop("Data misses the following columns that are required: %s", missing)
    }
  }

close.LocalData <-
  function(ldat)
  {
    dbDisconnect(ldat$conn)
  }


append_data <-
  function(ldat, table_name, dat)
  {
    check_colnames(ldat, table_name, dat)
    dbWriteTable(ldat$conn, table_name, dat, append = T, row.names = F)
  }

get_data <-
  function(ldat, table_name, condition = NULL)
  {
    if (dbExistsTable(ldat$conn, table_name)) {
      q <- paste("SELECT * FROM", table_name)
      if (!is.null(condition)) {
        q <- paste(q, "WHERE", condition)
      }
      dat <- dbGetQuery(ldat$conn, q)
      return (dat)
    }
    dat <- data.frame(matrix(NA,nrow=0, ncol=length(ldat$table_info[[table_name]])), stringsAsFactors = F)
    names(dat) <- ldat$table_info[[table_name]]
    return (dat)
  }


delete_data <-
  function(ldat, table_name, condition = NULL)
  {
    if (dbExistsTable(ldat$conn, table_name)) {
      if (is.null(condition)) {
        dbExecute(ldat$conn, paste("DROP TABLE", table_name))
      } else {
        dbExecute(ldat$conn, paste("DELETE FROM", table_name, "WHERE", condition))
      }
    }
  }


delete_all_tables <-
  function(ldat)
  {
    for (table_name in names(ldat$table_info)) {
      delete_data(ldat, table_name)
    }
  }

file_loc <-
  function(ldat)
  {
    ldat$dbfile
  }

