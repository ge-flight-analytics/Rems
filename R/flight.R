sp_chr <- c("\\.", "\\^", "\\(", "\\)", "\\[", "\\]", "\\{", "\\}",
            "\\-", "\\+", "\\?", "\\!", "\\*", "\\$", "\\|", "\\&")
exclude_dirs <- c('Download Information', 'Download Review', 'Processing',
                  'Profile 16 Extra Data', 'Flight Review', 'Data Information',
                  'Operational Information', 'Operational Information (ODW2)',
                  'Weather Information', 'Profiles', 'Profile')

flight <-
  function(conn, ems_id, data_file = NULL)
  {
    obj <- list()
    class(obj) <- "Flight"
    obj$ems_id       <- ems_id
    obj$connection   <- conn
    obj$db_id        <- NULL
    obj$metadata     <- NULL
    obj$trees        <- list(fieldtree= data.frame(), dbtree=data.frame(), kvmaps=data.frame())
    obj <- load_tree(obj, data_file)
    obj
  }


load_tree <-
  function(flt, file_name)
  {
    if (is.null(flt$metadata)) {
      flt$metadata <- localdata(file_name)
    } else {
      if ((!is.null(file_name)) && (flt$metadata.file_loc(flt$metadata) != file.path(file_name))) {
        close.LocalData(flt$metadata)
        flt$metadata <- localdata(file_name)
      }
    }
    flt$trees <- list(fieldtree = get_fieldtree(flt),
                      dbtree    = get_dbtree(flt),
                      kvmaps    = get_kvmaps(flt))
    flt
  }


save_tree <-
  function(flt, file_name = NULL)
  {
    if ( (!is.null(file_name)) && (file_loc(flt$metadata) != file_name) ) {
      file.copy(file_loc(flt$metadata), file_name)
      close.LocalData(flt$metadata)
      flt$metadata <- localdata(file_name)
    }
    save_fieldtree(flt)
    save_dbtree(flt)
    save_kvmaps(flt)
  }


get_fieldtree <-
  function(flt)
  {
    if (is.null(flt$db_id)) {
      cols <- flt$metadata$table_info$fieldtree
      dat  <- data.frame(matrix(NA,nrow=0,ncol=length(cols)), stringsAsFactors = F)
      names(dat) <- cols
      return(dat)
    } else {
      dat <- get_data(flt$metadata, sprintf("ems_id = %d and db_id = '%s'", flt$ems_id, flt$db_id))
      return(dat)
    }
  }


save_fieldtree <-
  function(flt)
  {
    if (length(flt$trees$fieldtree) > 0) {
      delete_data(flt$metadata, 'fieldtree', sprintf("ems_id = %d and db_id = '%s'", flt$ems_id, flt$db_id))
      append_data(flt$metadata, 'fieldtree', flt$trees$fieldtree)
    }
  }


get_dbtree <-
  function(flt)
  {
    tr <- get_data(flt$metadata, 'dbtree', paste("ems_id =", flt$ems_id))
    if (length(tr) < 1) {
      dbroot <- list(ems_id = flt$ems_id,
                     id     = "[-hub-][entity-type-group][[--][internal-type-group][root]]",
                     name   = "<root>",
                     nodetype = "root",
                     parent_id = "")
      flt$trees$dbtree <- data.frame(dbroot, stringsAsFactors = F)
      flt <- update_children(flt, dbroot, treetype = 'dbtree')
      flt <- update_tree(flt, 'fdw', treetype = 'dbtree', exclude_tree = "APM Events")
      save_dbtree(flt)
      tr <- flt$trees$dbtree
    }
    tr
  }


save_dbtree <-
  function(flt)
  {
    if (length(flt$trees$dbtree) > 0) {
      delete_data(flt$metadata, 'dbtree', sprintf("ems_id = %d", flt$ems_id))
      append_data(flt$metadata, 'dbtree', flt$trees$dbtree)
    }
  }


get_kvmaps <-
  function(flt)
  {
    get_data(flt$metadata, 'kvmaps', paste("ems_id =", flt$ems_id))
  }


save_kvmaps <-
  function(flt)
  {
    if (length(flt$trees$kvmaps) > 0) {
      delete_data(flt$metadata, 'kvmaps', sprintf("ems_id = %d", flt$ems_id))
      append_data(flt$metadata, 'kvmaps', flt$trees$kvmaps)
    }
  }


set_database.Flight <-
  function(flt, dbname)
  {
    tr <- flt$trees$dbtree
    flt$db_id <- tr[tr$nodetype=="database" & grepl(treat_spchar(dbname), tr$name, ignore.case=T), 'id']
    flt$trees$fieldtree <- get_fieldtree(flt)
    flt <- update_children(get_database.Flight(flt), treetype= "fieldtree")
    cat("Using database '%s'.\n", get_database.Flight(flt)$name)
    flt
  }


get_database.Flight <-
  function(flt)
  {
    tr <- flt$trees$dbtree
    return(as.list(tr[tr$db_id==flt$db_id, ]))
  }


db_request <-
  function(flt, parent)
  {
    body <- NULL
    if (parent$nodetype=="database_group") {
      body <- list('groupId' = parent$id)
    }
    r    <- request(flt$connection,
                    uri_keys = c('database','group'),
                    uri_args = flt$ems_id,
                    body = body)
    ##  Get the children fields/field groups
    d <- content(r)

    d1 <- list()
    if (length(d$databases) > 0) {
      d1 <- lapply(d$databases, function(x) list(ems_id    = parent$ems_id,
                                                  id        = x$id,
                                                  nodetype  = 'database',
                                                  name      = x$pluralName,
                                                  parent_id = parent$id))
    }
    d2 <- list()
    if (length(d$groups) > 0) {
      d2 <- lapply(d$groups, function(x) list(ems_id    = parent$ems_id,
                                              id        = x$id,
                                              nodetype  = 'database_group',
                                              name      = x$pluralName,
                                              parent_id = parent$id))
    }
    return(list(d1=d1, d2=d2))
  }


fl_request <-
  function(flt, parent)
  {
    body <- NULL
    if (parent$nodetype=="field_group") {
      body <- list('groupId' = parent$id)
    }
    r    <- request(flt$connection,
                    uri_keys = c('database','field_group'),
                    uri_args = c(flt$ems_id, flt$db_id),
                    body = body)
    ##  Get the children fields/field groups
    d <- content(r)

    d1 <- list()
    if (length(d$fields) > 0) {
      d1 <- lapply(d$fields, function(x) list(ems_id    = parent$ems_id,
                                              db_id     = flt$db_id,
                                               id        = x$id,
                                               nodetype  = 'fields',
                                               type      = x$type,
                                               name      = x$name,
                                               parent_id = parent$id))
    }
    d2 <- list()
    if (length(d$groups) > 0) {
      d2 <- lapply(d$groups, function(x) list(ems_id    = parent$ems_id,
                                              db_id     = flt$db_id,
                                              id        = x$id,
                                              nodetype  = 'field_group',
                                              type      = '',
                                              name      = x$name,
                                              parent_id = parent$id))
    }
    return(list(d1=d1, d2=d2))
  }


add_subtree <-
  function(flt, parent, exclude_tree = c(), treetype = c('fieldtree', 'dbtree')) {

    cat(paste("On %s (%s)...\n", parent$name, parent$nodetype))

    if (treetype == 'dbtree') {
      searchtype <- 'database'
      res <- db_request(flt, parent)
    } else {
      searchtype <- 'fieldtree'
      res <- fl_request(flt, parent)
    }

    if (length(res$d1) > 0) {
      flt$trees[[treetype]] <- rbind(flt$trees[[treetype]], lls_to_df(res$d1), stringsAsFactors=F)
      plural <- if (length(res$d1) > 1) "s" else ""
      cat(sprintf("-- Added %d %s%s\n", length(res$d1), searctype, plural))
    }


    for (x in res$d2) {
      flt$trees[[treetype]] <- rbind(flt$trees[[treetype]], x, stringsAsFactors=F)
      if ( (length(exclude_tree) == 0) || (all(sapply(exclude_tree, function(x) !grepl(x, x$name)))) ) {
        flt <- add_subtree(flt, exclude_tree, treetype)
      }
    }
    flt
  }


get_children <-
  function(flt, parent_id, treetype = c('fieldtree','dbtree'))
  {
    tr <- flt$trees[[treetype]]
    return( tr[tr$parent_id %in% parent_id, ])
  }


remove_subtree <-
  function(flt, parent, treetype = c('fieldtree','dbtree'))
  {
    tr <- flt$trees[[treetype]]


    # Update the instance tree by deleting children
    flt$trees[[treetype]] <- tr[tr$parent_id != parent_id, ]

    # Iterate and do recursive removal of children of children
    leaftype <- if (treetype=='fieldtree') 'field' else 'database'
    chld <- tr[(tr$parent_id == parent$id) & (tr$nodetype!=leaftype), ]
    if (nrow(chld) > 0) {
      for (i in 1:nrow(chld)) {
        flt <- remove_subtree(flt, chld[i,], treetype)
      }
    }
    flt
  }


update_children <-
  function(flt, parent, treetype = c('fieldtree', 'dbtree'))
  {

    cat(paste("On %s (%s)...\n", parent$name, parent$nodetype))

    if (treetype == 'dbtree') {
      searchtype <- 'database'
      res <- db_request(flt, parent)
    } else {
      searchtype <- 'fieldtree'
      res <- fl_request(flt, parent)
    }

    tr <- flt$trees[[treetype]]
    flt$trees[[treetype]] <- subset(tr, (nodetype==searchtype) & (parent_id == parent$id))

    if (length(res$d1) > 0) {
      flt$trees[[treetype]] <- rbind(flt$trees[[treetype]], lls_to_df(res$d1), stringsAsFactors=F)
      plural <- if (length(res$d1) > 1) "s" else ""
      cat(sprintf("-- Added %d %s%s\n", length(res$d1), searctype, plural))
    }
    # If there is an array of groups as children add any that appeared new and remove who does not.
    old_groups <- subset(tr, (nodetype==paste(searchtype, "group", sep="_")) & (parent_id==parent$id))
    old_ones   <- old_groups$id
    new_ones   <- sapply(res$d2, function(x) x$id)

    rm_id <- setdiff(old_ones, new_ones)
    for (x in subset(old_groups, id %in% rm_id)) {
      flt <- remove_subtree(flt, x, treetype)
    }

    add_id <- setdif(new_ones, old_ones)
    for (x in res$d2) {
      if (x$id %in% add_id) {
        flt$trees[[treetype]] <- rbind(flt$trees[[treetype]], x, stringsAsFactors=F)
      }
    }
    flt
  }


update_tree <-
  function(flt, path, exclude_tree = c(), treetype=c('fieldtree','dbtree'))
  {
    searchtype <- if(treetype=="field") 'fieldtree' else 'dbtree'

    path <- tolower(path)
    for ( i in seq_along(tolower(path)) ) {
      p <- treat_spchar(path[i])
      if (i == 1) {
        tr <- flt$trees[[treetype]]
        parent <- tr[grepl(p, tr$name, ignore.case = T), ]
      } else {
        flt     <- update_children(flt, parent, treetype=treetype)
        chld_df <- get_children(flt, parent$id, treetype=treetype)
        child   <- subset(chld_df, grepl(p, name, ignore.case = T))
        parent  <- child
      }
      if (nrow(parent) == 0) {
        stop(sprintf("Search keyword '%s' did not return any %s group.", path[i], searchtype))
      }
      ptype <- paste(searchtype, "group", sep="_")
      parent <- parent[parent$nodetype == ptype, ]
      parent <- get_shortest(parent)
    }
    cat(sprintf("=== Starting to add subtree from '%s' (%s) ===\n", parent$name, parent$nodetype))
    flt <- remove_subtree(flt, parent, treetype=treetype)
    flt <- add_subtree(flt, parent, exclude_tree, treetype = treetype)
    return(flt)
  }


make_default_tree <-
  function(flt)
  {
    dbnode <- get_database.Flight(flt)
    flt <- remove_subtree(flt, dbnode, treetype="fieldtree")
    flt <- add_subtree(flt, dbnode, exclude_tree=exclude_dirs, treetype="fieldtree")
    flt
  }


search_fields <-
  function(flt, ...)
  {
    flist <- list(...)
    res   <- list()
    for ( f in flist ) {
      if ( length(f) == 1 ) {
        # Single keyword case
        names <- tolower(flt$tree$name)
        for ( x in sp_chr ) {
          f <- gsub(x, paste("\\", x, sep=""), f)
        }
        df <- subset(flt$tree, (nodetype=='field') & ((f==names) | grepl(f, names, ignore.case = T)) )
        df <- df[order(nchar(df$name)), ]
        res[[length(res)+1]] <- as.list(df[1,])

      } else if ( length(f) > 1 ){
        # Vector of hierarchical keyword set
        chld <- flt$tree
        for ( i in seq_along(f) ) {
          for ( x in sp_chr ) {
            f[i] <- gsub(x, paste("\\", x, sep=""), f[i])
          }
          names   <- tolower(chld$name)
          node_id <- chld[(f[i]==names) | grep(f[i], names, ignore.case = T), 'id']
          if (i < length(f)) {
            chld    <- subset(flt$tree, parent %in% node_id)
          } else {
            chld    <- subset(chld, (nodetype=='field') & ((f[i]==names) | grepl(f[i], names, ignore.case = T)) )
          }
        }
        res[[length(res)+1]] <- as.list( chld[order(nchar(chld$name))[1], ] )
      }
    }

    if ( any(is.na(sapply(res, function(x) x$id))) ) {
      stop("Some of the search results for give field keywords did not return fields. Please check if the keywords are valid.")
    }

    # if ( length(res) == 1 ) {
    #   res <- res[[1]]
    # }
    return(res)
  }


list_allvalues <-
  function(flt, field = NULL, field_id = NULL, in_list=FALSE)
  {
    fld_id <- field_id

    if ( is.null(field_id) ) {
      fld <- search_fields(flt, field)[[1]]
      fld_type <- fld$type
      fld_id   <- fld$id
      if ( fld_type != "discrete" )  {
        stop("Queried field should be discrete type to get the list of possible values.")
      }
    }

    if (flt$key_maps$has_key(fld_id)) {
      kmap <- flt$key_maps$get(fld_id)
    } else {
      db_id <- flt$database$id
      cat("Getting key-value mappings from API. (Caution: runway ID takes much longer)\n")
      r <- request(flt$connection,
                   uri_keys = c('database', 'field'),
                   uri_args = c(flt$ems_id, db_id, fld_id))
      kmap <- content(r)$discreteValues
      flt$key_maps$set(fld_id, kmap)
    }

    if ( in_list ) {
      return(kmap)
    }
    return(kmap)
  }


get_value_id <-
  function(flt, value, field=NULL, field_id=NULL)
  {
    val_map <- list_allvalues(flt, field = field, field_id = field_id, in_list = T)
    id <- which(val_map==value)

    if ( length(id)==0 ) {
      stop(sprintf("The queried value '%s' is not part of the possible values of the discrete field.", value))
    }
    id <- as.integer(names(id))
    return(id)
  }


lls_to_df <-
  function(lls)
  {
    for (i in 1:length(lls)) {
      if (i==1) {
        dat <- data.frame(lls[[i]], stringsAsFactors = F)
      } else {
        dat <- rbind(dat, lls[[i]])
      }
    }
    dat
  }
