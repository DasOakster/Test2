#-----------------------------------------------------------------------------------------------------------------------------------
# Function Library to Manage Pimberly Data - Test File in Git Repo - New Amends (And Again)
#-----------------------------------------------------------------------------------------------------------------------------------

# get.channel.data (channel.token, pim.env)
# parse.pim.data (pim.data)
# get.parent.data (feed.token, pim.env, parent.list)
# get.item.data (feed.token, pim.env, item.list)
# put.attribute.data(df, attribute, feed token)
# put.multi.attribute.data(df,feed token)
# parse.pim.link.data (product.data,link)

#-----------------------------------------------------------------------------------------------------------------------------------
# get.channel.data function
# Returns a list of products called down from a specified Pimberly channel
# Takes a Channel token and either the Production or Sandbox as parameters
# Uses the parse.pim.data function to create a product.data list of Primary ID, Attribute and Value
#-----------------------------------------------------------------------------------------------------------------------------------

get.channel.data <- function(channel.token, pim.env, link = "") {
  
  # Initialise to the first page
  since.id <- ""
  i <- 1
  link <- link

  #-----------------------------------------------------------------------------------------------------------------------------------  

  # Repeat until the maxID = "" i.e. the last page
  repeat {
    
    # Modify URL depending on pim.environment
    if (pim.env == "Production") url <- paste0("https://pimber.ly/api/v2.2/products/", since.id)
    if (pim.env == "Sandbox") url <- paste0("https://sandbox.pimber.ly/api/v2.2/products/", since.id)
  
    #-----------------------------------------------------------------------------------------------------------------------------------    
    
      # Get the payload.  Uses library httr
      payload <- GET(url, httr::add_headers(Authorization = channel.token))
      
      # Convert the payload into a list.  Uses library Jsonlite.  Return the list to the global scope
      page.data <- fromJSON(content(payload, "text"), simplifyVector = FALSE)
      
      # Check on the first call if the channel is empty and terminate
      if(i == 1 && length(page.data) == 1) {
        
        message(paste0("Error ", page.data[["error"]][["status"]], " :", page.data[["error"]][["message"]]))
        stop()
        
      }

      #-----------------------------------------------------------------------------------------------------------------------------------      
      
      # Extracts the product data from the payload and appends it into a single list
      if (exists("pim.data")) {
        # Append to the existing list
        
        pim.data <- c(pim.data, page.data[["data"]])
        
      } else { # Create a new list
        
        pim.data <- page.data[["data"]]
      
      }
      
      #-----------------------------------------------------------------------------------------------------------------------------------      
      
      # Returns the next page id required for the next call
      message(paste0("Page: ",i))
      since.id <- paste0("?sinceId=", page.data[["maxId"]])
      
      # Ens the process if the maxID is blank i.e. the end page
      if (since.id == "?sinceId=") {
        pim.data <<- pim.data #Return the dataframe
        message(paste0("get.channel.data:\n\tProcess successful\n\tProducts retrived = ",length(pim.data)))
        message("Parsing Pim Data to Product Data")
        parse.pim.data(pim.data)
        
        # If the link parameter was supplied for items or parents add the id as an extra column
        if(link != "") parse.pim.link.data(product.data, link)
        
        break
        
      }
      
        i <- i + 1
      
      #-----------------------------------------------------------------------------------------------------------------------------------
  }
  
} # End Function

#-----------------------------------------------------------------------------------------------------------------------------------
# parse.pim.data function
# Returns a parseed list of products called down from a specified Pimberly channel
# Takes a list downloaded from Pimberly
#-----------------------------------------------------------------------------------------------------------------------------------

parse.pim.data <- function(pim.data){
# Parse the variant data into the required format for complete.names
for(i in 1:length(pim.data)) {
  
  # The list is nested.  This flattens the list
  product.record <- unlist(pim.data[[i]])
  
  # Converts the list to a data frame and renames the columns
  product.record <- ldply(product.record, data.frame)
  names(product.record) <- c("Attribute", "Value")
  primary.id <- filter(product.record, Attribute == "Primary ID")
  primary.id <- as.character(primary.id[1,2])
  product.record$Primary.ID <- primary.id
  product.record <- product.record[,c(3,1,2)]
  
  # Appends the product records to a data frame holding all the data
  #  On the first run it will create the permaenent data frame
  if(exists("product.data")) {
    
    product.data <- suppressWarnings(bind_rows(product.data, product.record))
    
  } else {
    
    product.data <- product.record
    
  }
  
}

  product.data <<- product.data
  
} # End Function

#-----------------------------------------------------------------------------------------------------------------------------------
# get.parent.data function
# Returns a parsed list of parent products called down from a feed channel
# Takes a list of parent.ids, a feed token and the type of environment - Sandbox or Production
#-----------------------------------------------------------------------------------------------------------------------------------

get.parent.data <- function(feed.token, pim.env, parent.list) {
  
  # Create the Parent endpoints
  if(pim.env =="Production") parent.list$product.endpoint <- paste0("https://pimber.ly/api/v2.2/products/",parent.list$Value,"?attributes=*")
  if(pim.env == "Sandbox") parent.list$product.endpoint <- paste0("https://sandbox.pimber.ly/api/v2.2/products/",parent.list$Value,"?attributes=*")
  parent.list <- unique(parent.list[,c("Value", "product.endpoint")])
  names(parent.list) <- c("Parent.ID", "Endpoint")
  
  # Create the list of Parent endpoints to call
  product.list <- unique(parent.list$Endpoint)
  
  # Iterate through the list of Endpoints and create a single list
  # Add the Parent ID as the Primary ID to create a Narrow file
  for(p in 1:length(product.list)) {
    
    # Return the Product data from the all products channel
    url <- product.list[p]
    primary.id <- parent.list[p,1]
    
    payload <- GET(url, httr::add_headers(Authorization = feed.token))
    parent.feed.data <- fromJSON(content(payload, "text"), simplifyVector = FALSE)
    
    # Converts the list to a data frame and renames the columns
    parent.feed.data <- ldply(parent.feed.data, data.frame)
    names(parent.feed.data) <- c("Attribute", "Value")
    parent.feed.data$Primary.ID <- primary.id
    parent.feed.data <- parent.feed.data[,c(3,1,2)]
    
    # Appends the product records to a data frame holding all the data
    #  On the first run it will create the permaenent data frame
    if(exists("parent.data")) {
      
      parent.data <- bind_rows(parent.data, parent.feed.data)
      
      
    } else {
      
      parent.data <- parent.feed.data
      
    }
    
  }
  
  parent.data <<- parent.data
  
  } # End Function

#-----------------------------------------------------------------------------------------------------------------------------------
# get.item.data function
# Returns a parsed list of item products called down from a feed channel
# Takes a list of item.ids, a feed token and the type of environment - Sandbox or Production
#-----------------------------------------------------------------------------------------------------------------------------------

get.item.data <- function(feed.token, pim.env, item.list) {
  
  # Create the item endpoints
  if(pim.env =="Production") item.list$product.endpoint <- paste0("https://pimber.ly/api/v2.2/products/",item.list$Value,"?attributes=*")
  if(pim.env == "Sandbox") item.list$product.endpoint <- paste0("https://sandbox.pimber.ly/api/v2.2/products/",item.list$Value,"?attributes=*")
  item.list <- unique(item.list[,c("Value", "product.endpoint")])
  names(item.list) <- c("item.ID", "Endpoint")
  
  # Create the list of item endpoints to call
  product.list <- unique(item.list$Endpoint)
  
  # Iterate through the list of Endpoints and create a single list
  # Add the item ID as the Primary ID to create a Narrow file
  for(p in 1:length(product.list)) {
    
    # Return the Product data from the all products channel
    url <- product.list[p]
    primary.id <- item.list[p,1]
    
    payload <- GET(url, httr::add_headers(Authorization = feed.token))
    item.feed.data <- fromJSON(content(payload, "text"), simplifyVector = FALSE)
    
    # Converts the list to a data frame and renames the columns
    item.feed.data <- ldply(item.feed.data, data.frame)
    names(item.feed.data) <- c("Attribute", "Value")
    item.feed.data$Primary.ID <- primary.id
    item.feed.data <- item.feed.data[,c(3,1,2)]
    
    # Appends the product records to a data frame holding all the data
    #  On the first run it will create the permaenent data frame
    if(exists("item.data")) {
      
      item.data <- bind_rows(item.data, item.feed.data)
      
      
    } else {
      
      item.data <- item.feed.data
      
    }
    
  }
  
  item.data <<- item.data
  
} # End Function

#-----------------------------------------------------------------------------------------------------------------------------------
# put.attribute.data function
# Takes a 2 column list - first column id, second column value to update
# Takes a list of products and values to update, the feed token and the Pimberly attribute name
#-----------------------------------------------------------------------------------------------------------------------------------

put.attribute.data <- function(attribute.data, attribute, feed.token) {

# PUT the updated data back into Pimberly
for(i in 1:nrow(attribute.data)){
  
  # Set the product to be updated and the value to update
  product.id <- as.character(attribute.data[i,1])
  upload.data <- as.character(attribute.data[i,2])
  
  # Set the URL including the product id and the attribute name to be updated 
  url <- paste0("https://sandbox.pimber.ly/api/v2.2/products/",product.id,"/attributes/",attribute)
  
  # Convert the data to JSON
  upload.data <- toJSON(upload.data,auto_unbox = TRUE)
  
  #Upload the data to Pimberly
  upload <- PUT(url, body = upload.data, httr::add_headers(Authorization = feed.token), content_type("application/json"))
  
}

}

#-----------------------------------------------------------------------------------------------------------------------------------
# put.multi.attribute.data function
# Takes a 3 column list - first column id, second column attribute, third column the value to update
# Takes a list of products and values to update, the feed token and the Pimberly attribute name
#-----------------------------------------------------------------------------------------------------------------------------------

put.multi.attribute.data <- function(attribute.data, feed.token) {
  
  # PUT the updated data back into Pimberly
  for(i in 1:nrow(attribute.data)){
    
    # Set the product to be updated and the value to update
    product.id <- as.character(attribute.data[i,1])
    attribute.name <- as.character(attribute.data[i,2])
    upload.data <- as.character(attribute.data[i,3])
    
    # Set the URL including the product id and the attribute name to be updated 
    url <- paste0("https://sandbox.pimber.ly/api/v2.2/products/",product.id,"/attributes/",attribute.name)
    
    # Convert the data to JSON
    upload.data <- toJSON(upload.data,auto_unbox = TRUE)
    
    #Upload the data to Pimberly
    upload <- PUT(url, body = upload.data, httr::add_headers(Authorization = feed.token), content_type("application/json"))
    
  }
  
}

#-----------------------------------------------------------------------------------------------------------------------------------
# parse.pim.data function
# Returns a parseed list of products called down from a specified Pimberly channel including a column for Parent or Item ID
# Takes a list downloaded from Pimberly and either Parent or Item as the link
# The link parameter is either 'Parent' or 'Item'
#-----------------------------------------------------------------------------------------------------------------------------------

parse.pim.link.data <- function(product.data, link){
  
  # The Item data is created by subsetting the Parent data for each of its item and appending to a dataframe
  if(link == "Item") {
    
    # Get the combinations of parent and item
    link.id <- product.data[grep("Item", product.data$Attribute),]
          
          for(i in 1:nrow(link.id)){
          
            # Get the parent id and the item id
            primary.id <- link.id[i,1]
            item.id <- link.id[i,3]
            
            
            parent.data <- filter(product.data, Primary.ID == primary.id)
            parent.data$Item.ID <- item.id
            
                # Appends the product records to a data frame holding all the data
                #  On the first run it will create the permaenent data frame
                if(exists("item.data")) {
                  
                  item.data <- suppressWarnings(bind_rows(item.data, parent.data))
                  
                } else {
                  
                  item.data <- parent.data
                  
                }
            
          }
    
    # Rename column and assign data to product.data
    product.data <- item.data
    colnames(product.data)[4] <- "Item.ID"
    
  }
  

  if(link == "Parent") { # Assumes each item has only one parent
    
    link.id <- product.data[grep("Parent", product.data$Attribute),]
    product.data$link <- link.id$Value[match(product.data$Primary.ID, link.id$Primary.ID)]
    colnames(product.data)[4] <- "Parent.ID"

  }
  
  product.data <<- product.data
    
} # End Function

#-----------------------------------------------------------------------------------------------------------------------------------
# get.channel.data function
# Returns a list of products called down from a specified Pimberly channel
# Takes a Channel token and either the Production or Sandbox as parameters
# Uses the parse.pim.data function to create a product.data list of Primary ID, Attribute and Value
#-----------------------------------------------------------------------------------------------------------------------------------

get.feed.data <- function(feed.token, pim.env) {
  
  # Initialise to the first page
  since.id <- ""
  i <- 1
  #-----------------------------------------------------------------------------------------------------------------------------------  
  
  # Repeat until the maxID = "" i.e. the last page
  repeat {
    
    # Modify URL depending on pim.environment
    if(pim.env =="Production") url <- paste0("https://pimber.ly/api/v2.2/products?extendResponse=1&attributes=*&",since.id)
    if(pim.env == "Sandbox") url <- paste0("https://sandbox.pimber.ly/api/v2.2/products?extendResponse=1&attributes=*&",since.id)
    
    #-----------------------------------------------------------------------------------------------------------------------------------    
    
    # Get the payload.  Uses library httr
    payload <- GET(url, httr::add_headers(Authorization = feed.token))
    
    # Convert the payload into a list.  Uses library Jsonlite.  Return the list to the global scope
    page.data <- fromJSON(content(payload, "text"), simplifyVector = FALSE)
    
    # Check on the first call if the channel is empty and terminate
    if(i == 1 && length(page.data) == 1) {
      
      message(paste0("Error ", page.data[["error"]][["status"]], " :", page.data[["error"]][["message"]]))
      stop()
      
    }
    
    #-----------------------------------------------------------------------------------------------------------------------------------      
    
    # Extracts the product data from the payload and appends it into a single list
    if (exists("pim.data")) {
      # Append to the existing list
      
      pim.data <- c(pim.data, page.data[["data"]])
      
    } else { # Create a new list
      
      pim.data <- page.data[["data"]]
      
    }
    
    #-----------------------------------------------------------------------------------------------------------------------------------      
    
    # Returns the next page id required for the next call
    message(paste0("Page: ",i,": ",since.id))
    since.id <- paste0("sinceId=", page.data[["maxId"]])
    
    # Ens the process if the maxID is blank i.e. the end page
    if (since.id == "sinceId=") {
      pim.data <<- pim.data #Return the dataframe
      message(paste0("get.feed.data:\n\tProcess successful\n\tProducts retrived = ",length(pim.data)))
      message("Parsing Pim Data to Product Data")
      parse.feed.data(pim.data)
  
      break
      
    }
    
    i <- i + 1
    
    #-----------------------------------------------------------------------------------------------------------------------------------
  }
  
} # End Function

#-----------------------------------------------------------------------------------------------------------------------------------
# parse.feed.data function
# Returns a parseed list of products called down from a specified Pimberly channel
# Takes a list downloaded from Pimberly
#-----------------------------------------------------------------------------------------------------------------------------------

parse.feed.data <- function(pim.data){
  # Parse the variant data into the required format for complete.names
  for(i in 1:length(pim.data)) {
    
    # The list is nested.  This flattens the list
    product.record <- unlist(pim.data[[i]])
    
    # Converts the list to a data frame and renames the columns
    product.record <- ldply(product.record, data.frame)
    names(product.record) <- c("Attribute", "Value")
    primary.id <- filter(product.record, Attribute == "primaryId")
    primary.id <- as.character(primary.id[1,2])
    product.record$Primary.ID <- primary.id
    product.record <- product.record[,c(3,1,2)]
    
    # Appends the product records to a data frame holding all the data
    #  On the first run it will create the permaenent data frame
    if(exists("product.data")) {
      
      product.data <- suppressWarnings(bind_rows(product.data, product.record))
      
    } else {
      
      product.data <- product.record
      
    }
    
    message(paste0("Product ",i, " of ", length(pim.data)))
  }
  
  product.data <<- product.data
  
} # End Function

#-----------------------------------------------------------------------------------------------------------------------------------
# get.product.parent function
# Returns a parsed list of parent products called down from a feed channel
# Takes a list of product ids, a feed token and the type of environment - Sandbox or Production
#-----------------------------------------------------------------------------------------------------------------------------------

get.product.parents <- function(feed.token, pim.env, parent.list, parent.id.only = FALSE) {
  
  # Iterate through the list of Endpoints and create a single list
  # Add the Parent ID as the Primary ID to create a Narrow file
  for(p in 1:nrow(parent.list)) {
    
    primary.id <- parent.list[p,1]
    
    # Create the Parent endpoints
    if(pim.env =="Production" && parent.id.only == FALSE) url <- paste0("https://pimber.ly/api/v2.2/products/",primary.id,"/parents?extendResponse=1&attributes=*")
    if(pim.env == "Sandbox" && parent.id.only == FALSE) url <- paste0("https://sandbox.pimber.ly/api/v2.2/products/",primary.id,"/parents?extendResponse=1&attributes=*")
    if(pim.env =="Production" && parent.id.only == TRUE) url <- paste0("https://pimber.ly/api/v2.2/products/",primary.id,"/parents")
    if(pim.env == "Sandbox" && parent.id.only == TRUE) url <- paste0("https://sandbox.pimber.ly/api/v2.2/products/",primary.id,"/parents")

    # Call the endpoint
    payload <- GET(url, httr::add_headers(Authorization = feed.token))
    
    # Get the Parent Data
    parent.feed.data <- fromJSON(content(payload, "text"), simplifyVector = FALSE)
    
    # Check if the product has a parent value and if so process the parent data
    if(length(parent.feed.data) > 1) {
    
        parent.id <- parent.feed.data[["data"]][[1]][["primaryId"]]
        
        # Converts the list to a data frame and renames the columns
        parent.feed.data <- ldply(parent.feed.data[["data"]], data.frame)
        parent.feed.data <- suppressWarnings(melt(parent.feed.data, id.vars = "primaryId", na.rm = TRUE)) #Coercion of factors causes warning
        parent.feed.data$Item.ID <- primary.id
        names(parent.feed.data) <- c("Primary.ID","Attribute", "Value","Item.ID")
        
        # Appends the product records to a data frame holding all the data
        #  On the first run it will create the permaenent data frame
        if(exists("parent.data")) {
          
          parent.data <- suppressWarnings(bind_rows(parent.data, parent.feed.data))
          
          
        } else {
          
          parent.data <- parent.feed.data
          
        }
        
      }
      
      }  
    
  if(exists("parent.data")) {
   parent.data <<- parent.data
  } else {
    
    message("No Parent Data Returned")
  }
  
} # End Function

#-----------------------------------------------------------------------------------------------------------------------------------
# get.product.item function
# Returns a parsed list of item products called down from a feed channel
# Takes a list of product ids, a feed token and the type of environment - Sandbox or Production
#-----------------------------------------------------------------------------------------------------------------------------------

get.product.items <- function(feed.token, pim.env, item.list, item.id.only = FALSE) {
  
  # Iterate through the list of Endpoints and create a single list
  # Add the Parent ID as the Primary ID to create a Narrow file
  for(p in 1:nrow(item.list)) {
    
    primary.id <- item.list[p,1]
    
    # Create the item endpoints
    if(pim.env =="Production" && item.id.only == FALSE) url <- paste0("https://pimber.ly/api/v2.2/products/",primary.id,"/items?extendResponse=1&attributes=*")
    if(pim.env == "Sandbox" && item.id.only == FALSE) url <- paste0("https://sandbox.pimber.ly/api/v2.2/products/",primary.id,"/items?extendResponse=1&attributes=*")
    if(pim.env =="Production" && item.id.only == TRUE) url <- paste0("https://pimber.ly/api/v2.2/products/",primary.id,"/items")
    if(pim.env == "Sandbox" && item.id.only == TRUE) url <- paste0("https://sandbox.pimber.ly/api/v2.2/products/",primary.id,"/items")
    
    # Call the endpoint
    payload <- GET(url, httr::add_headers(Authorization = feed.token))
    
    # Get the item Data
    item.feed.data <- fromJSON(content(payload, "text"), simplifyVector = FALSE)
    
    # Check if the product has a item value and if so process the item data
    if(length(item.feed.data) > 1) {
      
      item.id <- item.feed.data[["data"]][[1]][["primaryId"]]
      
      # Converts the list to a data frame and renames the columns
      item.feed.data <- ldply(item.feed.data[["data"]], data.frame)
      item.feed.data <- suppressWarnings(melt(item.feed.data, id.vars = "primaryId", na.rm = TRUE)) #Coercion of factors causes warning
      item.feed.data$Item.ID <- primary.id
      names(item.feed.data) <- c("Primary.ID","Attribute", "Value","Parent.ID")
      
      # Appends the product records to a data frame holding all the data
      #  On the first run it will create the permaenent data frame
      if(exists("item.data")) {
        
        item.data <- suppressWarnings(bind_rows(item.data, item.feed.data))
        
        
      } else {
        
        item.data <- item.feed.data
        
      }
      
    }
    
  }  
  
  if(exists("item.data")) {
    item.data <<- item.data
  } else {
    
    message("No item Data Returned")
  }
  
} # End Function