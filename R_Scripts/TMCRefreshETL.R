#### TMCRefreshETL.R
#Author: Danel Shields (daniel.shields@abbott.com)
#Create Date: 09/28/2022
#Notes: A script to read the Nielsen extract files, join as applicalbe, load into BOA for later processing.
#       Also, staging data from Retail Velocity Sandbox to BOA + Item Overrides and Fact Plug/Override Files
#       Script to summarize staged data is refreshed and basic queries sent when done.
#
#       Final Table list on BOA:  T_Niel_Extract_UPC_LN, T_Niel_Extract_Total_LN, T_Niel_Extract_Item_LN, 
#                                 T_Niel_Extract_UPC_ITN, T_Niel_Extract_Total_ITN, T_Niel_Extract_Item_ITN, 
#                                 RV_Item, T_Amazon, T_Kroger, T_Overrides_and_Plugs, T_ItemOverrides


#### Load Packages ----------------------------------------------------------

# #first, common packages needed and used in this code and/or common functions
# #then, custom packages hosted on Github for common functions
# Set Java Home ####
# if(file.exists('C:\\Program Files\\Java\\jre1.8.0_321')) {
#   Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_321')
#   
# } else if (file.exists('C:\\Program Files\\Java\\jre1.8.0_291')) {
#   Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_291')
#   
# } else if (file.exists('C:\\Program Files\\Java\\jre1.8.0_271')) {
#   Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_271')
#   
# } else if (file.exists('C:\\Program Files\\Java\\jre1.8.0_45')) {
#   Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_45')
# }

# library(readxl)
# library(readtext)
# library(htmlTable)
# library(mailR)
# library(odbc)
# library(lubridate)
# library(R.utils)
# library(devtools)
# library(dplyr)
# library(tidyr)
# library(stringr)
# 
# install_github("ANPD-Data-Analytics/Connections")
# install_github("ANPD-Data-Analytics/CommonFunctions")
# 
# library(Connections)
# library(CommonFunctions)
# 
# 
# #### Load Functions ----------------------------------------------------------
# 
# #Pull in the shared fuctions and variables needed for this script 
# Message2Log <- CommonFunctions::Message2Log
# postDataToBOA2 <- CommonFunctions::postDataToBOA2
# qryBOAExecute <- CommonFunctions::qryBOAExecute
# qrySandboxReturn <- CommonFunctions::qrySandboxReturn
# sendEmailNoAtt <- CommonFunctions::sendEmailNoAtt
# DataTeam <- Connections::DataTeam
# sysUser511 <- Connections::sysUser511

# # TMC20 directory ####
# directoryLoc <- "//fcrpdfile02/WRKStApps1$/Retail Sales/Retail Reports/TMC20/"
# 
# # Define SQL Query Directory ####
# SQLdirectoryMainLoc <- paste0(directoryLoc,"SQL_Queries/")
# #SQLPath <- paste0('C:/Users/', Connections::sysUser511, '/OneDrive - Abbott/WeeklyRefresh/', collapse = NULL) # Replaced with SQLdirectoryMainLoc
# 
# # NielsenExtracts Folder ####
# NielsenExtractsLoc <- paste0(directoryLoc,"NielsenExtracts/")
# #directoryLoc = paste0('C:/Users/', Connections::sysUser511, '/OneDrive - Abbott/WeeklyRefresh/LatestWeek/', collapse = NULL) # Replaced with NielsenExtractsLoc
# 
# # Plug_XL_CSV Folder ####
# Plug_XL_CSVLoc <- paste0(directoryLoc,"Plug_XL_CSVLoc/")

tryCatch( {

#Open Log File and Start Logging - to Desktop
#LogFile <- paste0("C:/Users/", Connections::sysUser511, "/Desktop/TMCRefreshETL.txt")
#LogEvent("Set directories", LogFile) #changed all LogEvent to Message2Log


#### ----
#### ----
#### Start LN NIelsen ----
#### ----

#### Ingest Files: LN Nielsen -------------------------------------------------

Message2Log("Begin Reading the Nielsen Extract Files: LN")

file1 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "LN_Corp_Report_Extract_Totals_fct") %>% 
          (function(fpath){
            ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
            return(fpath[which.max(ftime)]) # returns the most recent file path
          })
file2 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "LN_Corp_Report_Extract_UPC_fct") %>% 
          (function(fpath){
            ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
            return(fpath[which.max(ftime)]) # returns the most recent file path
          })
file3 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "LN_Corp_Report_Extract_UPC_prd_") %>% 
          (function(fpath){
            ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
            return(fpath[which.max(ftime)]) # returns the most recent file path
          })
file4 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "LN_Corp_Report_Extract_UPC_prdc") %>% 
          (function(fpath){
            ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
            return(fpath[which.max(ftime)]) # returns the most recent file path
          })
file5 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "LN_Corp_Report_Extract_UPC_mrkt") %>% 
          (function(fpath){
            ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
            return(fpath[which.max(ftime)]) # returns the most recent file path
          })
file6 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "LN260WeekTMCItemAttributes_fct") %>% 
          (function(fpath){
            ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
            return(fpath[which.max(ftime)]) # returns the most recent file path
          })
 
Totals <- read.table(file1, header = TRUE, sep = "|", fill = TRUE, quote="\\")
xxxfctDF <- read.table(file2, header = TRUE, sep = "|", fill = TRUE, quote="\\")
xxxprdDF <- read.table(file3, header = TRUE, sep = "|", fill = TRUE, quote="\\")
xxxprdcDF <- read.table(file4, header = TRUE, sep = "|", fill = TRUE, quote="\\")
xxxmktDF <- read.table(file5, header = TRUE, sep = "|", fill = TRUE, quote="\\")
Items <- read.table(file6, header = TRUE, sep = "|", fill = TRUE, quote="\\")

xxxprdcDF$PRODUCT.KEY <- as.numeric(xxxprdcDF$PRODUCT.KEY)

#merge all the data together into one flat dataframe:: T_Niel_Extract_UPC_LN
all_dataUPC <- xxxfctDF %>%
              left_join(xxxprdDF, by='Period.Key') %>%
              left_join(xxxprdcDF, by='PRODUCT.KEY') %>%
              left_join(xxxmktDF, by='Market.Key') 


#clean up the columns and names for the ITN UPC Table:: T_Niel_Extract_UPC_LN
all_dataUPC <- all_dataUPC %>%
              mutate(Period_Desc = gsub('1 w/e ', '', all_dataUPC$Period.Description))

all_dataUPC$Period_Desc <- mdy(all_dataUPC$Period_Desc)

all_dataUPC$DB <- 'LN'

names(all_dataUPC) <- c('Market_Id', 'Product_Id', 'Period_Id', 'Dollars', 'Units', 'EQ', 'Period_Desc_Long', 'ABT_Segment', 
                        'UPC', 'ABT_Brand','ABT_Category','ABT_Container_Liquid','ABT_Count_Grp_Form','ABT_Diet_Grp','ABT_Flavor',
                        'ABT_Flavor_Grp','ABT_Form','ABT_Form_Type','ABT_Manufacturer','ABT_Pack','ABT_Size_Liquid','ABT_Subbrand',
                        'ABT_Subsegment','Brand_High','Brand_Low','Brand_Owner','Brand_Owner_High','Flavor','Form','Item','Outer_Pack_Size',
                        'Package_General_Shape','Serving_Per_Container','Serving_Size_HouseHold', 'Total_Size', 'ABT_Count_Size', 
                        'Market_Desc', 'Period_Desc', 'DB')

all_dataUPC$UPC <- paste0(str_pad(all_dataUPC$UPC, width = 12,side = 'left', pad = "0"))

#clean up "temporary" files/dataframes.
#rm(list=ls(),'grep("xxx",ls())') 


#Clean/Organize the LN Totals File Data

Totals$ABT_Formula_Ty <- 'NA'
Totals$ABT_Formula_SubTy <- 'NA'
Totals$ABT_Formula_Reb_NonReb <- 'NA'
Totals$ABT_Subbrand_Grp <- 'NA'
Totals$ABT_Pack_Size <- 'NA'
Totals$ABT_Size_Range <- 'NA'
Totals$ABT_Natural_Organic <- 'NA'
Totals$ABT_Multi <- 'NA'

#clean up the columns and names for the ITN UPC Table:: T_Niel_Extract_UPC_LN
Totals <- Totals %>%
  mutate(Period_Desc = gsub('1 w/e ', '', Totals$Period.Description))

Totals$Period_Desc <- mdy(Totals$Period_Desc)
Totals$DB <- 'LN'

names(Totals) <- c('Market_Desc' ,'ABT_Segment' ,'ABT_Subsegment' ,'ABT_Manufacturer' ,'ABT_Brand' ,'ABT_Subbrand' ,
                   'ABT_Form' ,'ABT_Flavor' ,'ABT_Count_Size' ,'Period_Desc_Long' , 'Dollars' , 'Units' , 'EQ' ,'ABT_Formula_Ty' ,'ABT_Formula_SubTy' ,
                   'ABT_Formula_Reb_NonReb' ,'ABT_Subbrand_Grp' ,'ABT_Pack_Size' ,'ABT_Size_Range' ,'ABT_Natural_Organic' ,
                   'ABT_Multi', 'Period_Desc', 'DB')

Totals <- Totals %>%
  select('DB', 'Market_Desc' ,'ABT_Segment' ,'ABT_Subsegment' ,'ABT_Manufacturer' ,'ABT_Brand' ,'ABT_Subbrand' ,
         'ABT_Form' ,'ABT_Flavor' ,'ABT_Count_Size' ,'Period_Desc' ,'ABT_Formula_Ty' ,'ABT_Formula_SubTy' ,
         'ABT_Formula_Reb_NonReb' ,'ABT_Subbrand_Grp' ,'ABT_Pack_Size' ,'ABT_Size_Range' ,'ABT_Natural_Organic' ,
         'ABT_Multi', 'Dollars' , 'Units' , 'EQ' )


names(Items) <- c('MARKET_DESC', 'ABT_Brand','ABT_Category','ABT_CONTAINER', 'ABT_COUNT_GROUP_FORM', 'ABT_DIET_GROUP',
                  'ABT_Flavor', 'ABT_FLAVOR_GROUP', 'ABT_Form', 'ABT_Form_Ty', 'ABT_Manufacturer', 'ABT_Pack',
                  'ABT_Segment', 'ABT_Size_Liq', 'ABT_Subbrand', 'ABT_Subsegment', 'Brand_High', 'Brand_Low',
                  'Brand_Owner', 'BRAND_OWNER_HIGH', 'FLAVOR', 'FORM', 'Item_Desc', 'OUTER_PACK_SIZE',
                  'PACKAGE_GENERAL_SHAPE', 'SERVING_PER_CONTAINER', 'SERVING_SIZE_HOUSEHOLD', 'TOTAL_SIZE',
                  'UPC', 'ABT_Count_Size', 'PERIOD_SUM', 'DOLLARS', 'UNITS', 'EQ')

Items <- Items %>%
  select('ABT_Brand','ABT_Category','ABT_Count_Size','ABT_Flavor','ABT_Form','ABT_Form_Ty','ABT_Manufacturer',
         'ABT_Pack','ABT_Segment','ABT_Size_Liq','ABT_Subbrand','ABT_Subsegment','Brand_High','Brand_Low',
         'Brand_Owner', 'ABT_COUNT_GROUP_FORM', 'Item_Desc','UPC')

Items$UPC <- paste0(str_pad(Items$UPC, width = 12,side = 'left', pad = "0"))

all_dataUPC <- all_dataUPC %>% mutate_all(as.character)
all_dataUPC <- all_dataUPC %>% mutate_at(c('Dollars', 'Units', 'EQ'), ~replace_na(.,"0"))
Items <- Items %>% mutate_all(as.character)
Totals <- Totals %>% mutate_all(as.character)
Totals <- Totals %>% mutate_at(c('Dollars', 'Units', 'EQ'), ~replace_na(.,"0"))


#### Load Data: LN Nielsen ------------------------------------------------------

Message2Log(paste0("SQL Server Load T_Niel_Extract_UPC_LN Table"))

postDataToBOA2(all_dataUPC, "T_Niel_Extract_UPC_LN", "mitest")



Message2Log(paste0("SQL Server Load T_Niel_Extract_Total_LN Table"))

postDataToBOA2(Totals, "T_Niel_Extract_Total_LN", "mitest")



Message2Log(paste0("SQL Server Load T_Niel_Extract_Item_LN Table"))

postDataToBOA2(Items, "T_Niel_Extract_Item_LN", "mitest")


#### ----
#### ----
#### Start ITN NIelsen ----
#### ----

#### Ingest Files: ITN Nielsen -------------------------------------------------

Message2Log("Begin Reading the Nielsen Extract Files: ITN")

file1 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "ITN_Corp_Report_Extract_Totals_fct") %>% 
  (function(fpath){
    ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
    return(fpath[which.max(ftime)]) # returns the most recent file path
  })
file2 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "ITN_Corp_Report_Extract_UPC_fct") %>% 
  (function(fpath){
    ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
    return(fpath[which.max(ftime)]) # returns the most recent file path
  })
file3 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "ITN_Corp_Report_Extract_UPC_prd_") %>% 
  (function(fpath){
    ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
    return(fpath[which.max(ftime)]) # returns the most recent file path
  })
file4 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "ITN_Corp_Report_Extract_UPC_prdc") %>% 
  (function(fpath){
    ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
    return(fpath[which.max(ftime)]) # returns the most recent file path
  })
file5 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "ITN_Corp_Report_Extract_UPC_mrkt") %>% 
  (function(fpath){
    ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
    return(fpath[which.max(ftime)]) # returns the most recent file path
  })
file6 <- dir(path = NielsenExtractsLoc, full.names = T, pattern = "ITN260WeekTMCItemAttributes_fct") %>% 
  (function(fpath){
    ftime <- file.mtime(fpath) # file.info(fpath)$ctime for file CREATED time
    return(fpath[which.max(ftime)]) # returns the most recent file path
  })

Totals <- read.table(file1, header = TRUE, sep = "|", fill = TRUE, quote="\\")
xxxfctDF <- read.table(file2, header = TRUE, sep = "|", fill = TRUE, quote="\\")
xxxprdDF <- read.table(file3, header = TRUE, sep = "|", fill = TRUE, quote="\\")
xxxprdcDF <- read.table(file4, header = TRUE, sep = "|", fill = TRUE, quote="\\")
xxxmktDF <- read.table(file5, header = TRUE, sep = "|", fill = TRUE, quote="\\")
Items <- read.table(file6, header = TRUE, sep = "|", fill = TRUE, quote="\\")

xxxprdcDF$PRODUCT.KEY <- as.numeric(xxxprdcDF$PRODUCT.KEY)

#merge all the data together into one flat dataframe:: T_Niel_Extract_UPC_LN
all_dataUPC <- xxxfctDF %>%
  left_join(xxxprdDF, by='Period.Key') %>%
  left_join(xxxprdcDF, by='PRODUCT.KEY') %>%
  left_join(xxxmktDF, by='Market.Key') 


#clean up the columns and names for the ITN UPC Table:: T_Niel_Extract_UPC_LN
all_dataUPC <- all_dataUPC %>%
  mutate(Period_Desc = gsub('1 w/e ', '', all_dataUPC$Period.Description))

all_dataUPC$Period_Desc <- mdy(all_dataUPC$Period_Desc)

all_dataUPC$DB <- 'ITN'


names(all_dataUPC) <- c('Market_Id', 'Product_Id', 'Period_Id', 'Dollars', 'Units', 'EQ', 'Period_Desc_Long', 'ABT_Category', 
                        'UPC', 'ABT_Brand','ABT_Brand_Fam', 'ABT_Fiber_TN', 'ABT_Flavor', 'ABT_Form', 'ABT_Formula_Form', 'ABT_Formula_Subty',
                        'ABT_Formula_Ty', 'ABT_Manufacturer', 'ABT_Natural_Organic', 'ABT_Pack', 'ABT_Pack_Size', 'ABT_Segment', 'ABT_Size_Range',
                        'ABT_Subbrand', 'ABT_Subbrand_Grp', 'ABT_Subsegment', 'ABT_Variant', 'Brand_High', 'Brand_Low', 'Brand_Owner', 
                        'Brand_Owner_High', 'Flavor', 'Form', 'Item', 'Outer_Pack_Size', 'Package_General_Shape', 'Serving_Per_Container', 
                        'Serving_Size_HouseHold', 'Total_Size', 'Yield', 'ABT_Formula_Reb_NonReb', 'ABT_Count_Size', 'ABT_Multi', 
                        'Market_Desc', 'Period_Desc', 'DB')

all_dataUPC$UPC <- paste0(str_pad(all_dataUPC$UPC, width = 12,side = 'left', pad = "0"))

#clean up "temporary" files/dataframes.
#rm(list=ls(),'grep("xxx",ls())') 


#Clean/Organize the ITN Totals File Data


#clean up the columns and names for the ITN UPC Table:: T_Niel_Extract_UPC_LN
Totals <- Totals %>%
  mutate(Period_Desc = gsub('1 w/e ', '', Totals$Period.Description))

Totals$Period_Desc <- mdy(Totals$Period_Desc)
Totals$DB <- 'ITN'

names(Totals) <- c('Market_Desc' ,'ABT_Segment' ,'ABT_Subsegment' ,'ABT_Manufacturer' ,'ABT_Brand' ,'ABT_Form' ,
                   'ABT_Flavor' ,'ABT_Formula_Ty' ,'ABT_Formula_SubTy' ,'ABT_Formula_Reb_NonReb' ,'ABT_Subbrand' ,
                   'ABT_Subbrand_Grp' ,'ABT_Pack_Size' ,'ABT_Size_Range' ,'ABT_Natural_Organic' ,'ABT_Count_Size' ,
                   'ABT_Multi' ,'Period_Desc_Long' ,'Dollars' , 'Units' , 'EQ', 'Period_Desc', 'DB')


Totals <- Totals %>%
  select('DB', 'Market_Desc' ,'ABT_Segment' ,'ABT_Subsegment' ,'ABT_Manufacturer' ,'ABT_Brand' ,'ABT_Form' ,
         'ABT_Flavor' ,'ABT_Formula_Ty' ,'ABT_Formula_SubTy' ,'ABT_Formula_Reb_NonReb' ,'ABT_Subbrand' ,
         'ABT_Subbrand_Grp' ,'ABT_Pack_Size' ,'ABT_Size_Range' ,'ABT_Natural_Organic' ,'ABT_Count_Size' ,
         'ABT_Multi' ,'Period_Desc' ,'Dollars' , 'Units' , 'EQ')

#Totals$DB <- 'ITN'

names(Items) <- c('MARKET_DESC', 'ABT_Brand','ABT_Brand_Family', 'ABT_Category', 'ABT_Fiber_Tn', 
                  'ABT_Flavor', 'ABT_Form', 'ABT_Formula_Formulation', 'ABT_Formula_SubTy', 'ABT_Formula_Ty', 
                  'ABT_Manufacturer', 'ABT_Natural_Organic', 'ABT_Pack', 'ABT_Pack_Size', 'ABT_Segment', 
                  'ABT_Size_Range', 'ABT_Subbrand', 'ABT_Subbrand_Grp', 'ABT_Subsegment', 'ABT_Variant', 
                  'Brand_High', 'Brand_Low', 'Brand_Owner', 'Brand_Owner_High', 'Flavor', 'Form', 'Item_Desc',
                  'Outer_Pack_Size', 'Package_General_Shape', 'Serving_Per_Container', 'Serving_Size_Household', 
                  'Total_Size', 'UPC', 'Yield', 'ABT_Formula_Reb_NonReb', 'ABT_Count_Size', 'ABT_Multi', 
                  'PERIOD_SUM', 'DOLLARS', 'UNITS', 'EQ')


Items <- Items %>%
  select('ABT_Brand','ABT_Brand_Family', 'ABT_Category', 'ABT_Fiber_Tn', 
         'ABT_Flavor', 'ABT_Form', 'ABT_Formula_Formulation', 'ABT_Formula_SubTy', 'ABT_Formula_Ty', 
         'ABT_Manufacturer', 'ABT_Natural_Organic', 'ABT_Pack', 'ABT_Pack_Size', 'ABT_Segment', 
         'ABT_Size_Range', 'ABT_Subbrand', 'ABT_Subbrand_Grp', 'ABT_Subsegment', 'ABT_Variant', 
         'Brand_High', 'Brand_Low', 'Brand_Owner', 'Brand_Owner_High', 'Flavor', 'Form', 'Item_Desc',
         'Outer_Pack_Size', 'Package_General_Shape', 'Serving_Per_Container', 'Serving_Size_Household', 
         'Total_Size', 'UPC', 'Yield', 'ABT_Formula_Reb_NonReb', 'ABT_Count_Size', 'ABT_Multi')

Items$UPC <- paste0(str_pad(Items$UPC, width = 12,side = 'left', pad = "0"))

all_dataUPC <- all_dataUPC %>% mutate_all(as.character)
all_dataUPC <- all_dataUPC %>% mutate_at(c('Dollars', 'Units', 'EQ'), ~replace_na(.,"0"))
Items <- Items %>% mutate_all(as.character)
Totals <- Totals %>% mutate_all(as.character)
Totals <- Totals %>% mutate_at(c('Dollars', 'Units', 'EQ'), ~replace_na(.,"0"))


#### Load Data: ITN Nielsen ------------------------------------------------------

Message2Log(paste0("SQL Server Load T_Niel_Extract_UPC_ITN Table"))

postDataToBOA2(all_dataUPC, "T_Niel_Extract_UPC_ITN", "mitest")



Message2Log(paste0("SQL Server Load T_Niel_Extract_Total_ITN Table"))

postDataToBOA2(Totals, "T_Niel_Extract_Total_ITN", "mitest")



Message2Log(paste0("SQL Server Load T_Niel_Extract_Item_ITN Table"))

postDataToBOA2(Items, "T_Niel_Extract_Item_ITN", "mitest")



#### ----
#### ----
#### Move Data: Retail Velocity->BOA ------------------------------------------------------
#### ----

Message2Log(paste0("Start RV Data Moves"))

#readtext RV_Item.txt, T_AmazonSQL, T_KrogerSQL
#get min/max date from prev nielsne extracts and gsub for min/max variables in Kroger and Amazon SQL.

minDate <- min(all_dataUPC$Period_Desc)
maxDate <- max(all_dataUPC$Period_Desc)

tSQL1 <- readtext(paste0(SQLdirectoryMainLoc, "RV_ItemSQL.txt"))
RV_ItemSQL <- tSQL1
#Replace the MIN_WEEK_VARIABLE with the minDate where applicable
RV_ItemSQL <- gsub("MIN_WEEK_VARIABLE",minDate, RV_ItemSQL)
#Replace the MAX_WEEK_VARIABLE with the maxDate where applicable
RV_ItemSQL <- gsub("MAX_WEEK_VARIABLE",maxDate, RV_ItemSQL)


tSQL2 <- readtext(paste0(SQLdirectoryMainLoc, "T_AmazonSQL.txt"))
T_AmazonSQL <- tSQL2
#Replace the MIN_WEEK_VARIABLE with the minDate where applicable
T_AmazonSQL <- gsub("MIN_WEEK_VARIABLE",minDate, T_AmazonSQL)
#Replace the MAX_WEEK_VARIABLE with the maxDate where applicable
T_AmazonSQL <- gsub("MAX_WEEK_VARIABLE",maxDate, T_AmazonSQL)


tSQL3 <- readtext(paste0(SQLdirectoryMainLoc, "T_KrogerSQL.txt"))
T_KrogerSQL <- tSQL3
#Replace the MIN_WEEK_VARIABLE with the minDate where applicable
T_KrogerSQL <- gsub("MIN_WEEK_VARIABLE",minDate, T_KrogerSQL)
#Replace the MAX_WEEK_VARIABLE with the maxDate where applicable
T_KrogerSQL <- gsub("MAX_WEEK_VARIABLE",maxDate, T_KrogerSQL)


tSQL4 <- readtext(paste0(SQLdirectoryMainLoc, "T_Consumption_UPC_ASINSQL.txt"))
T_Consumption_UPC_ASINSQL <- tSQL4
#Replace the MIN_WEEK_VARIABLE with the minDate where applicable
T_Consumption_UPC_ASINSQL <- gsub("MIN_WEEK_VARIABLE",minDate, T_Consumption_UPC_ASINSQL)
#Replace the MAX_WEEK_VARIABLE with the maxDate where applicable
T_Consumption_UPC_ASINSQL <- gsub("MAX_WEEK_VARIABLE",maxDate, T_Consumption_UPC_ASINSQL)


Message2Log(paste0("Query RV: RV_ItemSQL"))
RV_ItemData <- qrySandboxReturn(RV_ItemSQL)

Message2Log(paste0("Query RV: T_AmazonSQL"))
T_AmazonData <- qrySandboxReturn(T_AmazonSQL)

Message2Log(paste0("Query RV: T_KrogerSQL"))
T_KrogerData <- qrySandboxReturn(T_KrogerSQL)

Message2Log(paste0("Query RV: T_Consumption_UPC_ASINSQL"))
T_Consumption_UPC_ASINData <- qrySandboxReturn(T_Consumption_UPC_ASINSQL)


Message2Log(paste0("Post to BOA: RV_ItemData"))
postDataToBOA2(RV_ItemData, "RV_Item", "mitest")

Message2Log(paste0("Post to BOA: T_AmazonData"))
postDataToBOA2(T_AmazonData, "T_Amazon", "mitest")

Message2Log(paste0("Post to BOA: T_KrogerData"))
postDataToBOA2(T_KrogerData, "T_Kroger", "mitest")

Message2Log(paste0("Post to BOA: T_Consumption_UPC_ASINData"))
postDataToBOA2(T_Consumption_UPC_ASINData, "T_Consumption_UPC_ASIN", "mitest")


#### ----
#### ----
#### Move Data: Overrides and Plugs->BOA ------------------------------------------------------
#### ----

Message2Log(paste0("Start Overrides and Plugs and Item Overrides"))

OaPfile = paste0(Plug_XL_FilesLoc, "Overrides_and_Plugs.xlsx")
#Read the File
OverridesAndPlugs <-
  read_excel(
    OaPfile,
    sheet = "Sheet1",
  )

OverridesAndPlugs <- OverridesAndPlugs %>% mutate_all(as.character)
OverridesAndPlugs$`Period Description` <- mdy(OverridesAndPlugs$`Period Description`)
OverridesAndPlugs$`$` <- as.numeric(OverridesAndPlugs$`$`)
OverridesAndPlugs$Units <- as.numeric(OverridesAndPlugs$Units)
OverridesAndPlugs$EQ <- as.numeric(OverridesAndPlugs$EQ)

OvRfile = paste0(Plug_XL_FilesLoc, "ItemOverrides.xlsx")
#Read the File
ItemOverrides <-
  read_excel(
    OvRfile,
    sheet = "Sheet1",
  )

ItemOverrides <- ItemOverrides %>% mutate_all(as.character)

Message2Log(paste0("Post to BOA: T_Overrides_and_Plugs"))
postDataToBOA2(OverridesAndPlugs, "T_Overrides_and_Plugs", "mitest")

Message2Log(paste0("Post to BOA: T_ItemOverrides"))
postDataToBOA2(ItemOverrides, "T_ItemOverrides", "mitest")


#### ----
#### ----
#### Validation: Extracts and ETL ------------------------------------------------------
#### ----

Message2Log(paste0("Run some basic validations on what we just loaded/staged"))
StageDataValidateSQL <- readtext(paste0(SQLdirectoryMainLoc, "TMC_Stage_Data_ValidationSQL.txt"))
qryBOAExecute(StageDataValidateSQL)

Valqry1 <- "
select a.Table_Loaded, sum(record_count) as rowsLoaded
from mitest.TMC_Stage_Data_Validation a 
group by a.Table_Loaded
order by a.Table_Loaded;"

Valqry2 <- "
select a.Nielsen_week_end_date, sum(record_count) as rowsLoaded
from mitest.TMC_Stage_Data_Validation a 
group by a.Nielsen_week_end_date
order by a.Nielsen_week_end_date;"

Valqry3 <- "
select a.ABT_Segment, sum(record_count) as rowsLoaded
from mitest.TMC_Stage_Data_Validation a 
group by a.ABT_Segment
order by a.ABT_Segment;"

Message2Log(paste0("Run TablesLoaded Validation Query"))
TablesLoaded <- qryBOAReturn(Valqry1)

Message2Log(paste0("Run WeeksLoaded Validation Query"))
WeeksLoaded <- qryBOAReturn(Valqry2)

Message2Log(paste0("Run SegmentsLoaded Validation Query"))
SegmentsLoaded <- qryBOAReturn(Valqry3)

CommonFunctions::Message2Log("Sending Complete email (with Validation Data) to ANPD-MI team")

to <- paste0(sysUser511, "@abbott.com")
#to <- DataTeam
#to <- "daniel.shields@abbott.com"
from <- "data.analytics@abbott.com"
subject <- "TMC 2.0 Data Loads Complete: Tables Loaded Summary"
body <- paste("\nFor more detail:  select * from mitest.TMC_Stage_Data_Validation;", 
              "\nSummary of tables and data staged.", "\n\n\n")
htmlbody = paste(body, htmlTable(TablesLoaded))

sendEmailNoAtt(to, from, subject, htmlbody)


subject <- "TMC 2.0 Data Loads Complete: Weeks Loaded Summary"
body <- paste("\nFor more detail:  select * from mitest.TMC_Stage_Data_Validation;", 
              "\nSummary of Weeks and data staged.", "\n\n\n")
htmlbody = paste(body, htmlTable(WeeksLoaded))

sendEmailNoAtt(to, from, subject, htmlbody)


subject <- "TMC 2.0 Data Loads Complete: Segments Loaded Summary"
body <- paste("\nFor more detail:  select * from mitest.TMC_Stage_Data_Validation;", 
                  "\nSummary of Segments and data staged.", "\n\n\n")
htmlbody = paste(body, htmlTable(SegmentsLoaded))

sendEmailNoAtt(to, from, subject, htmlbody)


}
, error = function(e) {
  to <- DataTeam
  to <- paste0(sysUser511, "@abbott.com")
  from <- "data.analytics@abbott.com"
  subject <- "Error caught in TMCRefreshETL.R script"
  body = e$message
  sendEmailNoAtt(to, from, subject, body)
}
)

#### Clear Environment ####
#rm(list = ls())
