## Install libraries ####
library(devtools)
library(dplyr)
library(odbc)
library(lubridate)
library(writexl)
install_github("ANPD-Data-Analytics/Connections")
install_github("ANPD-Data-Analytics/CommonFunctions")
library(Connections)
library(CommonFunctions)
qrySandboxReturn <- CommonFunctions::qrySandboxReturn
prevweekday <- CommonFunctions::prevweekday

# Change Flag if L26 Week File is from the Plug Template ####
FlgPlugOutput <- 1

AMZ_LatestWeek <- "\\\\FCRPDFile02\\WRKStApps1$\\Retail Sales\\Power BI\\OCR Sandbox\\OCR Data\\"
files <- list.files(path= AMZ_LatestWeek ,pattern="velocity_OCR Latest *",full.names = TRUE,recursive = TRUE)
dirs <- dirname(files)
lastfile <- tapply(files,dirs,function(v) v[which.max(file.mtime(v))])
AMZLatest <- read.csv(lastfile)
AMZTMCPlug <- AMZLatest %>% select(., `Period Description` = week_beginning ,  `UPC` = asin ,  `$` = sales , Units = units)
  #AMZTMCPlug <- AMZLatest %>% select(., week_beginning = `Period Description`, asin = `UPC`, sales = `$`, Units = Units)
suppressWarnings(AMZTMCPlug$Units <- as.numeric(AMZTMCPlug$Units))
suppressWarnings(AMZTMCPlug$`$` <- as.numeric(AMZTMCPlug$`$`))

EQChar <- "SELECT   RT.RETAILER_NAME AS RETAILER
                          ,IT.ABBOTT_PRODUCT
                          ,IT.NIELSEN_UPC
                          ,CI.IDX_CUSTOMER_ITEM_CODE
                          ,CI.CUST_ITEM_NBR
                          ,IT.EQ_FACTOR
                          ,CI.U_SKU_UPC_RATIO
                  
                  FROM   AbbottSandbox.dbo.vvHistoryWeek HW
                        ,AbbottSandbox.dbo.vvCustomerItem CI
                        ,AbbottSandbox.dbo.vvItem IT
                        ,AbbottSandbox.dbo.vvRetailer RT
                  
                  WHERE HW.IDX_CUSTOMER_ITEM_CODE = CI.IDX_CUSTOMER_ITEM_CODE
                  AND   IT.IDX_ITEM = CI.IDX_ITEM
                  AND   HW.IDX_RETAILER = RT.IDX_RETAILER
                  AND   HW.IDX_RETAILER IN (71)
                  AND   EQ_FACTOR is not NULL
                  "

CurrentOCRAll <- qrySandboxReturn(EQChar)
CurrentOCRAlldf <- CurrentOCRAll

CurrentOCRAlldf$newU_SKU_UPC_RATIO <- ifelse(is.na(CurrentOCRAlldf$U_SKU_UPC_RATIO)==TRUE, "1",CurrentOCRAlldf$U_SKU_UPC_RATIO)
CurrentOCRAlldf$newU_SKU_UPC_RATIO <- as.numeric(CurrentOCRAlldf$newU_SKU_UPC_RATIO)
CurrentOCRAlldf$EQ_FACTOR <- as.numeric(CurrentOCRAlldf$EQ_FACTOR)
CurrentOCRAlldf <- CurrentOCRAlldf %>% mutate(newEQFactor_RATIO = newU_SKU_UPC_RATIO * EQ_FACTOR)

TMC_EQJoin <- CurrentOCRAlldf %>% select(., `UPC` = CUST_ITEM_NBR ,`UPC2` = NIELSEN_UPC, `EQ_Factor` = newEQFactor_RATIO, `Orig.EQ` = EQ_FACTOR)
TMC_EQJoin <- distinct(TMC_EQJoin)

# Join ####
AMZTMCPlug_wEQ <- left_join(AMZTMCPlug, TMC_EQJoin)
AMZTMCPlug_wEQ <- AMZTMCPlug_wEQ %>% mutate(EQ = Units * EQ_Factor)
Out_AMZTMCPlug_wEQ <- AMZTMCPlug_wEQ %>% select(., `Period Description`, `UPC`, `UPC2`, `$` , Units, `EQ`)
Out_AMZTMCPlug_wEQ$EQ <- ifelse(Out_AMZTMCPlug_wEQ$Units==0, 0, Out_AMZTMCPlug_wEQ$EQ)
Out_AMZTMCPlug_wEQ$`Period Description` <- parse_date_time(Out_AMZTMCPlug_wEQ$`Period Description`, c("ymd", "mdY")) 
suppressWarnings(Out_AMZTMCPlug_wEQ$`Period Description` <-   format(prevweekday(prevweekday(Out_AMZTMCPlug_wEQ$`Period Description`, 14), 7), "%m-%d-%Y"))

Out_AMZTMCPlug_wEQ <- filter(Out_AMZTMCPlug_wEQ, !is.na(EQ) | EQ != "")
Out_AMZTMCPlug_wEQ$UPC <-  ifelse((Out_AMZTMCPlug_wEQ$UPC2=='000000000000' | is.na(Out_AMZTMCPlug_wEQ$UPC2)), Out_AMZTMCPlug_wEQ$UPC, Out_AMZTMCPlug_wEQ$UPC2)
Out_AMZTMCPlug_wEQ <- Out_AMZTMCPlug_wEQ %>% select(., `Period Description`, `UPC`, `$` , Units, `EQ`)
Out_AMZTMCPlug_wEQ <- unique(Out_AMZTMCPlug_wEQ)
Out_AMZTMCPlug_wEQ <- filter(Out_AMZTMCPlug_wEQ, `$` != 0 & `EQ` != 0 & `Units` != 0)

Out_AMZTMCPlug_wEQ1 <- Out_AMZTMCPlug_wEQ %>% group_by(`Period Description`,UPC) %>% summarise(`$` = sum(`$`), Units = sum(`Units`), `EQ` = sum(`EQ`)) 
writexl::write_xlsx(Out_AMZTMCPlug_wEQ1,"\\\\FCRPDFile02\\WRKStApps1$\\Retail Sales\\Power BI\\OCR Sandbox\\OCR Data\\TMC.Plug.Override.xlsx")
