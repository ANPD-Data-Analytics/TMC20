require(renv)
renv::restore(confirm = FALSE)
#renv::snapshot()

ValidationOnly <- 1

# Start tryCatch ####  
tryCatch( {
  
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Load Libraries ####
  require(odbc)
  require(readxl)
  require(tidyverse)
  require(stringr)
  require(lubridate)
  require(readtext)
  require(data.table)
  require(tictoc)
  require(openxlsx)
  require(rmarkdown)
  require(htmltools)
  require(flexdashboard)
  require(DT)
  require(plotly)
  require(rlist)
  require(htmlwidgets)
  require(rpivotTable)
  require(kableExtra)
  require(scales)
  require(dplyr)
  require(readtext)
  require(mailR)
  require(htmlTable)
  require(AzureRMR)
  require(AzureStor)
  require(lubridate)
  require(dplyr)
  
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Source GIT connections & functions ####
require(devtools)
install_github("ANPD-Data-Analytics/Connections",force = TRUE)
install_github("ANPD-Data-Analytics/CommonFunctions")
library(Connections)
library(CommonFunctions)
sysUser511 <- Connections::sysUser511
odbcConnStr <- Connections::odbcConnStr
odbcConnStrBOA <- Connections::odbcConnStrBOA
qryBOAReturn <- CommonFunctions::qryBOAReturn
qryBOAExecute <- CommonFunctions::qryBOAExecute
qrySandboxReturn <- CommonFunctions::qrySandboxReturn
postDataToBOA2 <- CommonFunctions::postDataToBOA2
sendEmailWAtt <- CommonFunctions::sendEmailWAtt
sendEmailNoAtt <- CommonFunctions::sendEmailNoAtt
DataTeam <- Connections::DataTeam
LogEvent <- CommonFunctions::LogEvent
Message2Log <- CommonFunctions::Message2Log
SumAll <- CommonFunctions::SumAll
cleanNAs <- CommonFunctions::cleanNAs
impactsummaryfun <- CommonFunctions::impactsummaryfun
get.objects <- CommonFunctions::get.objects

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Define Directories ####
directoryLoc <- "//fcrpdfile02/WRKStApps1$/Retail Sales/Retail Reports/TMC20/"
ValidationdirectoryLoc <- paste0(directoryLoc,"Validation_Reports/")
CSV_XLdirectoryLoc <- paste0(directoryLoc,"CSV_XL_Files/")

# Define SQL Query Directory
SQLdirectoryLoc <- paste0(directoryLoc,"SQL_Queries/SQL_Queries.xlsx")
SQLdirectoryMainLoc <- paste0(directoryLoc,"SQL_Queries/")

# Define RMD Directory
RScriptdirectoryLoc <- paste0(directoryLoc,"R_Scripts/")

# NielsenExtracts Folder 
NielsenExtractsLoc <- paste0(directoryLoc,"NielsenExtracts/")

# Plug_XL Folder 
Plug_XL_FilesLoc <- paste0(directoryLoc,"Plug_XL_Files/")

# WeeklyRefresh Folder 
#WeeklyRefresh <- paste0("C:/Users/",sysUser511,"/OneDrive - Abbott/WeeklyRefresh/")

# Define Log File ####
log <- paste0(directoryLoc,"/Log_Files/",Sys.Date(),".txt")
log_file <- file(log, open="a")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Record Start Time ####
Starttime <- Sys.time()

if (ValidationOnly == 1){
  Message2Log("Running Validation Only.")
}
  

 if (ValidationOnly == 0){
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Pull NielsenFiles - BOAFileDownload.R ####
Message2Log("Starting TMC, Pulling Nielsen Extract Files")
source(paste0(RScriptdirectoryLoc,"BOAFileDownload.R"))

KeepObjects <- get.objects()
Cleanup <- data.frame(get.objects(paste0(RScriptdirectoryLoc,"BOAFileDownload.R"),exception = KeepObjects))
rm(list = Cleanup[1:nrow(Cleanup),1])

# ! Remember to move to WeeklyRefresh folder, if necessary still ! #

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Run Refresh ETL TMCRefreshETL.R ####
Message2Log("Running TMCRefreshETL.R")
#source(paste0(WeeklyRefresh,"TMCRefreshETL.R")) # from WeeklyRefresh folder
source(paste0(RScriptdirectoryLoc,"TMCRefreshETL.R")) # from TMC20 folder

KeepObjects <- get.objects()
Cleanup <- data.frame(get.objects(paste0(RScriptdirectoryLoc,"TMCRefreshETL.R"),exception = KeepObjects))
rm(list = Cleanup[1:nrow(Cleanup),1])

 }

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Run Step #1 & #2 SQL Table Updates ####
Message2Log("Running Step 1 & 2 SQL Table Updates")
#Step1_TMC_PROD_Consumption <- readtext(paste0(WeeklyRefresh, "Step#1_TMC_PROD_Consumption.txt"))$text
Step1_TMC_PROD_Consumption <- readtext(paste0(SQLdirectoryMainLoc, "Step#1_TMC_PROD_Consumption.txt"))$text
qryBOAExecute(Step1_TMC_PROD_Consumption)

#Step2_TMC_ITEM_MASTER <-readtext(paste0(WeeklyRefresh, "Step#2_TMC_ITEM_MASTER.txt"))$text
Step2_TMC_ITEM_MASTER <-readtext(paste0(SQLdirectoryMainLoc, "Step#2_TMC_ITEM_MASTER.txt"))$text
qryBOAExecute(Step2_TMC_ITEM_MASTER)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Pull SQL Queries ####
Message2Log("Pulling SQL Queries from XL sheet")
  SQL_Queries <- read_excel(SQLdirectoryLoc, sheet = "TMC20_test", col_names = TRUE, col_types = NULL, na = "", skip = 0)
  
  # To use when updating from GIT 
  #SQLPath <- "C:/Users/Public/Documents/CentralRepo/AutomationScripts/SQL_Queries.xlsx"
  #SQL_Queries <- read_excel(SQLPath, sheet = "TMC20_test", col_names = TRUE, col_types = NULL, na = "", skip = 0)
  
  
  # QRY Calendar 
  Calendar <- qryBOAReturn(SQL_Queries$Calendar[1])%>% select("Period Description" = Date, Year)
  CalendarMonth <- qryBOAReturn(SQL_Queries$Calendar[1])%>% select("Period Description" = Date, Year, Month, MonthText)
  
  # QRY BOA Prod Files
  Consumption_Char <- qryBOAReturn(SQL_Queries$Consumption_Char[1]) 
  Consumption_UPC_ASIN <- qryBOAReturn(SQL_Queries$Consumption_UPC_ASIN[1]) 
  Consumption <- qryBOAReturn(SQL_Queries$Consumption[1]) 
  
  # QRY BOA Test Files
  Test_Consumption_Char <- qryBOAReturn(SQL_Queries$Test_Consumption_Char[1]) 
  Test_Consumption_UPC_ASIN <- qryBOAReturn(SQL_Queries$Test_Consumption_UPC_ASIN[1]) 
  Test_Consumption <- qryBOAReturn(SQL_Queries$Test_Consumption[1]) 
  Test_Consumption_LW <- qryBOAReturn(SQL_Queries$Test_Consumption[1])
  Test_Nielsen_Char <- qryBOAReturn(SQL_Queries$Test_Nielsen_Char[1])
  
  # UPC_combined table - using Test Consumption table - used throughout all
  UPC_combined <- Test_Consumption
  
  # Validation Queries  
  Unmapped_Items <- qryBOAReturn(SQL_Queries$Unmapped_Items[1])
  Retailer_Weeks <- qryBOAReturn(SQL_Queries$Retailer_Weeks[1])
  Test_LW <- qryBOAReturn(SQL_Queries$Test_LW[1])
  Prod_LW <- qryBOAReturn(SQL_Queries$Prod_LW[1])
  L9_Sales_0_Units <- qryBOAReturn(SQL_Queries$L9_Sales_0_Units[1])
  L9_Sales_Units_0_EQ <- qryBOAReturn(SQL_Queries$L9_Sales_Units_0_EQ[1])
  Test_Dupe_UPC <- qryBOAReturn(SQL_Queries$Test_Dupe_UPC[1])
  N_Weeks_Expected <- qryBOAReturn(SQL_Queries$N_Weeks_Expected[1])
  
  # Retail Consumption Extract Queries
  SandboxMfrSegQuery <- qryBOAReturn(SQL_Queries$SandboxMfrSegQuery[1])
  SandboxMfrSegQuery2 <- qryBOAReturn(SQL_Queries$SandboxMfrSegQuery2[1])

# Define Dates ####
newDates <- Test_Consumption %>% distinct(`Period Description`)
oldDates <- Consumption %>% distinct(`Period Description`)
UPCDates <- inner_join(newDates, oldDates)
Test_Consumption <- Test_Consumption %>% inner_join(.,UPCDates)
maxWE <- max(Test_Consumption$`Period Description`)
curdate <- Sys.Date()
curtime <- format(Sys.time(), "%H%M%OS")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Prep Validation Files ####
Message2Log("Start of Validation File Prep")

## Create naFormSegment.csv ####
# Removed OES TN BF KT override  -- 11/2
# Replace "N/A" string with NA
Characteristics_naFormSegment <- Test_Consumption_Char %>% mutate_if(is.character, list( ~ na_if(., "N/A")))
naFormSegment <- Characteristics_naFormSegment %>% filter(is.na(`Formula Segment`) & NIELSEN_SEGMENT == 'INFANT FORMULA UA')
write_csv(naFormSegment, paste0(CSV_XLdirectoryLoc,"naFormSegment.csv"))

## Create naChar.csv ####
naChar <-
  Test_Consumption_Char %>% filter(
    is.na(UPC) | is.na(NIELSEN_SEGMENT) |
      is.na(Manufacturer) |
      ((`Formula Segment` == "N/A" | is.na(`Formula Segment`)) &
         NIELSEN_SEGMENT == 'INFANT FORMULA UA') |
      (
        (Subbrand == "N/A" | is.na(Subbrand)) & (
          NIELSEN_SEGMENT == 'ORAL ELECTROLYTES' |
            NIELSEN_SEGMENT == 'TODDLER NUTRITION'
        ) &
          NIELSEN_MANUFACTURER == 'Abbott Nutrition'
      ) 
  ) %>%
  mutate(
    fieldMissing = case_when(
      is.na(UPC) ~ "UPC",
      is.na(Manufacturer) ~ "Manufacturer",
      ((`Formula Segment` == "N/A" | is.na(`Formula Segment`)) &
         NIELSEN_SEGMENT == 'INFANT FORMULA UA') ~ "Formula Segment",
      (
        (Subbrand == "N/A" | is.na(Subbrand)) & (
          NIELSEN_SEGMENT == 'ORAL ELECTROLYTES' |
            NIELSEN_SEGMENT == 'TODDLER NUTRITION'
        ) &
          NIELSEN_MANUFACTURER == 'Abbott Nutrition'
      ) ~ "Subbrand"
    )) %>% select(fieldMissing, everything())

write_csv(naChar, paste0(CSV_XLdirectoryLoc,"naChar.csv"))
Message2Log("Saved naChar.csv")

## Create VelocityCompSummary.csv ####
# Removed Market Description case_when  for ' 11/2
Velocity <- read_csv(paste0("\\\\fcrpdfile02\\WRKStApps1$\\Retail Sales\\Retail Reports\\TMC\\","Velocity Extracts/VelocityValidate.csv"))
names(Velocity) <- c("Market Description", "Period Description", "UPC", "AN_BRAND_FAMILY", "$", "Units", "EQ")
Velocity <- Velocity %>% mutate(`Period Description` = as.Date(`Period Description`)) 

#Align data
#velocity is Walgreens + some rite aid this is to get closer to the value in Velocity
UPC_combined_Vel1 <- UPC_combined 
markets <- intersect(Velocity$`Market Description`, UPC_combined_Vel1$`Market Description`)
VelDates <- Velocity %>% distinct(`Period Description`)
VelDates2 <- UPC_combined_Vel1 %>% distinct(`Period Description`)
VelDates2$`Period Description` <- as.Date(VelDates2$`Period Description`)
veldates <- intersect(VelDates, VelDates2)
UPC_combined_VelTest <- UPC_combined_Vel1 %>% filter(`Market Description` %in% markets & `Period Description` %in% veldates$`Period Description`)
Velocity <- Velocity %>% filter(ymd(`Period Description`) %in% ymd(veldates$`Period Description`))

Brands1 <- Velocity %>% select(UPC, AN_BRAND_FAMILY) %>% distinct(.)
Velocity <- Velocity %>% select(-AN_BRAND_FAMILY)

#in this is it only what existed in TMC, then use other brands link
#in velocity summary use the vel one
drop <- c("$", "EQ", "Units")
fieldnamesin <- UPC_combined_VelTest %>% select(-one_of(drop))
fieldnamesin <- names(fieldnamesin)
fieldnamesout <- c("Period Description", "Market Description", "UPC", "$ Diff", "Units Diff", "EQ Diff")

Velocity_Summary <- Velocity %>% left_join(.,Brands1) %>% select(-UPC) %>% SumAll(.)
VelBrands <- Velocity_Summary %>% distinct(AN_BRAND_FAMILY)
Brands <- Test_Consumption_Char %>% distinct(UPC, AN_BRAND_FAMILY)
UPC_combined_Summary <- UPC_combined_VelTest %>% left_join(.,Brands) %>% select(-UPC) %>% 
  filter(AN_BRAND_FAMILY %in% VelBrands$AN_BRAND_FAMILY) %>% SumAll(.)

#LW
LWDate <- max(Velocity_Summary$`Period Description`)
LWDateLY <- LWDate - weeks(52)

VelBrandsOut <-
  Velocity_Summary %>% mutate(
    LW = case_when(`Period Description` == LWDate ~ `$`),
    LWLY = case_when(`Period Description` == LWDateLY ~
                       `$`)
  ) %>%
  select(-c("$", "Units", "EQ", "Period Description")) %>% SumAll(.) %>% filter(!(is.na(AN_BRAND_FAMILY))) %>%
  mutate(VelLWLYPercent = round(((LW - LWLY) / LWLY) * 100, digits = 2))

UPCBrandsOut <-
  UPC_combined_Summary %>% mutate(
    LW = case_when(`Period Description` == LWDate ~ `$`),
    LWLY = case_when(`Period Description` == LWDateLY ~
                       `$`)
  ) %>%
  select(-c("$", "Units", "EQ", "Period Description")) %>% SumAll(.) %>% filter(!(is.na(AN_BRAND_FAMILY))) %>%
  mutate(TMCLWLYPercent = round(((LW - LWLY) / LWLY) * 100, digits = 2))

UPCBrandsOut2 <- UPCBrandsOut %>% select(-c(LW, LWLY))
VelBrandsOut2 <- VelBrandsOut %>% select(-c(LW, LWLY))
LWLYOut <-
  UPCBrandsOut2 %>% left_join(., VelBrandsOut2) %>% 
  mutate(VarLW = TMCLWLYPercent -
           VelLWLYPercent) 

#L13
L13Date <-
  Velocity %>% distinct(`Period Description`) %>% filter(
    `Period Description` <= max(`Period Description`) &
      `Period Description` >= (max(`Period Description`) - weeks(12))
  )
L13DateLY <- L13Date$`Period Description` - weeks(52)
L13Date <- unique(L13Date$`Period Description`)
VelBrandsOut <-
  Velocity_Summary %>% mutate(
    L13 = case_when(`Period Description` %in% L13Date ~ `$`),
    L13LY = case_when(`Period Description` %in% L13DateLY ~
                        `$`)
  ) %>%
  select(-c("$", "Units", "EQ", "Period Description")) %>% SumAll(.) %>% filter(!(is.na(AN_BRAND_FAMILY))) %>%
  mutate(VelL13LYPercent = round(((L13 - L13LY) / L13LY) * 100, digits = 2))
UPCBrandsOut <-
  UPC_combined_Summary %>% mutate(
    L13 = case_when(as.Date(`Period Description`) %in% L13Date ~ `$`),
    L13LY = case_when(as.Date(`Period Description`) %in% L13DateLY ~
                        `$`)
  ) %>%
  select(-c("$", "Units", "EQ", "Period Description")) %>% SumAll(.) %>% filter(!(is.na(AN_BRAND_FAMILY))) %>%
  mutate(TMCL13LYPercent = round(((L13 - L13LY) / L13LY) * 100, digits = 2))
UPCBrandsOut2 <- UPCBrandsOut %>% select(-c(L13, L13LY))
VelBrandsOut2 <- VelBrandsOut %>% select(-c(L13, L13LY))
L13LYOut <-
  UPCBrandsOut2 %>% left_join(., VelBrandsOut2) %>%
  mutate(Var13W = TMCL13LYPercent - VelL13LYPercent)

VelCompOut <- left_join(LWLYOut, L13LYOut) %>%
  mutate(HighVar = case_when(abs(Var13W) > 30 | abs(VarLW) > 30 ~ 1,
                             TRUE ~ 0))
write_csv(VelCompOut, paste0(CSV_XLdirectoryLoc,"VelocityCompSummary.csv"))
Message2Log("Saved VelocityCompSummary.csv")

testVelOut <- if_else(max(VelCompOut$HighVar) < 1,
                      0,
                      1)

## Create VelocityCompSummary2.csv ####
#create a diff per L26 of velocity and UPC
L26Date <-
  Velocity %>% distinct(`Period Description`) %>% filter(
    `Period Description` <= max(`Period Description`) &
      `Period Description` >= (max(`Period Description`) - weeks(25))
  )
L26Date <- unique(L26Date$`Period Description`)
VelBrandsOut2 <-
  Velocity_Summary %>% filter(`Period Description` %in% L26Date) %>%
  select(-c("Units", "EQ")) %>% SumAll(.) %>% filter(!(is.na(AN_BRAND_FAMILY))) %>% 
  mutate(`$ Vel` = round(`$`)) %>% select(-`$`)
UPCBrandsOut2 <-
  UPC_combined_Summary %>% filter(as.Date(`Period Description`) %in% L26Date) %>%
  select(-c("Units", "EQ")) %>% SumAll(.) %>% filter(!(is.na(AN_BRAND_FAMILY))) %>% 
  mutate(`$ TMC` = `$`) %>% select(-`$`)
Compout <- inner_join(VelBrandsOut2, UPCBrandsOut2) %>% mutate(Diff = round(((`$ Vel`/`$ TMC`)-1)*100, digits = 2)) %>%
  group_by(`Market Description`, `AN_BRAND_FAMILY`) %>% mutate(flg_lw = case_when(`Period Description` == LWDate~1,
                                                                                  TRUE~0),
                                                               mediandiff = round(median(Diff), digits = 2),
                                                               stdevdiff5 = round(sd(Diff)*5, digits = 2),
                                                               minexp = round(mediandiff-stdevdiff5, digits = 2),
                                                               maxexp = round(mediandiff+stdevdiff5, digits = 2),
                                                               HighVar2 = case_when(Diff > maxexp | Diff < minexp ~ 1,
                                                                                    TRUE ~ 0))
write_csv(Compout, paste0(CSV_XLdirectoryLoc,"VelocityCompSummary2.csv"))
Message2Log("Saved VelocityCompSummary2.csv")

## Create NewSubbrands.csv *Prev Incorrect* ####
# Check for new subbrands
old_Subbrands <- Consumption_Char %>% distinct(Subbrand)
new_Subbrands <- Test_Consumption_Char %>% distinct(Subbrand)
newSubbrands1 <- anti_join(new_Subbrands, old_Subbrands) %>% left_join(., Test_Consumption_Char)
write_csv(newSubbrands1, paste0(CSV_XLdirectoryLoc,"NewSubbrands.csv"))
Message2Log("Saved NewSubbrands.csv")

## Create itemsMissingEQ.csv ####
#are there entries with units but no EQ?
#if so, fail, but these can be added to the item overrides
#looking only where $ > 5000
testEQ <-
  Test_Consumption %>% select(-`Period Description`) %>% SumAll(.) %>% filter((!is.na(Units) &
                                                                             round(Units, digits = 0) != 0) &
                                                                            (EQ == 0 |
                                                                               (is.na(EQ)))) %>% filter(abs(Units) >= 5) %>% filter(abs(`$`) >= 5000)

testEQ <- testEQ %>% distinct(`Market Description`, UPC) %>%
  left_join(., Test_Consumption_Char) %>%
  filter(is.na(EQ_FACTOR) | EQ_FACTOR == 0 | EQ_FACTOR == "N/A")
testEQOut <- if_else(nrow(testEQ)>0,1,0)
write_csv(testEQ,
          paste0(CSV_XLdirectoryLoc, "itemsMissingEQ.csv"))

## Create LWDolOutlierwide.csv ####
maxWE <- max(UPC_combined$`Period Description`)

#Latest week
scoreDol <- UPC_combined %>%
  filter(`Period Description` <= maxWE &
           `Period Description` >= (maxWE - weeks(13))) %>%
  select(`Period Description`, `Market Description`, UPC, `$`) %>%
  SumAll(.) %>%
  group_by(`Market Description`, UPC) %>%
  mutate(
    meanDol = round(mean(`$`)),
    meanDolDif = `$` - mean(`$`),
    sdDol = sd(`$`),
    z_score = meanDolDif / sdDol
  ) %>% 
  ungroup(.)

lwDol <- scoreDol %>%
  filter(`Period Description` == maxWE &
           abs(z_score) >= 3 &
           meanDol > 10000) %>%
  distinct(UPC, `Market Description`) %>%
  left_join(., scoreDol)

write_csv(lwDol, paste0(CSV_XLdirectoryLoc,"LWDolOutlier.csv"))

lwDolOut <- lwDol %>% select(`Period Description`, `Market Description`, UPC, `$`) %>% 
  tidyr::pivot_wider(names_from = `Period Description`, 
                     values_from = `$`)

write_csv(lwDolOut, paste0(CSV_XLdirectoryLoc,"LWDolOutlierwide.csv"))


## Create LWEQOutlierwide.csv ####
scoreEQ <- UPC_combined %>%
  filter(`Period Description` <= maxWE &
           `Period Description` >= (maxWE - weeks(13))) %>%
  select(`Period Description`, `Market Description`, UPC, `EQ`) %>%
  SumAll(.) %>%
  group_by(`Market Description`, UPC) %>%
  mutate(
    meanEQ = round(mean(`EQ`)),
    meanEQDif = `EQ` - mean(`EQ`),
    sdEQ = sd(`EQ`),
    z_score = meanEQDif / sdEQ
  ) %>% 
  ungroup(.)

lwEQ <- scoreEQ %>%
  filter(`Period Description` == maxWE &
           abs(z_score) >= 3 &
           meanEQ > 10000) %>%
  distinct(UPC, `Market Description`) %>%
  left_join(., scoreEQ)

write_csv(lwEQ, paste0(CSV_XLdirectoryLoc,"LWEQOutlier.csv"))

lwEQOut <- lwEQ %>% select(`Period Description`, `Market Description`, UPC, EQ) %>% 
  tidyr::pivot_wider(names_from = `Period Description`, 
                     values_from = `EQ`)
write_csv(lwEQOut, paste0(CSV_XLdirectoryLoc,"LWEQOutlierwide.csv"))
Message2Log("Saved LWEQOutlierwide.csv")

## Create RetailerWeeksOut.csv ####
### Check Retailer Weeks matches total weeks 
  ### Excluding Kmart and AO Market? #
RetailerWeeksOut <- Retailer_Weeks %>% filter(`Market Description` != 'Kmart') %>% filter(`Market Description` != 'AO Market')
N_Weeks_Expected <- as.integer(N_Weeks_Expected[1])
numWeeksTest <- if_else(min(RetailerWeeksOut$N_Weeks) != N_Weeks_Expected | max(RetailerWeeksOut$N_Weeks) != N_Weeks_Expected, 1,0)

### Check Latest Week for Missing Retailer
Markets <- RetailerWeeksOut %>% select(`Market Description`)
numRetailersTest <- if_else(!identical(setdiff(Markets$`Market Description`, unique(RetailerWeeksOut$`Market Description`)), character(0)), 1, 0)
write_csv(RetailerWeeksOut, paste0(CSV_XLdirectoryLoc, "RetailerWeeksOut.csv"))
Message2Log("Saved RetailerWeeksOut.csv")

## Create dupUPCs.csv ####
dupUPCs <- Test_Dupe_UPC
dupUPCsTest <- if_else(sum(dupUPCs$UPC1)!=sum(dupUPCs$UPC2),1,0)
write_csv(dupUPCs, paste0(CSV_XLdirectoryLoc, "dupUPCs.csv"))
Message2Log("Saved dupUPCs.csv")

## Create Unmapped_Items.csv ####
Unmapped_ItemsTest <- if_else((nrow(Unmapped_Items)!= 0),1,0)
write_csv(Unmapped_Items, paste0(CSV_XLdirectoryLoc, "Unmapped_Items.csv"))
Message2Log("Saved Unmapped_Items.csv")

## Create IFUAEQWarn.csv ####
# Warn if above 50K in a week
IFUAEQWarndata <- left_join(Test_Consumption,Test_Consumption_Char, 'UPC')
IFUAEQWarn <-
  IFUAEQWarndata %>% filter(NIELSEN_SEGMENT == "INFANT FORMULA UA"  &
                        '$' > 50000 &
                        (is.na(EQ) | EQ == 0))
IFUAEQWarnlist <- IFUAEQWarn
IFUAEQWarn <- if_else(nrow(IFUAEQWarn) > 0, 1, 0)
write_csv(Unmapped_Items, paste0(CSV_XLdirectoryLoc, "IFUAEQWarn.csv"))
Message2Log("Saved IFUAEQWarn.csv")

## Create itemsMissinginRetailChar.csv ####
# Check for missing items in Cons, not Char
CharItems <- Test_Consumption_Char %>% distinct(UPC)
ConsItems <- Test_Consumption %>% distinct(UPC)
checkNielsenChar <- anti_join(ConsItems,CharItems)
checkNielsenCharOut <- if_else(nrow(checkNielsenChar)>0,1,0)
write_csv(checkNielsenChar, paste0(CSV_XLdirectoryLoc, "itemsMissinginRetailChar.csv"))
Message2Log("Saved itemsMissinginRetailChar.csv")

## Create CostcoTotals.csv ####
# Check Costco Totals by Segment/Date
MaxCostcoData <- filter(Test_Consumption_LW,`Market Description` == 'Costco') 
MaxCostcoData <- left_join(MaxCostcoData, Test_Consumption_Char, "UPC") %>%  group_by(`Period Description`,NIELSEN_SEGMENT) %>% summarise("$" = sum(`$`))
MaxCostcoDates <- max(MaxCostcoData$`Period Description`)
MaxCostcoIFUADates <- filter(MaxCostcoData,NIELSEN_SEGMENT == 'INFANT FORMULA UA') 
MaxCostcoIFUADates <- max(MaxCostcoIFUADates$`Period Description`)
write_csv(MaxCostcoData, paste0(CSV_XLdirectoryLoc, "CostcoTotals.csv"))
Message2Log("Saved CostcoTotals.csv")

## Create Retail Consumption Extracts ####
Characteristics1 <- Test_Consumption_Char
Consumption1 <- Test_Consumption_LW

#limit to 104 weeks to keep file size down
startweek <- max(Consumption1$`Period Description`)-weeks(155)
Consumption1 <- Consumption1 %>% filter(`Period Description` >= startweek)

Consumption_1 <-
  Consumption1 %>% filter(
    `Market Description` != "Amazon" &
      `Market Description` != "Costco" &
      `Market Description` != "WIC Direct"
  ) %>%
  mutate(
    `Market Description` = case_when(
      `Market Description` == "Kroger" ~ "AO Food",
      TRUE ~ `Market Description`
    )
  ) %>%
  SumAll(.)
Characteristics_1 <- Consumption_1 %>% distinct(UPC) %>% inner_join(., Characteristics1)
Characteristics_1 <- Characteristics_1 %>% select ('UPC'
                                                  ,`ITEM` = TMC_ITEM_DESC
                                                  ,`ABT_CATEGORY` = NIELSEN_CATEGORY
                                                  , `ABT_SEGMENT` = ABT_SEGMENT
                                                  ,`ABT_SUBSEGMENT` = NIELSEN_ABT_SUBSEGMENT
                                                  ,`ABT_FORMULA REB/NON REBATED` = ABT_FORMULA_REB_NON_REBATED
                                                  ,`ABT_FORMULA TYPE` = `ABT_FORMULA TYPE`
                                                  ,`ABT_FORMULA SUBTYPE` = `ABT_FORMULA SUBTYPE`
                                                  ,`ABT_MANUFACTURER` = ABT_MANUFACTURER
                                                  ,'ABT_BRAND'
                                                  ,`ABT_BRAND FAMILY` = AN_BRAND_FAMILY
                                                  ,`ABT_SUBBRAND GROUP` = TMC_SUBBRAND_CUSTOM
                                                  ,`ABT_SUBBRAND` = ABT_SUBBRAND
                                                  ,`ABT_FORMULA FORMULATION` = NIELSEN_ABT_FORMULATION
                                                  ,`ABT_NATURAL/ORGANIC` = NIELSEN_ABT_NAT_ORG
                                                  ,`ABT_VARIANT` = NIELSEN_VARIANT
                                                  #,'ABT_FIBER TN' = NA_character_
                                                  ,`ABT_FORM TYPE` = NIELSEN_ABT_FORM
                                                  ,`ABT_FORM` = NIELSEN_ABT_FORM
                                                  ,`ABT_PACK` = NIELSEN_ABT_PACK
                                                  ,`ABT_PACK SIZE` = NIELSEN_PACK_SIZE
                                                  ,`ABT_SIZE RANGE` = `ABT_SIZE RANGE`
                                                  #,'ABT_CONTAINER (LIQUID)' = NA_character_
                                                  #,'ABT_SIZE (LIQUID)'  = NA_character_
                                                  #,'ABT_COUNT GROUP (FORM)'  = NA_character_
                                                  ,`ABT_COUNT SIZE` = `ABT_COUNT SIZE`
                                                  ,`ABT_FLAVOR GROUP` = NIELSEN_ABT_FLAVOR_GROUP
                                                  ,`ABT_FLAVOR` = ABT_FLAVOR
                                                  ,`ABT_MULTI` = ABT_FLAVOR
                                                  ,`inc_share` = inc_share)

Characteristics_1 <-
  Characteristics_1 %>% mutate(
    'ABT_FIBER TN' = NA_character_
    ,'ABT_CONTAINER (LIQUID)' = NA_character_
    ,'ABT_SIZE (LIQUID)'  = NA_character_
    ,'ABT_COUNT GROUP (FORM)'  = NA_character_)

fieldnames <- c(
  'UPC'
  ,'ITEM'
  ,'ABT_CATEGORY'
  ,'ABT_SEGMENT'
  ,'ABT_SUBSEGMENT'
  ,'ABT_FORMULA REB/NON REBATED'
  ,'ABT_FORMULA TYPE'
  ,'ABT_FORMULA SUBTYPE'
  ,'ABT_MANUFACTURER'
  ,'ABT_BRAND FAMILY'
  ,'ABT_BRAND'
  ,'ABT_SUBBRAND GROUP'
  ,'ABT_SUBBRAND'
  ,'ABT_FORMULA FORMULATION'
  ,'ABT_NATURAL/ORGANIC'
  ,'ABT_VARIANT'
  ,'ABT_FIBER TN'
  ,'ABT_FORM TYPE'
  ,'ABT_FORM'
  ,'ABT_PACK'
  ,'ABT_PACK SIZE'
  ,'ABT_SIZE RANGE'
  ,'ABT_CONTAINER (LIQUID)'
  ,'ABT_SIZE (LIQUID)'
  ,'ABT_COUNT GROUP (FORM)'
  ,'ABT_COUNT SIZE'
  ,'ABT_FLAVOR GROUP'
  ,'ABT_FLAVOR'
  ,'ABT_MULTI'
  ,'inc_share')

Characteristics_1 <- Characteristics_1 %>% select(all_of(fieldnames))

wb <- openxlsx::createWorkbook()
openxlsx::addWorksheet(wb, "Consumption")
openxlsx::writeData(wb,
                    sheet = "Consumption",
                    as.data.frame(Consumption_1),
                    rowNames = FALSE)
openxlsx::addWorksheet(wb, "Characteristics")
openxlsx::writeData(wb,
                    sheet = "Characteristics",
                    as.data.frame(Characteristics_1),
                    rowNames = FALSE)
openxlsx::saveWorkbook(wb, file = paste0(CSV_XLdirectoryLoc,"RetailExtract.xlsx"), overwrite = TRUE)
Message2Log("Saved RetailExtract.xlsx")

Consumption_2 <-
  Consumption1 %>% filter(
    `Market Description` != "Amazon" &
      `Market Description` != "Costco" &
      `Market Description` != "WIC Direct"
  ) %>%
  SumAll(.)
Characteristics_2 <- Consumption_2 %>% distinct(UPC) %>% inner_join(., Characteristics_1) # made change from Characteristics1 to Characteristics_1

wb <- openxlsx::createWorkbook()
openxlsx::addWorksheet(wb, "Consumption")
openxlsx::writeData(wb,
                    sheet = "Consumption",
                    as.data.frame(Consumption_2),
                    rowNames = FALSE)
openxlsx::addWorksheet(wb, "Characteristics")
openxlsx::writeData(wb,
                    sheet = "Characteristics",
                    as.data.frame(Characteristics_2),
                    rowNames = FALSE)
openxlsx::saveWorkbook(wb, file = paste0(CSV_XLdirectoryLoc,"RetailKrogExtract.xlsx"), overwrite = TRUE)
Message2Log("Saved RetailKrogExtract.xlsx")

## Create MfrSeg* .csv ####
MfrSeg <- SandboxMfrSegQuery

ConsumptionAbbott <- MfrSeg %>%
  filter(MANUFACTURER == "Abbott")

ConsumptionTotal <- MfrSeg %>%
  mutate(MANUFACTURER = "Total") %>%
  SumAll(.)

MfrSeg2 <- bind_rows(ConsumptionAbbott, ConsumptionTotal)

#additional Abbott vs Other manufacturer extracts
write_csv(MfrSeg, paste0(CSV_XLdirectoryLoc,"MfrSegTotal2.csv"))
write_csv(MfrSeg2, paste0(CSV_XLdirectoryLoc,"MfrSegTotalValues.csv"))

MfrSeg <- SandboxMfrSegQuery

#this is used in Power BI for extracts
write_csv(MfrSeg, paste0(CSV_XLdirectoryLoc,"MfrSegTotal.csv"))

#This dataset creates a combined summarized view for Power BI 
Characteristics <- Test_Consumption_Char
# UPC_combined <- UPC_combined

## Create L4Growth csv's ####
#L4 growth by retailer + brand
L4Date <-
  UPC_combined %>% distinct(`Period Description`) %>% filter(
    `Period Description` <= max(`Period Description`) &
      `Period Description` >= (max(`Period Description`) - weeks(3))
  )
L4DateLY <- L4Date$`Period Description` - weeks(52)
L4Date <- unique(L4Date$`Period Description`)
SumOut <-
  UPC_combined %>% mutate(
    L4 = case_when(`Period Description` %in% L4Date ~ `EQ`),
    L4LY = case_when(`Period Description` %in% L4DateLY ~
                       `EQ`)
  ) 
Brands <- Characteristics %>% filter(inc_share != "NO") %>% distinct(UPC, AN_BRAND_FAMILY)
SumOut <- SumOut %>% left_join(., Brands)%>%
  select(-c("$", "Units", "EQ", "Period Description", "UPC")) %>% SumAll(.) %>%
  mutate(L4Growth = L4 - L4LY) %>% select(-c("L4", "L4LY"))
write_csv(SumOut, paste0(CSV_XLdirectoryLoc,"L4GrowthRetailer.csv"), na = "")
Message2Log("Saved L4GrowthRetailer.csv")

SumOut2 <-
  UPC_combined %>% mutate(
    L4 = case_when(`Period Description` %in% L4Date ~ `EQ`),
    L4LY = case_when(`Period Description` %in% L4DateLY ~
                       `EQ`)
  ) 
Subbrands <- Characteristics %>% filter(inc_share != "NO") %>% distinct(UPC, Subbrand)
SumOut2 <- SumOut2 %>% left_join(., Subbrands)%>%
  select(-c("$", "Units", "EQ", "Period Description", "UPC", "Market Description")) %>% SumAll(.) %>%
  mutate(L4Growth =  L4 - L4LY) %>% select(-c("L4", "L4LY"))
write_csv(SumOut2, paste0(CSV_XLdirectoryLoc,"L4GrowthSubbrand.csv"), na = "")
Message2Log("Saved L4GrowthSubbrand.csv")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Process Log ####
Message2Log("Creating Pass/Fail Log File")
logfile <- paste0(RScriptdirectoryLoc,"TMC20log.Rmd")

## Check Latest Week Test & Prod Tables ####
if(exists("Test_LW") & exists("Prod_LW")){
  logout0 <- paste0("Test consumption data LW: ", format(Test_LW[1], "%Y-%m-%d"),". Prod consumption data LW: ", format(Prod_LW[1], "%Y-%m-%d"),".")
  textout <- paste(logout0, sep = "\n")
  print(logout0)
  } else{
  logout0 <- paste0("FAILED: Could not pull date from consumption tables")
  textout <- paste(logout0, sep = "\n")
  #stop(logout0)
  }

## Check Retailer Weeks ####
if (numWeeksTest==0) {
  logout1 <- "PASSED: Retailers are all in output, and the number of weeks per retailer match."
  textout <- paste(textout, logout1, sep = "\n")
  print(logout1)
} else{
  logout1 <- paste0("FAILED: Retailer/weeks included issue. Refer to the dataframe RetailerWeeksOut. [link](file:",CSV_XLdirectoryLoc, "RetailerWeeksOut.csv)")
  textout <- paste(textout, logout1, sep = "\n")
  #stop(logout1)
}

## Check Items Missing EQ ####
if (testEQOut==0) {
  logout2 <- "PASSED: There are no items with sales and 0 EQ."
  textout <- paste(textout, logout2, sep = "\n")
  print(logout2)
} else{
  logout2 <- paste0("FAILED: There are items with sales and 0 EQ. [link](file:",CSV_XLdirectoryLoc, "itemsMissingEQ.csv)")
  textout <- paste(textout, logout2, sep = "\n")
  #stop(logout2)
}

## Check for Dupe UPCs ####
if (dupUPCsTest==0) {
  logout3 <- "PASSED: There are no duplicate UPCs in the Characteristics table."
  textout <- paste(textout, logout3, sep = "\n")
  print(logout3)
} else{
  logout3 <- paste0("FAILED: There are duplicate UPCs in the Characteristics table. [link](file:",CSV_XLdirectoryLoc, "dupUPCs.csv)")
  textout <- paste(textout, logout3, sep = "\n")
  #stop(logout3)
}

## Check for Unmapped Items ####
if (Unmapped_ItemsTest==0) {
  logout4 <- "PASSED: There are no unmapped items in the Characteristics table."
  textout <- paste(textout, logout4, sep = "\n")
  print(logout4)
} else{
  logout4 <- paste0("FAILED: There are unmapped items in the Characteristics table. [link](file:",CSV_XLdirectoryLoc, "Unmapped_Items.csv)")
  textout <- paste(textout, logout4, sep = "\n")
  #stop(logout4)
}

if (IFUAEQWarn==0){
  logout5 <- "PASSED: No unknown EQ's for IFUA above 50K."
  textout <- paste(textout, logout5, sep = "\n")
  print(logout5)
} else{
  logout5 <- paste0("FAILED: No unknown EQ's for IFUA above 50K. Need to validate EQ and add to override. [link](file:",CSV_XLdirectoryLoc, "IFUAEQWarn.csv)")
  textout <- paste(textout, logout5, sep = "\n")
  #stop(logout5)
}

if (testVelOut==0) {
  logout6 <- "PASSED: Variance of Connect data compared with Velocity within expected range."
  textout <- paste(textout, logout6, sep = "\n")
  print(logout6)
} else{
  logout6 <- "FAILED: Variance of Connect data compared with Velocity not within expected range."
  textout <- paste(textout, logout6, sep = "\n")
  #stop(logout6)
}

if (checkNielsenCharOut==0) {
  logout7 <- "PASSED: Retail Extract Characteristics table Consumption_Nielsen_Char has all items."
  textout <- paste(textout, logout7, sep = "\n")
  print(logout7)
} else{
  logout7 <- paste0("FAILED: Retail Extract Characteristics table Consumption_Nielsen_Char does not have all items. [link](file:",CSV_XLdirectoryLoc, "itemsMissinginRetailChar.csv)")
  textout <- paste(textout, logout7, sep = "\n")
  #stop(logout7)
}

if (MaxCostcoDates==maxWE) {
  logout8 <- "PASSED: IFUA Costco Competitive contains latest week."
  textout <- paste(textout, logout8, sep = "\n")
  print(logout8)
} else{
  logout8 <- paste0("FAILED: IFUA Costco Competitive does not contain the latest week. [link](file:",CSV_XLdirectoryLoc, "CostcoTotals.csv)")
  textout <- paste(textout, logout8, sep = "\n")
  #stop(logout8)
}

# Run TMC20log.Rmd ####
mycat <- function(text){
  cat(gsub(pattern = "\n", replacement = '</br>  \n', x = text),file = logfile)
}
textout1 <- gsub(pattern = "PASSED", replacement = '<span style="background-color:green; color:white;">PASSED</span>', x = textout)
textout1 <- gsub(pattern = "FAILED", replacement = '<span style="background-color:red; color:white;">FAILED</span>', x = textout1)
mycat(textout1)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Prep Validation Objects ####
Message2Log("Prep Validation Objects")

## Test Char ####
Char1 <- Test_Consumption_Char %>%
  distinct(UPC, NIELSEN_SEGMENT, `Manufacturer`)

sharetemptot <- Test_Consumption %>%
  left_join(., Char1) %>%
  select(
    `Period Description`,
    `Market Description`,
    NIELSEN_SEGMENT,
    `Manufacturer`,
    `$`,
    `EQ`
  ) %>%
  SumAll(.)

## Current Char ####
Char1_old <- Consumption_Char %>%
  distinct(UPC, NIELSEN_SEGMENT, `Manufacturer`)

sharetemptot_old <- Consumption %>%
  left_join(., Char1_old) %>%
  select(
    `Period Description`,
    `Market Description`,
    NIELSEN_SEGMENT,
    `Manufacturer`,
    `$`,
    `EQ`
  ) %>%
  SumAll(.) %>%
  select("$ Previous" = `$`,
         "EQ Previous" = `EQ`,
         everything()) %>%
  ungroup(.)

sharetemptot_out <- inner_join(sharetemptot, sharetemptot_old) %>%
  mutate(`$ Diff` = `$` - `$ Previous`,
         `EQ Diff` = `EQ` - `EQ Previous`) %>% select(
           `Period Description`,
           `Market Description`,
           NIELSEN_SEGMENT,
           `Manufacturer`,
           `$`,
           `$ Previous`,
           `$ Diff`,
           `EQ`,
           `EQ Previous`,
           `EQ Diff`
         )
compretandmfr <-
  sharetemptot_out %>% filter(`$ Diff` != 0 |
                                `EQ Diff` != 0) %>%
  mutate(flg_2021 =
           case_when(
             `Period Description` >= mdy(
               '01/09/2021') &
               `Period Description` < mdy('01/08/2022') ~ 1,
             TRUE ~ 0
           ),
         flg_2022 =
           case_when(`Period Description` >= mdy('01/08/2022') ~ 1, TRUE ~ 0)
  )

## Validation by Retailer,Manufacturer,Segment,Subbrand ####
Char1 <- Test_Consumption_Char %>%
  distinct(UPC, NIELSEN_SEGMENT, `Manufacturer`, Subbrand)
sharetemptotUPC <- Test_Consumption %>%
  left_join(., Char1) %>%
  select(
    `Period Description`,
    `Market Description`,
    NIELSEN_SEGMENT,
    `Manufacturer`,
    Subbrand,
    UPC,
    `$`,
    `EQ`
  ) %>%
  SumAll(.)

Char1_old <- Consumption_Char %>%
  distinct(UPC, NIELSEN_SEGMENT, `Manufacturer`, Subbrand)
sharetemptotUPC_old <- Consumption %>%
  left_join(., Char1_old) %>%
  select(
    `Period Description`,
    `Market Description`,
    NIELSEN_SEGMENT,
    `Manufacturer`,
    Subbrand,
    UPC,
    `$`,
    `EQ`
  ) %>%
  SumAll(.) %>%
  select("$ Previous" = `$`,
         "EQ Previous" = `EQ`,
         everything()) %>%
  ungroup(.)

sharetemptotUPC_out <- full_join(sharetemptotUPC, sharetemptotUPC_old) %>%
  SumAll(.) %>% 
  mutate(`$ Diff` = `$` - `$ Previous`,
         `EQ Diff` = `EQ` - `EQ Previous`) %>% select(
           `Period Description`,
           `Market Description`,
           NIELSEN_SEGMENT,
           `Manufacturer`,
           Subbrand,
           UPC,
           `$`,
           `$ Previous`,
           `$ Diff`,
           `EQ`,
           `EQ Previous`,
           `EQ Diff`
         ) %>% filter(`$ Diff` != 0 |
                        `EQ Diff` != 0) %>%
  mutate(flg_2021 =
           case_when(
             `Period Description` >= mdy(
               '01/09/2021') &
               `Period Description` < mdy('01/08/2022') ~ 1,
             TRUE ~ 0
           ),
         flg_2022 =
           case_when(`Period Description` >= mdy('01/08/2022') ~ 1, TRUE ~ 0)
  )

## New/Dropped Comps ####
newasinUPC <- Test_Consumption_UPC_ASIN %>% filter(!is.na(asin)) %>% distinct(.)
prevasinUPC <- Consumption_UPC_ASIN %>% filter(!is.na(asin)) %>% distinct(.)
newasinAssign <- anti_join(newasinUPC, prevasinUPC)
newasinAssign1 <- newasinAssign %>% distinct(asin)
droppedasinAssign <- anti_join(prevasinUPC, newasinUPC) %>% anti_join(.,newasinAssign1)
droppedasinAssign1 <- anti_join(prevasinUPC, newasinUPC) %>% distinct(.)

newUPCRetailer <- Test_Consumption %>% distinct(UPC, `Market Description`)
prevUPCRetailer <- Consumption %>% distinct(UPC, `Market Description`)
newUPCs <- Test_Consumption_Char %>% distinct(UPC)
prevUPCs <- Consumption_Char %>% distinct(UPC)
newUPCChars <- Test_Consumption_Char %>% select(UPC, NIELSEN_SEGMENT, TMC_SUBBRAND_CUSTOM)
prevUPCChars <- Consumption_Char %>% select(UPC, NIELSEN_SEGMENT, TMC_SUBBRAND_CUSTOM)
newTestUPCs <- anti_join(newUPCs, prevUPCs) %>% 
  left_join(.,newUPCRetailer) %>% left_join(.,newUPCChars)
droppedTestUPCs <- anti_join(prevUPCs, newUPCs) %>% 
  left_join(.,prevUPCRetailer) %>% left_join(.,prevUPCChars) %>%
  #Excluding items noted in AMZ
  anti_join(.,droppedasinAssign1)
#full list
droppedTestUPCs1 <- anti_join(prevUPCs, newUPCs) 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Latest Week ####
Test_Consumption_LW <- filter(UPC_combined, `Period Description` == max(UPC_combined$`Period Description`))
newTestUPCs_LW <- Test_Consumption_LW %>% filter(UPC %in% newTestUPCs$UPC) %>% left_join(.,newUPCRetailer) %>% left_join(.,newUPCChars) %>% distinct(.)  # added 10/31
restatement_new_UPCs <- UPC_combined %>% filter(UPC %in% newTestUPCs$UPC) %>% arrange(.,desc(`Period Description`))

Consumption_Char_New <- select(Test_Consumption_Char,c(NIELSEN_SEGMENT,Subbrand,UPC))
restatement_new_UPCs <- left_join(restatement_new_UPCs,Consumption_Char_New)

restatement_new_UPCs <- restatement_new_UPCs %>% select(UPC,`Period Description`,`NIELSEN_SEGMENT`,`Market Description`,Subbrand,`$`)  %>%
  pivot_wider(names_from = `Period Description`,
              values_from = `$`, values_fill = 0)

if (nrow(restatement_new_UPCs)>0){
restatement_new_UPCs$`$ Sum Res_Period` <- rowSums(restatement_new_UPCs[ ,c(6:ncol(restatement_new_UPCs))],na.rm=TRUE) 

DateCol <- sapply(colnames(restatement_new_UPCs), function(x) !all(is.na(as.Date(as.character(x),format="%Y-%m-%d")))) %>% as.data.frame(.)
DateColNames <- which(colnames(restatement_new_UPCs) == rownames(DateCol) & DateCol$.==TRUE)

i <- min(DateColNames)
ii <- max(DateColNames)

#restatement_new_UPCs[i:ii] <- restatement_new_UPCs[i:ii]  %>% mutate_if(is.numeric, ~ prettyNum(round(., 0), big.mark = ","))
restatement_new_UPCs <- restatement_new_UPCs %>% mutate_if(is.numeric, ~ prettyNum(round(., 0), big.mark = ",")) %>% arrange(NIELSEN_SEGMENT)

write_csv(restatement_new_UPCs, paste0(CSV_XLdirectoryLoc,"restatement_new_UPCs.csv"))
Message2Log("Saving restatement_new_UPCs.csv")
}

allTestUPCs_LW <- Test_Consumption_LW %>% filter(`Period Description` == max(Test_Consumption_LW$`Period Description`))
write_csv(allTestUPCs_LW, paste0(CSV_XLdirectoryLoc,"allTestUPCs_LW.csv"))
Message2Log("Saving allTestUPCs_LW.csv")

# Monthly Breakdown ####
sharetot_month <- left_join(sharetemptot_out,CalendarMonth)
sharetot_month1 <- sharetot_month %>% select(NIELSEN_SEGMENT,Manufacturer, `MonthText`, `$`, `$ Previous`, `$ Diff`, EQ, `EQ Previous`, `EQ Diff`) %>%
                                  SumAll(.) %>% group_by(NIELSEN_SEGMENT, MonthText) %>%
                                  mutate(`$ Share` = round((`$`/sum(`$`)) * 100, digits = 2),
                                         `EQ Share` = round((EQ/sum(EQ)) * 100, digits = 2),
                                         `$ Share Previous` = round((`$ Previous`/sum(`$ Previous`)) * 100, digits = 2),
                                         `EQ Share Previous` = round((`EQ Previous`/sum(`EQ Previous`)) * 100, digits = 2), `$ Share Diff` = `$ Share` -
                                         `$ Share Previous`, `EQ Share Diff` = `EQ Share` - `EQ Share Previous`,
                                         `$ % Change` = round((`$ Diff`/`$ Previous`) * 100, digits = 2),
                                         `EQ % Change` = round((`EQ Diff`/`EQ Previous`) * 100, digits = 2),
                                         `$ % Tot Change` = round((`$ Diff`/sum(`$ Previous`)) * 100, digits = 2),
                                         `EQ % Tot Change` = round((`EQ Diff`/sum(`EQ Previous`)) * 100, digits = 2)) %>% ungroup(.) %>%
                                  select(NIELSEN_SEGMENT, MonthText,
                                         Manufacturer, `$`, `$ Previous`, `$ Diff`, `$ % Change`,
                                         `$ % Tot Change`, `$ Share`, `$ Share Previous`, `$ Share Diff`,
                                         EQ, `EQ Previous`, `EQ Diff`, `EQ % Change`, `EQ % Tot Change`,
                                         `EQ Share`, `EQ Share Previous`, `EQ Share Diff`) %>%
                                  filter(MonthText == unique(sharetot_month$MonthText[nrow(sharetot_month)])) %>%
                                   mutate_if(is.numeric, ~ prettyNum(round(., 2), big.mark = ",")) %>% arrange(NIELSEN_SEGMENT)


write_csv(sharetot_month1, paste0(CSV_XLdirectoryLoc,"share_Lmonth_Change.csv"))
Message2Log("Saving share_Lmonth_Change.csv")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Validation ####

## Run CompareTest_Prod_TMC20.Rmd - HTML flexdashboard Output ####
Message2Log("Running CompareTest_Prod_TMC20.Rmd")
rmarkdown::render(paste0(RScriptdirectoryLoc,"CompareTest_Prod_TMC20.Rmd"))

### Archive/Move ####
file.copy(from = paste0(RScriptdirectoryLoc,"CompareTest_Prod_TMC20.html"), 
          to = paste0(directoryLoc,"LogArchive/CompareTest_Prod_TMC20.html",curdate,"_",curtime,".html"), 
          copy.mode = TRUE, copy.date = FALSE)
file.rename(from = paste0(RScriptdirectoryLoc,"CompareTest_Prod_TMC20.html"),
            to = paste0(ValidationdirectoryLoc, "CompareTest_Prod_TMC20.html"))

## Run TMCValidationReport_TMC20.Rmd - HTML Segment Reports ####
Message2Log("Running TMCValidationReport_TMC20.Rmd - Segment HTML Reports")
outdate <- Sys.Date()
Segments <-c("INFANT FORMULA UA","ORAL ELECTROLYTES","TODDLER NUTRITION","BALANCED NUTRITION","DIABETES NUTRITION")

for (Segment in Segments) {
  rmarkdown::render(paste0(RScriptdirectoryLoc, "TMCValidationReport_TMC20.Rmd"),
                    output_file = paste0(ValidationdirectoryLoc, outdate,'_', Segment, '_TMC_Validation', '.html'),
                    params = list(Segment = Segment))
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Run TMC20Plan.R ####
Message2Log("Running TMC20Plan.R - Updating Plan")
#source(paste0(RScriptdirectoryLoc,"TMC20Plan.R"))

KeepObjects <- get.objects()
Cleanup <- data.frame(get.objects(paste0(RScriptdirectoryLoc,"TMC20Plan.R"),exception = KeepObjects))
rm(list = Cleanup[1:nrow(Cleanup),1])

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# Record End Time ####
Endtime <- Sys.time()
RunTime <- as.character(paste0("Script Ran for ",(round(difftime(Endtime, Starttime, units = "mins"),digits = 0))," Minutes."))
Message2Log(RunTime)

close(log_file)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ####
# End tryCatch ####
},error = function(e){
  message('Caught an error!')
  print(e)
},finally = {
  message('All done, quitting.')
}
)
