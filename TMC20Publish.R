## Publish Retailer Data & TMC ####
sysUser511 <- Sys.getenv("username")

# Define Directories ####
directoryLoc <- "//fcrpdfile02/WRKStApps1$/Retail Sales/Retail Reports/TMC20/"
SQLdirectoryMainLoc <- paste0(directoryLoc,"SQL_Queries/")
CSV_XLdirectoryLoc <- paste0(directoryLoc,"CSV_XL_Files/")
RScriptdirectoryLoc <- paste0(directoryLoc,"R_Scripts/")

# Run Step #3 SQL Publish to Prod Updates ####
#Step3_TMC_PUBLISH_TO_PROD <- readtext(paste0(WeeklyRefresh, "Step#3_TMC_PUBLISH_TO_PROD.txt"))$text
Step3_TMC_PUBLISH_TO_PROD <- readtext(paste0(SQLdirectoryMainLoc, "Step#3_TMC_PUBLISH_TO_PROD.txt"))$text
qryBOAExecute(Step3_TMC_PUBLISH_TO_PROD)

####
system('powershell -ExecutionPolicy Bypass -file "//fcrpdfile02/WRKStApps1$/Retail Sales/Retail Reports/TMC20/CSV_XL_Files/saveExcel2.ps1"')

# Save to the Walmart team One-Drive ####
directoryLoc1 <- paste0("C:/Users/",
                        sysUser511,
                        "/OneDrive - Abbott/Nielsen 104WK Item_Market_Week/")
file.copy(from = paste0(CSV_XLdirectoryLoc,"RetailExtract.xlsx"), 
          to = paste0(directoryLoc1,"RetailExtract.xlsx"), 
          copy.mode = TRUE, copy.date = TRUE, overwrite = TRUE)

# Save Timestamp & Refresh Power BI w/ Power Automate ####
directoryLoc2 <- paste0("C:/Users/",
                        sysUser511,
                       "/OneDrive - Abbott/TMC Prod/")

if(file.exists(paste0(directoryLoc2, "ProdCompleted.xlsx"))){
  ProdCompleted <- read_excel(paste0(directoryLoc2, "ProdCompleted.xlsx")) %>% 
    mutate(`Prod_Complete` = as.POSIXct(`Prod_Complete`))
}else{
  directoryLoc2 <- paste0("C:/Users/",
                          sysUser511,
                           "/OneDrive - Abbott/Documents - NA-ANPD-MIDATA/TMC Prod/")
  ProdCompleted <- read_excel(paste0(directoryLoc2, "ProdCompleted.xlsx")) %>% 
    mutate(`Prod_Complete` = as.POSIXct(`Prod_Complete`))
}

#append latest timestamp
curtime <- tibble(Prod_Complete = Sys.time())
ProdCompleted <- bind_rows(ProdCompleted, curtime)

# Write ProdCompleted.xlsx file
wb <- openxlsx::createWorkbook()
openxlsx::addWorksheet(wb, "Sheet1")
openxlsx::writeData(wb,
                    sheet = "Sheet1",
                    as.data.frame(ProdCompleted),
                    rowNames = FALSE)
openxlsx::saveWorkbook(wb, file = paste0(directoryLoc2,"ProdCompleted.xlsx"), overwrite = TRUE)


# Email Kroger file from TMCExtractMail.R ####
source(paste0(RScriptdirectoryLoc,"TMCExtractMail.R"))
