# require(readtext)
# require(mailR)
# require(htmltools)

emailfile <- readtext("V:/Retail Sales/Retail Reports/TMC20/R_Scripts/TMC.RetailExtracts.Email.File.htm")$text

to <- c("data.analytics@abbott.com","Jeffrey.Fassbender@abbott.com")
sendby <- "data.analytics@abbott.com"
subject <- "TMC Kroger Retail Extracts File"
body <- emailfile
attachment <-("V:/Retail Sales/Retail Reports/TMC20/CSV_XL_Files/RetailKrogExtract.xlsx")
  
sendEmailWAtt (to, sendby, subject, body, attachment) 

