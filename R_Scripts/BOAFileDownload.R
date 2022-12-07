
# Move current files to Archive Folder ####
Message2Log("Archiving last week's Nielsen files")
ExtractFolder <- "V:/Retail Sales/Retail Reports/TMC20/NielsenExtracts/"
ArchiveFiles <- list.files(ExtractFolder,recursive = TRUE, include.dirs = FALSE) 
ArchiveFolder <- paste0(ExtractFolder,"/Archive/")
file.rename(paste0(ExtractFolder,ArchiveFiles),paste0(ArchiveFolder,ArchiveFiles))

# Cleanup Archive Folder, Keep only 12 week history & first date of each month prior ###
Message2Log("Deleting files over 12 weeks old, excluding first date of each month")
FolderCleanup <- file.info(dir(path = paste0(ArchiveFolder), full.names = TRUE))
DeleteFiles <- subset(FolderCleanup,difftime(Sys.time(), FolderCleanup$mtime, units = "weeks") > 12) %>% arrange(desc(mtime))
DeleteFiles$mtime <- date(DeleteFiles$mtime) %>% format(., "%Y-%m-%d")
KeepFileDates <- data.table(DeleteFiles, keep.rownames = TRUE) %>% select("mtime") %>% 
  mutate(month = month(mtime), year= year(mtime)) %>% 
  group_by(month,year) %>% 
  unique(.) %>%
  arrange(mtime) %>% 
  slice(1) %>% 
  ungroup()
anti_join <- anti_join(DeleteFiles, KeepFileDates, by = "mtime")
  if(ncol(anti_join)>0){
    files <- row.names(anti_join)
    sapply(files, file.remove)
  }
rm(ArchiveFolderCleanup, DeleteFiles)

# Loop until all .complete files present ####
Message2Log("Loop until all .complete files present")

CountComplete <- 0
while(CountComplete < 7){
  
# Connect to blob ####
bl_endp_key <- storage_endpoint("https://anpdmidanielsenstorage.dfs.core.windows.net", key="1OoiBc1/d68URyCCbofQ6A625u5TFkRzDMvn/UW7iHrp7R18hiJMaCelKgRmFvQOFyDBXzHEvUkOpTX3SVUFYg==")

# Find extract folder & files ####
cont <- storage_container(bl_endp_key, "nielsenextracts")
list_files_blob <- list_storage_files(cont)
list_files_blob_df <- as.data.frame(list_files_blob)
FILTER_list_files_blob_df <- list_files_blob_df %>% filter(grepl('*.completed', name))

# Find 2 .completed files ####
MaxCompleteDate <- max(FILTER_list_files_blob_df$lastModified) %>% date(.)
Complete_Mdate <- FILTER_list_files_blob_df %>% filter(date(lastModified) == date(MaxCompleteDate) | date(lastModified) == (date(MaxCompleteDate)-1))
CountComplete <- nrow(Complete_Mdate)
 if(CountComplete >= 7) {
   #message("Found all 7 .complete files.")
   Message2Log("Found all 7 .complete files.")
   Found_Time <- Sys.time()
 } else {
   #message(paste("Found", CountComplete , "complete files. Waiting for all 7."))
   Message2Log(paste("Found", CountComplete , "complete files. Waiting for all 7."))
     Sys.sleep(600)}
}

Sys.sleep(300)

Complete_message <- paste("Found",CountComplete,"complete extract files, latest from", as.character(MaxCompleteDate),"found at", as.character(Found_Time))
Message2Log(Complete_message)

# Find ITN/LN files ####
ITN_Files <- list_files_blob_df  %>% filter(grepl('ITN*', name)) 
ITN_Files_Mdate <- ITN_Files %>% filter(date(lastModified) == date(MaxCompleteDate) | date(lastModified) == (date(MaxCompleteDate)-1))
CountITN <- nrow(ITN_Files_Mdate)
#message(paste("Found", CountITN , "recent ITN files."))
ITN_message <- (paste("Found", CountITN , "recent ITN files."))
Message2Log(ITN_message)

LN_Files <- list_files_blob_df  %>% filter(grepl('LN*', name)) 
LN_Files_Mdate <- LN_Files %>% filter(date(lastModified) == date(MaxCompleteDate) | date(lastModified) == (date(MaxCompleteDate)-1))
CountLN <- nrow(LN_Files_Mdate)
#message(paste("Found", CountLN , "recent LN files."))
LN_message <- (paste("Found", CountLN , "recent LN files."))
Message2Log(LN_message)

# Combine into summary table ####
All_Files_Mdate <- bind_rows(ITN_Files_Mdate,LN_Files_Mdate)

# Download all files with latest date in completed to local folder ####
src <- c(All_Files_Mdate$name)
dest <- file.path(ExtractFolder, src)
storage_multidownload(cont, src, dest, overwrite = TRUE)
