
# # Overwrite LBE with Actuals for Previous Quarter
 overwriteLBE <- FALSE

 directoryLoc1 <- paste0("C:/Users/",
                         sysUser511,
                         "/OneDrive - Abbott/Plan and LBE Update/")
 LBEdirectoryLoc <-
   "//fcrpdfile02/WRKStApps1$/Retail Sales/Retail Reports/TMC/"


# Copy IFUA segment plan file to directory
file.copy(from = paste0(directoryLoc1,"IF_Segment_LBE.xlsx"), 
          to = paste0(LBEdirectoryLoc,"IF_Segment_LBE.xlsx"), 
          copy.mode = TRUE, copy.date = TRUE, overwrite = TRUE)

# Copy plan LBE file to directory
file.copy(from = paste0(directoryLoc1,"LBEData1.xlsx"), 
          to = paste0(LBEdirectoryLoc,"LBEData1.xlsx"), 
          copy.mode = TRUE, copy.date = TRUE, overwrite = TRUE)

PlanCharacteristics <- qryBOAReturn("SELECT * from mitest.T_Consumption_Char") %>%
  select(UPC, ABT_BRAND, ABT_FORMULA_REB_NON_REBATED) %>% filter(
    ABT_BRAND == 'Ensure' |
      ABT_BRAND == 'Glucerna' |
      ABT_BRAND == 'Similac' |
      ABT_BRAND == 'Elecare' |
      ABT_BRAND == 'Go & Grow' |
      ABT_BRAND == 'Rcf' |
      ABT_BRAND == 'Pediasure' |
      ABT_BRAND == 'Pedialyte' |
      UPC == 'NWR_RETURN_PLUG' | 
      UPC == 'WIC_RETURN_PLUG'
  ) %>% 
  mutate(ABT_BRAND = case_when (
      ABT_BRAND == 'Similac' ~ 'Similac',
      ABT_BRAND == 'Elecare'  ~ 'Similac',
      ABT_BRAND == 'Go & Grow'  ~ 'Similac',
      ABT_BRAND == 'Rcf'  ~ 'Similac',
      UPC == 'NWR_RETURN_PLUG'  ~ 'Similac',
      UPC == 'WIC_RETURN_PLUG' ~ 'Similac',
      TRUE ~ ABT_BRAND)         ) %>%
  
  select("PlanBrand" = ABT_BRAND, everything())
PlanConsumption <- qryBOAReturn("SELECT * from mitest.T_Consumption") %>% select("Date" = "Period Description", everything())
PlanCalendar <- qryBOAReturn("SELECT * from mitest.T_ConsumptionCalendar") %>%
  select("Date", "Year", "Qtr") %>%
  mutate(QtrNum = as.numeric(gsub('Q', '', Qtr))) %>%
  select(-"Qtr")
PlanConsumption <- inner_join(PlanConsumption, PlanCharacteristics)
PlanConsumption_temp <- PlanConsumption %>% 
  filter(PlanBrand == 'Similac' #|
         #  PlanBrand == 'Elecare' |
          # PlanBrand == 'Go & Grow' |
           #PlanBrand == 'Rcf' |
           #UPC == 'NWR_RETURN_PLUG' | 
           #UPC == 'WIC_RETURN_PLUG'
         ) %>% 
  mutate(PlanBrand = case_when(PlanBrand == 'Similac' & ABT_FORMULA_REB_NON_REBATED == 'Rebated' ~ "Similac WIC",
                               PlanBrand == 'Similac' & ABT_FORMULA_REB_NON_REBATED == 'Non-Rebated' ~ "Similac NonWIC"#,
                              # PlanBrand == 'Elecare' & ABT_FORMULA_REB_NON_REBATED == 'Rebated' ~ "Similac WIC",
                              # PlanBrand == 'Elecare' & ABT_FORMULA_REB_NON_REBATED == 'Non-Rebated' ~ "Similac NonWIC",
                              # PlanBrand == 'Go & Grow' & ABT_FORMULA_REB_NON_REBATED == 'Rebated' ~ "Similac WIC",
                              # PlanBrand == 'Go & Grow' & ABT_FORMULA_REB_NON_REBATED == 'Non-Rebated' ~ "Similac NonWIC",
                              # PlanBrand == 'Rcf' & ABT_FORMULA_REB_NON_REBATED == 'Rebated' ~ "Similac WIC",
                              # PlanBrand == 'Rcf' & ABT_FORMULA_REB_NON_REBATED == 'Non-Rebated' ~ "Similac NonWIC",   
                              # UPC == 'NWR_RETURN_PLUG' ~ "Similac NonWIC",
                              # UPC == 'WIC_RETURN_PLUG' ~ "Similac WIC"                               
                              ))
PlanConsumption <-
  bind_rows(PlanConsumption, PlanConsumption_temp) %>%
##  filter(!((PlanBrand == "Ensure" |
##              PlanBrand == "Glucerna")  &
##             (
##               `Market Description` == "Nielsen Ecom" |
##                 `Market Description` == "Target AO"
##             )
##  )) %>% 
  mutate(PlanBrand = case_when(PlanBrand == 'Similac' ~ "Similac Total",
                              # PlanBrand == 'Elecare' ~ "Similac Total",
                              # PlanBrand == 'Go & Grow' ~ "Similac Total",
                              # PlanBrand == 'Rcf' ~ "Similac Total",
                              # is.na(PlanBrand) == TRUE & UPC == 'NWR_RETURN_PLUG' ~ "Similac Total",
                              # is.na(PlanBrand) == TRUE & UPC == 'WIC_RETURN_PLUG' ~ "Similac Total",
                                      TRUE ~ PlanBrand)) %>%
  select(-c("ABT_FORMULA_REB_NON_REBATED", "UPC", "Market Description")) %>% SumAll(.)
PlanConsumption <-
  PlanConsumption %>% inner_join(., PlanCalendar) %>%
  mutate(PlanLBE = paste0(`Year`, " Actuals")) %>%
  select(-c("Year", "QtrNum")) %>% SumAll(.)

maxconsumptiondate <- max(PlanConsumption$"Date")
maxconsumptiondatetbl <- PlanCalendar %>% filter(`Date` == maxconsumptiondate)
maxqtr <- max(maxconsumptiondatetbl$QtrNum)
maxyr <- max(maxconsumptiondatetbl$Year)
  
PlanConsumption <-
  PlanConsumption %>% mutate(
    `Date` = case_when(
      PlanLBE == "2022 Actuals" ~ ymd(`Date`),
      PlanLBE == "2021 Actuals" ~ ymd(`Date`) + lubridate::weeks(52),
      PlanLBE == "2020 Actuals" ~ ymd(`Date`) + lubridate::weeks(104),
      PlanLBE == "2019 Actuals" ~ ymd(`Date`) + lubridate::weeks(156),
      PlanLBE == "2018 Actuals" ~ ymd(`Date`) + lubridate::weeks(208)
    )
  ) %>% filter(!is.na(`Date`))

PlanConsumption <- PlanConsumption %>% 
  select(PlanBrand, "Date", `$`, EQ, PlanLBE) %>%
  pivot_longer(., cols = c(`$`, EQ),names_to = "EQ_DOL", values_to = "Value")%>%
  select(PlanBrand, "Date", "Value", PlanLBE, EQ_DOL)  
#Append plan and change LBE when quarter is complete (or month complete)

planfile <- read_excel(paste0(LBEdirectoryLoc,"LBEData1.xlsx")) %>% 
  select("PlanBrand" = "AN_BRAND_FAMILY", "Date", "Value", "PlanLBE", "EQ_DOL")
planfile <- bind_rows(planfile,PlanConsumption) %>% 
  inner_join(.,PlanCalendar)%>%
  mutate(EQ_DOL = case_when(EQ_DOL == 'EQ'~ "EQ",
                            EQ_DOL == '$' ~ "Retail $",
                            EQ_DOL == 'Retail $' ~ "Retail $",
                            TRUE ~ "Error"  ) ###CG added case statement 20220428
    
  )


#Overwrite LBE with Actuals
if(overwriteLBE == TRUE){
  #for lbe if < max qrt for current year then actual
  planfile1 <- planfile %>% filter(!(grepl('LBE', `PlanLBE`) & `QtrNum` < maxqtr))
  planfile2 <- planfile %>%
    filter(`PlanLBE` == paste0(maxyr, " Actuals") & `QtrNum` < maxqtr) %>%
    mutate(`PlanLBE` = paste0(maxyr, " LBE", maxqtr))
  planfile <- bind_rows(planfile1, planfile2)
}


planfile <- planfile %>% select(-c("Year", "QtrNum"))
planfile <- planfile %>% mutate(`Date` = as.Date(`Date`))
write_csv(planfile, paste0(LBEdirectoryLoc,"LBEDataOut.csv"))      ###    LBEDataOut.csv is what PBI needs                       
