library(XML)
library(DBI)
library(RSQLite)
library(tidyverse)
library(readxl)
library(tidyquant)
library(openxlsx)

source("qDBSQLite.R", local=TRUE, echo=FALSE)
source("qDBSQLite_utility.R", local=TRUE, echo=FALSE)

lag_cum_chg <- function(x) {
    x[is.na(x)] <- 0
    y <- cumprod(1/(1+x))
    y <- lag(y)
    y[is.na(y)] <- 1
    return (y)
}

lag_cum_chg_na <- function(x) {
	y <- lag_cum_chg(x)
    y[is.na(x)] <- NA
    return (y)
}

pct_chg <- function(x) {
    # assign NA to 0 as well
    x[which(x==0)] <- NA
    x0 <- lag(na.locf(x, na.rm=FALSE))
    y <- (x - x0) / abs(x0)
    y[is.infinite(y)|is.nan(y)] <- NA
    return (y)
}

get_max_date <- function(dbconn)
{
    tmp_sql <- "SELECT a1.sec_id, a1.id_yahoo,
                    IFNULL((SELECT MAX(a2.Date) FROM stock_return a2
                        WHERE a2.sec_id = a1.sec_id
                        AND a2.volume IS NOT NULL), '1900-01-01') AS [Date]
                FROM stocks a1"

    tmp_df <- tryCatch(
                qDBSQLite_SendStatement(dbconn, tmp_sql),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                })

    return (tmp_df)
}

#qMain <- function(chrFileParam, dtStart, dtEnd)
qMain <- function()
{
    file_stock_list <- "HK.xlsx"

    dbconn <<- dbConnect(RSQLite::SQLite(), "", flags=SQLITE_RW, extended_types=TRUE)

    on.exit(
        tryCatch(
            dbDisconnect(dbconn),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
        })
    )

    tmp_file_path <- "C:/App/R/StockData_v3"
    tmp_file_db <- c("hk_stock_return.db", "hk_stock_price.db")
    tmp_file_db <- file.path(tmp_file_path, tmp_file_db)
    db_alias <- str_replace(basename(tmp_file_db), pattern="([.]db)$", "")

    for(i in 1:length(tmp_file_db)) {
        tmp_sql <- sprintf("ATTACH DATABASE '%s' AS %s", tmp_file_db[i], db_alias[i])
        cat(sprintf("\t%s\n", tmp_sql))
        rs <- dbSendStatement(dbconn, tmp_sql)
        dbClearResult(rs)
    } # for (db

    # Update Table Stocks
    tmp_sheets <- c("stock", "etf")

    # openxlsx::readWorkbook only accepts 1 sheet each time
    # Set simplify=FALSE to return a list of data.frame
    tmp_obj <- sapply(tmp_sheets, function(x) readWorkbook(file_stock_list, sheet=x),
                simplify=FALSE, USE.NAMES=FALSE)
    tmp_df <- do.call("rbind", tmp_obj)

    tmp_df <- tmp_df %>%
                distinct(sec_id, .keep_all=TRUE)

    tmp_chrTmpTable <- "tmp_db_stocks"
    tmp_chrKeys <- c("sec_id")
    rc <- create_temp_table(dbconn, tmp_df, tmp_chrTmpTable, tmp_chrKeys)

    source_tbl <- "tmp_db_stocks"
    target_tbl <- "stocks"
    rc <- table_update(dbconn, target_tbl, source_tbl)

    # get last date for each stock return
    tmp_dfMaxDate <- get_max_date(dbconn)
    tmp_df_all <- NULL

#    n <- nrow(tmp_dfMaxDate)
    n <- 3
    for (i in 1:n) {
        if (i %% 100 < 1) cat(sprintf("\t%.2f pct completed.\n", 100*i/n))
        tmp_sec_id <- tmp_dfMaxDate$sec_id[i]
        tmp_id_yahoo <- tmp_dfMaxDate$id_yahoo[i]
        tmp_start_date <- tmp_dfMaxDate$Date[i]
        tmp_end_date <- Sys.Date()

        cat(sprintf("\tProcess %s\n", tmp_id_yahoo))
        tmp_df <- tq_get(tmp_id_yahoo, from=tmp_start_date, to=tmp_end_date)

        tmp_df_all <- bind_rows(tmp_df_all, tmp_df)
    
    } # for (i

    tmp_var_price <- c("open", "high", "low", "close", "adj_close")
    tmp_var_volume <- c("volume")

    tmp_dfData <- tmp_df_all %>%
                    mutate(volume=replace_na(volume, 0)) %>%
                    rename(id_yahoo=symbol, Date=date, adj_close=adjusted)

    tmp_dfData <- tmp_dfData %>%
                    inner_join(tmp_dfMaxDate, by="id_yahoo", suffix=c("", "_tmp")) %>%
                    select(sec_id, Date, any_of(tmp_var_price), any_of(tmp_var_volume))

    tmp_dfP <- tmp_dfData %>%
                select(sec_id, Date, any_of(tmp_var_price)) %>%
                group_by(sec_id) %>%
                arrange(Date, .by_group=TRUE) %>%
                mutate(across(any_of(tmp_var_price), ~pct_chg(.),
                    .names="{.col}_chg")) %>%
                ungroup()

    tmp_dfV <- tmp_dfData %>%
                filter(volume > 0) %>%
                select(sec_id, Date, any_of(tmp_var_volume)) %>%
                group_by(sec_id) %>%
                arrange(Date, .by_group=TRUE) %>%
                mutate(across(any_of(tmp_var_volume), ~pct_chg(.),
                    .names="{.col}_chg")) %>%
                ungroup()

    tmp_dfPV <- tmp_dfP %>%
                left_join(tmp_dfV, by=c("sec_id", "Date"))

    tmp_dfP_max_date <- tmp_dfP %>%
                    filter(close > 0) %>%
                    group_by(sec_id) %>%
                    summarize(Date=max(Date, na.rm=TRUE)) %>%
                    ungroup()

    tmp_dfV_max_date <- tmp_dfV %>%
                    filter(volume > 0) %>%
                    group_by(sec_id) %>%
                    summarize(Date=max(Date, na.rm=TRUE)) %>%
                    ungroup()

    tmp_dfP_last <- tmp_dfP %>%
                    inner_join(tmp_dfP_max_date, by=c("sec_id", "Date")) %>%
                    select(sec_id, Date, any_of(tmp_var_price))

    tmp_dfV_last <- tmp_dfV %>%
                    inner_join(tmp_dfV_max_date, by=c("sec_id", "Date")) %>%
                    select(sec_id, Date, any_of(tmp_var_volume))

    #save(list=c("tmp_df_all", "tmp_dfMaxDate"), envir=pos.to.env(1), file="test.RData")

}