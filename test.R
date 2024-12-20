testDebug <- function()
{
    load("test.RData")

    tmp_dfData <- tmp_df_all

    tmp_dfData <- tmp_dfData %>%
                    mutate(volume=replace_na(volume, 0)) %>%
                    rename(id_yahoo=symbol, Date=date)

    tmp_var_price <- c("open", "high", "low", "close", "adjusted")
    tmp_var_volume <- c("volume")

    tmp_dfP <- tmp_dfData %>%
                select(id_yahoo, Date, any_of(tmp_var_price)) %>%
                group_by(id_yahoo) %>%
                arrange(Date, .by_group=TRUE) %>%
                mutate(across(any_of(tmp_var_price), ~pct_chg(.),
                    .names="{.col}_chg")) %>%
                ungroup()

    tmp_dfV <- tmp_dfData %>%
                filter(volume > 0) %>%
                select(id_yahoo, Date, any_of(tmp_var_volume)) %>%
                group_by(id_yahoo) %>%
                arrange(Date, .by_group=TRUE) %>%
                mutate(across(any_of(tmp_var_volume), ~pct_chg(.),
                    .names="{.col}_chg")) %>%
                ungroup()

    tmp_dfPV <- tmp_dfP %>%
                left_join(tmp_dfV, by=c("id_yahoo", "Date"))

    tmp_dfP_max_date <- tmp_dfP %>%
                    filter(close > 0) %>%
                    group_by(id_yahoo) %>%
                    summarize(Date=max(Date, na.rm=TRUE)) %>%
                    ungroup()

    tmp_dfV_max_date <- tmp_dfV %>%
                    filter(volume > 0) %>%
                    group_by(id_yahoo) %>%
                    summarize(Date=max(Date, na.rm=TRUE)) %>%
                    ungroup()

    tmp_dfP_last <- tmp_dfP %>%
                    inner_join(tmp_dfP_max_date, by=c("id_yahoo", "Date")) %>%
                    select(id_yahoo, Date, any_of(tmp_var_price))

    tmp_dfV_last <- tmp_dfV %>%
                    inner_join(tmp_dfV_max_date, by=c("id_yahoo", "Date")) %>%
                    select(id_yahoo, Date, any_of(tmp_var_volume))

}

testDebug_2 <- function()
{
    tmp_var_price <- c("open", "high", "low", "close", "adjusted")
    tmp_var_volume <- c("volume")

    tmp_dfPV_chg <- tmp_dfPV %>%
                    select(id_yahoo, Date, open_chg, high_chg, low_chg,
                        close_chg, adjusted_chg, volume_chg) %>%
                    rename_with(
                        ~str_replace(.x, "[_]chg", ""), .col=ends_with("_chg")
                    )

    tmp_dfPV_derived <- tmp_dfPV_chg %>%
        left_join(tmp_dfP_last, by=c("id_yahoo", "Date"), suffix=c("", "_last")) %>%
        left_join(tmp_dfV_last, by=c("id_yahoo", "Date"), suffix=c("", "_last")) %>%
        group_by(id_yahoo) %>%
        arrange(desc(Date), .by_group=TRUE) %>%
        fill(ends_with("_last"), .direction="down") %>%
        mutate(
#            across(any_of(tmp_var_price), ~lag_cum_chg(.)),
            across(any_of(tmp_var_price), ~lag_cum_chg_na(.)),
            across(any_of(tmp_var_volume), ~lag_cum_chg_na(.)),
            ) %>%
        mutate(
            O=(open_last*open),
            H=(high_last*high),
            L=(low_last*low),
            C=(close_last*close),
            V=(volume_last*volume),
            Ad=(adjusted_last*adjusted),
        )

    tmp_dfPV_1 <- tmp_dfPV %>%
                        select(id_yahoo, Date, open, high, low, close, volume, adjusted)

    tmp_dfPV_derived_1 <- tmp_dfPV_derived %>%
                        select(id_yahoo, Date, O, H, L, C, V, Ad)

    tmp_dfPV_check <-  tmp_dfPV_1 %>%
                        left_join(tmp_dfPV_derived_1, by=c("id_yahoo", "Date"), suffix=c("", "_1"))

    tmp_dfPV_check <- tmp_dfPV_check %>%
                        mutate(
                            open_diff = open - O,
                            high_diff = high - H,
                            low_diff = low - L,
                            close_diff = close - C,
                            volume_diff = volume - V,
                            adjusted_diff = adjusted - Ad)



}

get_max_date_1 <- function(dbconn)
{
    tmp_sql <- "SELECT a1.sec_id, a1.id_yahoo,
                    (SELECT MAX(a2.Date) FROM stock_return a2
                        WHERE a2.sec_id = a1.sec_id
                        AND a2.volume IS NOT NULL) AS [Date]
                FROM stocks a1"

    tmp_df <- tryCatch(
                qDBSQLite_SendStatement(dbconn, tmp_sql),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                })

    return (tmp_df)
}
