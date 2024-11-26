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

    tmp_dfPV_chg <- tmp_dfPV %>%
                    select(id_yahoo, Date, open_chg, high_chg, low_chg,
                        close_chg, adjusted_chg, volume_chg) %>%
                    rename(open=open_chg, high=high_chg, low=low_chg,
                        close=close_chg, adjusted=adjusted_chg,
                        volume=volume_chg)

    tmp_dfPV_derived <- tmp_dfP_last %>%
        inner_join(tmp_dfPV_chg, by="id_yahoo", suffix=c("_last", "")) %>%
        group_by(id_yahoo) %>%
        arrange(desc(Date), .by_group=TRUE) %>%
        mutate(
            across(any_of(tmp_var_price), ~lag_cum_chg(.)),
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

}
