hk_stock_return.db
hk_stock_price.db

table: stocks
table: stock_return (sec_id, price_date, open, high, low, close, volume, adj_close)
table: stock_last_price (sec_id, price_date, open, high, low, close, volume, adj_close)

table: stock_price (sec_id, price_date, open, high, low, close, volume, adj_close)


CREATE TABLE stocks (
    sec_id INTEGER NOT NULL,
    name NVARCHAR(50),
    ccy NVARCHAR(3),
    exch_yahoo NVARCHAR(5),
    id_yahoo NVARCHAR(15),
    PRIMARY KEY(sec_id)
);

CREATE TABLE stock_return (
    sec_id INTEGER NOT NULL,
    Date DATE NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    adj_close REAL,
    PRIMARY KEY(sec_id, Date)
);

CREATE TABLE last_price (
    sec_id INTEGER NOT NULL,
    Date DATE NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    PRIMARY KEY(sec_id)
);

CREATE TABLE last_volume (
    sec_id INTEGER NOT NULL,
    Date DATE NOT NULL,
    volume REAL,
    PRIMARY KEY(sec_id)
);

CREATE TABLE stock_price (
    sec_id INTEGER NOT NULL,
    Date DATE NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    adj_close REAL,
    PRIMARY KEY(sec_id, Date)
);

--------------------------------------------------------------------------------
SELECT a1.sec_id, a1.id_yahoo,
    IFNULL((SELECT MAX(a2.Date) FROM stock_return a2
        WHERE a2.sec_id = a1.sec_id
        AND a2.volume > 0), '1900-01-01') AS [Date]
FROM stocks a1


--------------------------------------------------------------------------------
INSERT INTO stocks VALUES(1, 'CK Hutchison Holdings Ltd', 'HKD', 'HK', '0001.HK');
INSERT INTO stocks VALUES(2, 'CLP Holdings Ltd', 'HKD', 'HK', '0002.HK');
INSERT INTO stocks VALUES(2825, 'W.I.S.E. - CSI HK 100 Tracker', 'HKD', 'HK', '2825.HK');
