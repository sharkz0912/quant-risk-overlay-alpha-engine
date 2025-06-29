-- Create ticker_metadata table
CREATE TABLE ticker_metadata (
    Ticker TEXT PRIMARY KEY,
    StartDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    SectorETF TEXT
);

-- Create ohlcv_tiingo table
CREATE TABLE ohlcv_tiingo (
    Ticker TEXT,
    Date DATE,
    Close FLOAT,
    High FLOAT,
    Low FLOAT,
    Open FLOAT,
    Volume BIGINT,
    AdjClose FLOAT,
    AdjHigh FLOAT,
    AdjLow FLOAT,
    AdjOpen FLOAT,
    AdjVolume BIGINT,
    DivCash FLOAT,
    SplitFactor FLOAT,
    PRIMARY KEY (Ticker, Date)
);

-- Create ohlcv_yfinance table
CREATE TABLE ohlcv_yfinance (
    Ticker TEXT,
    Date DATE,
    Close FLOAT,
    High FLOAT,
    Low FLOAT,
    Open FLOAT,
    Volume BIGINT,
    AdjClose FLOAT,
    AdjHigh FLOAT,
    AdjLow FLOAT,
    AdjOpen FLOAT,
    AdjVolume BIGINT,
    DivCash FLOAT,
    SplitFactor FLOAT,
    PRIMARY KEY (Ticker, Date)
);

-- Create sector_etf_prices table
CREATE TABLE sector_etf_prices (
    ETF TEXT,
    Date DATE,
    Close FLOAT,
    High FLOAT,
    Low FLOAT,
    Open FLOAT,
    Volume BIGINT,
    AdjClose FLOAT,
    AdjHigh FLOAT,
    AdjLow FLOAT,
    AdjOpen FLOAT,
    AdjVolume BIGINT,
    DivCash FLOAT,
    SplitFactor FLOAT,
    PRIMARY KEY (ETF, Date)
);

-- Create macro_indicators table
CREATE TABLE macro_indicators (
    Ticker TEXT,
    Date DATE,
    Close FLOAT,
    High FLOAT,
    Low FLOAT,
    Open FLOAT,
    Volume BIGINT,
    PRIMARY KEY (Ticker, Date)
);
