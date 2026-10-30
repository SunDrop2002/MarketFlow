CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time BIGINT NOT NULL,
    close_time BIGINT NOT NULL,

    open NUMERIC(28, 8) NOT NULL,
    high NUMERIC(28, 8) NOT NULL,
    low NUMERIC(28, 8) NOT NULL,
    close NUMERIC(28, 8) NOT NULL,

    volume_btc NUMERIC(28, 8) NOT NULL,
    volume_usdt NUMERIC(28, 8),
    trade_count BIGINT,
    taker_buy_volume_btc NUMERIC(28, 8),
    taker_buy_volume_usdt NUMERIC(28, 8),

    price_change NUMERIC(28, 8),
    price_change_pct NUMERIC(28, 8),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (symbol, interval, open_time),

    CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0),
    CHECK (high >= low),
    CHECK (open BETWEEN low AND high),
    CHECK (close BETWEEN low AND high),
    CHECK (volume_btc >= 0),
    CHECK (close_time >= open_time)
);
