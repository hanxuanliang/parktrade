import pandas as pd
import akshare as ak
from icecream import ic

from parktrade.tools import market


def stock_history_ak(
    code: str = "002444",
    start_date: str = "1970-01-01",
    end_date: str = "2100-01-01",
    interval: str = market.K_DAY,
) -> pd.DataFrame:
    k_type = market.K_TYPE[interval]
    fetch_fn = ak.stock_zh_a_hist_min_em
    if isinstance(k_type, str):
        fetch_fn = ak.stock_zh_a_hist
        start_date = start_date.replace("-", "")
        end_date = end_date.replace("-", "")

    df = fetch_fn(
        symbol=code,
        start_date=start_date,
        end_date=end_date,
        period=str(market.K_TYPE[interval]),
        adjust="qfq",
    ).rename(
        columns={
            "日期": "trade_time",
            "时间": "trade_time",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "涨跌幅": "change_pct",
            "涨跌额": "change",
            "成交量": "volume",
            "成交额": "amount",
            "振幅": "amplitude",
            "换手率": "turnover_ratio",
        }
    )

    df["stock_code"] = code
    df["trade_time"] = pd.to_datetime(df["trade_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["trade_date"] = pd.to_datetime(df["trade_time"]).dt.strftime("%Y-%m-%d")
    return df


if __name__ == "__main__":
    ic(
        stock_history_ak(
            code="000001",
            start_date="2020-01-01",
            end_date="2024-12-31",
            interval=market.K_30m,
        )
    )
    ic(
        stock_history_ak(
            code="000001",
            start_date="2020-01-01",
            end_date="2020-12-31",
            interval=market.K_DAY,
        )
    )
