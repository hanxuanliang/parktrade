import numpy as np
import pandas as pd
import baostock as bs
from icecream import ic

from parktrade.tools import market


def stock_history_bs(
    code: str = "002444",
    start_date: str = "1970-01-01",
    end_date: str = "2100-01-01",
    interval: str = market.K_DAY,
) -> pd.DataFrame:
    K_TYPE = {
        market.K_DAY: "d",
        market.K_WEEK: "w",
        market.K_MONTH: "m",
        market.K_5m: 5,
        market.K_15m: 15,
        market.K_30m: 30,
        market.K_60m: 60,
    }

    fields = "date,time,code,open,high,low,close,volume,amount"
    if isinstance(K_TYPE[interval], str):
        fields = "date,code,open,high,low,close,volume,amount"

    rs = bs.query_history_k_data_plus(
        code=market.bs_code(code),
        start_date=str(start_date),
        end_date=str(end_date),
        frequency=str(K_TYPE[interval]),
        fields=fields,
        adjustflag="3",
    )
    data = rs.get_data()
    if not data.empty:
        rename_columns = {
            "code": "stock_code",
            "date": "trade_date",
            "time": "trade_time",
        }
        df = data.rename(columns=rename_columns)
        df["stock_code"] = code
        if "trade_time" in df.columns:
            df["trade_time"] = df["trade_time"].apply(
                lambda x: pd.to_datetime(
                    f"{x:17}"[:4]
                    + "-"
                    + f"{x:17}"[4:6]
                    + "-"
                    + f"{x:17}"[6:8]
                    + " "
                    + f"{x:17}"[8:10]
                    + ":"
                    + f"{x:17}"[10:12]
                    + ":"
                    + f"{x:17}"[12:14]
                )
            )
        else:
            df["trade_time"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d 00:00:00")
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
        df["open"] = df["open"].astype(np.float64)
        df["high"] = df["high"].astype(np.float64)
        df["low"] = df["low"].astype(np.float64)
        df["close"] = df["close"].astype(np.float64)
        df["volume"] = df["volume"].astype(np.float64)
        df["amount"] = df["amount"].astype(np.float64)

        return df


if __name__ == "__main__":
    bs.login()
    ic(
        stock_history_bs(
            code="000001",
            start_date="2023-01-01",
            end_date="2024-12-31",
            interval=market.K_30m,
        )
    )
    ic(
        stock_history_bs(
            code="600000",
            start_date="2023-01-01",
            end_date="2024-12-31",
            interval=market.K_DAY,
        )
    )
