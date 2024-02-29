from typing import Dict, List
import pandas as pd
import multitasking
from retry import retry
from tqdm import tqdm
import time

from parktrade.tools import market
from parktrade.oda.adata import stock_history_ad
from parktrade.oda.baostock import stock_history_bs

MAX_CONNECTIONS = 32


def get_quote_history_multi(
    codes: List[str],
    start_date: str = "1970-01-01",
    end_date: str = "2100-01-01",
    fqt: str = market.K_DAY,
    **kwargs,
) -> Dict[str, pd.DataFrame]:
    if len(codes) == 0:
        return {}
    if len(codes) >= 64:
        raise ValueError("The number of codes should be less than 64")

    dfs: Dict[str, pd.DataFrame] = {}
    total = len(codes)

    @multitasking.task
    @retry(tries=3, delay=1)
    def start(code: str):
        _df = get_quote_history(code, start_date, end_date, fqt, **kwargs)
        dfs[code] = _df
        _df.to_csv(f"./parktrade/data/{code}-{fqt}.csv", index=False)

        pbar.update(1)
        pbar.set_description_str(f"📦 Processing => {code}")

    pbar = tqdm(total=total)
    for code in codes:
        if len(multitasking.get_active_tasks()) > MAX_CONNECTIONS:
            time.sleep(3)
        start(code)
    multitasking.wait_for_tasks()
    pbar.close()

    return dfs


def get_quote_history(
    code: str,
    start_date: str = "1970-01-01",
    end_date: str = "2100-01-01",
    fqt: str = market.K_DAY,
    **kwargs,
) -> pd.DataFrame:
    if fqt in [market.K_DAY, market.K_WEEK, market.K_MONTH]:
        return stock_history_ad(code, start_date, end_date, fqt, **kwargs)

    return stock_history_bs(code, start_date, end_date, fqt, **kwargs)


def into_code(code_csv: str) -> pd.DataFrame:
    df = pd.read_csv(code_csv)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)

    filtered_df = df.query(
        "~(short_name.str.contains('ST') | short_name.str.contains('B股') | "
        "list_date == '1900-01-01' | "
        "~(stock_code.str.startswith('00') | stock_code.str.startswith('60')) | stock_code.str.startswith('20'))",
        engine="python",
    )

    return filtered_df


if __name__ == "__main__":
    codes = into_code("./parktrade/symbol.csv").get("stock_code").tolist()

    codes = [codes[i : i + 50] for i in range(0, len(codes), 50)]
    for code_batch in codes:
        get_quote_history_multi(
            code_batch, start_date="2023-06-01", end_date="2024-02-29", fqt=market.K_30m
        )
