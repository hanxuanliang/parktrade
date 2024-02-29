import pandas as pd
import adata as ad

from parktrade.tools import market


def stock_history_ad(
    code: str = "002444",
    start_date: str = "1970-01-01",
    _end_date: str = "2100-01-01",
    level: str = market.K_DAY,
) -> pd.DataFrame:
    """Fetch stock history use adata."""
    K_TYPE = {
        market.K_DAY: 1,
        market.K_WEEK: 2,
        market.K_MONTH: 3,
    }
    k_type = K_TYPE[level]

    # TODO adata 目前不支持分钟级别数据获取
    return ad.stock.market.get_market(
        stock_code=code, k_type=k_type, start_date=start_date
    )
