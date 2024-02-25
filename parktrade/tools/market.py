K_DAY = "day"
K_WEEK = "week"
K_MONTH = "month"
K_5m = "5m"
K_15m = "15m"
K_30m = "30m"
K_60m = "60m"

K_TYPE = {
    K_DAY: "daily",
    K_WEEK: "weekly",
    K_MONTH: "monthly",
    K_5m: 5,
    K_15m: 15,
    K_30m: 30,
    K_60m: 60,
}


def ak_code(stock_code: str) -> str:
    return __normalize_code(stock_code, "ak")


def bs_code(stock_code: str) -> str:
    return __normalize_code(stock_code, "bs")


def __normalize_code(stock_code: str, ty: str) -> str:
    """
    Normalize stock code to the format
    """
    mapping = {"6": "sh", "300": "sz", "200": "sz"}
    dot = "" if ty == "ak" else "."

    prefix = mapping.get(stock_code[:1], "sz")
    return f"{prefix}{dot}{stock_code}"


if __name__ == "__main__":
    print(ak_code("600848"))
    print(bs_code("600848"))
    print(ak_code("300508"))
    print(bs_code("300508"))
    print(ak_code("000001"))
    print(bs_code("000001"))
    print(ak_code("002572"))
    print(bs_code("002572"))
