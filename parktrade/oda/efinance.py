from icecream import ic
import efinance as ef


if __name__ == "__main__":
    stock_code = "000001"
    frequency = 30

    res = ef.stock.get_quote_history(stock_code, klt=frequency)
    ic(res)
