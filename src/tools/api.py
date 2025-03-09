import os
import pandas as pd
import requests

from data.cache import get_cache
from data.models import (
    CompanyNews,
    CompanyNewsResponse,
    FinancialMetrics,
    FinancialMetricsResponse,
    Price,
    PriceResponse,
    LineItem,
    LineItemResponse,
    InsiderTrade,
    InsiderTradeResponse,
)

# Global cache instance
_cache = get_cache()


def get_prices(ticker: str, start_date: str, end_date: str) -> list[Price]:
    """Fetch price data from cache or API."""
    # Check cache first
    if cached_data := _cache.get_prices(ticker):
        # Filter cached data by date range and convert to Price objects
        filtered_data = [Price(**price) for price in cached_data if start_date <= price["time"] <= end_date]
        if filtered_data:
            return filtered_data

    # 使用聚合数据 API 获取股票价格数据
    headers = {}
    api_key = os.environ.get("JUHE_API_KEY")
    if not api_key:
        raise Exception("请在环境变量中设置 JUHE_API_KEY")

    # 聚合数据股票历史行情接口
    url = "http://web.juhe.cn/finance/stock/hs"
    params = {
        "gid": ticker,  # 股票代码，如 sh601009
        "key": api_key,
        "start": start_date,  # 开始日期，如 2020-01-01
        "end": end_date,      # 结束日期，如 2020-12-31
    }
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"获取数据失败: {ticker} - {response.status_code} - {response.text}")
    
    data = response.json()
    if data.get("error_code") != 0:
        raise Exception(f"API 错误: {data.get('reason')}")
    
    # 转换数据格式为 Price 对象
    prices = []
    for item in data.get("result", {}).get("data", []):
        price = Price(
            time=item.get("date"),
            open=float(item.get("open")),
            high=float(item.get("high")),
            low=float(item.get("low")),
            close=float(item.get("close")),
            volume=float(item.get("volume")),
            ticker=ticker
        )
        prices.append(price)

    if not prices:
        return []

    # Cache the results as dicts
    _cache.set_prices(ticker, [p.model_dump() for p in prices])
    return prices


def get_financial_metrics(
    ticker: str,
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> list[FinancialMetrics]:
    """Fetch financial metrics from cache or API."""
    # Check cache first
    if cached_data := _cache.get_financial_metrics(ticker):
        # Filter cached data by date and limit
        filtered_data = [FinancialMetrics(**metric) for metric in cached_data if metric["report_period"] <= end_date]
        filtered_data.sort(key=lambda x: x.report_period, reverse=True)
        if filtered_data:
            return filtered_data[:limit]

    # 使用聚合数据 API 获取财务指标
    api_key = os.environ.get("JUHE_API_KEY")
    if not api_key:
        raise Exception("请在环境变量中设置 JUHE_API_KEY")
    
    # 这里使用聚合数据的财务报表接口
    url = "http://web.juhe.cn/finance/stock/findata"
    params = {
        "gid": ticker,
        "type": "report",  # 报表类型：report 财报
        "key": api_key
    }
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"获取数据失败: {ticker} - {response.status_code} - {response.text}")
    
    data = response.json()
    if data.get("error_code") != 0:
        raise Exception(f"API 错误: {data.get('reason')}")
    
    # 转换数据格式为 FinancialMetrics 对象
    financial_metrics = []
    for item in data.get("result", {}).get("data", [])[:limit]:
        # 根据您的 FinancialMetrics 模型结构进行适配
        metric = FinancialMetrics(
            ticker=ticker,
            report_period=item.get("date"),
            market_cap=float(item.get("total_market_cap", 0)),
            # 其他字段根据实际 API 返回和您的模型结构进行映射
        )
        financial_metrics.append(metric)

    if not financial_metrics:
        return []

    # Cache the results as dicts
    _cache.set_financial_metrics(ticker, [m.model_dump() for m in financial_metrics])
    return financial_metrics


def search_line_items(
    ticker: str,
    line_items: list[str],
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> list[LineItem]:
    """Fetch line items from API."""
    # 使用国内 API 获取财务数据项
    api_key = os.environ.get("JUHE_API_KEY")
    if not api_key:
        raise Exception("请在环境变量中设置 JUHE_API_KEY")
    
    # 这里可能需要多次调用不同接口来获取所需的财务数据项
    # 以下是示例实现
    url = "http://web.juhe.cn/finance/stock/findata"
    params = {
        "gid": ticker,
        "type": "report",
        "key": api_key
    }
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"获取数据失败: {ticker} - {response.status_code} - {response.text}")
    
    data = response.json()
    if data.get("error_code") != 0:
        raise Exception(f"API 错误: {data.get('reason')}")
    
    # 处理数据并返回 LineItem 对象
    search_results = []
    
    return search_results[:limit]


def get_insider_trades(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[InsiderTrade]:
    """Fetch insider trades from cache or API."""
    # Check cache first
    if cached_data := _cache.get_insider_trades(ticker):
        # Filter cached data by date range
        filtered_data = [InsiderTrade(**trade) for trade in cached_data 
                        if (start_date is None or (trade.get("transaction_date") or trade["filing_date"]) >= start_date)
                        and (trade.get("transaction_date") or trade["filing_date"]) <= end_date]
        filtered_data.sort(key=lambda x: x.transaction_date or x.filing_date, reverse=True)
        if filtered_data:
            return filtered_data

    # 使用聚合数据 API 获取内部交易数据
    api_key = os.environ.get("JUHE_API_KEY")
    if not api_key:
        raise Exception("请在环境变量中设置 JUHE_API_KEY")

    all_trades = []
    
    # 聚合数据高管持股变动接口
    url = "http://web.juhe.cn/finance/stock/executives"
    params = {
        "gid": ticker,  # 股票代码
        "key": api_key
    }
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"获取数据失败: {ticker} - {response.status_code} - {response.text}")
    
    data = response.json()
    if data.get("error_code") != 0:
        raise Exception(f"API 错误: {data.get('reason')}")
    
    # 转换数据格式为 InsiderTrade 对象
    for item in data.get("result", {}).get("data", []):
        # 根据日期过滤
        filing_date = item.get("date")
        if (start_date and filing_date < start_date) or filing_date > end_date:
            continue
            
        trade = InsiderTrade(
            ticker=ticker,
            filing_date=filing_date,
            transaction_date=filing_date,  # 可能需要调整
            insider_name=item.get("name"),
            insider_title=item.get("position"),
            transaction_type=item.get("change_type", ""),
            shares=float(item.get("change_shares", 0)),
            price=float(item.get("price", 0)) if item.get("price") else None,
            value=float(item.get("change_amount", 0)) if item.get("change_amount") else None,
            shares_owned=float(item.get("total_shares", 0)) if item.get("total_shares") else None,
        )
        all_trades.append(trade)

    if not all_trades:
        return []

    # 按日期排序
    all_trades.sort(key=lambda x: x.transaction_date or x.filing_date, reverse=True)
    
    # 限制返回数量
    all_trades = all_trades[:limit]

    # Cache the results
    _cache.set_insider_trades(ticker, [trade.model_dump() for trade in all_trades])
    return all_trades


def get_company_news(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[CompanyNews]:
    """Fetch company news from cache or API."""
    # Check cache first
    if cached_data := _cache.get_company_news(ticker):
        # Filter cached data by date range
        filtered_data = [CompanyNews(**news) for news in cached_data 
                        if (start_date is None or news["date"] >= start_date)
                        and news["date"] <= end_date]
        filtered_data.sort(key=lambda x: x.date, reverse=True)
        if filtered_data:
            return filtered_data

    # 使用聚合数据 API 获取公司新闻
    api_key = os.environ.get("JUHE_API_KEY")
    if not api_key:
        raise Exception("请在环境变量中设置 JUHE_API_KEY")

    all_news = []
    
    # 聚合数据股票新闻接口
    url = "http://web.juhe.cn/finance/stock/news"
    params = {
        "gid": ticker,  # 股票代码
        "key": api_key
    }
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"获取数据失败: {ticker} - {response.status_code} - {response.text}")
    
    data = response.json()
    if data.get("error_code") != 0:
        raise Exception(f"API 错误: {data.get('reason')}")
    
    # 转换数据格式为 CompanyNews 对象
    for item in data.get("result", {}).get("data", []):
        news_date = item.get("date")
        
        # 根据日期过滤
        if (start_date and news_date < start_date) or news_date > end_date:
            continue
            
        news = CompanyNews(
            ticker=ticker,
            date=news_date,
            title=item.get("title", ""),
            url=item.get("url", ""),
            source=item.get("source", ""),
            summary=item.get("content", ""),  # 使用内容作为摘要
        )
        all_news.append(news)

    if not all_news:
        return []

    # 按日期排序
    all_news.sort(key=lambda x: x.date, reverse=True)
    
    # 限制返回数量
    all_news = all_news[:limit]

    # Cache the results
    _cache.set_company_news(ticker, [news.model_dump() for news in all_news])
    return all_news



def get_market_cap(
    ticker: str,
    end_date: str,
) -> float | None:
    """Fetch market cap from the API."""
    financial_metrics = get_financial_metrics(ticker, end_date)
    market_cap = financial_metrics[0].market_cap
    if not market_cap:
        return None

    return market_cap


def prices_to_df(prices: list[Price]) -> pd.DataFrame:
    """Convert prices to a DataFrame."""
    df = pd.DataFrame([p.model_dump() for p in prices])
    df["Date"] = pd.to_datetime(df["time"])
    df.set_index("Date", inplace=True)
    numeric_cols = ["open", "close", "high", "low", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.sort_index(inplace=True)
    return df


# Update the get_price_data function to use the new functions
def get_price_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    prices = get_prices(ticker, start_date, end_date)
    return prices_to_df(prices)
