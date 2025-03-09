import os
import pandas as pd
import requests
import time
import json
from datetime import datetime

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

    # 如果没有缓存或范围内没有数据，从新浪财经API获取
    # 转换股票代码格式（添加市场前缀）
    if ticker.startswith('6'):
        formatted_ticker = f'sh{ticker}'
    else:
        formatted_ticker = f'sz{ticker}'
    
    # 转换日期格式
    start_timestamp = int(time.mktime(datetime.strptime(start_date, "%Y-%m-%d").timetuple()))
    end_timestamp = int(time.mktime(datetime.strptime(end_date, "%Y-%m-%d").timetuple()))
    
    # 新浪财经历史数据API
    url = f"https://finance.sina.com.cn/realstock/company/{formatted_ticker}/hisdata.js?d={start_date}&end={end_date}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # 解析响应数据
        # 注意：实际实现需要根据新浪API的实际返回格式进行调整
        data = response.json()
        
        prices = []
        for item in data:
            price = Price(
                ticker=ticker,
                time=item.get("date"),
                open=float(item.get("open")),
                high=float(item.get("high")),
                low=float(item.get("low")),
                close=float(item.get("close")),
                volume=float(item.get("volume")),
                adj_close=float(item.get("close")),  # 可能需要调整
            )
            prices.append(price)
        
        # 缓存结果
        if prices:
            _cache.set_prices(ticker, [p.model_dump() for p in prices])
        
        return prices
    except Exception as e:
        # 备选方案：使用东方财富数据
        try:
            # 东方财富API格式
            if ticker.startswith('6'):
                market_id = '1'  # 上证
            else:
                market_id = '0'  # 深证
                
            url = f"http://push2his.eastmoney.com/api/qt/stock/kline/get?secid={market_id}.{ticker}&fields=f1,f2,f3,f4,f5,f6,f7,f8&klt=101&fqt=0&beg={start_date.replace('-', '')}&end={end_date.replace('-', '')}"
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            klines = data.get('data', {}).get('klines', [])
            
            prices = []
            for kline in klines:
                parts = kline.split(',')
                if len(parts) >= 7:
                    price = Price(
                        ticker=ticker,
                        time=parts[0],
                        open=float(parts[1]),
                        close=float(parts[2]),
                        high=float(parts[3]),
                        low=float(parts[4]),
                        volume=float(parts[5]),
                        adj_close=float(parts[2]),  # 东方财富可能没有复权价
                    )
                    prices.append(price)
            
            # 缓存结果
            if prices:
                _cache.set_prices(ticker, [p.model_dump() for p in prices])
            
            return prices
        except Exception as e:
            raise Exception(f"获取价格数据失败: {ticker} - {str(e)}")

    return []


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

    # 从东方财富获取财务指标
    try:
        # 东方财富财务指标API
        url = f"http://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/ZYZBAjaxNew?type=0&code={ticker}"
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        # 解析东方财富返回的财务数据
        # 注意：实际实现需要根据东方财富API的实际返回格式进行调整
        
        financial_metrics = []
        for item in data.get('data', [])[:limit]:
            metrics = FinancialMetrics(
                ticker=ticker,
                report_period=item.get('REPORT_DATE'),
                revenue=item.get('TOTAL_OPERATE_INCOME'),
                net_income=item.get('PARENT_NETPROFIT'),
                eps=item.get('BASIC_EPS'),
                market_cap=None,  # 需要单独获取
                # 其他财务指标...
            )
            financial_metrics.append(metrics)
        
        # 缓存结果
        if financial_metrics:
            _cache.set_financial_metrics(ticker, [m.model_dump() for m in financial_metrics])
        
        return financial_metrics
    except Exception as e:
        raise Exception(f"获取财务指标失败: {ticker} - {str(e)}")

    return []


def search_line_items(
    ticker: str,
    line_items: list[str],
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> list[LineItem]:
    """Fetch line items from API."""
    # If not in cache or insufficient data, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    url = "https://api.financialdatasets.ai/financials/search/line-items"

    body = {
        "tickers": [ticker],
        "line_items": line_items,
        "end_date": end_date,
        "period": period,
        "limit": limit,
    }
    response = requests.post(url, headers=headers, json=body)
    if response.status_code != 200:
        raise Exception(f"Error fetching data: {ticker} - {response.status_code} - {response.text}")
    data = response.json()
    response_model = LineItemResponse(**data)
    search_results = response_model.search_results
    if not search_results:
        return []

    # Cache the results
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

    # If not in cache or insufficient data, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    all_trades = []
    current_end_date = end_date
    
    while True:
        url = f"https://api.financialdatasets.ai/insider-trades/?ticker={ticker}&filing_date_lte={current_end_date}"
        if start_date:
            url += f"&filing_date_gte={start_date}"
        url += f"&limit={limit}"
        
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error fetching data: {ticker} - {response.status_code} - {response.text}")
        
        data = response.json()
        response_model = InsiderTradeResponse(**data)
        insider_trades = response_model.insider_trades
        
        if not insider_trades:
            break
            
        all_trades.extend(insider_trades)
        
        # Only continue pagination if we have a start_date and got a full page
        if not start_date or len(insider_trades) < limit:
            break
            
        # Update end_date to the oldest filing date from current batch for next iteration
        current_end_date = min(trade.filing_date for trade in insider_trades).split('T')[0]
        
        # If we've reached or passed the start_date, we can stop
        if current_end_date <= start_date:
            break

    if not all_trades:
        return []

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

    # If not in cache or insufficient data, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    all_news = []
    current_end_date = end_date
    
    while True:
        url = f"https://api.financialdatasets.ai/news/?ticker={ticker}&end_date={current_end_date}"
        if start_date:
            url += f"&start_date={start_date}"
        url += f"&limit={limit}"
        
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error fetching data: {ticker} - {response.status_code} - {response.text}")
        
        data = response.json()
        response_model = CompanyNewsResponse(**data)
        company_news = response_model.news
        
        if not company_news:
            break
            
        all_news.extend(company_news)
        
        # Only continue pagination if we have a start_date and got a full page
        if not start_date or len(company_news) < limit:
            break
            
        # Update end_date to the oldest date from current batch for next iteration
        current_end_date = min(news.date for news in company_news).split('T')[0]
        
        # If we've reached or passed the start_date, we can stop
        if current_end_date <= start_date:
            break

    if not all_news:
        return []

    # Cache the results
    _cache.set_company_news(ticker, [news.model_dump() for news in all_news])
    return all_news



def get_market_cap(
    ticker: str,
    end_date: str,
) -> float | None:
    """Fetch market cap from the API."""
    try:
        # 从新浪或东方财富获取市值数据
        if ticker.startswith('6'):
            market_id = '1'  # 上证
        else:
            market_id = '0'  # 深证
            
        url = f"http://push2.eastmoney.com/api/qt/stock/get?secid={market_id}.{ticker}&fields=f57,f58,f116"
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        # f116通常是总市值（单位：元）
        market_cap = data.get('data', {}).get('f116')
        
        if market_cap:
            # 转换为亿元
            return float(market_cap) / 100000000
        
        # 如果上面的方法失败，尝试从财务指标中获取
        financial_metrics = get_financial_metrics(ticker, end_date)
        if financial_metrics and financial_metrics[0].market_cap:
            return financial_metrics[0].market_cap
    except Exception:
        pass
    
    return None


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
