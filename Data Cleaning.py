import pandas as pd

data_dict = {
    'time_id': '09:32:01',
    'current_total': 1385,
    'current_profit': -30,
    'total_buy_price': 1415,
    'call_id': 1,
    'put_id': 1
}

call_dict = {
                "time_id": '09:32:01',
                "call_id": 1,
                "current_total": 0,
                "current_profit": 0,
                "qty": 0,
                "buy_count": 0,
                "target_percent": 0,
                "total_buy_price": 0
            }

put_dict = {
                "time_id": '09:32:01',
                "put_id": 1,
                "current_total": 1385,
                "current_profit": -30,
                "qty": 2,
                "buy_count": 1,
                "target_percent": 65,
                "total_buy_price": 1415
}

ticker_dict_calls = {
              "time_id": ["09:32:01", "09:32:01", "09:32:01", "09:32:01"],
              "ticker_symbol": ['AMD', 'AMD~2', 'NVDA', 'NVDA~2'],
              "in_trade": [False, False, False, False],
              "trade_closed": [True, True, True, True],
              "dd_executed": [False, False, False, False],
              "dd_position": [0, 0, 0, 0],
              "current_profit": [0, 0, 0, 0],
              "total_in": [0, 0, 0, 0],
              "max_percentage": [0, 0, 0, 0],
              "above_profit_target": [False, False, False, False],
              "profit_target_reached": [False, False, False, False],
              "alt_ticker": ["AMD~2", "AMD~2", "AMD~2", "AMD~2"],
              "last_5": [[], [], [], []]
}

ticker_dict_puts = {
              "time_id": ["09:32:01", "09:32:01", "09:32:01", "09:32:01"],
              "ticker_symbol": ['AMD', 'AMD~2', 'NVDA', 'NVDA~2'],
              "in_trade": [False, False, True, False],
              "trade_closed": [False, True, False, True],
              "dd_executed": [False, False, False, False],
              "dd_position": [0, 0, 0, 0],
              "current_profit": [-5, 0, -25,0],
              "total_in": [455, 0, 960, 0],
              "max_percentage": [0, 0, 0, 0],
              "above_profit_target": [False, False, False, False],
              "profit_target_reached": [False, False, False, False],
              "alt_ticker": ["AMD~2", "AMD~2", "AMD~2", "AMD~2"],
              "last_5": [[], [], [], []]
}

ticker_trend_dict = {
              "ticker_symbol": ['AMD', 'AMD~2', 'NVDA', 'NVDA~2'],
              "time_id": ["09:32:01", "09:32:01", "09:32:01", "09:32:01"],
              "call_count": [0, 0, 0, 0],
              "put_count": [0, 0, 0, 0]
}

data = pd.DataFrame.from_dict([data_dict], orient='columns')
calls = pd.DataFrame.from_dict([call_dict], orient='columns')
puts = pd.DataFrame.from_dict([put_dict], orient='columns')
ticker_calls = pd.DataFrame.from_dict([ticker_dict_calls], orient='columns')
ticker_puts = pd.DataFrame.from_dict([ticker_dict_puts], orient='columns')
pd.set_option('display.max_columns', None)
print(data.head())
print(calls.head())
print(puts.head())
print(ticker_calls.head())
print(ticker_puts.head())
