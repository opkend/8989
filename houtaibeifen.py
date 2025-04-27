import requests
import pandas as pd
import numpy as np
import time
import os
import json
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== 配置 ==========
PUSHED_FILE = "pushed_signals.txt"
SIGNALS_FILE = "signals.json"
STATS_FILE = "signals_stats.json"
POSITION_FILE = "position.json"   # 持仓状态持久化文件  # 新增

TELEGRAM_TOKEN = "7949541164:AAEny221PysiPNrYIyBJ0qM23vln2tE7YwI"
TELEGRAM_CHAT_ID = "1367665516"

INTERVAL = "15m"
PERIOD = 16
MULTIPLIER = 3.4
REFRESH_SECONDS = 600
MAX_SYMBOLS = 200
THREAD_WORKERS = 12

# ========== 工具函数保持不变 ==========

def load_pushed():
    if not os.path.exists(PUSHED_FILE):
        return set()
    with open(PUSHED_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def save_pushed(pushed_keys):
    with open(PUSHED_FILE, "w", encoding="utf-8") as f:
        for k in pushed_keys:
            f.write(k + "\n")

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        r = requests.get(url, params=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"[{datetime.now()}] 电报推送异常: {e}")

def get_usdt_perpetual_symbols_with_volume(min_volume=10_000_000):
    url_exchange = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    url_24h = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    for _ in range(3):
        try:
            symbols_info = requests.get(url_exchange, timeout=10).json()["symbols"]
            all_usdt_perpetual = set(s["symbol"] for s in symbols_info
                if s["quoteAsset"] == "USDT"
                and s["contractType"] == "PERPETUAL"
                and s["status"] == "TRADING"
            )
            tickers = requests.get(url_24h, timeout=10).json()
            df_tickers = pd.DataFrame(tickers)
            df_tickers = df_tickers[df_tickers["symbol"].isin(all_usdt_perpetual)]
            df_tickers["quoteVolume"] = df_tickers["quoteVolume"].astype(float)
            df_filtered = df_tickers[df_tickers["quoteVolume"] >= min_volume]
            symbols = df_filtered["symbol"].tolist()
            return symbols
        except Exception as ex:
            print(f"[{datetime.now()}] 获取币种异常: {ex}")
            time.sleep(2)
    return []

def get_klines(symbol, interval="15m", limit=1000, retry=3):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    last_err = None
    for attempt in range(retry):
        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()
            if not isinstance(data, list):
                raise Exception("非K线列表数据")
            df = pd.DataFrame(data, columns=[
                "open_time","open","high","low","close","volume","close_time",
                "qav","nt","tbb","tbq","ignore"
            ])
            df["open"] = df["open"].astype(float)
            df["high"] = df["high"].astype(float)
            df["low"] = df["low"].astype(float)
            df["close"] = df["close"].astype(float)
            df["volume"] = df["volume"].astype(float)
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")
            if len(df) < 10:
                raise Exception("K线数据太少")
            return df
        except Exception as ex:
            last_err = ex
            time.sleep(1.5)
    print(f"{symbol} 获取K线失败: {last_err}")
    return pd.DataFrame()

def ATR(df, period):
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    return atr

def supertrend(df, period=16, multiplier=3.4):
    atr = ATR(df, period)
    hl2 = (df["high"] + df["low"]) / 2
    up = hl2 - (multiplier * atr)
    dn = hl2 + (multiplier * atr)
    trend = np.ones(len(df))
    for i in range(1, len(df)):
        prev_close = df["close"].iloc[i-1]
        if prev_close > dn.iloc[i-1]:
            trend[i] = 1
        elif prev_close < up.iloc[i-1]:
            trend[i] = -1
        else:
            trend[i] = trend[i-1]
            if trend[i] == 1 and up.iloc[i] < up.iloc[i-1]:
                up.iloc[i] = up.iloc[i-1]
            if trend[i] == -1 and dn.iloc[i] > dn.iloc[i-1]:
                dn.iloc[i] = dn.iloc[i-1]
    return pd.Series(trend, index=df.index)

def calc_ma_ema(df):
    df["ma80"] = df["close"].rolling(80).mean()
    df["ema120"] = df["close"].ewm(span=120, adjust=False).mean()
    df["ema160"] = df["close"].ewm(span=160, adjust=False).mean()
    return df

def get_all_higher_timeframe_trends(symbols, interval="1h", period=16, multiplier=3.4):
    trends = {}
    with ThreadPoolExecutor(max_workers=THREAD_WORKERS) as pool:
        futures = {pool.submit(get_klines, symbol, interval, period+50): symbol for symbol in symbols}
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                df = fut.result()
                if len(df) < period + 2:
                    trends[symbol] = 0
                else:
                    t = supertrend(df, period, multiplier)
                    trends[symbol] = t.iloc[-2]
            except Exception as ex:
                trends[symbol] = 0
                print(f"{symbol} 高周期K线异常: {ex}")
    return trends

def get_btc_eth_trend(interval="15m", period=16, multiplier=3.4):
    res = {}
    for sym in ["BTCUSDT", "ETHUSDT"]:
        df = get_klines(sym, interval=interval, limit=period+50)
        if len(df) < period+2:
            res[sym] = 0
        else:
            t = supertrend(df, period, multiplier)
            res[sym] = t.iloc[-2]
    return res

def batch_get_klines(symbols, interval, min_len=200):
    results = {}
    with ThreadPoolExecutor(max_workers=THREAD_WORKERS) as pool:
        futures = {pool.submit(get_klines, symbol, interval): symbol for symbol in symbols}
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                df = fut.result()
                if df.empty or len(df) < min_len:
                    continue
                results[symbol] = df
            except Exception as ex:
                print(f"{symbol} 批量K线异常: {ex}")
    return results

def keltner_channel(df, ema_period=20, atr_period=14, atr_mult=2):
    ema = df['close'].ewm(span=ema_period, adjust=False).mean()
    atr = ATR(df, atr_period)
    upper = ema + atr_mult * atr
    lower = ema - atr_mult * atr
    return ema, upper, lower

def calc_vwap(df):
    price = (df["high"] + df["low"] + df["close"]) / 3
    cum_vol = df["volume"].cumsum()
    cum_pv = (price * df["volume"]).cumsum()
    vwap = cum_pv / cum_vol
    return vwap

def calc_rsi(df, period=14):
    delta = df["close"].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(com=period-1, adjust=False).mean()
    ma_down = down.ewm(com=period-1, adjust=False).mean()
    rsi = 100 * ma_up / (ma_up + ma_down)
    return rsi

def calc_bollinger_bandwidth(df, period=20):
    ma = df["close"].rolling(period).mean()
    std = df["close"].rolling(period).std()
    upper = ma + 2 * std
    lower = ma - 2 * std
    width = (upper - lower) / ma
    return width

def calc_obv(df):
    obv = [0]
    for i in range(1, len(df)):
        if df["close"].iloc[i] > df["close"].iloc[i-1]:
            obv.append(obv[-1] + df["volume"].iloc[i])
        elif df["close"].iloc[i] < df["close"].iloc[i-1]:
            obv.append(obv[-1] - df["volume"].iloc[i])
        else:
            obv.append(obv[-1])
    return pd.Series(obv, index=df.index)

def calc_macd(df, fast=12, slow=26, signal=9):
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    return macd, signal_line

# ========== 信号生成函数保持不变 ==========

def trend_reverse_backtest(signals, df_dict, period=PERIOD, multiplier=MULTIPLIER):
    results = []
    profits = []
    losses = []
    for sig in signals:
        symbol = sig["币种"]
        entry_time = sig["时间"]
        entry_price = sig["当前价格"]
        sig_type = sig["信号类型"]
        df = df_dict.get(symbol)
        if df is None or sig_type not in ["Buy", "Sell"]:
            continue
        # entry_time已为字符串
        entry_idx = df[df["close_time"] == pd.to_datetime(entry_time)].index
        if len(entry_idx) == 0:
            continue
        entry_idx = entry_idx[0]
        exit_idx = None

        # 平多
        if sig_type == "Buy":
            for i in range(entry_idx + 1, len(df) - 4):
                cur_below_ma = (
                    df["close"].iloc[i] < df["ma80"].iloc[i]
                    and df["close"].iloc[i] < df["ema120"].iloc[i]
                    and df["close"].iloc[i] < df["ema160"].iloc[i]
                )
                i4_below_ma = (
                    df["close"].iloc[i + 4] < df["ma80"].iloc[i + 4]
                    and df["close"].iloc[i + 4] < df["ema120"].iloc[i + 4]
                    and df["close"].iloc[i + 4] < df["ema160"].iloc[i + 4]
                )
                if cur_below_ma and i4_below_ma:
                    exit_idx = i + 4
                    break

        # 平空
        elif sig_type == "Sell":
            for i in range(entry_idx + 1, len(df) - 4):
                cur_above_ma = (
                    df["close"].iloc[i] > df["ma80"].iloc[i]
                    and df["close"].iloc[i] > df["ema120"].iloc[i]
                    and df["close"].iloc[i] > df["ema160"].iloc[i]
                )
                i4_above_ma = (
                    df["close"].iloc[i + 4] > df["ma80"].iloc[i + 4]
                    and df["close"].iloc[i + 4] > df["ema120"].iloc[i + 4]
                    and df["close"].iloc[i + 4] > df["ema160"].iloc[i + 4]
                )
                if cur_above_ma and i4_above_ma:
                    exit_idx = i + 4
                    break

        if exit_idx is not None:
            exit_price = df["close"].iloc[exit_idx]
            pnl = (exit_price - entry_price) if sig_type == "Buy" else (entry_price - exit_price)
            results.append(pnl)
            if pnl > 0:
                profits.append(pnl)
            else:
                losses.append(abs(pnl))
    if len(results) == 0:
        return 0.0, 0, 0, 0, 0.0
    win_rate = sum(1 for x in results if x > 0) / len(results)
    lose_rate = sum(1 for x in results if x <= 0) / len(results)
    trades = len(results)
    pnl = sum(1 for x in results if x > 0) - sum(1 for x in results if x <= 0)
    profit_sum = sum(profits)
    loss_sum = sum(losses)
    profit_factor = profit_sum / loss_sum if loss_sum > 0 else float('inf')
    return win_rate, lose_rate, trades, pnl, profit_factor

def generate_signals(df, trend, symbol, higher_trend, last_position, btc_eth_trend):
    ema, upper, lower = keltner_channel(df)
    df["keltner_upper"] = upper
    df["keltner_lower"] = lower
    df["vwap"] = calc_vwap(df)
    df["rsi"] = calc_rsi(df)
    df["boll_width"] = calc_bollinger_bandwidth(df)
    df["obv"] = calc_obv(df)
    macd, macd_signal = calc_macd(df)
    df["macd"] = macd
    df["macd_signal"] = macd_signal

    signals = []
    buy_active = last_position.get(symbol, None) == "Buy"
    sell_active = last_position.get(symbol, None) == "Sell"
    # 记录：i+4那根K线如果刚平仓，也要判断能否反向开仓
    pending_reverse_check = None

    for i in range(max(160, PERIOD + 20), len(df) - 5):
        # === Buy信号逻辑 ===
        # 仅未持有Buy仓位才允许新开Buy
        if not buy_active:
            # 当前K线首次从下方突破三均线
            on_ma = (df["close"].iloc[i] > df["ma80"].iloc[i] and
                     df["close"].iloc[i] > df["ema120"].iloc[i] and
                     df["close"].iloc[i] > df["ema160"].iloc[i])
            prev_on_ma = (df["close"].iloc[i - 1] > df["ma80"].iloc[i - 1] and
                          df["close"].iloc[i - 1] > df["ema120"].iloc[i - 1] and
                          df["close"].iloc[i - 1] > df["ema160"].iloc[i - 1])
            if on_ma and not prev_on_ma:
                # 后续4根确认都在三均线上方
                window_ok = True
                for j in range(1, 5):
                    if not (df["close"].iloc[i + j] > df["ma80"].iloc[i + j] and
                            df["close"].iloc[i + j] > df["ema120"].iloc[i + j] and
                            df["close"].iloc[i + j] > df["ema160"].iloc[i + j]):
                        window_ok = False
                        break
                if not window_ok:
                    continue

                # 趋势窗口5根K线有1次supertrend为1即可
                if not (trend.iloc[i:i + 5] == 1).any():
                    continue

                # 突破K线涨幅限制（收盘/开盘 <= 3%）
                entry_open = df["open"].iloc[i]
                entry_close = df["close"].iloc[i]
                if (entry_close - entry_open) / entry_open > 0.03:
                    continue

                # 整体涨幅限制（i+4收盘/突破收盘 <= 4%）
                confirm_close = df["close"].iloc[i + 4]
                if (confirm_close - entry_close) / entry_close > 0.04:
                    continue

                # 其他辅助评分与原版一致
                suggest_multi_timeframe = "√" if trend.iloc[i] == 1 and higher_trend == 1 else "×"
                btc_trend = btc_eth_trend.get("BTCUSDT", 0)
                eth_trend = btc_eth_trend.get("ETHUSDT", 0)
                market_emotion = "√" if (trend.iloc[i] == 1 and btc_trend == 1 and eth_trend == 1) else "×"
                cur_vol = df["volume"].iloc[i + 4]
                mean_vol = df["volume"].iloc[i - 20 + 4:i + 5].mean()
                suggest_high_vol = "√" if cur_vol > mean_vol else "×"
                keltner_ok = (
                    (df["close"].iloc[i + 4] > df["keltner_upper"].iloc[i + 4] and
                     df["close"].iloc[i + 4] > df["vwap"].iloc[i + 4] and
                     df["rsi"].iloc[i + 4] > 55)
                )
                kvw_tag = "√" if keltner_ok else "×"
                recent_boll_width = df["boll_width"].iloc[max(0, i - 200):i + 4]
                width_threshold = np.percentile(recent_boll_width.dropna(), 10) if len(recent_boll_width.dropna()) > 0 else 0
                boll_tag = "√" if (df["boll_width"].iloc[i + 4] < width_threshold) else "×"
                obv_tag = "√" if df["obv"].iloc[i + 4] > df["obv"].rolling(30).max().iloc[i + 3] else "×"
                macd_status = "√" if df["macd"].iloc[i + 4] > df["macd_signal"].iloc[i + 4] else "×"
                adhesion = np.abs(df["ma80"] - df["ema120"]) + np.abs(df["ma80"] - df["ema160"]) + np.abs(df["ema120"] - df["ema160"])
                adhesion_norm = adhesion / df["close"]
                adhesion_tag_s = "√" if adhesion_norm.iloc[i + 4] < 0.01 else "×"
                score = sum([
                    suggest_multi_timeframe == "√",
                    suggest_high_vol == "√",
                    kvw_tag == "√",
                    boll_tag == "√",
                    obv_tag == "√",
                    macd_status == "√",
                    market_emotion == "√",
                    adhesion_tag_s == "√"
                ])
                cur_atr = ATR(df.iloc[:i + 5], 14).iloc[-1]
                prev_low = df["low"].iloc[max(0, i - 8):i].min()
                stop_loss = round(confirm_close - 1.5 * cur_atr, 4)
                take_profit = round(confirm_close + 2.5 * cur_atr, 4)
                stop_loss_ref = round(prev_low, 4)
                signals.append({
                    "币种": symbol,
                    "信号类型": "Buy",
                    "时间": df["close_time"].iloc[i + 4],
                    "当前价格": confirm_close,
                    "多周期共振": suggest_multi_timeframe,
                    "KVW辅助": kvw_tag,
                    "布林带收敛": boll_tag,
                    "OBV突破": obv_tag,
                    "MACD辅助": macd_status,
                    "市场情绪共振": market_emotion,
                    "成交量放大": suggest_high_vol,
                    "均线粘合度": adhesion_tag_s,
                    "信号强度": score,
                    "止损建议": stop_loss,
                    "止盈建议": take_profit,
                    "前低/前高止损": stop_loss_ref,
                    "持仓建议": "新开仓",
                    "信号索引": i + 4,
                    "方向": "Buy"
                })
                buy_active = True
                sell_active = False
                last_position[symbol] = "Buy"

        # === Sell信号逻辑 ===
        if not sell_active:
            on_ma = (df["close"].iloc[i] < df["ma80"].iloc[i] and
                     df["close"].iloc[i] < df["ema120"].iloc[i] and
                     df["close"].iloc[i] < df["ema160"].iloc[i])
            prev_on_ma = (df["close"].iloc[i - 1] < df["ma80"].iloc[i - 1] and
                          df["close"].iloc[i - 1] < df["ema120"].iloc[i - 1] and
                          df["close"].iloc[i - 1] < df["ema160"].iloc[i - 1])
            if on_ma and not prev_on_ma:
                # 后续4根确认都在三均线下方
                window_ok = True
                for j in range(1, 5):
                    if not (df["close"].iloc[i + j] < df["ma80"].iloc[i + j] and
                            df["close"].iloc[i + j] < df["ema120"].iloc[i + j] and
                            df["close"].iloc[i + j] < df["ema160"].iloc[i + j]):
                        window_ok = False
                        break
                if not window_ok:
                    continue

                # 趋势窗口5根K线有1次supertrend为-1即可
                if not (trend.iloc[i:i + 5] == -1).any():
                    continue

                # 跌破K线跌幅限制（收盘/开盘 <= 3%）
                entry_open = df["open"].iloc[i]
                entry_close = df["close"].iloc[i]
                if (entry_open - entry_close) / entry_open > 0.03:
                    continue

                # 整体跌幅限制（i收盘 - i+4收盘 <= 5%）
                confirm_close = df["close"].iloc[i + 4]
                if (entry_close - confirm_close) / entry_close > 0.05:
                    continue

                suggest_multi_timeframe = "√" if trend.iloc[i] == -1 and higher_trend == -1 else "×"
                btc_trend = btc_eth_trend.get("BTCUSDT", 0)
                eth_trend = btc_eth_trend.get("ETHUSDT", 0)
                market_emotion = "√" if (trend.iloc[i] == -1 and btc_trend == -1 and eth_trend == -1) else "×"
                cur_vol = df["volume"].iloc[i + 4]
                mean_vol = df["volume"].iloc[i - 20 + 4:i + 5].mean()
                suggest_high_vol = "√" if cur_vol > mean_vol else "×"
                keltner_ok = (
                    (df["close"].iloc[i + 4] < df["keltner_lower"].iloc[i + 4] and
                     df["close"].iloc[i + 4] < df["vwap"].iloc[i + 4] and
                     df["rsi"].iloc[i + 4] < 45)
                )
                kvw_tag = "√" if keltner_ok else "×"
                recent_boll_width = df["boll_width"].iloc[max(0, i - 200):i + 4]
                width_threshold = np.percentile(recent_boll_width.dropna(), 10) if len(recent_boll_width.dropna()) > 0 else 0
                boll_tag = "√" if (df["boll_width"].iloc[i + 4] < width_threshold) else "×"
                obv_tag = "√" if df["obv"].iloc[i + 4] < df["obv"].rolling(30).min().iloc[i + 3] else "×"
                macd_status = "√" if df["macd"].iloc[i + 4] < df["macd_signal"].iloc[i + 4] else "×"
                adhesion = np.abs(df["ma80"] - df["ema120"]) + np.abs(df["ma80"] - df["ema160"]) + np.abs(df["ema120"] - df["ema160"])
                adhesion_norm = adhesion / df["close"]
                adhesion_tag_s = "√" if adhesion_norm.iloc[i + 4] < 0.01 else "×"
                score = sum([
                    suggest_multi_timeframe == "√",
                    suggest_high_vol == "√",
                    kvw_tag == "√",
                    boll_tag == "√",
                    obv_tag == "√",
                    macd_status == "√",
                    market_emotion == "√",
                    adhesion_tag_s == "√"
                ])
                cur_atr = ATR(df.iloc[:i + 5], 14).iloc[-1]
                prev_high = df["high"].iloc[max(0, i - 8):i].max()
                stop_loss = round(confirm_close + 1.5 * cur_atr, 4)
                take_profit = round(confirm_close - 2.5 * cur_atr, 4)
                stop_loss_ref = round(prev_high, 4)
                signals.append({
                    "币种": symbol,
                    "信号类型": "Sell",
                    "时间": df["close_time"].iloc[i + 4],
                    "当前价格": confirm_close,
                    "多周期共振": suggest_multi_timeframe,
                    "KVW辅助": kvw_tag,
                    "布林带收敛": boll_tag,
                    "OBV突破": obv_tag,
                    "MACD辅助": macd_status,
                    "市场情绪共振": market_emotion,
                    "成交量放大": suggest_high_vol,
                    "均线粘合度": adhesion_tag_s,
                    "信号强度": score,
                    "止损建议": stop_loss,
                    "止盈建议": take_profit,
                    "前低/前高止损": stop_loss_ref,
                    "持仓建议": "新开仓",
                    "信号索引": i + 4,
                    "方向": "Sell"
                })
                sell_active = True
                buy_active = False
                last_position[symbol] = "Sell"

        # === 平仓信号逻辑 ===
        # Buy平仓（已持有Buy，当前K线跌破均线，i+4也在均线下方）
        if buy_active:
            cur_below_ma = (
                df["close"].iloc[i] < df["ma80"].iloc[i] and
                df["close"].iloc[i] < df["ema120"].iloc[i] and
                df["close"].iloc[i] < df["ema160"].iloc[i]
            )
            i4_below_ma = (
                df["close"].iloc[i + 4] < df["ma80"].iloc[i + 4] and
                df["close"].iloc[i + 4] < df["ema120"].iloc[i + 4] and
                df["close"].iloc[i + 4] < df["ema160"].iloc[i + 4]
            )
            if cur_below_ma and i4_below_ma:
                # 生成平多
                signals.append({
                    "币种": symbol,
                    "信号类型": "Buy平仓",
                    "时间": df["close_time"].iloc[i + 4],
                    "当前价格": df["close"].iloc[i + 4],
                    "持仓建议": "反向信号平多",
                    "信号索引": i + 4,
                    "方向": "Sell"
                })
                buy_active = False
                last_position[symbol] = "Sell"  # 表示已平仓

                # 判断能否在同一根K线开Sell（反向）
                # 直接在i+4上按Sell信号逻辑判定
                j = i + 4
                if not sell_active and j < len(df) - 5:
                    on_ma = (df["close"].iloc[j] < df["ma80"].iloc[j] and
                             df["close"].iloc[j] < df["ema120"].iloc[j] and
                             df["close"].iloc[j] < df["ema160"].iloc[j])
                    prev_on_ma = (df["close"].iloc[j - 1] < df["ma80"].iloc[j - 1] and
                                  df["close"].iloc[j - 1] < df["ema120"].iloc[j - 1] and
                                  df["close"].iloc[j - 1] < df["ema160"].iloc[j - 1])
                    # 需是首次跌破
                    if on_ma and not prev_on_ma:
                        # 后续4根确认都在三均线下方
                        window_ok = True
                        for k in range(1, 5):
                            if not (df["close"].iloc[j + k] < df["ma80"].iloc[j + k] and
                                    df["close"].iloc[j + k] < df["ema120"].iloc[j + k] and
                                    df["close"].iloc[j + k] < df["ema160"].iloc[j + k]):
                                window_ok = False
                                break
                        if window_ok:
                            # 趋势窗口5根K线有1次supertrend为-1即可
                            if (trend.iloc[j:j + 5] == -1).any():
                                entry_open = df["open"].iloc[j]
                                entry_close = df["close"].iloc[j]
                                if (entry_open - entry_close) / entry_open <= 0.03:
                                    confirm_close = df["close"].iloc[j + 4]
                                    if (entry_close - confirm_close) / entry_close <= 0.05:
                                        suggest_multi_timeframe = "√" if trend.iloc[j] == -1 and higher_trend == -1 else "×"
                                        btc_trend = btc_eth_trend.get("BTCUSDT", 0)
                                        eth_trend = btc_eth_trend.get("ETHUSDT", 0)
                                        market_emotion = "√" if (trend.iloc[j] == -1 and btc_trend == -1 and eth_trend == -1) else "×"
                                        cur_vol = df["volume"].iloc[j + 4]
                                        mean_vol = df["volume"].iloc[j - 20 + 4:j + 5].mean()
                                        suggest_high_vol = "√" if cur_vol > mean_vol else "×"
                                        keltner_ok = (
                                            (df["close"].iloc[j + 4] < df["keltner_lower"].iloc[j + 4] and
                                             df["close"].iloc[j + 4] < df["vwap"].iloc[j + 4] and
                                             df["rsi"].iloc[j + 4] < 45)
                                        )
                                        kvw_tag = "√" if keltner_ok else "×"
                                        recent_boll_width = df["boll_width"].iloc[max(0, j - 200):j + 4]
                                        width_threshold = np.percentile(recent_boll_width.dropna(), 10) if len(recent_boll_width.dropna()) > 0 else 0
                                        boll_tag = "√" if (df["boll_width"].iloc[j + 4] < width_threshold) else "×"
                                        obv_tag = "√" if df["obv"].iloc[j + 4] < df["obv"].rolling(30).min().iloc[j + 3] else "×"
                                        macd_status = "√" if df["macd"].iloc[j + 4] < df["macd_signal"].iloc[j + 4] else "×"
                                        adhesion = np.abs(df["ma80"] - df["ema120"]) + np.abs(df["ma80"] - df["ema160"]) + np.abs(df["ema120"] - df["ema160"])
                                        adhesion_norm = adhesion / df["close"]
                                        adhesion_tag_s = "√" if adhesion_norm.iloc[j + 4] < 0.01 else "×"
                                        score = sum([
                                            suggest_multi_timeframe == "√",
                                            suggest_high_vol == "√",
                                            kvw_tag == "√",
                                            boll_tag == "√",
                                            obv_tag == "√",
                                            macd_status == "√",
                                            market_emotion == "√",
                                            adhesion_tag_s == "√"
                                        ])
                                        cur_atr = ATR(df.iloc[:j + 5], 14).iloc[-1]
                                        prev_high = df["high"].iloc[max(0, j - 8):j].max()
                                        stop_loss = round(confirm_close + 1.5 * cur_atr, 4)
                                        take_profit = round(confirm_close - 2.5 * cur_atr, 4)
                                        stop_loss_ref = round(prev_high, 4)
                                        signals.append({
                                            "币种": symbol,
                                            "信号类型": "Sell",
                                            "时间": df["close_time"].iloc[j + 4],
                                            "当前价格": confirm_close,
                                            "多周期共振": suggest_multi_timeframe,
                                            "KVW辅助": kvw_tag,
                                            "布林带收敛": boll_tag,
                                            "OBV突破": obv_tag,
                                            "MACD辅助": macd_status,
                                            "市场情绪共振": market_emotion,
                                            "成交量放大": suggest_high_vol,
                                            "均线粘合度": adhesion_tag_s,
                                            "信号强度": score,
                                            "止损建议": stop_loss,
                                            "止盈建议": take_profit,
                                            "前低/前高止损": stop_loss_ref,
                                            "持仓建议": "新开仓",
                                            "信号索引": j + 4,
                                            "方向": "Sell"
                                        })
                                        sell_active = True
                                        last_position[symbol] = "Sell"

        # Sell平仓（已持有Sell，当前K线突破均线，i+4也在均线上方）
        if sell_active:
            cur_above_ma = (
                df["close"].iloc[i] > df["ma80"].iloc[i] and
                df["close"].iloc[i] > df["ema120"].iloc[i] and
                df["close"].iloc[i] > df["ema160"].iloc[i]
            )
            i4_above_ma = (
                df["close"].iloc[i + 4] > df["ma80"].iloc[i + 4] and
                df["close"].iloc[i + 4] > df["ema120"].iloc[i + 4] and
                df["close"].iloc[i + 4] > df["ema160"].iloc[i + 4]
            )
            if cur_above_ma and i4_above_ma:
                # 生成平空
                signals.append({
                    "币种": symbol,
                    "信号类型": "Sell平仓",
                    "时间": df["close_time"].iloc[i + 4],
                    "当前价格": df["close"].iloc[i + 4],
                    "持仓建议": "反向信号平空",
                    "信号索引": i + 4,
                    "方向": "Buy"
                })
                sell_active = False
                last_position[symbol] = "Buy"  # 表示已平仓

                # 判断能否在同一根K线开Buy（反向）
                j = i + 4
                if not buy_active and j < len(df) - 5:
                    on_ma = (df["close"].iloc[j] > df["ma80"].iloc[j] and
                             df["close"].iloc[j] > df["ema120"].iloc[j] and
                             df["close"].iloc[j] > df["ema160"].iloc[j])
                    prev_on_ma = (df["close"].iloc[j - 1] > df["ma80"].iloc[j - 1] and
                                  df["close"].iloc[j - 1] > df["ema120"].iloc[j - 1] and
                                  df["close"].iloc[j - 1] > df["ema160"].iloc[j - 1])
                    # 需是首次上穿
                    if on_ma and not prev_on_ma:
                        # 后续4根确认都在三均线上方
                        window_ok = True
                        for k in range(1, 5):
                            if not (df["close"].iloc[j + k] > df["ma80"].iloc[j + k] and
                                    df["close"].iloc[j + k] > df["ema120"].iloc[j + k] and
                                    df["close"].iloc[j + k] > df["ema160"].iloc[j + k]):
                                window_ok = False
                                break
                        if window_ok:
                            # 趋势窗口5根K线有1次supertrend为1即可
                            if (trend.iloc[j:j + 5] == 1).any():
                                entry_open = df["open"].iloc[j]
                                entry_close = df["close"].iloc[j]
                                if (entry_close - entry_open) / entry_open <= 0.03:
                                    confirm_close = df["close"].iloc[j + 4]
                                    if (confirm_close - entry_close) / entry_close <= 0.04:
                                        suggest_multi_timeframe = "√" if trend.iloc[j] == 1 and higher_trend == 1 else "×"
                                        btc_trend = btc_eth_trend.get("BTCUSDT", 0)
                                        eth_trend = btc_eth_trend.get("ETHUSDT", 0)
                                        market_emotion = "√" if (trend.iloc[j] == 1 and btc_trend == 1 and eth_trend == 1) else "×"
                                        cur_vol = df["volume"].iloc[j + 4]
                                        mean_vol = df["volume"].iloc[j - 20 + 4:j + 5].mean()
                                        suggest_high_vol = "√" if cur_vol > mean_vol else "×"
                                        keltner_ok = (
                                            (df["close"].iloc[j + 4] > df["keltner_upper"].iloc[j + 4] and
                                             df["close"].iloc[j + 4] > df["vwap"].iloc[j + 4] and
                                             df["rsi"].iloc[j + 4] > 55)
                                        )
                                        kvw_tag = "√" if keltner_ok else "×"
                                        recent_boll_width = df["boll_width"].iloc[max(0, j - 200):j + 4]
                                        width_threshold = np.percentile(recent_boll_width.dropna(), 10) if len(recent_boll_width.dropna()) > 0 else 0
                                        boll_tag = "√" if (df["boll_width"].iloc[j + 4] < width_threshold) else "×"
                                        obv_tag = "√" if df["obv"].iloc[j + 4] > df["obv"].rolling(30).max().iloc[j + 3] else "×"
                                        macd_status = "√" if df["macd"].iloc[j + 4] > df["macd_signal"].iloc[j + 4] else "×"
                                        adhesion = np.abs(df["ma80"] - df["ema120"]) + np.abs(df["ma80"] - df["ema160"]) + np.abs(df["ema120"] - df["ema160"])
                                        adhesion_norm = adhesion / df["close"]
                                        adhesion_tag_s = "√" if adhesion_norm.iloc[j + 4] < 0.01 else "×"
                                        score = sum([
                                            suggest_multi_timeframe == "√",
                                            suggest_high_vol == "√",
                                            kvw_tag == "√",
                                            boll_tag == "√",
                                            obv_tag == "√",
                                            macd_status == "√",
                                            market_emotion == "√",
                                            adhesion_tag_s == "√"
                                        ])
                                        cur_atr = ATR(df.iloc[:j + 5], 14).iloc[-1]
                                        prev_low = df["low"].iloc[max(0, j - 8):j].min()
                                        stop_loss = round(confirm_close - 1.5 * cur_atr, 4)
                                        take_profit = round(confirm_close + 2.5 * cur_atr, 4)
                                        stop_loss_ref = round(prev_low, 4)
                                        signals.append({
                                            "币种": symbol,
                                            "信号类型": "Buy",
                                            "时间": df["close_time"].iloc[j + 4],
                                            "当前价格": confirm_close,
                                            "多周期共振": suggest_multi_timeframe,
                                            "KVW辅助": kvw_tag,
                                            "布林带收敛": boll_tag,
                                            "OBV突破": obv_tag,
                                            "MACD辅助": macd_status,
                                            "市场情绪共振": market_emotion,
                                            "成交量放大": suggest_high_vol,
                                            "均线粘合度": adhesion_tag_s,
                                            "信号强度": score,
                                            "止损建议": stop_loss,
                                            "止盈建议": take_profit,
                                            "前低/前高止损": stop_loss_ref,
                                            "持仓建议": "新开仓",
                                            "信号索引": j + 4,
                                            "方向": "Buy"
                                        })
                                        buy_active = True
                                        last_position[symbol] = "Buy"
    return signals

# ========== 增量处理与持仓状态持久化 ==========

def load_last_position():
    """加载持仓状态，断点续跑用。"""
    if os.path.exists(POSITION_FILE):
        try:
            with open(POSITION_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {}

def save_last_position(last_position):
    """保存当前持仓状态。"""
    with open(POSITION_FILE, "w", encoding="utf-8") as f:
        json.dump(last_position, f, ensure_ascii=False, indent=2)

def load_all_signals():
    """加载历史信号，用于补齐增量处理。"""
    if os.path.exists(SIGNALS_FILE):
        try:
            with open(SIGNALS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return []

def get_latest_signal_time_for_symbol(signals, symbol):
    """获取指定币种最后一条信号的时间。"""
    # 找到最大时间
    last_time = None
    for sig in signals[::-1]:  # 倒序找
        if sig.get("币种") == symbol:
            t = pd.to_datetime(sig.get("时间"))
            if (last_time is None) or (t > last_time):
                last_time = t
    return last_time

def main():
    print("启动增量处理+持仓状态持久化采集推送服务")
    # 1. 加载signals历史、pushed、last_position
    all_signals = load_all_signals()
    all_signals_keys = set(f"{s['币种']}_{s['信号类型']}_{s['时间']}" for s in all_signals)
    pushed = load_pushed()
    last_position = load_last_position()  # <--- 增量处理核心

    while True:
        beijing_now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8)))
        print(f"[{beijing_now}] 本轮开始，获取币种列表")
        symbols = get_usdt_perpetual_symbols_with_volume(10_000_000)
        print(f"[{beijing_now}] 当前监控币种数量：{len(symbols)}")
        higher_trends = get_all_higher_timeframe_trends(symbols, interval="1h", period=PERIOD, multiplier=MULTIPLIER)
        btc_eth_trend = get_btc_eth_trend(interval=INTERVAL, period=PERIOD, multiplier=MULTIPLIER)
        df_dict = batch_get_klines(symbols, INTERVAL, min_len=200)

        for idx, symbol in enumerate(df_dict):
            try:
                df = df_dict[symbol]
                df = calc_ma_ema(df)
                trend = supertrend(df, PERIOD, MULTIPLIER)

                # ====================== 增量处理核心 ===============================
                # 找到本币种最后一条信号的时间，只处理之后的新K线
                last_sig_time = get_latest_signal_time_for_symbol(all_signals, symbol)
                # 获取最新K线时间
                all_close_times = df["close_time"].tolist()
                if last_sig_time is not None:
                    # 只处理大于最后信号时间的区间
                    # 允许前后有重叠，避免漏信号，但后面用信号唯一key去重
                    new_rows_mask = [t > last_sig_time for t in all_close_times]
                else:
                    # 第一次运行，全部处理
                    new_rows_mask = [True for t in all_close_times]

                # 只生成新信号（注意：generate_signals遍历所有K线，但只会生成当前last_position下的新信号，且后续信号唯一key去重）
                signals = generate_signals(
                    df, trend, symbol,
                    higher_trends.get(symbol, 0),
                    last_position,
                    btc_eth_trend
                )

                # 增量信号筛选，只保存“新K线”上的信号（时间大于上次信号时间）
                new_signals = []
                for sig in signals:
                    sig_time = pd.to_datetime(sig["时间"])
                    if (last_sig_time is None) or (sig_time > last_sig_time):
                        sig_key = f"{sig['币种']}_{sig['信号类型']}_{sig['时间']}"
                        if sig_key not in all_signals_keys:
                            # 推送处理
                            if (pd.to_datetime(sig['时间']) >= beijing_now - timedelta(minutes=15)) and (sig_key not in pushed):
                                msg = f"【新信号】{sig['币种']} {sig['信号类型']} {sig['时间']} 价格:{sig.get('当前价格','')}\n信号强度:{sig.get('信号强度','')}\n止损:{sig.get('止损建议','')}, 止盈:{sig.get('止盈建议','')}"
                                send_telegram_message(msg)
                                print(f"[{beijing_now}] 新信号已推送: {sig['币种']} {sig['信号类型']}")
                                pushed.add(sig_key)
                                save_pushed(pushed)
                            # 保存新信号
                            sig_for_store = dict(sig)
                            if hasattr(sig_for_store["时间"], "strftime"):
                                sig_for_store["时间"] = sig_for_store["时间"].strftime('%Y-%m-%d %H:%M:%S')
                            all_signals.append(sig_for_store)
                            all_signals_keys.add(sig_key)
                            new_signals.append(sig_for_store)
                            # 更新持仓状态
                            # 只要是Buy/Sell/Buy平仓/Sell平仓信号就更新
                            if sig["信号类型"] in ("Buy", "Sell", "Buy平仓", "Sell平仓"):
                                # 仓位方向就是Buy/Sell/None（平仓后置None）
                                if "平仓" in sig["信号类型"]:
                                    # 平仓后置None
                                    last_position[symbol] = "None"
                                else:
                                    last_position[symbol] = sig["信号类型"]
                            save_last_position(last_position)
                # ================================================================
            except Exception as e:
                print(f"{symbol} 指标与信号异常: {e}")
                continue

        # 保存所有信号到文件（全量历史，格式不变）
        try:
            with open(SIGNALS_FILE, "w", encoding="utf-8") as f:
                json.dump(all_signals, f, ensure_ascii=False, indent=2)
            print(f"[{beijing_now}] 已保存信号到 {SIGNALS_FILE}，共{len(all_signals)}条")
        except Exception as ex:
            print(f"保存信号失败: {ex}")

        # 回测统计保持不变
        try:
            win_rate, lose_rate, trades, pnl, profit_factor = trend_reverse_backtest(all_signals, df_dict)
            stats = {
                "win_rate": win_rate,
                "lose_rate": lose_rate,
                "trades": trades,
                "pnl": pnl,
                "profit_factor": profit_factor
            }
            with open(STATS_FILE, "w", encoding="utf-8") as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)
            print(f"[{beijing_now}] 已保存回测统计到 {STATS_FILE}")
        except Exception as ex:
            print(f"保存回测统计失败: {ex}")

        print(f"[{beijing_now}] 本轮推送结束，休眠 {REFRESH_SECONDS} 秒")
        time.sleep(REFRESH_SECONDS)

if __name__ == "__main__":
    main()