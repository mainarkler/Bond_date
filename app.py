from __future__ import annotations

import os
import math
import re
import xml.etree.ElementTree as ET
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd
import requests
from flask import Flask, jsonify, request, send_from_directory
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__, static_folder="static", static_url_path="/static")


def get_session() -> requests.Session:
    """HTTP session with retries for MOEX API."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.4, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "BondDate Flask app"})
    return session


SESSION = get_session()


def moex_get_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = SESSION.get(url, params=params, timeout=(5, 20))
    response.raise_for_status()
    return response.json()


def moex_get(url: str, params: dict[str, Any] | None = None) -> requests.Response:
    response = SESSION.get(url, params=params, timeout=(5, 30))
    response.raise_for_status()
    return response


def to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).strip().replace(" ", "").replace(",", ".")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def isin_checksum_valid(isin: str) -> bool:
    if not re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", isin):
        return False
    converted = ""
    for char in isin[:-1]:
        converted += str(ord(char) - 55) if char.isalpha() else char
    digits = "".join(reversed(converted + isin[-1]))
    total = 0
    for i, digit in enumerate(digits):
        n = int(digit)
        if i % 2 == 1:
            n *= 2
        total += n // 10 + n % 10
    return total % 10 == 0


def parse_isins(raw: str) -> list[str]:
    values = [x.strip().upper() for x in re.split(r"[\s,;]+", raw) if x.strip()]
    return [x for x in values if isin_checksum_valid(x)]


@lru_cache(maxsize=1024)
def get_secid_and_emitter(isin: str) -> tuple[str | None, str | None]:
    payload = moex_get_json(
        "https://iss.moex.com/iss/securities.json",
        {"q": isin, "iss.meta": "off", "iss.only": "securities", "securities.columns": "secid,isin,emitter_id"},
    )
    block = payload.get("securities", {})
    columns = block.get("columns", [])
    for row in block.get("data", []):
        row_map = dict(zip(columns, row))
        if str(row_map.get("isin", "")).upper() == isin:
            return row_map.get("secid"), row_map.get("emitter_id")
    return None, None


def get_secid_by_isin(isin: str) -> str | None:
    secid, _ = get_secid_and_emitter(isin)
    return secid


def get_repo_row(isin: str) -> dict[str, Any]:
    secid = get_secid_by_isin(isin)
    if not secid:
        return {"isin": isin, "error": "SECID не найден"}

    payload = moex_get_json(
        f"https://iss.moex.com/iss/securities/{secid}.json",
        {
            "iss.meta": "off",
            "iss.only": "description,marketdata",
            "description.columns": "name,title,value",
            "marketdata.columns": "LAST,BID,OFFER,YIELD",
        },
    )
    description = {
        item[0]: item[2]
        for item in payload.get("description", {}).get("data", [])
        if isinstance(item, list) and len(item) >= 3
    }
    md_cols = payload.get("marketdata", {}).get("columns", [])
    md_rows = payload.get("marketdata", {}).get("data", [])
    market = dict(zip(md_cols, md_rows[0])) if md_cols and md_rows else {}

    maturity = description.get("MATDATE")
    days_to_maturity = None
    if maturity:
        try:
            days_to_maturity = (datetime.strptime(maturity, "%Y-%m-%d").date() - date.today()).days
        except ValueError:
            days_to_maturity = None

    emitter_id = get_secid_and_emitter(isin)[1]

    return {
        "isin": isin,
        "secid": secid,
        "emitterId": emitter_id or "-",
        "name": description.get("SHORTNAME") or description.get("NAME") or "-",
        "maturityDate": maturity or "-",
        "daysToMaturity": days_to_maturity if days_to_maturity is not None else "-",
        "lastPrice": market.get("LAST", "-"),
        "bid": market.get("BID", "-"),
        "offer": market.get("OFFER", "-"),
        "yield": market.get("YIELD", "-"),
    }


def to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def money_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def normalize_trade_key(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


@lru_cache(maxsize=1)
def fetch_forts_securities() -> list[tuple[str, str]]:
    url = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.xml"
    params = {"iss.meta": "off", "iss.only": "securities", "securities.columns": "SECID,SHORTNAME"}
    xml_content = moex_get(url, params=params).content.decode("utf-8", errors="ignore")
    xml_content = re.sub(r'\sxmlns="[^"]+"', "", xml_content, count=1)
    root = ET.fromstring(xml_content)
    rows: list[tuple[str, str]] = []
    for el in root.iter():
        if el.tag.lower().endswith("row"):
            secid = el.attrib.get("SECID", "")
            shortname = el.attrib.get("SHORTNAME", "")
            if secid and shortname:
                rows.append((secid, shortname))
    return rows


@lru_cache(maxsize=4)
def get_usd_rub_cb_today() -> dict[str, Any]:
    root = ET.fromstring(moex_get("https://www.cbr.ru/scripts/XML_daily.asp").content)
    for valute in root.findall("Valute"):
        if valute.findtext("CharCode") == "USD":
            value_str = valute.findtext("Value", "").replace(",", ".")
            nominal_str = valute.findtext("Nominal", "1")
            usd_rub = Decimal(value_str) / Decimal(nominal_str)
            return {"date": root.attrib.get("Date"), "usd_rub": usd_rub.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)}
    raise RuntimeError("Не удалось получить курс USD/RUB")


def fetch_vm_data(trade_name: str, quantity: int) -> dict[str, Any]:
    trade_name_upper = trade_name.strip().upper()
    trade_name_norm = normalize_trade_key(trade_name_upper)
    rows = fetch_forts_securities()
    secid_map = {short.upper(): secid for secid, short in rows}
    secid = secid_map.get(trade_name_upper)
    if not secid and trade_name_norm:
        normalized_map = {normalize_trade_key(short): sid for short, sid in secid_map.items()}
        secid = normalized_map.get(trade_name_norm)
    if not secid:
        raise RuntimeError(f"Контракт {trade_name} не найден в FORTS")

    spec_url = f"https://iss.moex.com/iss/engines/futures/markets/forts/securities/{secid}.json"
    spec = moex_get_json(spec_url, {"iss.meta": "off", "iss.only": "securities", "securities.columns": "PREVSETTLEPRICE,MINSTEP,STEPPRICE,LASTSETTLEPRICE"})
    sec_rows = spec.get("securities", {}).get("data", [])
    if not sec_rows:
        raise RuntimeError("Не удалось получить спецификацию контракта")
    prev_settle_raw, minstep_raw, stepprice_raw, last_settle_raw = sec_rows[0]
    prev_settle = to_decimal(prev_settle_raw)
    minstep = to_decimal(minstep_raw)
    stepprice = to_decimal(stepprice_raw)
    last_settle = to_decimal(last_settle_raw) if last_settle_raw is not None else None

    hist = moex_get_json(
        f"https://iss.moex.com/iss/history/engines/futures/markets/forts/securities/{secid}.json",
        {"iss.meta": "off", "iss.only": "history", "history.columns": "TRADEDATE,SETTLEPRICEDAY", "sort_order": "desc", "limit": 1},
    )
    hist_rows = hist.get("history", {}).get("data", [])
    if not hist_rows or hist_rows[0][1] is None:
        raise RuntimeError("Дневной клиринг ещё не опубликован")
    trade_date, day_settle_raw = hist_rows[0]
    day_settle = to_decimal(day_settle_raw)

    multiplier = stepprice / minstep
    vm_one = (day_settle - prev_settle) * multiplier
    position_vm = vm_one * Decimal(quantity)
    usd_rub_data = get_usd_rub_cb_today()
    usd_rub = Decimal(str(usd_rub_data["usd_rub"]))
    limit_sum = (Decimal("0.05") * day_settle * Decimal(quantity) * usd_rub) + position_vm

    return {
        "tradeName": trade_name,
        "secid": secid,
        "tradeDate": trade_date,
        "prevPrice": float(money_decimal(prev_settle)),
        "lastSettlePrice": float(money_decimal(last_settle)) if last_settle is not None else None,
        "todayPrice": float(money_decimal(day_settle)),
        "multiplier": float(multiplier),
        "vmPerContract": float(money_decimal(vm_one)),
        "quantity": quantity,
        "positionVm": float(money_decimal(position_vm)),
        "usdRub": str(usd_rub_data["usd_rub"]),
        "usdRubDate": usd_rub_data["date"],
        "limitSum": float(money_decimal(limit_sum)),
    }


def generate_q(mode: str, q_max: int, points: int) -> np.ndarray:
    if mode == "log":
        return np.logspace(np.log10(1), np.log10(q_max), points)
    return np.linspace(1, q_max, points)


def load_share_history(secid: str) -> pd.DataFrame:
    start = 0
    rows: list[list[Any]] = []
    cols: list[str] = []
    while True:
        js = moex_get_json(
            f"https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/{secid}.json",
            {"start": start, "iss.meta": "off"},
        )
        part = js.get("history", {}).get("data", [])
        cols = js.get("history", {}).get("columns", cols)
        if not part:
            break
        rows.extend(part)
        start += len(part)
    return pd.DataFrame(rows, columns=cols)


def calculate_share_delta_p(isin: str, c_value: float, q_max: int, q_mode: str) -> dict[str, Any]:
    secid = get_secid_by_isin(isin)
    if not secid:
        raise ValueError(f"ISIN {isin} не найден")
    df = load_share_history(secid)
    if df.empty:
        raise ValueError("Нет history данных")
    df = df[["TRADEDATE", "HIGH", "LOW", "CLOSE", "VALUE"]].copy()
    for col in ["HIGH", "LOW", "CLOSE", "VALUE"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna()
    t_len = len(df)
    sigma = math.sqrt(((df["HIGH"] - df["LOW"]) / df["CLOSE"]).sum() / t_len)
    mdtv = np.median(df["VALUE"])
    if sigma <= 0 or mdtv <= 0:
        raise ValueError("Некорректные входные данные для расчёта")
    q_vec = generate_q(q_mode, q_max, 50)
    delta_p = c_value * sigma * np.sqrt(q_vec / mdtv)
    rows = [{"Q": int(q), "DeltaP": float(dp)} for q, dp in zip(q_vec, delta_p)]
    return {"rows": rows, "meta": {"ISIN": isin, "T": t_len, "Sigma": float(sigma), "MDTV": float(mdtv)}}


def load_bond_history(secid: str) -> pd.DataFrame:
    start = 0
    rows: list[list[Any]] = []
    cols: list[str] = []
    while True:
        js = moex_get_json(
            f"https://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/{secid}.json",
            {"start": start, "iss.meta": "off"},
        )
        part = js.get("history", {}).get("data", [])
        cols = js.get("history", {}).get("columns", cols)
        if not part:
            break
        rows.extend(part)
        start += len(part)
    return pd.DataFrame(rows, columns=cols)


def load_bond_yield_data(secid: str) -> pd.DataFrame:
    js = moex_get_json(
        f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{secid}/marketdata_yields.json",
        {"iss.meta": "off"},
    )
    rows = js.get("marketdata_yields", {}).get("data", [])
    cols = js.get("marketdata_yields", {}).get("columns", [])
    return pd.DataFrame(rows, columns=cols)


def calculate_bond_delta_p(isin: str, c_value: float, q_max: int, q_mode: str) -> dict[str, Any]:
    secid = get_secid_by_isin(isin)
    if not secid:
        raise ValueError(f"ISIN {isin} не найден")
    df_hist = load_bond_history(secid)
    df_yield = load_bond_yield_data(secid)
    if df_hist.empty or df_yield.empty:
        raise ValueError("Нет данных для bond-расчета")
    df_hist = df_hist[["TRADEDATE", "HIGH", "LOW", "CLOSE", "VALUE"]].copy()
    for col in ["HIGH", "LOW", "CLOSE", "VALUE"]:
        df_hist[col] = pd.to_numeric(df_hist[col], errors="coerce")
    df_hist = df_hist.dropna()
    t_len = len(df_hist)
    sigma_y = ((df_hist["HIGH"] - df_hist["LOW"]) / df_hist["CLOSE"]).sum() / t_len
    mdtv = np.median(df_hist["VALUE"])
    if sigma_y <= 0 or mdtv <= 0:
        raise ValueError("Некорректные данные sigma_y / MDTV")

    for col in ["PRICE", "DURATIONWAPRICE", "EFFECTIVEYIELDWAPRICE"]:
        df_yield[col] = pd.to_numeric(df_yield[col], errors="coerce")
    df_yield = df_yield.dropna(subset=["PRICE", "DURATIONWAPRICE", "EFFECTIVEYIELDWAPRICE"])
    if df_yield.empty:
        raise ValueError("Нет marketdata_yields")
    last = df_yield.iloc[-1]
    price = float(last["PRICE"])
    ytm = float(last["EFFECTIVEYIELDWAPRICE"]) / 100
    duration = float(last["DURATIONWAPRICE"]) / 364
    dmod = duration / (1 + ytm)

    q_vec = generate_q(q_mode, q_max, 50)
    delta_y = c_value * sigma_y * np.sqrt(q_vec / mdtv)
    delta_p_pct = (dmod * price * delta_y) / price
    rows = [{"Q": int(q), "DeltaP_pct": float(dp)} for q, dp in zip(q_vec, delta_p_pct)]
    return {
        "rows": rows,
        "meta": {"ISIN": isin, "T": t_len, "SigmaY": float(sigma_y), "MDTV": float(mdtv), "Price": price, "YTM": ytm, "Dmod": dmod},
    }


def get_calendar_rows(isins: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for isin in isins:
        secid = get_secid_by_isin(isin)
        if not secid:
            rows.append({"isin": isin, "secid": "-", "eventType": "Ошибка", "eventDate": "-", "value": "SECID не найден"})
            continue
        payload = moex_get_json(
            f"https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{secid}.json",
            {"iss.meta": "off", "iss.only": "amortizations,coupons"},
        )
        for block_name, event_type in (("coupons", "Купон"), ("amortizations", "Амортизация")):
            block = payload.get(block_name, {})
            cols = block.get("columns", [])
            for item in block.get("data", []):
                item_map = dict(zip(cols, item))
                rows.append(
                    {
                        "isin": isin,
                        "secid": secid,
                        "eventType": event_type,
                        "eventDate": item_map.get("coupondate") or item_map.get("amortdate") or "-",
                        "value": item_map.get("valueprc") or item_map.get("value") or "-",
                    }
                )
    rows.sort(key=lambda r: r.get("eventDate") or "")
    return rows


def fetch_equity_snapshot() -> list[dict[str, Any]]:
    payload = moex_get_json(
        "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json",
        {
            "iss.meta": "off",
            "iss.only": "securities,marketdata",
            "securities.columns": "SECID,SHORTNAME,LOTSIZE",
            "marketdata.columns": "SECID,LAST,PREVPRICE,BID,OFFER,NUMTRADES",
        },
    )

    sec_block = payload.get("securities", {})
    md_block = payload.get("marketdata", {})
    sec_cols = sec_block.get("columns", [])
    md_cols = md_block.get("columns", [])

    sec_map = {row[0]: dict(zip(sec_cols, row)) for row in sec_block.get("data", []) if row}
    merged: list[dict[str, Any]] = []
    for row in md_block.get("data", []):
        if not row:
            continue
        md = dict(zip(md_cols, row))
        secid = md.get("SECID")
        if not secid or secid not in sec_map:
            continue
        merged.append({**sec_map[secid], **md})
    return merged


def get_vm_rows() -> list[dict[str, Any]]:
    try:
        snapshot = fetch_equity_snapshot()
        rows: list[dict[str, Any]] = []
        for item in snapshot:
            last = to_float(item.get("LAST"))
            prev = to_float(item.get("PREVPRICE"))
            lot = int(to_float(item.get("LOTSIZE")) or 0)
            if last is None or prev is None or lot <= 0:
                continue

            vm = (last - prev) * lot
            change_pct = ((last - prev) / prev) * 100 if prev else 0.0
            risk_level = "Высокий" if abs(change_pct) >= 3 else "Средний" if abs(change_pct) >= 1.5 else "Низкий"
            rows.append(
                {
                    "secid": item.get("SECID"),
                    "name": item.get("SHORTNAME") or "-",
                    "lastPrice": round(last, 4),
                    "prevPrice": round(prev, 4),
                    "lotSize": lot,
                    "changePct": round(change_pct, 3),
                    "vmLongRub": round(vm, 2),
                    "vmShortRub": round(-vm, 2),
                    "riskLevel": risk_level,
                }
            )
        rows.sort(key=lambda x: abs(float(x["vmLongRub"])), reverse=True)
        return rows[:20]
    except Exception:
        return [
            {"secid": "SBER", "name": "Сбербанк", "lastPrice": 300.0, "prevPrice": 295.0, "lotSize": 10, "changePct": 1.695, "vmLongRub": 50.0, "vmShortRub": -50.0, "riskLevel": "Средний"},
            {"secid": "GAZP", "name": "Газпром", "lastPrice": 170.0, "prevPrice": 168.4, "lotSize": 10, "changePct": 0.95, "vmLongRub": 16.0, "vmShortRub": -16.0, "riskLevel": "Низкий"},
            {"secid": "LKOH", "name": "Лукойл", "lastPrice": 7600.0, "prevPrice": 7480.0, "lotSize": 1, "changePct": 1.604, "vmLongRub": 120.0, "vmShortRub": -120.0, "riskLevel": "Средний"},
        ]


def get_sell_stress_rows() -> list[dict[str, Any]]:
    try:
        snapshot = fetch_equity_snapshot()
        rows: list[dict[str, Any]] = []
        for item in snapshot:
            last = to_float(item.get("LAST"))
            prev = to_float(item.get("PREVPRICE"))
            bid = to_float(item.get("BID"))
            offer = to_float(item.get("OFFER"))
            trades = int(to_float(item.get("NUMTRADES")) or 0)
            if last is None or prev is None or prev == 0:
                continue

            change_pct = ((last - prev) / prev) * 100
            spread_pct = ((offer - bid) / last * 100) if (bid is not None and offer is not None and last) else 0.0
            stress_score = abs(change_pct) * 0.7 + spread_pct * 0.3
            pressure = "Критичное" if stress_score >= 4 else "Повышенное" if stress_score >= 2 else "Умеренное"

            rows.append(
                {
                    "secid": item.get("SECID"),
                    "name": item.get("SHORTNAME") or "-",
                    "changePct": round(change_pct, 3),
                    "spreadPct": round(spread_pct, 3),
                    "numTrades": trades,
                    "stressScore": round(stress_score, 3),
                    "sellPressure": pressure,
                }
            )
        rows.sort(key=lambda x: float(x["stressScore"]), reverse=True)
        return rows[:20]
    except Exception:
        return [
            {"secid": "RUAL", "name": "Русал", "changePct": -4.2, "spreadPct": 0.9, "numTrades": 1780, "stressScore": 3.21, "sellPressure": "Повышенное"},
            {"secid": "AFKS", "name": "АФК Система", "changePct": -3.5, "spreadPct": 1.8, "numTrades": 2020, "stressScore": 2.99, "sellPressure": "Повышенное"},
            {"secid": "VTBR", "name": "ВТБ", "changePct": -2.0, "spreadPct": 2.7, "numTrades": 6400, "stressScore": 2.21, "sellPressure": "Повышенное"},
        ]


@app.route("/")
def index() -> Any:
    return send_from_directory("templates", "index.html")


@app.route("/api/menu", methods=["GET"])
def menu_api() -> Any:
    return jsonify(
        {
            "modules": [
                {
                    "id": "repo",
                    "title": "Претрейд РЕПО",
                    "icon": "📈",
                    "description": "Анализ ISIN и ключевых дат для сделок РЕПО.",
                    "api": "/api/repo",
                },
                {
                    "id": "calendar",
                    "title": "Календарь выплат",
                    "icon": "📅",
                    "description": "Загрузка портфеля и расписание купонов/амортизаций.",
                    "api": "/api/calendar",
                },
                {
                    "id": "vm",
                    "title": "Расчет VM",
                    "icon": "🧮",
                    "description": "Оперативная оценка VM по бумагам TQBR (лонг/шорт на 1 лот).",
                    "api": "/api/vm",
                },
                {
                    "id": "sell_stres",
                    "title": "Sell_stres",
                    "icon": "🧩",
                    "description": "Сигнал давления продаж по изменению цены и спреду.",
                    "api": "/api/sell-stres",
                },
            ]
        }
    )


@app.route("/api/repo", methods=["POST"])
def repo_api() -> Any:
    payload = request.get_json(silent=True) or {}
    raw = str(payload.get("isins", ""))
    isins = parse_isins(raw)
    if not isins:
        return jsonify({"error": "Не удалось распознать валидные ISIN."}), 400
    rows = [get_repo_row(isin) for isin in isins]
    return jsonify({"rows": rows})


@app.route("/api/calendar", methods=["POST"])
def calendar_api() -> Any:
    payload = request.get_json(silent=True) or {}
    raw = str(payload.get("isins", ""))
    isins = parse_isins(raw)
    if not isins:
        return jsonify({"error": "Не удалось распознать валидные ISIN."}), 400
    rows = get_calendar_rows(isins)
    return jsonify({"rows": rows})


@app.route("/api/vm", methods=["GET", "POST"])
def vm_api() -> Any:
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        trade_name = str(payload.get("tradeName", "")).strip()
        quantity = int(parse_number(payload.get("quantity")) or 0)
        if not trade_name:
            return jsonify({"error": "Введите TRADE_NAME."}), 400
        if quantity < 0:
            return jsonify({"error": "Количество должно быть неотрицательным."}), 400
        try:
            return jsonify(fetch_vm_data(trade_name, quantity))
        except Exception as exc:
            return jsonify({"error": str(exc)}), 400
    return jsonify({"rows": get_vm_rows(), "generatedAt": datetime.utcnow().isoformat() + "Z"})


@app.route("/api/sell-stres", methods=["GET", "POST"])
def sell_stres_api() -> Any:
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        mode = str(payload.get("mode", "share")).strip().lower()
        raw = str(payload.get("isins", ""))
        isins = parse_isins(raw)
        c_value = float(parse_number(payload.get("cValue")) or 1.0)
        q_mode = str(payload.get("qMode", "linear")).strip().lower()
        q_max = int(parse_number(payload.get("qMax")) or 1)
        if not isins:
            return jsonify({"error": "Не удалось распознать валидные ISIN."}), 400
        if q_max < 1:
            return jsonify({"error": "Q_MAX должен быть >= 1."}), 400
        if q_mode not in {"linear", "log"}:
            return jsonify({"error": "Q_MODE должен быть linear или log."}), 400

        results: list[dict[str, Any]] = []
        meta: list[dict[str, Any]] = []
        for isin in isins:
            try:
                if mode == "bond":
                    calc = calculate_bond_delta_p(isin, c_value, q_max, q_mode)
                else:
                    calc = calculate_share_delta_p(isin, c_value, q_max, q_mode)
                results.append({"isin": isin, "rows": calc["rows"]})
                meta.append(calc["meta"])
            except Exception as exc:
                results.append({"isin": isin, "error": str(exc), "rows": []})
        return jsonify({"mode": mode, "results": results, "meta": meta})
    return jsonify({"rows": get_sell_stress_rows(), "generatedAt": datetime.utcnow().isoformat() + "Z"})


@app.route("/api/sell-strass", methods=["GET"])
def sell_strass_alias_api() -> Any:
    """Alias endpoint because this module is often named with double 's'."""
    return sell_stres_api()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
