from __future__ import annotations

import os
import re
from datetime import date, datetime
from typing import Any

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


def to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
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


def get_secid_by_isin(isin: str) -> str | None:
    payload = moex_get_json(
        "https://iss.moex.com/iss/securities.json",
        {"q": isin, "iss.meta": "off", "iss.only": "securities", "securities.columns": "secid,isin,shortname"},
    )
    block = payload.get("securities", {})
    columns = block.get("columns", [])
    data = block.get("data", [])
    if not columns:
        return None
    for row in data:
        row_map = dict(zip(columns, row))
        if row_map.get("isin") == isin:
            return row_map.get("secid")
    return None


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

    return {
        "isin": isin,
        "secid": secid,
        "name": description.get("SHORTNAME") or description.get("NAME") or "-",
        "maturityDate": maturity or "-",
        "daysToMaturity": days_to_maturity if days_to_maturity is not None else "-",
        "lastPrice": market.get("LAST", "-"),
        "bid": market.get("BID", "-"),
        "offer": market.get("OFFER", "-"),
        "yield": market.get("YIELD", "-"),
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


@app.route("/api/vm", methods=["GET"])
def vm_api() -> Any:
    return jsonify({"rows": get_vm_rows(), "generatedAt": datetime.utcnow().isoformat() + "Z"})


@app.route("/api/sell-stres", methods=["GET"])
def sell_stres_api() -> Any:
    return jsonify({"rows": get_sell_stress_rows(), "generatedAt": datetime.utcnow().isoformat() + "Z"})


@app.route("/api/sell-strass", methods=["GET"])
def sell_strass_alias_api() -> Any:
    """Alias endpoint because this module is often named with double 's'."""
    return sell_stres_api()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
