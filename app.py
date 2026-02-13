from __future__ import annotations

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
                    "description": "Расчет вариационной маржи для инструментов FORTS.",
                    "api": "/api/vm",
                },
                {
                    "id": "sell_stres",
                    "title": "Sell_stres",
                    "icon": "🧩",
                    "description": "Оценка рыночного давления для акций и облигаций.",
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
    return jsonify(
        {
            "message": "Демо API для VM. Подключите расчет из основной бизнес-логики.",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
    )


@app.route("/api/sell-stres", methods=["GET"])
def sell_stres_api() -> Any:
    return jsonify(
        {
            "message": "Демо API для Sell_stres. Подключите расчет из основной бизнес-логики.",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
