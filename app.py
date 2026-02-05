
import csv
import math
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import BytesIO, StringIO

import pandas as pd
import requests
import streamlit as st
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------
# Streamlit page setup
# ---------------------------
st.set_page_config(page_title="РЕПО претрейд", page_icon="📈", layout="wide")
st.title("📈 РЕПО претрейд")

# ---------------------------
# Session state defaults
# ---------------------------
if "results" not in st.session_state:
    st.session_state["results"] = None
if "file_loaded" not in st.session_state:
    st.session_state["file_loaded"] = False
if "last_file_name" not in st.session_state:
    st.session_state["last_file_name"] = None
if "active_view" not in st.session_state:
    st.session_state["active_view"] = "home"

# ---------------------------
# Main navigation
# ---------------------------
def trigger_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()


if st.session_state["active_view"] != "home":
    if st.button("⬅️ На главную"):
        st.session_state["active_view"] = "home"
        trigger_rerun()

if st.session_state["active_view"] == "home":
    st.subheader("🏠 Главное меню")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 📈 Претрейд РЕПО")
        st.caption("Анализ ISIN и ключевых дат бумаг для сделок РЕПО.")
        if st.button("Открыть", key="open_repo", use_container_width=True):
            st.session_state["active_view"] = "repo"
            trigger_rerun()
    with col2:
        st.markdown("### 📅 Календарь выплат")
        st.caption("Загрузка портфеля и построение календаря купонов и погашений.")
        if st.button("Открыть", key="open_calendar", use_container_width=True):
            st.session_state["active_view"] = "calendar"
            trigger_rerun()
    st.stop()

# ---------------------------
# HTTP session with retries
# ---------------------------
def build_http_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "python-requests/iss-moex-script"})
    return session


HTTP_SESSION = build_http_session()


def request_get(url: str, timeout: int = 15):
    response = HTTP_SESSION.get(url, timeout=timeout)
    response.raise_for_status()
    return response


def parse_number(value):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        cleaned = str(value).strip().replace(" ", "").replace(",", ".")
        if cleaned == "":
            return None
        return float(cleaned)
    except Exception:
        return None


# ---------------------------
# Safe CSV/Excel reading helpers
# ---------------------------
def safe_read_csv_string(content: str) -> pd.DataFrame:
    content = content.replace("\r\n", "\n").strip()
    sample = content[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
        sep = dialect.delimiter
    except Exception:
        sep = ","
    try:
        df = pd.read_csv(StringIO(content), sep=sep, dtype=str)
        df.columns = [c.strip().upper() for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()


def safe_read_filelike(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name
    try:
        if name.lower().endswith(".csv"):
            raw = uploaded_file.getvalue().decode("utf-8-sig")
            return safe_read_csv_string(raw)
        return pd.read_excel(uploaded_file, dtype=str)
    except Exception:
        return pd.DataFrame()


# ---------------------------
# ISIN validation (format + checksum)
# ---------------------------
def isin_format_valid(isin: str) -> bool:
    if not isin or not isinstance(isin, str):
        return False
    return bool(re.match(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$", isin.strip().upper()))


def isin_checksum_valid(isin: str) -> bool:
    """ISIN checksum (Luhn-like). Returns True for valid ISINs."""
    if not isin_format_valid(isin):
        return False
    s = isin.strip().upper()
    converted = ""
    for ch in s[:-1]:
        if ch.isdigit():
            converted += ch
        else:
            converted += str(ord(ch) - 55)
    digits = converted + s[-1]
    arr = [int(x) for x in digits]
    total = 0
    parity = len(arr) % 2
    for i, d in enumerate(arr):
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


# ---------------------------
# Load TQOB/TQCB board XML caches (for fallback)
# ---------------------------
@st.cache_data(ttl=3600)
def fetch_board_xml(board: str):
    url = (
        "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/"
        f"{board.lower()}/securities.xml?marketprice_board=3&iss.meta=off"
    )
    try:
        r = request_get(url, timeout=20)
        xml_content = r.content.decode("utf-8", errors="ignore")
        xml_content = re.sub(r'\sxmlns="[^"]+"', "", xml_content, count=1)
        root = ET.fromstring(xml_content)
        mapping = {}
        for el in root.iter():
            if el.tag.lower().endswith("row"):
                attrs = {k.upper(): v for k, v in el.attrib.items()}
                isin = attrs.get("ISIN", "").strip().upper()
                secid = attrs.get("SECID", "").strip().upper()
                emitterid = attrs.get("EMITTERID", "").strip()
                if isin:
                    mapping[isin] = {"SECID": secid or None, "EMITTERID": emitterid or None, **attrs}
        return mapping
    except Exception:
        return {}


TQOB_MAP = fetch_board_xml("tqob")
TQCB_MAP = fetch_board_xml("tqcb")

# ---------------------------
# Fetch emitter & secid (with caching)
# ---------------------------
@st.cache_data(ttl=3600)
def fetch_emitter_and_secid(isin: str):
    isin = str(isin).strip().upper()
    if not isin:
        return None, None
    emitter_id = None
    secid = None

    try:
        url = f"https://iss.moex.com/iss/securities/{isin}.json"
        r = request_get(url, timeout=10)
        data = r.json()
        securities = data.get("securities", {})
        cols = securities.get("columns", [])
        rows = securities.get("data", [])
        if rows and cols:
            first = rows[0]
            col_map = {c.upper(): i for i, c in enumerate(cols)}
            if "EMITTER_ID" in col_map:
                emitter_id = first[col_map["EMITTER_ID"]]
            elif "EMITTERID" in col_map:
                emitter_id = first[col_map["EMITTERID"]]
            if "SECID" in col_map:
                secid = first[col_map["SECID"]]
    except Exception:
        pass

    if not secid:
        try:
            url = f"https://iss.moex.com/iss/securities/{isin}.xml?iss.meta=off"
            r = request_get(url, timeout=10)
            xml_content = r.content.decode("utf-8", errors="ignore")
            xml_content = re.sub(r'\sxmlns="[^"]+"', "", xml_content, count=1)
            root = ET.fromstring(xml_content)
            for node in root.iter():
                name_attr = (node.attrib.get("name") or "").upper()
                val_attr = node.attrib.get("value") or ""
                if name_attr == "SECID":
                    secid = val_attr
                elif name_attr in ("EMITTER_ID", "EMITTERID"):
                    emitter_id = val_attr
        except Exception:
            pass

    if not secid:
        mapping = TQOB_MAP.get(isin) or TQCB_MAP.get(isin)
        if mapping:
            secid = mapping.get("SECID")
            if not emitter_id:
                emitter_id = mapping.get("EMITTERID")

    return emitter_id, secid


# ---------------------------
# Core: get bond data per ISIN
# ---------------------------
@st.cache_data(ttl=3600)
def get_bond_data(isin: str):
    isin = str(isin).strip().upper()
    try:
        emitter_id, secid = fetch_emitter_and_secid(isin)
        secname = maturity_date = put_date = call_date = None
        record_date = coupon_date = None
        coupon_currency = None
        coupon_value = None
        coupon_value_rub = None
        coupon_value_prc = None

        if secid:
            try:
                url_info = f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{secid}.json"
                r = request_get(url_info, timeout=10)
                data_info = r.json()
                rows_info = data_info.get("securities", {}).get("data", [])
                cols_info = data_info.get("securities", {}).get("columns", [])
                if rows_info and cols_info:
                    info = dict(zip([c.upper() for c in cols_info], rows_info[0]))
                    secname = info.get("SECNAME") or info.get("SEC_NAME") or secname
                    maturity_date = info.get("MATDATE") or maturity_date
                    put_date = info.get("PUTOPTIONDATE") or put_date
                    call_date = info.get("CALLOPTIONDATE") or call_date
            except Exception:
                pass

        if not secname or not maturity_date:
            try:
                url_info_isin = f"https://iss.moex.com/iss/securities/{isin}.json"
                r = request_get(url_info_isin, timeout=10)
                data_info_isin = r.json()
                rows = data_info_isin.get("securities", {}).get("data", [])
                cols = data_info_isin.get("securities", {}).get("columns", [])
                if rows and cols:
                    info = dict(zip([c.upper() for c in cols], rows[0]))
                    secname = secname or info.get("SECNAME") or info.get("SEC_NAME")
                    maturity_date = maturity_date or info.get("MATDATE") or info.get("MATDATE")
                    put_date = put_date or info.get("PUTOPTIONDATE") or info.get("PUT_OPTION_DATE")
                    call_date = call_date or info.get("CALLOPTIONDATE") or info.get("CALL_OPTION_DATE")
            except Exception:
                pass

        coupons_data = None
        columns_coupons = []
        bondization_faceunit = None

        def try_fetch_bondization(identifier: str):
            try:
                url_coupons = (
                    "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/"
                    f"bondization/{identifier}.json?iss.only=coupons,bondization&iss.meta=off"
                )
                r = request_get(url_coupons, timeout=10)
                data = r.json()
                coupons = data.get("coupons", {}).get("data", [])
                cols = data.get("coupons", {}).get("columns", [])
                bond_rows = data.get("bondization", {}).get("data", [])
                bond_cols = data.get("bondization", {}).get("columns", [])
                faceunit = None
                if bond_rows and bond_cols:
                    bond_info = dict(zip([c.upper() for c in bond_cols], bond_rows[0]))
                    faceunit = bond_info.get("FACEUNIT") or bond_info.get("FACEUNIT_S")
                return coupons, cols, faceunit
            except Exception:
                return None, [], None

        if secid:
            coupons_data, columns_coupons, bondization_faceunit = try_fetch_bondization(secid)

        if isin and (not coupons_data or not columns_coupons):
            coupons_data_fallback, columns_coupons_fallback, bondization_faceunit_fallback = try_fetch_bondization(isin)
            if coupons_data_fallback and columns_coupons_fallback:
                coupons_data = coupons_data_fallback
                columns_coupons = columns_coupons_fallback
            if not bondization_faceunit:
                bondization_faceunit = bondization_faceunit_fallback or bondization_faceunit

        if coupons_data and columns_coupons:
            df_coupons = pd.DataFrame(coupons_data, columns=columns_coupons)
            cols_upper = [c.upper() for c in df_coupons.columns]
            df_coupons.columns = cols_upper
            today = pd.to_datetime(datetime.today().date())
            possible_coupon_date_cols = [c for c in cols_upper if "COUPON" in c and "DATE" in c]
            possible_record_date_cols = [c for c in cols_upper if "RECORD" in c and "DATE" in c]

            def next_future_date(series):
                try:
                    s = pd.to_datetime(series, errors="coerce")
                    s = s[s >= today]
                    if not s.empty:
                        return s.min().strftime("%Y-%m-%d")
                except Exception:
                    pass
                return None

            coupon_found = None
            for col in possible_coupon_date_cols:
                candidate = next_future_date(df_coupons[col])
                if candidate:
                    coupon_found = candidate
                    break
            if not coupon_found:
                all_dates = []
                for col in df_coupons.columns:
                    try:
                        s = pd.to_datetime(df_coupons[col], errors="coerce")
                        s = s[s >= today]
                        if not s.empty:
                            all_dates.append(s.min())
                    except Exception:
                        pass
                if all_dates:
                    coupon_found = min(all_dates).strftime("%Y-%m-%d")
            coupon_date = coupon_found

            record_found = None
            for col in possible_record_date_cols:
                candidate = next_future_date(df_coupons[col])
                if candidate:
                    record_found = candidate
                    break
            record_date = record_found

            if bondization_faceunit:
                coupon_currency = bondization_faceunit
            else:
                faceunit_cols = [c for c in df_coupons.columns if "FACEUNIT" in c or c == "FACEUNIT_S"]
                if faceunit_cols:
                    for c in faceunit_cols:
                        vals = df_coupons[c].dropna().astype(str)
                        if not vals.empty and vals.iloc[0].strip():
                            coupon_currency = vals.iloc[0].strip()
                            break

            val_col = None
            val_rub_col = None
            val_prc_col = None
            for c in df_coupons.columns:
                uc = c.upper()
                if uc in ("VALUE", "VALUE_COUPON", "COUPONVALUE") and not val_col:
                    val_col = c
                if "VALUE_RUB" in uc and not val_rub_col:
                    val_rub_col = c
                if uc in ("VALUEPRC", "VALUE_PRC", "VALUE%") and not val_prc_col:
                    val_prc_col = c
            if not val_col:
                for c in df_coupons.columns:
                    if re.match(r"^VALUE$", c, flags=re.IGNORECASE):
                        val_col = c
                        break
            if not val_rub_col:
                for c in df_coupons.columns:
                    if re.search(r"RUB", c, flags=re.IGNORECASE):
                        if "VALUE" in c.upper() or "RUB" in c.upper():
                            val_rub_col = c
                            break
            if not val_prc_col:
                for c in df_coupons.columns:
                    if re.search(r"PRC|PERC|%|PERCENT", c, flags=re.IGNORECASE):
                        val_prc_col = c
                        break

            chosen_row = None
            if coupon_date:
                for c in possible_coupon_date_cols:
                    try:
                        mask = pd.to_datetime(df_coupons[c], errors="coerce").dt.strftime("%Y-%m-%d") == coupon_date
                        rows = df_coupons[mask]
                        if not rows.empty:
                            chosen_row = rows.iloc[0]
                            break
                    except Exception:
                        pass
            if chosen_row is None:
                for idx in range(len(df_coupons)):
                    row = df_coupons.iloc[idx]
                    has_val = False
                    for colcheck in (val_col, val_rub_col, val_prc_col):
                        try:
                            if colcheck and pd.notnull(row.get(colcheck)):
                                has_val = True
                                break
                        except Exception:
                            pass
                    if has_val:
                        chosen_row = row
                        break

            def norm_str(v):
                if v is None or (isinstance(v, float) and math.isnan(v)):
                    return None
                try:
                    return str(v)
                except Exception:
                    return None

            if chosen_row is not None:
                coupon_value = norm_str(chosen_row.get(val_col)) if val_col else None
                coupon_value_rub = norm_str(chosen_row.get(val_rub_col)) if val_rub_col else None
                coupon_value_prc = norm_str(chosen_row.get(val_prc_col)) if val_prc_col else None

        if (not record_date or not coupon_date or not coupon_currency) and (TQOB_MAP or TQCB_MAP):
            mapping = TQOB_MAP.get(isin) or TQCB_MAP.get(isin)
            if mapping:
                if not record_date:
                    record_date = mapping.get("RECORDDATE") or mapping.get("RECORD_DATE") or mapping.get("RECORD")
                if not coupon_date:
                    coupon_date = mapping.get("COUPONDATE") or mapping.get("COUPON_DATE") or mapping.get("COUPON")
                if not coupon_currency:
                    coupon_currency = mapping.get("FACEUNIT") or mapping.get("FACEUNIT_S") or mapping.get("FACEUNIT")

        def fmt(date):
            if pd.isna(date) or not date:
                return None
            try:
                return pd.to_datetime(date).strftime("%Y-%m-%d")
            except Exception:
                return None

        return {
            "ISIN": isin,
            "Код эмитента": emitter_id or "",
            "Наименование инструмента": secname or "",
            "Дата погашения": fmt(maturity_date),
            "Дата оферты Put": fmt(put_date),
            "Дата оферты Call": fmt(call_date),
            "Дата фиксации купона": fmt(record_date),
            "Дата купона": fmt(coupon_date),
            "Валюта купона": coupon_currency or "",
            "Купон в валюте": coupon_value or "",
            "Купон в Руб": coupon_value_rub or "",
            "Купон %": coupon_value_prc or "",
        }
    except Exception:
        return {
            "ISIN": isin,
            "Код эмитента": "",
            "Наименование инструмента": "",
            "Дата погашения": None,
            "Дата оферты Put": None,
            "Дата оферты Call": None,
            "Дата фиксации купона": None,
            "Дата купона": None,
            "Валюта купона": "",
            "Купон в валюте": "",
            "Купон в Руб": "",
            "Купон %": "",
        }


# ---------------------------
# Calendar helpers
# ---------------------------
@st.cache_data(ttl=3600)
def get_bond_schedule(isin: str):
    isin = str(isin).strip().upper()
    emitter_id, secid = fetch_emitter_and_secid(isin)
    maturity_date = put_date = call_date = None
    coupon_events = {}
    facevalue = None

    if secid:
        try:
            url_info = f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{secid}.json"
            r = request_get(url_info, timeout=10)
            data_info = r.json()
            rows_info = data_info.get("securities", {}).get("data", [])
            cols_info = data_info.get("securities", {}).get("columns", [])
            if rows_info and cols_info:
                info = dict(zip([c.upper() for c in cols_info], rows_info[0]))
                maturity_date = info.get("MATDATE") or maturity_date
                put_date = info.get("PUTOPTIONDATE") or put_date
                call_date = info.get("CALLOPTIONDATE") or call_date
        except Exception:
            pass

    if not maturity_date:
        try:
            url_info_isin = f"https://iss.moex.com/iss/securities/{isin}.json"
            r = request_get(url_info_isin, timeout=10)
            data_info_isin = r.json()
            rows = data_info_isin.get("securities", {}).get("data", [])
            cols = data_info_isin.get("securities", {}).get("columns", [])
            if rows and cols:
                info = dict(zip([c.upper() for c in cols], rows[0]))
                maturity_date = maturity_date or info.get("MATDATE")
                put_date = put_date or info.get("PUTOPTIONDATE") or info.get("PUT_OPTION_DATE")
                call_date = call_date or info.get("CALLOPTIONDATE") or info.get("CALL_OPTION_DATE")
        except Exception:
            pass

    def try_fetch_bondization(identifier: str):
        try:
            url_coupons = (
                "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/"
                f"bondization/{identifier}.json?iss.only=coupons,bondization&iss.meta=off"
            )
            r = request_get(url_coupons, timeout=10)
            data = r.json()
            coupons = data.get("coupons", {}).get("data", [])
            cols = data.get("coupons", {}).get("columns", [])
            bond_rows = data.get("bondization", {}).get("data", [])
            bond_cols = data.get("bondization", {}).get("columns", [])
            bond_info = None
            if bond_rows and bond_cols:
                bond_info = dict(zip([c.upper() for c in bond_cols], bond_rows[0]))
            return coupons, cols, bond_info
        except Exception:
            return None, [], None

    coupons_data = columns_coupons = None
    bond_info = None
    if secid:
        coupons_data, columns_coupons, bond_info = try_fetch_bondization(secid)

    if isin and (not coupons_data or not columns_coupons):
        coupons_data_fallback, columns_coupons_fallback, bond_info_fallback = try_fetch_bondization(isin)
        if coupons_data_fallback and columns_coupons_fallback:
            coupons_data = coupons_data_fallback
            columns_coupons = columns_coupons_fallback
        if not bond_info:
            bond_info = bond_info_fallback or bond_info

    if bond_info:
        for key, value in bond_info.items():
            if key.startswith("FACEVALUE") and value not in (None, "", "None"):
                facevalue = parse_number(value)
                if facevalue is not None:
                    break

    if coupons_data and columns_coupons:
        df_coupons = pd.DataFrame(coupons_data, columns=columns_coupons)
        df_coupons.columns = [c.upper() for c in df_coupons.columns]
        date_cols = [c for c in df_coupons.columns if "COUPON" in c and "DATE" in c]
        if not date_cols:
            date_cols = [c for c in df_coupons.columns if c.endswith("DATE")]

        val_rub_col = next((c for c in df_coupons.columns if "VALUE_RUB" in c), None)
        val_col = next(
            (c for c in df_coupons.columns if c in ("VALUE", "VALUE_COUPON", "COUPONVALUE")),
            None,
        )
        for _, row in df_coupons.iterrows():
            coupon_date = None
            for col in date_cols:
                dt = pd.to_datetime(row.get(col), errors="coerce")
                if pd.notna(dt):
                    coupon_date = dt.strftime("%Y-%m-%d")
                    break
            if not coupon_date:
                continue
            raw_value = row.get(val_rub_col) if val_rub_col else row.get(val_col)
            coupon_value = parse_number(raw_value)
            if coupon_value is None:
                continue
            coupon_events[coupon_date] = coupon_value

    def fmt(date):
        if pd.isna(date) or not date:
            return None
        try:
            return pd.to_datetime(date).strftime("%Y-%m-%d")
        except Exception:
            return None

    return {
        "ISIN": isin,
        "Код эмитента": emitter_id or "",
        "Дата погашения": fmt(maturity_date),
        "Дата оферты Put": fmt(put_date),
        "Дата оферты Call": fmt(call_date),
        "Купоны": coupon_events,
        "Номинал": facevalue,
    }


# ---------------------------
# Parallel fetch with safe progress updates
# ---------------------------
def fetch_isins_parallel(isins, max_workers=10, show_progress=True):
    results = []
    total = len(isins)
    if total == 0:
        return results

    progress_bar = None
    progress_text = None
    if show_progress:
        try:
            progress_bar = st.progress(0)
            progress_text = st.empty()
        except Exception:
            progress_bar = None
            progress_text = None

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_isin = {executor.submit(get_bond_data, isin): isin for isin in isins}
        for future in as_completed(future_to_isin):
            isin = future_to_isin[future]
            try:
                data = future.result()
            except Exception:
                data = {
                    "ISIN": isin,
                    "Код эмитента": "",
                    "Наименование инструмента": "",
                    "Дата погашения": None,
                    "Дата оферты Put": None,
                    "Дата оферты Call": None,
                    "Дата фиксации купона": None,
                    "Дата купона": None,
                    "Валюта купона": "",
                    "Купон в валюте": "",
                    "Купон в Руб": "",
                    "Купон %": "",
                }
            results.append(data)
            completed += 1
            if progress_bar:
                try:
                    progress_bar.progress(completed / total)
                except Exception:
                    pass
            if progress_text:
                try:
                    progress_text.text(f"Обработано {completed}/{total} ISIN")
                except Exception:
                    pass
    try:
        time.sleep(0.12)
    except Exception:
        pass
    return results


# ---------------------------
# Calendar view
# ---------------------------
if st.session_state["active_view"] == "calendar":
    st.subheader("📅 Календарь выплат")
    st.markdown(
        "Введите список бумаг и их количество, чтобы построить календарь выплат "
        "(купоны, погашения, оферты) по вашему портфелю."
    )
    st.markdown("**Ручной ввод:** `ISIN | Amount` (количество бумаг).")
    calendar_input = st.text_area(
        "Введите список бумаг",
        height=160,
        placeholder="RU000A0JX0J2 | 100\nRU000A0ZZZY1 | 50",
        key="calendar_manual_input",
    )
    if st.button("Построить календарь", key="build_calendar"):
        raw_lines = [line.strip() for line in calendar_input.splitlines() if line.strip()]
        entries = []
        invalid_isins = []
        for line in raw_lines:
            parts = [p.strip() for p in re.split(r"[,.;|/\t]+", line) if p.strip()]
            if not parts:
                continue
            isin = parts[0].upper()
            amount = parse_number(parts[1]) if len(parts) > 1 else 1.0
            if amount is None or amount <= 0:
                amount = 1.0
            if not isin_format_valid(isin) or not isin_checksum_valid(isin):
                invalid_isins.append(isin)
                continue
            entries.append({"ISIN": isin, "Amount": amount})

        if invalid_isins:
            st.warning(
                "Некорректные ISIN пропущены: "
                f"{', '.join(invalid_isins[:10])}{'...' if len(invalid_isins) > 10 else ''}"
            )
        if not entries:
            st.error("Нет валидных ISIN для построения календаря.")
        else:
            max_workers = st.sidebar.slider("Параллельных потоков (workers)", 2, 40, 10, key="calendar_workers")
            timeline_data = {}
            all_dates = set()
            with st.spinner("Загружаем расписание выплат..."):
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_entry = {
                        executor.submit(get_bond_schedule, entry["ISIN"]): entry for entry in entries
                    }
                    for future in as_completed(future_to_entry):
                        entry = future_to_entry[future]
                        isin = entry["ISIN"]
                        amount = entry["Amount"]
                        try:
                            schedule = future.result()
                        except Exception:
                            schedule = {}
                        row = {}
                        today = datetime.today().date()
                        for date, value in schedule.get("Купоны", {}).items():
                            scaled = value * amount if value is not None else None
                            if scaled is None:
                                continue
                            try:
                                coupon_date = pd.to_datetime(date).date()
                            except Exception:
                                continue
                            if coupon_date >= today:
                                row[date] = row.get(date, 0) + scaled
                                all_dates.add(date)
                        facevalue = schedule.get("Номинал")
                        event_date = schedule.get("Дата погашения")
                        if event_date and facevalue is not None:
                            try:
                                maturity_date = pd.to_datetime(event_date).date()
                            except Exception:
                                maturity_date = None
                            if maturity_date and maturity_date >= today:
                                row[event_date] = row.get(event_date, 0) + facevalue * amount
                                all_dates.add(event_date)
                        timeline_data[isin] = row

            sorted_dates = sorted(all_dates)
            df_timeline = pd.DataFrame(index=[e["ISIN"] for e in entries], columns=sorted_dates, dtype=float)
            for isin, row in timeline_data.items():
                for date, value in row.items():
                    df_timeline.loc[isin, date] = value
            st.dataframe(df_timeline, use_container_width=True)
    st.stop()

# ---------------------------
# REPO duration settings
# ---------------------------
st.subheader("⚙️ Настройки длительности РЕПО")
if "overnight" not in st.session_state:
    st.session_state["overnight"] = False
if "extra_days" not in st.session_state:
    st.session_state["extra_days"] = 2

if st.button("🔄 Очистить форму"):
    st.session_state["overnight"] = False
    st.session_state["extra_days"] = 2
    st.session_state["results"] = None
    st.session_state["file_loaded"] = False
    st.session_state["last_file_name"] = None
    trigger_rerun()

overnight = st.checkbox("Overnight РЕПО", key="overnight")
extra_days_input = st.number_input(
    "Дней РЕПО:",
    min_value=2,
    max_value=366,
    step=1,
    disabled=st.session_state["overnight"],
    key="extra_days",
)
if st.session_state["overnight"]:
    st.markdown(
        "<span style='color:gray'>Дополнительные дни отключены при включенном Overnight</span>",
        unsafe_allow_html=True,
    )
days_threshold = 2 if st.session_state["overnight"] else 1 + st.session_state["extra_days"]
st.write(f"Текущее значение границы выплат: {days_threshold} дн.")

# ---------------------------
# UI: input tabs
# ---------------------------
st.subheader("📤 Загрузка или ввод ISIN")
tab1, tab2 = st.tabs(["📁 Загрузить файл", "✍️ Ввести вручную"])

with tab1:
    uploaded_file = st.file_uploader("Загрузите Excel или CSV с колонкой ISIN", type=["xlsx", "xls", "csv"])
    st.write("Пример шаблона (скачайте и заполните колонку ISIN):")
    sample_csv = "ISIN\nRU000A0JX0J2\nRU000A0ZZZY1\n"
    st.download_button("Скачать шаблон CSV", data=sample_csv, file_name="template_isin.csv", mime="text/csv")

with tab2:
    isin_input = st.text_area("Введите или вставьте ISIN (через Ctrl+V, пробел или запятую)", height=150)
    if st.button("🔍 Получить данные по введённым ISIN"):
        raw_text = isin_input.strip()
        if raw_text:
            isins = re.split(r"[\s,;]+", raw_text)
            isins = [i.strip().upper() for i in isins if i.strip()]
            invalid_format = [i for i in isins if not isin_format_valid(i)]
            invalid_checksum = [i for i in isins if isin_format_valid(i) and not isin_checksum_valid(i)]
            if invalid_format:
                st.warning(
                    f"Некорректные по формату ISIN будут пропущены: {', '.join(invalid_format[:10])}"
                    f"{'...' if len(invalid_format) > 10 else ''}"
                )
            if invalid_checksum:
                st.info(
                    f"ISIN с неверной контрольной суммой (будут пропущены): {', '.join(invalid_checksum[:10])}"
                    f"{'...' if len(invalid_checksum) > 10 else ''}"
                )
            isins = [i for i in isins if isin_format_valid(i) and isin_checksum_valid(i)]
            if not isins:
                st.error("Нет валидных ISIN для обработки.")
            else:
                max_workers = st.sidebar.slider("Параллельных потоков (workers)", 2, 40, 10)
                with st.spinner("Запрос данных..."):
                    results = fetch_isins_parallel(isins, max_workers=max_workers, show_progress=True)
                st.session_state["results"] = pd.DataFrame(results)
                st.success("✅ Данные успешно получены!")

# ---------------------------
# File upload handling
# ---------------------------
if uploaded_file:
    if not st.session_state["file_loaded"] or uploaded_file.name != st.session_state["last_file_name"]:
        st.session_state["file_loaded"] = True
        st.session_state["last_file_name"] = uploaded_file.name

        df = safe_read_filelike(uploaded_file)
        if df.empty:
            st.error("❌ Не удалось прочитать файл или файл пуст.")
            st.stop()
        df.columns = [c.strip().upper() for c in df.columns]

        if "ISIN" not in df.columns:
            candidates = []
            for c in df.columns:
                try:
                    if df[c].dropna().astype(str).str.match(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$").any():
                        candidates.append(c)
                except Exception:
                    continue
            if len(candidates) == 1:
                df.rename(columns={candidates[0]: "ISIN"}, inplace=True)
                st.info(f"Авто-детект: колонка '{candidates[0]}' использована как ISIN")
            else:
                st.error("❌ В файле должна быть колонка 'ISIN' или одна колонка с ISIN-подобными значениями.")
                st.stop()

        isins = df["ISIN"].dropna().unique().tolist()
        isins = [str(x).strip().upper() for x in isins if str(x).strip()]
        invalid_fmt = [i for i in isins if not isin_format_valid(i)]
        invalid_chk = [i for i in isins if isin_format_valid(i) and not isin_checksum_valid(i)]
        if invalid_fmt:
            st.warning(
                f"Некорректные по формату ISIN пропущены: {', '.join(invalid_fmt[:10])}"
                f"{'...' if len(invalid_fmt) > 10 else ''}"
            )
        if invalid_chk:
            st.info(
                f"ISIN с неверной контрольной суммой пропущены: {', '.join(invalid_chk[:10])}"
                f"{'...' if len(invalid_chk) > 10 else ''}"
            )
        isins = [i for i in isins if isin_format_valid(i) and isin_checksum_valid(i)]

        st.write(f"Найдено {len(isins)} валидных уникальных ISIN для обработки.")
        if isins:
            max_workers = st.sidebar.slider("Параллельных потоков (workers)", 2, 40, 10)
            with st.spinner("Запрос данных по файлу..."):
                results = fetch_isins_parallel(isins, max_workers=max_workers, show_progress=True)
            st.session_state["results"] = pd.DataFrame(results)
            st.success("✅ Данные успешно получены из файла!")

# ---------------------------
# Load emitter reference (optional)
# ---------------------------
@st.cache_data(ttl=3600)
def fetch_emitter_names():
    url = "https://raw.githubusercontent.com/mainarkler/Bond_date/refs/heads/main/Pifagr_name_with_emitter.csv"
    try:
        df_emitters = pd.read_csv(url, dtype=str)
        df_emitters.columns = [c.strip() for c in df_emitters.columns]
        return df_emitters
    except Exception:
        return pd.DataFrame(columns=["Issuer", "EMITTER_ID"])


df_emitters = fetch_emitter_names()

# ---------------------------
# Styling helper
# ---------------------------
def style_df(row):
    if pd.isna(row.get("Наименование инструмента")) or row.get("Наименование инструмента") in [None, "", "None"]:
        return ["background-color: DimGray; color: white"] * len(row)
    today = datetime.today().date()
    danger_threshold = today + timedelta(days=days_threshold)
    key_dates = ["Дата погашения", "Дата оферты Put", "Дата оферты Call", "Дата фиксации купона", "Дата купона"]
    colors = ["" for _ in row]
    for i, col in enumerate(row.index):
        if col in key_dates and pd.notnull(row[col]):
            try:
                d = pd.to_datetime(row[col]).date()
                if d <= danger_threshold:
                    colors[i] = "background-color: Chocolate"
            except Exception:
                pass
    if any(c == "background-color: Chocolate" for c in colors):
        colors = ["background-color: SandyBrown" if c == "" else c for c in colors]
    return colors

# ---------------------------
# Show results (table + export) with filter for orange-highlighted rows
# ---------------------------
if st.session_state["results"] is not None:
    df_res = st.session_state["results"].copy()

    if "Код эмитента" in df_res.columns and not df_emitters.empty:
        try:
            df_res = df_res.merge(df_emitters, how="left", left_on="Код эмитента", right_on="EMITTER_ID")
            df_res["Эмитент"] = df_res.get("Issuer")
            df_res.drop(columns=["Issuer", "EMITTER_ID"], inplace=True, errors="ignore")
            cols = df_res.columns.tolist()
            if "Эмитент" in cols and "Код эмитента" in cols:
                cols.remove("Эмитент")
                idx = cols.index("Код эмитента")
                cols.insert(idx + 1, "Эмитент")
                df_res = df_res[cols]
            st.session_state["results"] = df_res
        except Exception:
            pass
    else:
        st.warning("⚠️ В данных нет колонки 'Код эмитента' — объединение со справочником пропущено.")

    st.markdown(f"**Всего записей:** {len(df_res)}")

    today = datetime.today().date()
    danger_threshold = today + timedelta(days=days_threshold)
    key_dates = ["Дата погашения", "Дата оферты Put", "Дата оферты Call", "Дата фиксации купона", "Дата купона"]

    mask_any = pd.Series(False, index=df_res.index)
    for col in key_dates:
        if col in df_res.columns:
            try:
                s = pd.to_datetime(df_res[col], errors="coerce").dt.date
                mask_any = mask_any | (s <= danger_threshold)
            except Exception:
                pass

    only_orange = st.checkbox("Показать бумаги с отсечкой в периоде", value=False)
    if only_orange:
        df_show = df_res[mask_any].copy()
        st.markdown(f"**Показано записей с отсечкой:** {len(df_show)}")
        if df_show.empty:
            st.info("Нет бумаг, попадающих под критерий (отсечки).")
    else:
        df_show = df_res

    st.dataframe(df_show.style.apply(style_df, axis=1), use_container_width=True)

    def to_excel_bytes(df: pd.DataFrame):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Данные")
        return output.getvalue()

    def to_csv_bytes(df: pd.DataFrame):
        return df.to_csv(index=False).encode("utf-8-sig")

    st.download_button(
        label="💾 Скачать результат (Excel)",
        data=to_excel_bytes(df_show),
        file_name="bond_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.download_button(
        label="💾 Скачать результат (CSV)",
        data=to_csv_bytes(df_show),
        file_name="bond_data.csv",
        mime="text/csv",
    )
else:
    st.info("👆 Загрузите файл или введите ISIN-ы вручную.")
