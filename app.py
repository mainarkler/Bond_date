import streamlit as st
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from io import BytesIO, StringIO
import os
import csv
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

# === Настройки страницы ===
st.set_page_config(page_title="РЕПО претрейд", page_icon="📈", layout="wide")
st.title("📈 РЕПО претрейд — улучшенная версия")

# === Session state ===
if "results" not in st.session_state:
    st.session_state["results"] = None
if "file_loaded" not in st.session_state:
    st.session_state["file_loaded"] = False
if "last_file_name" not in st.session_state:
    st.session_state["last_file_name"] = None

# === Настройки длительности РЕПО ===
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
    st.rerun()

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
    st.markdown("<span style='color:gray'>Дополнительные дни отключены при включенном Overnight</span>", unsafe_allow_html=True)
days_threshold = 2 if st.session_state["overnight"] else 1 + st.session_state["extra_days"]
st.write(f"Текущее значение границы выплат: {days_threshold} дн.")

# === Connection: session with retries ===
session = requests.Session()
# sensible retry strategy to handle 429/5xx transient errors
retries = Retry(total=5, backoff_factor=0.8, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "POST"])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)
session.headers.update({"User-Agent": "python-requests/iss-moex-script"})

def request_get(url, timeout=15):
    """Wrapper to centralize GET requests and handle exceptions uniformly."""
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        # bubble up the exception to caller to handle logging/status
        raise

# === Безопасное чтение CSV/Excel из строки/BytesIO ===
def safe_read_csv_string(content: str):
    content = content.replace('\r\n', '\n').strip()
    sample = content[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
        sep = dialect.delimiter
    except Exception:
        sep = ","
    try:
        df = pd.read_csv(StringIO(content), sep=sep, dtype=str)
        df.columns = [c.strip().upper() for c in df.columns]
        return df
    except Exception as e:
        st.warning(f"⚠️ Ошибка при парсинге CSV: {e}")
        return pd.DataFrame()

def safe_read_filelike(uploaded_file):
    name = uploaded_file.name
    try:
        if name.lower().endswith(".csv"):
            raw = uploaded_file.getvalue().decode("utf-8-sig")
            return safe_read_csv_string(raw)
        else:
            # Excel or other supported by pandas
            return pd.read_excel(uploaded_file, dtype=str)
    except Exception as e:
        st.warning(f"⚠️ Ошибка при чтении загруженного файла {name}: {e}")
        return pd.DataFrame()

# === Кэширование XML TQOB и TQCB ===
@st.cache_data(ttl=3600)
def fetch_board_xml(board: str):
    url = f"https://iss.moex.com/iss/engines/stock/markets/bonds/boards/{board.lower()}/securities.xml?marketprice_board=3&iss.meta=off"
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
    except Exception as e:
        st.warning(f"⚠️ Не удалось загрузить {board}: {e}")
        return {}

TQOB_MAP = fetch_board_xml("tqob")
TQCB_MAP = fetch_board_xml("tqcb")

# === Функция поиска эмитента и SECID ===
@st.cache_data(ttl=3600)
def fetch_emitter_and_secid(isin: str):
    isin = str(isin).strip().upper()
    if not isin:
        return None, None
    emitter_id = None
    secid = None

    # 1) JSON по ISIN (часто отрабатывает)
    try:
        url = f"https://iss.moex.com/iss/securities/{isin}.json"
        r = request_get(url, timeout=10)
        data = r.json()
        securities = data.get("securities", {})
        cols = securities.get("columns", [])
        rows = securities.get("data", [])
        if rows:
            first = rows[0]
            col_map = {c.upper(): i for i, c in enumerate(cols)}
            if "EMITTER_ID" in col_map:
                emitter_id = first[col_map.get("EMITTER_ID")]
            elif "EMITTERID" in col_map:
                emitter_id = first[col_map.get("EMITTERID")]
            if "SECID" in col_map:
                secid = first[col_map.get("SECID")]
    except Exception:
        pass

    # 2) XML по ISIN (fallback)
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
                elif name_attr == "EMITTER_ID" or name_attr == "EMITTERID":
                    emitter_id = val_attr
        except Exception:
            pass

    # 3) XML-борды (TQOB/TQCB) — уже загруженные мапы
    if not secid:
        m = TQOB_MAP.get(isin) or TQCB_MAP.get(isin)
        if m:
            secid = m.get("SECID")
            if not emitter_id:
                emitter_id = m.get("EMITTERID")

    return emitter_id, secid

# === Функция получения данных по ISIN ===
@st.cache_data(ttl=3600)
def get_bond_data(isin):
    isin = str(isin).strip().upper()
    try:
        emitter_id, secid = fetch_emitter_and_secid(isin)
        secname = maturity_date = put_date = call_date = None
        record_date = coupon_date = None
        coupon_currency = None
        coupon_value = None
        coupon_value_rub = None
        coupon_value_prc = None

        # --- Попытка получить информацию по SECID (если есть) ---
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

        # --- Если SECID нет или данные неполные: пытаемся получить инфо по ISIN (fallback) ---
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

        # --- Купоны: ищем ближайший будущий COUPONDATE и RECORDDATE + валюту и значения купона ---
        coupons_data = None
        columns_coupons = []
        bondization_currency = None

        def try_fetch_bondization(identifier):
            try:
                url_coupons = (
                    "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/"
                    f"bondization/{identifier}.json?iss.only=coupons,bondization&iss.meta=off"
                )
                r = request_get(url_coupons, timeout=10)
                data = r.json()
                coupons = data.get("coupons", {}).get("data", [])
                cols = data.get("coupons", {}).get("columns", [])
                bondization_rows = data.get("bondization", {}).get("data", [])
                bondization_cols = data.get("bondization", {}).get("columns", [])
                faceunit = None
                if bondization_rows and bondization_cols:
                    bondization_info = dict(zip([c.upper() for c in bondization_cols], bondization_rows[0]))
                    faceunit = bondization_info.get("FACEUNIT") or bondization_info.get("FACEUNIT_S")
                return coupons, cols, faceunit
            except Exception:
                return None, [], None

        if secid:
            coupons_data, columns_coupons, bondization_currency = try_fetch_bondization(secid)

        if isin:
            coupons_data_fallback = columns_coupons_fallback = None
            bondization_currency_fallback = None
            if not coupons_data or not columns_coupons:
                coupons_data_fallback, columns_coupons_fallback, bondization_currency_fallback = try_fetch_bondization(isin)
                if coupons_data_fallback and columns_coupons_fallback:
                    coupons_data = coupons_data_fallback
                    columns_coupons = columns_coupons_fallback
            if not bondization_currency:
                if bondization_currency_fallback is None:
                    _, _, bondization_currency_fallback = try_fetch_bondization(isin)
                bondization_currency = bondization_currency_fallback or bondization_currency

        # Если получили купоны — найдём ближайшие будущие даты (robust to different column names)
        if coupons_data and columns_coupons:
            df_coupons = pd.DataFrame(coupons_data, columns=columns_coupons)
            cols_upper = [c.upper() for c in df_coupons.columns]
            df_coupons.columns = cols_upper

            today = pd.to_datetime(datetime.today().date())

            possible_coupon_cols = [c for c in cols_upper if "COUPON" in c and "DATE" in c]
            possible_record_cols = [c for c in cols_upper if "RECORD" in c and "DATE" in c]

            def next_future_date(series):
                try:
                    s = pd.to_datetime(series, errors="coerce")
                    s = s[s >= today + pd.Timedelta(days=0)]
                    if not s.empty:
                        nxt = s.min()
                        return nxt.strftime("%Y-%m-%d")
                except Exception:
                    pass
                return None

            coupon_found = None
            for col in possible_coupon_cols:
                candidate = next_future_date(df_coupons[col])
                if candidate:
                    coupon_found = candidate
                    break

            record_found = None
            for col in possible_record_cols:
                candidate = next_future_date(df_coupons[col])
                if candidate:
                    record_found = candidate
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
            record_date = record_found

            # --- Попытка извлечь валюту купона и значения купона из таблицы coupons ---
            # faceunit может быть в bondization_currency или в колонке FACEUNIT в coupons
            if not bondization_currency:
                # попробуем найти колонку FACEUNIT (в разных вариантах имен)
                faceunit_cols = [c for c in df_coupons.columns if "FACEUNIT" in c or c == "FACEUNIT_S"]
                if faceunit_cols:
                    # берем первый ненулевой
                    for c in faceunit_cols:
                        vals = df_coupons[c].dropna().astype(str)
                        if not vals.empty and vals.iloc[0].strip():
                            bondization_currency = vals.iloc[0].strip()
                            break

            coupon_currency = bondization_currency or coupon_currency

            # значения купонов: ищем VALUE / VALUE_RUB / VALUEPRC (или похожие)
            val_col = None
            val_rub_col = None
            val_prc_col = None
            for c in df_coupons.columns:
                uc = c.upper()
                if uc in ("VALUE", "COUPONVALUE", "VALUE_COUPON") and not val_col:
                    val_col = c
                if uc in ("VALUE_RUB", "VALUE_RUBS", "VALUE_RUB_L", "VALUE_RUBS") and not val_rub_col:
                    val_rub_col = c
                if uc in ("VALUEPRC", "VALUE_PRC", "VALUEPRC_") and not val_prc_col:
                    val_prc_col = c
            # fallback: try to find numeric columns named value / value_rub / valueprc by regex
            import math
            if not val_col:
                for c in df_coupons.columns:
                    if re.match(r'VALUE($|_)', c, flags=re.IGNORECASE):
                        val_col = c
                        break
            if not val_rub_col:
                for c in df_coupons.columns:
                    if re.match(r'VALUE.*RUB', c, flags=re.IGNORECASE):
                        val_rub_col = c
                        break
            if not val_prc_col:
                for c in df_coupons.columns:
                    if re.search(r'PRC|PERC|%|PERCENT', c, flags=re.IGNORECASE):
                        # avoid columns like PRIMARY_BOARDID etc.
                        if "VALUE" in c or "PRC" in c or "PERC" in c:
                            val_prc_col = c
                            break

            # выберем ближайший купон (строку) — ту же дату coupon_date, чтобы брать значения именно для ближайшего купона
            chosen_row = None
            if coupon_date:
                # ищем строку с этой coupon_date
                candidate_cols = [c for c in df_coupons.columns if "COUPON" in c and "DATE" in c]
                for c in candidate_cols:
                    try:
                        mask = pd.to_datetime(df_coupons[c], errors="coerce").dt.strftime("%Y-%m-%d") == coupon_date
                        rows = df_coupons[mask]
                        if not rows.empty:
                            chosen_row = rows.iloc[0]
                            break
                    except Exception:
                        pass
            if chosen_row is None:
                # fallback — возьмём первую строку с ненулевыми VALUE/VALUE_RUB/VALUEPRC по порядку
                for idx in range(len(df_coupons)):
                    row = df_coupons.iloc[idx]
                    # prefer rows with date in future
                    try:
                        # pick first where at least one of value columns non-null
                        ok = False
                        for colcheck in [val_col, val_rub_col, val_prc_col]:
                            if colcheck and pd.notnull(row.get(colcheck)):
                                ok = True
                                break
                        if ok:
                            chosen_row = row
                            break
                    except Exception:
                        pass

            # извлечение значений из выбранной строки
            def get_row_value(row, col):
                if row is None or col is None:
                    return None
                try:
                    v = row.get(col)
                    if pd.isna(v) or v == "":
                        return None
                    # привести к строке как есть, для чисел оставить точку
                    return str(v)
                except Exception:
                    return None

            coupon_value = get_row_value(chosen_row, val_col)
            coupon_value_rub = get_row_value(chosen_row, val_rub_col)
            coupon_value_prc = get_row_value(chosen_row, val_prc_col)

        coupon_currency = coupon_currency or bondization_currency

        # fallback на XML-борды, если ещё нет дат или валюты:
        if (not record_date or not coupon_date or not coupon_currency) and (TQOB_MAP or TQCB_MAP):
            m = TQOB_MAP.get(isin) or TQCB_MAP.get(isin)
            if m:
                if not record_date:
                    record_date = m.get("RECORDDATE") or m.get("RECORD_DATE") or m.get("RECORD")
                if not coupon_date:
                    coupon_date = m.get("COUPONDATE") or m.get("COUPON_DATE") or m.get("COUPON")
                if not coupon_currency:
                    coupon_currency = m.get("FACEUNIT") or m.get("FACEUNIT_S") or m.get("FACEUNIT")

        # --- Форматирование дат (гарантируем None или YYYY-MM-DD) ---
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
            "Валюта купона (raw)": bondization_currency or "",
            "Status": "OK" if secname or maturity_date else "Not found",
        }

    except Exception as e:
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
            "Валюта купона (raw)": "",
            "Status": f"Error: {str(e)[:120]}",
        }

# === Параллельная обработка с прогрессом и возможностью настройки workers ===
def fetch_isins_parallel(isins, max_workers=10, progress_key=None):
    results = []
    total = len(isins)
    if total == 0:
        return results

    progress_bar = None
    progress_text = None
    if progress_key:
        progress_bar = st.progress(0, key=f"{progress_key}_bar")
        progress_text = st.empty()

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_isin = {executor.submit(get_bond_data, isin): isin for isin in isins}
        for future in as_completed(future_to_isin):
            isin = future_to_isin[future]
            try:
                data = future.result()
            except Exception as e:
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
                    "Валюта купона (raw)": "",
                    "Status": f"Error: {str(e)[:120]}",
                }
            results.append(data)
            completed += 1
            if progress_bar:
                progress_bar.progress(completed / total)
            if progress_text:
                progress_text.text(f"Обработано {completed}/{total} ISIN")
    if progress_key:
        # small pause so user sees 100%
        time.sleep(0.2)
    return results

# === Интерфейс ввода ===
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
            # validate basic ISIN shape
            isin_pattern = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')
            invalid = [i for i in isins if not isin_pattern.match(i)]
            if invalid:
                st.warning(f"Некорректные ISIN пропущены: {', '.join(invalid[:10])}{'...' if len(invalid)>10 else ''}")
                isins = [i for i in isins if isin_pattern.match(i)]
            max_workers = st.sidebar.slider("Параллельных потоков (workers)", 2, 30, 10)
            results = fetch_isins_parallel(isins, max_workers=max_workers, progress_key="manual_input")
            st.session_state["results"] = pd.DataFrame(results)
            st.success("✅ Данные успешно получены!")

# === Обработка файла ===
if uploaded_file:
    if not st.session_state["file_loaded"] or uploaded_file.name != st.session_state["last_file_name"]:
        st.session_state["file_loaded"] = True
        st.session_state["last_file_name"] = uploaded_file.name
        df = safe_read_filelike(uploaded_file)
        if df.empty:
            st.error("❌ Не удалось прочитать файл или файл пуст.")
            st.stop()
        if "ISIN" not in df.columns:
            # try to auto-detect column with ISIN-like values
            candidates = [c for c in df.columns if df[c].dropna().astype(str).str.match(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$').any()]
            if len(candidates) == 1:
                df.rename(columns={candidates[0]: "ISIN"}, inplace=True)
                st.info(f"Авто-детект: колонка '{candidates[0]}' использована как ISIN")
            else:
                st.error("❌ В файле должна быть колонка 'ISIN' или одна колонка с ISIN-подобными значениями.")
                st.stop()

        isins = df["ISIN"].dropna().unique().tolist()
        isins = [str(x).strip().upper() for x in isins if str(x).strip()]
        # validate ISINs
        isin_pattern = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')
        invalid = [i for i in isins if not isin_pattern.match(i)]
        if invalid:
            st.warning(f"Некорректные ISIN пропущены: {', '.join(invalid[:10])}{'...' if len(invalid)>10 else ''}")
            isins = [i for i in isins if isin_pattern.match(i)]

        st.write(f"Найдено {len(isins)} уникальных ISIN для обработки.")
        max_workers = st.sidebar.slider("Параллельных потоков (workers)", 2, 30, 10)
        results = fetch_isins_parallel(isins, max_workers=max_workers, progress_key="file_upload")
        st.session_state["results"] = pd.DataFrame(results)
        st.success("✅ Данные успешно получены из файла!")

# === Подгрузка справочника эмитентов ===
@st.cache_data(ttl=3600)
def fetch_emitter_names():
    url = "https://raw.githubusercontent.com/mainarkler/Bond_date/refs/heads/main/Pifagr_name_with_emitter.csv"
    try:
        df_emitters = pd.read_csv(url, dtype=str)
        df_emitters.columns = [c.strip() for c in df_emitters.columns]
        return df_emitters
    except Exception as e:
        st.warning(f"⚠️ Не удалось загрузить справочник эмитентов: {e}")
        return pd.DataFrame(columns=["Issuer", "EMITTER_ID"])

df_emitters = fetch_emitter_names()

# === Стилизация таблицы ===
def style_df(row):
    if pd.isna(row.get("Наименование инструмента")) or row.get("Наименование инструмента") in [None, "None", ""]:
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
            except:
                pass
    if any(c == "background-color: Chocolate" for c in colors):
        colors = ["background-color: SandyBrown" if c == "" else c for c in colors]
    return colors

# === Вывод результатов ===
if st.session_state["results"] is not None:
    df_res = st.session_state["results"].copy()

    # convert Status to a column if missing
    if "Status" not in df_res.columns:
        df_res["Status"] = "OK"

    # merge emitters if possible
    if "Код эмитента" in df_res.columns and not df_emitters.empty:
        df_res = df_res.merge(df_emitters, how="left", left_on="Код эмитента", right_on="EMITTER_ID")
        df_res["Эмитент"] = df_res["Issuer"]
        df_res.drop(columns=["Issuer", "EMITTER_ID"], inplace=True, errors="ignore")

        cols = df_res.columns.tolist()
        if "Эмитент" in cols and "Код эмитента" in cols:
            cols.remove("Эмитент")
            idx = cols.index("Код эмитента")
            cols.insert(idx + 1, "Эмитент")
            df_res = df_res[cols]

        st.session_state["results"] = df_res
    else:
        st.warning("⚠️ В данных нет колонки 'Код эмитента' — объединение со справочником пропущено.")

    # show counts and quick filters
    st.markdown(f"**Всего записей:** {len(df_res)}")
    status_counts = df_res["Status"].value_counts().to_dict()
    st.write("Статусы:", status_counts)

    # allow simple filtering by status
    statuses = list(df_res["Status"].unique())
    chosen_statuses = st.multiselect("Фильтр по статусу", options=statuses, default=statuses)
    df_show = df_res[df_res["Status"].isin(chosen_statuses)]

    st.dataframe(df_show.style.apply(style_df, axis=1), use_container_width=True)

    # export helpers
    def to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Данные")
        return output.getvalue()

    def to_csv_bytes(df):
        return df.to_csv(index=False).encode("utf-8-sig")

    st.download_button(
        label="💾 Скачать результат (Excel)",
        data=to_excel(df_show),
        file_name="bond_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.download_button(
        label="💾 Скачать результат (CSV)",
        data=to_csv_bytes(df_show),
        file_name="bond_data.csv",
        mime="text/csv",
    )

    # quick action: rerun selected ISINs
    if st.button("🔁 Повторно запросить все ISIN"):
        isins_all = df_res["ISIN"].dropna().unique().tolist()
        max_workers = st.sidebar.slider("Параллельных потоков (workers) при повторном запросе", 2, 30, 10)
        with st.spinner("Повторный запрос..."):
            new_results = fetch_isins_parallel(isins_all, max_workers=max_workers, progress_key="requery")
        st.session_state["results"] = pd.DataFrame(new_results)
        st.experimental_rerun()
else:
    st.info("👆 Загрузите файл или введите ISIN-ы вручную.")
