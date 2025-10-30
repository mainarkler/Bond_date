import streamlit as st
import pandas as pd
import requests
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from io import BytesIO, StringIO
import os
import csv
import re

# === Настройки страницы ===
st.set_page_config(page_title="РЕПО претрейд", page_icon="📈", layout="wide")
st.title("📈 РЕПО претрейд")

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
days_threshold = 3 if st.session_state["overnight"] else 1 + st.session_state["extra_days"]
st.write(f"Текущее значение границы выплат: {days_threshold} дн.")

# === Безопасное чтение CSV ===
def safe_read_csv(path):
    if not os.path.exists(path):
        st.warning(f"⚠️ Файл не найден: {path}")
        return pd.DataFrame()
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            content = f.read().replace('"', '').replace("'", "").strip()
        sample = content[:2048]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
            sep = dialect.delimiter
        except Exception:
            sep = ","
        df = pd.read_csv(StringIO(content), sep=sep, dtype=str)
        df.columns = [c.strip().upper() for c in df.columns]
        return df
    except Exception as e:
        st.warning(f"⚠️ Ошибка при чтении файла {os.path.basename(path)}: {e}")
        return pd.DataFrame()

# === MOEX API ===
session = requests.Session()
session.headers.update({"User-Agent": "python-requests/iss-moex-script"})

# === Функция поиска SECID ===
def fetch_sec_id(isin: str):
    isin = str(isin).strip()
    if not isin:
        return None

    # --- Стандартный запрос JSON ---
    try:
        url = f"https://iss.moex.com/iss/securities/{isin}.json"
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        securities = data.get("securities", {})
        cols = securities.get("columns", [])
        rows = securities.get("data", [])
        if rows:
            for i, c in enumerate(cols):
                if c.upper() == "SECID":
                    return rows[0][i]
    except Exception:
        pass

    # --- Стандартный запрос XML ---
    try:
        url = f"https://iss.moex.com/iss/securities/{isin}.xml?iss.meta=off"
        r = session.get(url, timeout=10)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for row in root.iter():
            name_attr = row.attrib.get("name") or row.attrib.get("NAME")
            if name_attr and name_attr.upper() == "SECID":
                return row.attrib.get("value") or row.attrib.get("VALUE")
    except Exception:
        pass

    # --- Если стандартный запрос не дал данных, ищем в TQOB (для ОФЗ) ---
    try:
        url_tqob = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQOB/securities.xml?iss.meta=off"
        r = session.get(url_tqob, timeout=10)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for row in root.iter("row"):
            if row.attrib.get("isin") == isin:
                return row.attrib.get("secid") or row.attrib.get("SECID")
    except Exception:
        pass

    return None

# === Получение данных по ISIN ===
def get_bond_data(isin):
    try:
        secid = fetch_sec_id(isin)

        # --- Информация о бумаге ---
        secname = maturity_date = put_date = call_date = None
        success = False

        # --- Стандартный запрос JSON по secid ---
        if secid:
            try:
                url_info = f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{secid}.json"
                response_info = requests.get(url_info, timeout=10)
                if response_info.status_code == 200:
                    data_info = response_info.json()
                    rows_info = data_info.get("securities", {}).get("data", [])
                    cols_info = data_info.get("securities", {}).get("columns", [])
                    if rows_info:
                        info = dict(zip(cols_info, rows_info[0]))
                        secname = info.get("SECNAME")
                        maturity_date = info.get("MATDATE")
                        put_date = info.get("PUTOPTIONDATE")
                        call_date = info.get("CALLOPTIONDATE")
                        success = True
            except Exception:
                pass

        # --- Купоны ---
        record_date = coupon_date = None
        if success and secid:
            try:
                url_coupons = f"https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{secid}.json?iss.only=coupons&iss.meta=off"
                response_coupons = requests.get(url_coupons, timeout=10)
                response_coupons.raise_for_status()
                data_coupons = response_coupons.json()
                coupons = data_coupons.get("coupons", {}).get("data", [])
                columns_coupons = data_coupons.get("coupons", {}).get("columns", [])
                if coupons:
                    df_coupons = pd.DataFrame(coupons, columns=columns_coupons)
                    today = pd.to_datetime(datetime.today().date())

                    def next_date(col):
                        if col in df_coupons:
                            future = pd.to_datetime(df_coupons[col], errors="coerce")
                            future = future[future >= today]
                            return future.min() if not future.empty else None
                        return None

                    record_date = next_date("recorddate")
                    coupon_date = next_date("coupondate")
            except Exception:
                record_date = coupon_date = None

        # --- Форматирование дат ---
        def fmt(date):
            if pd.isna(date) or not date:
                return None
            try:
                return pd.to_datetime(date).strftime("%Y-%m-%d")
            except Exception:
                return None

        return {
            "ISIN": isin,
            "SECID": secid,
            "Наименование инструмента": secname,
            "Дата погашения": fmt(maturity_date),
            "Дата оферты Put": fmt(put_date),
            "Дата оферты Call": fmt(call_date),
            "Дата фиксации купона": fmt(record_date),
            "Дата купона": fmt(coupon_date),
        }

    except Exception as e:
        st.warning(f"Ошибка при обработке {isin}: {e}")
        return None

# === Интерфейс ввода ===
st.subheader("📤 Загрузка или ввод ISIN")
tab1, tab2 = st.tabs(["📁 Загрузить файл", "✍️ Ввести вручную"])

with tab1:
    uploaded_file = st.file_uploader("Загрузите Excel или CSV с колонкой ISIN:", type=["xlsx", "xls", "csv"])

with tab2:
    isin_input = st.text_area(
        "Введите или вставьте ISIN-ы (через Enter, пробел или запятую):",
        placeholder="ISINs",
        height=150
    )
    if st.button("🔍 Получить данные по введённым ISIN"):
        raw_text = isin_input.strip()
        if raw_text:
            isins = re.split(r"[\s,;]+", raw_text)
            isins = [i.strip().upper() for i in isins if i.strip()]
            results = []
            progress_bar = st.progress(0)
            for idx, isin in enumerate(isins, start=1):
                data = get_bond_data(isin)
                if data:
                    results.append(data)
                progress_bar.progress(idx / len(isins))
                time.sleep(0.1)
            st.session_state["results"] = pd.DataFrame(results)
            st.success("✅ Данные успешно получены!")

# === Обработка файла ===
if uploaded_file:
    if not st.session_state["file_loaded"] or uploaded_file.name != st.session_state["last_file_name"]:
        st.session_state["file_loaded
