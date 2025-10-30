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
import openpyxl

# === Настройки страницы ===
st.set_page_config(page_title="РЕПО претрейд risc's", page_icon="📈", layout="wide")
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

# === Загрузка справочников ===
EMITTER_FILE = https://raw.githubusercontent.com/mainarkler/Bond_date/main/Pifagr_name_with_emitter.csv
RATING_FILE = r"C:\Desktop\code\App\scor.csv"

df_emitters = safe_read_csv(EMITTER_FILE)
df_rating = safe_read_csv(RATING_FILE)

if not df_rating.empty:
    rating_col = next((c for c in df_rating.columns if "RATING" in c.upper()), None)
    issuer_col = next((c for c in df_rating.columns if "ISSUER" in c.upper()), None)
    if rating_col and issuer_col:
        df_rating = df_rating.rename(columns={rating_col: "RATING", issuer_col: "ISSUER"})
        df_rating["RATING"] = pd.to_numeric(df_rating["RATING"], errors="coerce")
    else:
        df_rating = pd.DataFrame(columns=["ISSUER", "RATING"])
else:
    df_rating = pd.DataFrame(columns=["ISSUER", "RATING"])

# === MOEX API ===
session = requests.Session()
session.headers.update({"User-Agent": "python-requests/iss-moex-emitter-id-script"})

def fetch_emitter_id(isin: str):
    isin = str(isin).strip()
    if not isin:
        return None
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
                if c.upper() == "EMITTER_ID":
                    return rows[0][i]
    except Exception:
        pass
    try:
        url = f"https://iss.moex.com/iss/securities/{isin}.xml?iss.meta=off"
        r = session.get(url, timeout=10)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for row in root.iter():
            name_attr = row.attrib.get("name") or row.attrib.get("NAME")
            if name_attr and name_attr.upper() == "EMITTER_ID":
                return row.attrib.get("value") or row.attrib.get("VALUE")
    except Exception:
        pass
    return None

# === Получение данных по ISIN ===
def get_bond_data(isin):
    try:
        emitter_id = fetch_emitter_id(isin)
        emitter_name = None

        if emitter_id and not df_emitters.empty:
            match = df_emitters[df_emitters["EMITTER_ID"] == str(emitter_id)]
            if not match.empty and "ISSUER" in match.columns:
                emitter_name = match.iloc[0]["ISSUER"]

        # --- Рейтинг ---
        rating = None
        if emitter_name and not df_rating.empty:
            match_r = df_rating[
                df_rating["ISSUER"].str.strip().str.lower() == emitter_name.strip().lower()
            ]
            if not match_r.empty:
                rating = match_r.iloc[0]["RATING"]

        # --- Лимит ---
        def calc_limit(emitter_name, rating):
            if not emitter_name:
                return "0%"
            if "МИНФИН РОССИИ" in emitter_name.upper():
                return "100%"
            try:
                r = float(rating)
                if r <= 12:
                    return "25%"
                elif 13 <= r < 18:
                    return "15%"
                else:
                    return "0%"
            except Exception:
                return "0%"

        limit = calc_limit(emitter_name, rating)

        # --- Информация о бумаге ---
        url_info = f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{isin}.json"
        response_info = requests.get(url_info, timeout=10)
        secname = maturity_date = put_date = call_date = None
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

        # --- Купоны ---
        url_coupons = f"https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{isin}.json?iss.only=coupons&iss.meta=off"
        try:
            response_coupons = requests.get(url_coupons, timeout=10)
            response_coupons.raise_for_status()
            data_coupons = response_coupons.json()
            coupons = data_coupons.get("coupons", {}).get("data", [])
            columns_coupons = data_coupons.get("coupons", {}).get("columns", [])
            record_date = coupon_date = None

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
            else:
                record_date = coupon_date = None
        except Exception:
            record_date = coupon_date = None

        # --- Форматирование ---
        def fmt(date):
            if pd.isna(date) or not date:
                return None
            try:
                return pd.to_datetime(date).strftime("%Y-%m-%d")
            except Exception:
                return None

        return {
            "ISIN": isin,
            "Код эмитента": emitter_id,
            "Наименование эмитента": emitter_name,
            "Рейтинг": rating,
            "Лимит": limit,
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
        st.session_state["file_loaded"] = True
        st.session_state["last_file_name"] = uploaded_file.name
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file, dtype=str)
        else:
            df = pd.read_excel(uploaded_file, dtype=str)
        if "ISIN" not in df.columns:
            st.error("❌ В файле должна быть колонка 'ISIN'.")
            st.stop()
        isins = df["ISIN"].dropna().unique().tolist()
        results = []
        progress_bar = st.progress(0)
        for idx, isin in enumerate(isins, start=1):
            data = get_bond_data(isin)
            if data:
                results.append(data)
            progress_bar.progress(idx / len(isins))
            time.sleep(0.1)
        st.session_state["results"] = pd.DataFrame(results)
        st.success("✅ Данные успешно получены из файла!")

# === Стилизация ===
def style_df(row):
    if (pd.isna(row["Наименование инструмента"]) or row["Наименование инструмента"] in [None, "None", ""]):
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
    df_res = st.session_state["results"]
    st.dataframe(df_res.style.apply(style_df, axis=1), use_container_width=True)

    def to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Данные")
        return output.getvalue()

    st.download_button(
        label="💾 Скачать результат (Excel)",
        data=to_excel(df_res),
        file_name="bond_data_with_limits.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("👆 Загрузите файл или введите ISIN-ы вручную.")
