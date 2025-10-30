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

# === MOEX API session ===
session = requests.Session()
session.headers.update({"User-Agent": "python-requests/iss-moex-script"})

# === Кэширование TQOB XML ===
@st.cache_data(ttl=3600)
def fetch_tqob_xml():
    url_tqob = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQOB/securities.xml?iss.meta=off"
    r = session.get(url_tqob, timeout=20)
    r.raise_for_status()
    return ET.fromstring(r.content)

tqob_root = fetch_tqob_xml()

# === Функция поиска эмитента и SECID ===
@st.cache_data(ttl=3600)
def fetch_emitter_and_secid(isin: str):
    isin = str(isin).strip()
    if not isin:
        return None, None

    emitter_id = None
    secid = None

    # --- Стандартный JSON ---
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
                    emitter_id = rows[0][i]
                if c.upper() == "SECID":
                    secid = rows[0][i]
    except Exception:
        pass

    # --- Стандартный XML ---
    if not emitter_id or not secid:
        try:
            url = f"https://iss.moex.com/iss/securities/{isin}.xml?iss.meta=off"
            r = session.get(url, timeout=10)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            for row in root.iter():
                name_attr = row.attrib.get("name") or row.attrib.get("NAME")
                if name_attr:
                    if name_attr.upper() == "EMITTER_ID" and not emitter_id:
                        emitter_id = row.attrib.get("value") or row.attrib.get("VALUE")
                    if name_attr.upper() == "SECID" and not secid:
                        secid = row.attrib.get("value") or row.attrib.get("VALUE")
        except Exception:
            pass

    # --- TQOB для ОФЗ ---
    if not secid or not emitter_id:
        for row in tqob_root.iter("row"):
            if row.attrib.get("isin") == isin:
                if not secid:
                    secid = row.attrib.get("secid") or row.attrib.get("SECID")
                if not emitter_id:
                    emitter_id = row.attrib.get("emitterid") or row.attrib.get("EMITTERID")

    return emitter_id, secid

# === Получение данных по ISIN ===
def get_bond_data(isin):
    try:
        emitter_id, secid = fetch_emitter_and_secid(isin)
        secname = maturity_date = put_date = call_date = None
        success = False

        # --- Информация о бумаге по SECID ---
        if secid:
            try:
                url_info = f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{secid}.json"
                r = session.get(url_info, timeout=10)
                if r.status_code == 200:
                    data_info = r.json()
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
                r = session.get(url_coupons, timeout=10)
                r.raise_for_status()
                data_coupons = r.json()
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

# === Параллельная обработка ===
def fetch_isins_parallel(isins):
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_isin = {executor.submit(get_bond_data, isin): isin for isin in isins}
        for future in as_completed(future_to_isin):
            data = future.result()
            if data:
                results.append(data)
    return results

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
            progress_bar = st.progress(0)
            results = fetch_isins_parallel(isins)
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
        results = fetch_isins_parallel(isins)
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

# === Добавление столбца 'Эмитент' ===
if not df_emitters.empty:
    df_res = df_res.merge(df_emitters, how="left", left_on="Код эмитента", right_on="EMITTER_ID")
    df_res["Эмитент"] = df_res["Issuer"]
    df_res.drop(columns=["Issuer", "EMITTER_ID"], inplace=True, errors="ignore")
    
    # Переставляем столбцы: 'Эмитент' вторым
    cols = df_res.columns.tolist()
    if "Эмитент" in cols and "Код эмитента" in cols:
        cols.remove("Эмитент")
        idx = cols.index("Код эмитента")
        cols.insert(idx + 1, "Эмитент")
        df_res = df_res[cols]

    st.session_state["results"] = df_res


    st.dataframe(df_res.style.apply(style_df, axis=1), use_container_width=True)

    def to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Данные")
        return output.getvalue()

    st.download_button(
        label="💾 Скачать результат (Excel)",
        data=to_excel(df_res),
        file_name="bond_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("👆 Загрузите файл или введите ISIN-ы вручную.")
