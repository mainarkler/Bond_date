import streamlit as st
import pandas as pd
import httpx
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from io import BytesIO
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
    st.session_state.update({
        "overnight": False,
        "extra_days": 2,
        "results": None,
        "file_loaded": False,
        "last_file_name": None
    })
    st.rerun()

overnight = st.checkbox("Overnight РЕПО", key="overnight")
extra_days_input = st.number_input(
    "Дней РЕПО:", min_value=2, max_value=366, step=1,
    disabled=overnight, key="extra_days"
)
days_threshold = 2 if overnight else 1 + st.session_state["extra_days"]
st.write(f"Текущее значение границы выплат: {days_threshold} дн.")

# === Кэширование TQOB XML ===
@st.cache_data(ttl=3600)
def fetch_tqob_xml():
    url_tqob = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQOB/securities.xml?iss.meta=off"
    r = httpx.get(url_tqob, timeout=20)
    r.raise_for_status()
    return ET.fromstring(r.content)

tqob_root = fetch_tqob_xml()

# === Кэширование эмитентов и secid по ISIN ===
@st.cache_data(ttl=3600)
def fetch_emitter_and_secid(isin: str):
    isin = str(isin).strip()
    if not isin:
        return None, None

    emitter_id = None
    secid = None

    # JSON запрос
    try:
        url = f"https://iss.moex.com/iss/securities/{isin}.json"
        r = httpx.get(url, timeout=10)
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
    except:
        pass

    # XML запрос
    if not emitter_id or not secid:
        try:
            url = f"https://iss.moex.com/iss/securities/{isin}.xml?iss.meta=off"
            r = httpx.get(url, timeout=10)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            for row in root.iter():
                name_attr = row.attrib.get("name") or row.attrib.get("NAME")
                if name_attr:
                    if name_attr.upper() == "EMITTER_ID" and not emitter_id:
                        emitter_id = row.attrib.get("value") or row.attrib.get("VALUE")
                    if name_attr.upper() == "SECID" and not secid:
                        secid = row.attrib.get("value") or row.attrib.get("VALUE")
        except:
            pass

    # TQOB для ОФЗ
    if not secid or not emitter_id:
        for row in tqob_root.iter("row"):
            if row.attrib.get("isin") == isin:
                if not secid:
                    secid = row.attrib.get("secid") or row.attrib.get("SECID")
                if not emitter_id:
                    emitter_id = row.attrib.get("emitterid") or row.attrib.get("EMITTERID")

    return emitter_id, secid

# === Асинхронная обработка ISIN ===
async def get_bond_data_async(isin):
    emitter_id, secid = fetch_emitter_and_secid(isin)
    if not secid:
        return {"ISIN": isin}

    result = {"ISIN": isin, "Код эмитента": emitter_id}
    try:
        # Информация о бумаге
        url_info = f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{secid}.json"
        r = httpx.get(url_info, timeout=10)
        if r.status_code == 200:
            data_info = r.json()
            rows_info = data_info.get("securities", {}).get("data", [])
            cols_info = data_info.get("securities", {}).get("columns", [])
            if rows_info:
                info = dict(zip(cols_info, rows_info[0]))
                result.update({
                    "Наименование инструмента": info.get("SECNAME"),
                    "Дата погашения": info.get("MATDATE"),
                    "Дата оферты Put": info.get("PUTOPTIONDATE"),
                    "Дата оферты Call": info.get("CALLOPTIONDATE")
                })
        # Купоны
        url_coupons = f"https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{secid}.json?iss.only=coupons&iss.meta=off"
        r = httpx.get(url_coupons, timeout=10)
        r.raise_for_status()
        data_coupons = r.json().get("coupons", {})
        df_coupons = pd.DataFrame(data_coupons.get("data", []), columns=data_coupons.get("columns", []))
        today = pd.to_datetime(datetime.today().date())
        for col in ["recorddate", "coupondate"]:
            if col in df_coupons:
                future = pd.to_datetime(df_coupons[col], errors="coerce")
                future = future[future >= today]
                result[f"Дата фиксации купона" if col=="recorddate" else f"Дата купона"] = future.min() if not future.empty else None
    except:
        pass

    # Форматирование дат
    for key in ["Дата погашения", "Дата оферты Put", "Дата оферты Call", "Дата фиксации купона", "Дата купона"]:
        if key in result and result[key]:
            try:
                result[key] = pd.to_datetime(result[key]).strftime("%Y-%m-%d")
            except:
                result[key] = None
    return result

async def fetch_isins_async(isins):
    tasks = [get_bond_data_async(isin) for isin in isins]
    return await asyncio.gather(*tasks)

def fetch_isins_parallel(isins):
    return asyncio.run(fetch_isins_async(isins))

# === Интерфейс ввода ===
st.subheader("📤 Загрузка или ввод ISIN")
tab1, tab2 = st.tabs(["📁 Загрузить файл", "✍️ Ввести вручную"])

uploaded_file = None
isin_input = ""

with tab1:
    uploaded_file = st.file_uploader("Загрузите Excel или CSV с колонкой ISIN", type=["xlsx", "xls", "csv"])
    if uploaded_file:
        if not st.session_state["file_loaded"] or uploaded_file.name != st.session_state["last_file_name"]:
            st.session_state["file_loaded"] = True
            st.session_state["last_file_name"] = uploaded_file.name
            if uploaded_file.name.endswith(".csv"):
                df = pd.read_csv(uploaded_file, usecols=["ISIN"], dtype=str)
            else:
                df = pd.read_excel(uploaded_file, usecols=["ISIN"], dtype=str)
            isins = df["ISIN"].dropna().unique().tolist()
            st.write(f"Найдено {len(isins)} ISIN")
            if st.button("🔍 Получить данные по ISIN из файла"):
                with st.spinner("Обработка..."):
                    results = fetch_isins_parallel(isins)
                    st.session_state["results"] = pd.DataFrame(results)
                    st.success("✅ Данные успешно получены!")

with tab2:
    isin_input = st.text_area("Введите ISIN (через пробел или запятую)", height=150)
    if st.button("🔍 Получить данные по введённым ISIN"):
        raw_text = isin_input.strip()
        if raw_text:
            isins = [i.strip().upper() for i in re.split(r"[\s,;]+", raw_text) if i.strip()]
            with st.spinner("Обработка..."):
                results = fetch_isins_parallel(isins)
                st.session_state["results"] = pd.DataFrame(results)
                st.success("✅ Данные успешно получены!")

# === Подгрузка справочника эмитентов ===
@st.cache_data(ttl=86400)
def fetch_emitter_names():
    url = "https://raw.githubusercontent.com/mainarkler/Bond_date/refs/heads/main/Pifagr_name_with_emitter.csv"
    try:
        df_emitters = pd.read_csv(url, dtype=str)
        df_emitters.columns = [c.strip() for c in df_emitters.columns]
        return df_emitters
    except:
        return pd.DataFrame(columns=["Issuer", "EMITTER_ID"])

df_emitters = fetch_emitter_names()

# === Векторная стилизация DataFrame ===
def style_df_vectorized(df):
    today = pd.to_datetime(datetime.today().date())
    danger_threshold = today + pd.to_timedelta(days_threshold, unit="d")
    key_dates = ["Дата погашения", "Дата оферты Put", "Дата оферты Call", "Дата фиксации купона", "Дата купона"]
    styles = pd.DataFrame("", index=df.index, columns=df.columns)
    for col in key_dates:
        if col in df:
            mask = pd.to_datetime(df[col], errors="coerce") <= danger_threshold
            styles.loc[mask, col] = "background-color: Chocolate"
    return styles

# === Вывод результатов ===
if st.session_state["results"] is not None:
    df_res = st.session_state["results"]
    if not df_emitters.empty:
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

    st.dataframe(df_res.style.apply(style_df_vectorized, axis=None), use_container_width=True)

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
