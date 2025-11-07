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
days_threshold = 2 if st.session_state["overnight"] else 1 + st.session_state["extra_days"]
st.write(f"Текущее значение границы выплат: {days_threshold} дн.")

# === Безопасное чтение CSV ===
def safe_read_csv(path):
    if not os.path.exists(path):
        st.warning(f"⚠️ Файл не найден: {path}")
        return pd.DataFrame()
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            content = f.read()
        content = content.replace('\r\n', '\n').strip()
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

# === Кэширование XML TQOB и TQCB ===
@st.cache_data(ttl=3600)
def fetch_board_xml(board: str):
    url = f"https://iss.moex.com/iss/engines/stock/markets/bonds/boards/{board.lower()}/securities.xml?marketprice_board=3&iss.meta=off"
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
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

# === Функция поиска эмитента и SECID ===
@st.cache_data(ttl=3600)
def fetch_emitter_and_secid(isin: str):
    isin = str(isin).strip().upper()
    if not isin:
        return None, None
    emitter_id = None
    secid = None

    # 1) JSON по ISIN
    try:
        url = f"https://iss.moex.com/iss/securities/{isin}.json"
        r = session.get(url, timeout=10)
        r.raise_for_status()
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
            r = session.get(url, timeout=10)
            r.raise_for_status()
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

    # 3) XML-борды (TQOB/TQCB)
    if not secid:
        m = TQOB_MAP.get(isin) or TQCB_MAP.get(isin)
        if m:
            secid = m.get("SECID")
            if not emitter_id:
                emitter_id = m.get("EMITTERID")

    return emitter_id, secid

# === Вспомогательные функции для получения и обработки данных ===

def _fmt_date(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return pd.to_datetime(val).strftime("%Y-%m-%d")
    except Exception:
        return None

def _next_future_date_from_dataframe(df, possible_cols):
    """
    Возвращает ближайшую дату >= today из указанных колонок df.
    Если найдено — возвращает строку YYYY-MM-DD, иначе None.
    """
    today = pd.to_datetime(datetime.today().date())
    candidates = []
    for col in possible_cols:
        if col in df.columns:
            try:
                s = pd.to_datetime(df[col], errors="coerce")
                s = s[s >= today]
                if not s.empty:
                    candidates.append(s.min())
            except Exception:
                pass
    if not candidates:
        return None
    nxt = min(candidates)
    return nxt.strftime("%Y-%m-%d")

def _any_future_date_in_df(df):
    """
    Поиск ближайшей даты >= today по всем колонкам (fallback).
    """
    today = pd.to_datetime(datetime.today().date())
    cand = []
    for col in df.columns:
        try:
            s = pd.to_datetime(df[col], errors="coerce")
            s = s[s >= today]
            if not s.empty:
                cand.append(s.min())
        except Exception:
            pass
    if not cand:
        return None
    return min(cand).strftime("%Y-%m-%d")

def fetch_coupons_by_identifier(identifier):
    """
    Попытка получить coupons через bondization/{identifier}.json
    Возвращает (coupon_date, record_date) — строки YYYY-MM-DD или None.
    """
    try:
        url_coupons = f"https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{identifier}.json?iss.only=coupons&iss.meta=off"
        r = session.get(url_coupons, timeout=12)
        r.raise_for_status()
        data = r.json()
        coupons = data.get("coupons", {}).get("data", [])
        cols = data.get("coupons", {}).get("columns", [])
        if coupons and cols:
            df = pd.DataFrame(coupons, columns=cols)
            df.columns = [c.upper() for c in df.columns]
            # возможные имена колонок
            possible_coupon_cols = [c for c in df.columns if "COUPON" in c and "DATE" in c]
            possible_record_cols = [c for c in df.columns if "RECORD" in c and "DATE" in c]
            coupon_date = _next_future_date_from_dataframe(df, possible_coupon_cols) or _any_future_date_in_df(df)
            record_date = _next_future_date_from_dataframe(df, possible_record_cols)
            return coupon_date, record_date
    except Exception:
        pass
    return None, None

def fetch_info_by_isin(isin):
    """
    Часть 1: получить данные строго по ISIN.
    Возвращает dict с нужными полями (значения могут быть None/пустые строки).
    """
    result = {
        "ISIN": isin,
        "Код эмитента": "",
        "Наименование инструмента": "",
        "Дата погашения": None,
        "Дата оферты Put": None,
        "Дата оферты Call": None,
        "Дата фиксации купона": None,
        "Дата купона": None,
    }

    isin = str(isin).strip().upper()
    if not isin:
        return result

    # Попробуем securities/{isin}.json
    try:
        url = f"https://iss.moex.com/iss/securities/{isin}.json"
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        sec = data.get("securities", {})
        cols = sec.get("columns", [])
        rows = sec.get("data", [])
        if rows and cols:
            row = rows[0]
            col_map = {c.upper(): i for i, c in enumerate(cols)}
            if "SECNAME" in col_map:
                result["Наименование инструмента"] = row[col_map["SECNAME"]] or result["Наименование инструмента"]
            elif "SEC_NAME" in col_map:
                result["Наименование инструмента"] = row[col_map["SEC_NAME"]] or result["Наименование инструмента"]

            if "EMITTER_ID" in col_map:
                result["Код эмитента"] = row[col_map["EMITTER_ID"]] or result["Код эмитента"]
            elif "EMITTERID" in col_map:
                result["Код эмитента"] = row[col_map["EMITTERID"]] or result["Код эмитента"]

            if "MATDATE" in col_map:
                result["Дата погашения"] = _fmt_date(row[col_map["MATDATE"]])
            elif "MATURITYDATE" in col_map:
                result["Дата погашения"] = _fmt_date(row[col_map["MATURITYDATE"]])
    except Exception:
        pass

    # Купоны по ISIN
    coupon_date, record_date = fetch_coupons_by_identifier(isin)
    result["Дата купона"] = coupon_date
    result["Дата фиксации купона"] = record_date

    return result

def fetch_info_by_secid(secid):
    """
    Часть 2: получить данные строго по SECID (все запросы по secid).
    Возвращает dict с нужными полями.
    """
    result = {
        "ISIN": None,  # secid-часть не всегда знает ISIN, внешняя функция присвоит ISIN при возврате
        "Код эмитента": "",
        "Наименование инструмента": "",
        "Дата погашения": None,
        "Дата оферты Put": None,
        "Дата оферты Call": None,
        "Дата фиксации купона": None,
        "Дата купона": None,
    }

    if not secid:
        return result

    # Попробуем engines/.../securities/{secid}.json
    try:
        url_info = f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{secid}.json"
        r = session.get(url_info, timeout=12)
        r.raise_for_status()
        data_info = r.json()
        sec = data_info.get("securities", {})
        cols = sec.get("columns", [])
        rows = sec.get("data", [])
        if rows and cols:
            row = rows[0]
            col_map = {c.upper(): i for i, c in enumerate(cols)}
            if "SECNAME" in col_map:
                result["Наименование инструмента"] = row[col_map["SECNAME"]] or result["Наименование инструмента"]
            elif "SEC_NAME" in col_map:
                result["Наименование инструмента"] = row[col_map["SEC_NAME"]] or result["Наименование инструмента"]

            # Матдата и опционы
            if "MATDATE" in col_map:
                result["Дата погашения"] = _fmt_date(row[col_map["MATDATE"]])
            if "PUTOPTIONDATE" in col_map:
                result["Дата оферты Put"] = _fmt_date(row[col_map["PUTOPTIONDATE"]])
            if "CALLOPTIONDATE" in col_map:
                result["Дата оферты Call"] = _fmt_date(row[col_map["CALLOPTIONDATE"]])

            if "EMITTERID" in col_map:
                result["Код эмитента"] = row[col_map["EMITTERID"]] or result["Код эмитента"]
            elif "EMITTER_ID" in col_map:
                result["Код эмитента"] = row[col_map["EMITTER_ID"]] or result["Код эмитента"]
    except Exception:
        pass

    # Купоны по SECID
    coupon_date, record_date = fetch_coupons_by_identifier(secid)
    result["Дата купона"] = coupon_date
    result["Дата фиксации купона"] = record_date

    return result

# === Основная функция: сначала ISIN, если не хватает — искать SECID и запрос по SECID ===
@st.cache_data(ttl=3600)
def get_bond_data(isin):
    isin = str(isin).strip().upper()
    base_template = {
        "ISIN": isin,
        "Код эмитента": "",
        "Наименование инструмента": "",
        "Дата погашения": None,
        "Дата оферты Put": None,
        "Дата оферты Call": None,
        "Дата фиксации купона": None,
        "Дата купона": None,
    }

    if not isin:
        return base_template

    # --- Часть 1: поиск и сбор данных по ISIN ---
    res_isin = fetch_info_by_isin(isin)

    # считаем, что результат успешен если есть хотя бы имя или матдата или ближайший купон
    has_nonempty = any([
        res_isin.get("Наименование инструмента"),
        res_isin.get("Дата погашения"),
        res_isin.get("Дата купона"),
        res_isin.get("Код эмитента"),
    ])

    if has_nonempty:
        # заполняем шаблон и возвращаем (ISIN уже установлен)
        out = base_template.copy()
        out.update({
            "Код эмитента": res_isin.get("Код эмитента") or "",
            "Наименование инструмента": res_isin.get("Наименование инструмента") or "",
            "Дата погашения": res_isin.get("Дата погашения"),
            "Дата оферты Put": res_isin.get("Дата оферты Put"),
            "Дата оферты Call": res_isin.get("Дата оферты Call"),
            "Дата фиксации купона": res_isin.get("Дата фиксации купона"),
            "Дата купона": res_isin.get("Дата купона"),
        })
        return out

    # --- Часть 2: если по ISIN ничего не найдено, найти SECID и взять данные по SECID ---
    emitter_id, secid = fetch_emitter_and_secid(isin)

    if secid:
        res_secid = fetch_info_by_secid(secid)
        out = base_template.copy()
        out.update({
            "Код эмитента": res_secid.get("Код эмитента") or emitter_id or "",
            "Наименование инструмента": res_secid.get("Наименование инструмента") or "",
            "Дата погашения": res_secid.get("Дата погашения"),
            "Дата оферты Put": res_secid.get("Дата оферты Put"),
            "Дата оферты Call": res_secid.get("Дата оферты Call"),
            "Дата фиксации купона": res_secid.get("Дата фиксации купона"),
            "Дата купона": res_secid.get("Дата купона"),
        })
        return out

    # --- fallback: если ничего не нашли ни по ISIN ни по SECID, пробуем борды TQOB/TQCB (если есть) ---
    m = TQOB_MAP.get(isin) or TQCB_MAP.get(isin)
    if m:
        out = base_template.copy()
        out.update({
            "Код эмитента": m.get("EMITTERID") or "",
            "Наименование инструмента": m.get("SECNAME") or m.get("NAME") or "",
            "Дата погашения": _fmt_date(m.get("MATDATE") or m.get("MATDATE")),
            "Дата фиксации купона": _fmt_date(m.get("RECORDDATE") or m.get("RECORD_DATE") or m.get("RECORD")),
            "Дата купона": _fmt_date(m.get("COUPONDATE") or m.get("COUPON_DATE") or m.get("COUPON")),
        })
        return out

    # Если совсем ничего — вернуть пустой шаблон
    return base_template

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
    uploaded_file = st.file_uploader("Загрузите Excel или CSV с колонкой ISIN", type=["xlsx", "xls", "csv"])

with tab2:
    isin_input = st.text_area("Введите или вставьте ISIN (через Ctrl+V, пробел или запятую)", height=150)
    if st.button("🔍 Получить данные по введённым ISIN"):
        raw_text = isin_input.strip()
        if raw_text:
            isins = re.split(r"[\s,;]+", raw_text)
            isins = [i.strip().upper() for i in isins if i.strip()]
            results = fetch_isins_parallel(isins)
            st.session_state["results"] = pd.DataFrame(results)
            st.success("✅ Данные успешно получены!")

# === Обработка файла ===
if uploaded_file:
    if not st.session_state["file_loaded"] or uploaded_file.name != st.session_state["last_file_name"]:
        st.session_state["file_loaded"] = True
        st.session_state["last_file_name"] = uploaded_file.name
        try:
            if uploaded_file.name.lower().endswith(".csv"):
                df = pd.read_csv(uploaded_file, dtype=str)
            else:
                df = pd.read_excel(uploaded_file, dtype=str)
        except Exception as e:
            st.error(f"Ошибка чтения загруженного файла: {e}")
            st.stop()

        df.columns = [c.strip().upper() for c in df.columns]
        if "ISIN" not in df.columns:
            st.error("❌ В файле должна быть колонка 'ISIN'.")
            st.stop()
        isins = df["ISIN"].dropna().unique().tolist()
        isins = [str(x).strip().upper() for x in isins if str(x).strip()]
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
    except Exception:
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
