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
        # убираем лишние кавычки, но аккуратно — не ломаем CSV
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

# === Кэширование XML TQOB и TQCB (устойчивый парсинг) ===
@st.cache_data(ttl=3600)
def fetch_board_xml(board: str):
    """
    Загружает XML-выгрузку MOEX для board (tqob/tqcb) и возвращает словарь ISIN -> атрибутная карта (dict)
    Для простоты мы сохраняем secid и emitterid (если есть).
    """
    url = f"https://iss.moex.com/iss/engines/stock/markets/bonds/boards/{board.lower()}/securities.xml?marketprice_board=3&iss.meta=off"
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
        xml_content = r.content.decode("utf-8", errors="ignore")

        # убрать namespace, если есть (позволяет безопасно парсить тег row)
        xml_content = re.sub(r'\sxmlns="[^"]+"', "", xml_content, count=1)
        root = ET.fromstring(xml_content)

        mapping = {}  # ISIN -> { "SECID": secid, "EMITTERID": emitterid, ... }
        for el in root.iter():
            # тег может быть '{...}row' — проверяем окончание
            if el.tag.lower().endswith("row"):
                # приводим ключи атрибутов к верхнему регистру для надежности
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

# отладочный вывод (удали или закомментируй в продакшн)
st.write("🔎 Проверка TQOB (пример):", TQOB_MAP.get("RU000A101N52"))

# === Функция поиска эмитента и SECID ===
@st.cache_data(ttl=3600)
def fetch_emitter_and_secid(isin: str):
    """
    Возвращает (emitter_id, secid).
    Логика:
      1) прямой запрос /iss/securities/{isin}.json
      2) прямой xml /iss/securities/{isin}.xml
      3) поиск в TQOB/TQCB мапах
      4) поиск через /iss/securities.json?q={isin}
    """
    isin = str(isin).strip().upper()
    if not isin:
        return None, None

    emitter_id = None
    secid = None

    # --- 1) прямой JSON ---
    try:
        url = f"https://iss.moex.com/iss/securities/{isin}.json"
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        securities = data.get("securities", {})
        cols = securities.get("columns", [])
        rows = securities.get("data", [])
        if rows:
            # берем только первую строку
            first = rows[0]
            col_map = {c.upper(): i for i, c in enumerate(cols)}
            if "EMITTER_ID" in col_map:
                emitter_id = first[col_map["EMITTER_ID"]]
            if "SECID" in col_map:
                secid = first[col_map["SECID"]]
    except Exception:
        # молча пропускаем — переходи к следующему шагу
        pass

    # --- 2) прямой XML ---
    if not emitter_id or not secid:
        try:
            url = f"https://iss.moex.com/iss/securities/{isin}.xml?iss.meta=off"
            r = session.get(url, timeout=10)
            r.raise_for_status()
            # возможно есть namespace — удаляем
            xml_content = r.content.decode("utf-8", errors="ignore")
            xml_content = re.sub(r'\sxmlns="[^"]+"', "", xml_content, count=1)
            root = ET.fromstring(xml_content)
            # поиск пар 'name'='EMITTER_ID' и 'name'='SECID' в result xml
            for node in root.iter():
                # элементы в xml могут иметь атрибуты name / value
                name_attr = (node.attrib.get("name") or node.attrib.get("NAME") or "").strip().upper()
                value_attr = (node.attrib.get("value") or node.attrib.get("VALUE") or "").strip()
                if name_attr == "EMITTER_ID" and not emitter_id:
                    emitter_id = value_attr
                if name_attr == "SECID" and not secid:
                    secid = value_attr
                # если нашли оба — можно прервать
                if emitter_id and secid:
                    break
        except Exception:
            pass

    # --- 3) поиск в TQOB / TQCB ---
    if not secid:
        m = TQOB_MAP.get(isin)
        if m and m.get("SECID"):
            secid = m.get("SECID")
            if not emitter_id:
                emitter_id = m.get("EMITTERID")
        else:
            m2 = TQCB_MAP.get(isin)
            if m2 and m2.get("SECID"):
                secid = m2.get("SECID")
                if not emitter_id:
                    emitter_id = m2.get("EMITTERID")

    # --- 4) глобальный поиск по q= (иногда помогает) ---
    if not secid:
        try:
            url = f"https://iss.moex.com/iss/securities.json?q={isin}&iss.meta=off"
            r = session.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            securities = data.get("securities", {})
            cols = securities.get("columns", [])
            rows = securities.get("data", [])
            if rows:
                col_map = {c.upper(): i for i, c in enumerate(cols)}
                for row in rows:
                    # иногда isin может быть не точным - проверяем вхождение
                    row_isin = None
                    if "ISIN" in col_map:
                        row_isin = (row[col_map["ISIN"]] or "").strip().upper()
                    if row_isin == isin or isin in [str(x).strip().upper() for x in row]:
                        if "SECID" in col_map:
                            secid = row[col_map["SECID"]]
                        if "EMITTER_ID" in col_map and not emitter_id:
                            emitter_id = row[col_map["EMITTER_ID"]]
                        if secid:
                            break
        except Exception:
            pass

    # нормализуем пустые строки в None
    if emitter_id == "":
        emitter_id = None
    if secid == "":
        secid = None

    return emitter_id, secid

# === Получение данных по ISIN ===
def get_bond_data(isin):
    try:
        emitter_id, secid = fetch_emitter_and_secid(isin)
        secname = maturity_date = put_date = call_date = None
        record_date = coupon_date = None

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
                        # ключи в JSON приходят в исходном регистре — используем как есть и в upper-версии
                        secname = info.get("SECNAME") or info.get("SEC_NAME") or info.get("SECNAME".lower())
                        maturity_date = info.get("MATDATE")
                        put_date = info.get("PUTOPTIONDATE")
                        call_date = info.get("CALLOPTIONDATE")
            except Exception:
                pass

        # --- Купоны ---
        if secid:
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
                pass

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
        }

    except Exception as e:
        st.warning(f"Ошибка при обработке {isin}: {e}")
        return {
            "ISIN": isin,
            "Код эмитента": "",
            "Наименование инструмента": "",
            "Дата погашения": None,
            "Дата оферты Put": None,
            "Дата оферты Call": None,
            "Дата фиксации купона": None,
            "Дата купона": None,
        }

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
