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
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        securities = data.get("securities", {})
        cols = securities.get("columns", [])
        rows = securities.get("data", [])
        if rows:
            first = rows[0]
            col_map = {c.upper(): i for i, c in enumerate(cols)}
            # некоторые поля имеют разное написание — пробуем несколько вариантов
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
    try:
        emitter_id, secid = fetch_emitter_and_secid(isin)
        secname = maturity_date = put_date = call_date = None
        record_date = coupon_date = None
        coupon_currency = None

        # --- Попытка получить информацию по SECID (если есть) ---
        if secid:
            try:
                url_info = f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{secid}.json"
                r = session.get(url_info, timeout=10)
                r.raise_for_status()
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
                r = session.get(url_info_isin, timeout=10)
                r.raise_for_status()
                data_info_isin = r.json()
                rows = data_info_isin.get("securities", {}).get("data", [])
                cols = data_info_isin.get("securities", {}).get("columns", [])
                if rows and cols:
                    info = dict(zip([c.upper() for c in cols], rows[0]))
                    secname = secname or info.get("SECNAME") or info.get("SEC_NAME")
                    maturity_date = maturity_date or info.get("MATDATE") or info.get("MATDATE")
                    # также пробуем варианты имён полей
                    put_date = put_date or info.get("PUTOPTIONDATE") or info.get("PUT_OPTION_DATE")
                    call_date = call_date or info.get("CALLOPTIONDATE") or info.get("CALL_OPTION_DATE")
            except Exception:
                pass

        # --- Купоны: ищем ближайший будущий COUPONDATE и RECORDDATE ---
        # Попробуем сначала statistics/bondization по SECID (если есть), затем по ISIN (fallback)
        coupons_data = None
        columns_coupons = []
        bondization_currency = None
        # helper to fetch bondization for either secid or isin
        def try_fetch_bondization(identifier):
            try:
                # statistics endpoint обычно принимает SECID; но попробуем подставить и ISIN (как fallback)
                url_coupons = (
                    "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/"
                    f"bondization/{identifier}.json?iss.only=coupons,bondization&iss.meta=off"
                )
                r = session.get(url_coupons, timeout=10)
                r.raise_for_status()
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
                coupons_data_fallback, columns_coupons_fallback, bondization_currency_fallback = (
                    try_fetch_bondization(isin)
                )
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
            # унифицируем имена колонок
            cols_upper = [c.upper() for c in df_coupons.columns]
            df_coupons.columns = cols_upper

            today = pd.to_datetime(datetime.today().date())

            # найти колонки потенциально называемые COUPONDATE / COUPON_DATE / COUPON_DATE etc.
            possible_coupon_cols = [c for c in cols_upper if "COUPON" in c and "DATE" in c]
            possible_record_cols = [c for c in cols_upper if "RECORD" in c and "DATE" in c]

            # вспомогательная функция: выбрать минимальную дату > today
            def next_future_date(series):
                try:
                    s = pd.to_datetime(series, errors="coerce")
                    s = s[s >= today + pd.Timedelta(days=0)]  # >= today
                    if not s.empty:
                        nxt = s.min()
                        # вернуть строку в формате YYYY-MM-DD
                        return nxt.strftime("%Y-%m-%d")
                except Exception:
                    pass
                return None

            # Найдём ближайший купон
            coupon_found = None
            for col in possible_coupon_cols:
                candidate = next_future_date(df_coupons[col])
                if candidate:
                    coupon_found = candidate
                    break

            # Найдём ближайшую дату фиксации купона (record date)
            record_found = None
            for col in possible_record_cols:
                candidate = next_future_date(df_coupons[col])
                if candidate:
                    record_found = candidate
                    break

            # Если не нашли через колонки с именами содержащими COUPON/RECORD — попытаемся искать по типу данных
            if not coupon_found:
                # проверим все колонки на будущие даты и возьмём ближайшую
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

            if not record_found:
                # аналогично для record (если нет явной колонки)
                # возможно RECORDDATE представлена как DATE + TYPE = RECORD — но мы делаем простую попытку
                record_found = None  # уже попытались выше

            coupon_date = coupon_found
            record_date = record_found

        coupon_currency = bondization_currency or coupon_currency

        # fallback на XML-борды, если ещё нет дат:
        if (not record_date or not coupon_date) and (TQOB_MAP or TQCB_MAP):
            m = TQOB_MAP.get(isin) or TQCB_MAP.get(isin)
            if m:
                # поля в мапе могут быть в разной форме; пробуем несколько ключей
                if not record_date:
                    record_date = m.get("RECORDDATE") or m.get("RECORD_DATE") or m.get("RECORD")
                if not coupon_date:
                    coupon_date = m.get("COUPONDATE") or m.get("COUPON_DATE") or m.get("COUPON")

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
            "Валюта купона": "",
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
