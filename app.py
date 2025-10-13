import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from io import BytesIO

st.set_page_config(page_title="Обработка ISIN", page_icon="📈", layout="wide")
st.title("📈 Получение данных по ISIN с MOEX")

# === Session state для сохранения данных между действиями ===
if "results" not in st.session_state:
    st.session_state["results"] = None
if "file_loaded" not in st.session_state:
    st.session_state["file_loaded"] = False
if "last_file_name" not in st.session_state:
    st.session_state["last_file_name"] = None

# === Настройки подсветки ===
st.subheader("⚙️ Настройки выделения дат")

# Инициализация session_state по умолчанию
if "overnight" not in st.session_state:
    st.session_state["overnight"] = False
if "extra_days" not in st.session_state:
    st.session_state["extra_days"] = 2

# === Очистка данных и формы настроек ===
if st.button("🔄 Очистить форму"):
    # Сброс формы настроек
    st.session_state["overnight"] = False
    st.session_state["extra_days"] = 2

    # Сброс данных и файла
    st.session_state["results"] = None
    st.session_state["file_loaded"] = False
    st.session_state["last_file_name"] = None

    # Перезапуск приложения, чтобы виджеты обновились
    st.rerun()

# Overnight чекбокс
overnight = st.checkbox(
    "Overnight (3 дня)",
    key="overnight",
)

# Дополнительные дни
extra_days_input = st.number_input(
    "Дополнительные дни (2–366):",
    min_value=2,
    max_value=366,
    step=1,
    disabled=st.session_state["overnight"],
    key="extra_days",  # значение берётся из session_state
)

# Динамическая подсказка
if st.session_state["overnight"]:
    st.markdown("<span style='color:gray'>Дополнительные дни отключены при включенном Overnight</span>", unsafe_allow_html=True)

# Расчёт days_threshold
days_threshold = 3 if st.session_state["overnight"] else 1 + st.session_state["extra_days"]

st.write(f"Текущее значение границы подсветки: {days_threshold} дн.")

# === Функция получения SECID (для ОФЗ и нестандартных инструментов) ===
def get_secid(isin):
    url = f"https://iss.moex.com/iss/securities.json?q={isin}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        if data.get("securities", {}).get("data"):
            for row in data["securities"]["data"]:
                if "TQCB" in row:
                    return row[0]
        return None
    except Exception:
        return None

# === Основная функция получения данных по ISIN ===
def get_bond_data(isin):
    try:
        # --- Попытка 1: стандартное API ---
        url_info = f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{isin}.json"
        response_info = requests.get(url_info, timeout=10)
        if response_info.status_code == 200:
            data_info = response_info.json()
            rows_info = data_info.get("securities", {}).get("data", [])
            columns_info = data_info.get("securities", {}).get("columns", [])

            if rows_info:
                info_dict = dict(zip(columns_info, rows_info[0]))
                secname = info_dict.get("SECNAME")
                maturity_date = info_dict.get("MATDATE")
                put_date = info_dict.get("PUTOPTIONDATE")
                call_date = info_dict.get("CALLOPTIONDATE")
            else:
                raise ValueError("Нет данных по ISIN")
        # --- Попытка 2: общий список (ОФЗ) ---
        else:
            secid = get_secid(isin)
            if not secid:
                return None
            url_info = f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{secid}.json"
            response_info = requests.get(url_info, timeout=10)
            response_info.raise_for_status()
            data_info = response_info.json()
            rows_info = data_info.get("securities", {}).get("data", [])
            columns_info = data_info.get("securities", {}).get("columns", [])
            if rows_info:
                info_dict = dict(zip(columns_info, rows_info[0]))
                secname = info_dict.get("SECNAME")
                maturity_date = info_dict.get("MATDATE")
                put_date = info_dict.get("PUTOPTIONDATE")
                call_date = info_dict.get("CALLOPTIONDATE")
            else:
                return None

        # --- Даты купонов ---
        url_coupons = f"https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{isin}.json?iss.only=coupons&iss.meta=off"
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
                    future_dates = pd.to_datetime(df_coupons[col], errors="coerce")
                    future_dates = future_dates[future_dates >= today]
                    return future_dates.min() if not future_dates.empty else None
                return None

            record_date = next_date("recorddate")
            coupon_date = next_date("coupondate")
        else:
            record_date = coupon_date = None

        def fmt(date):
            if pd.isna(date) or not date:
                return None
            try:
                return pd.to_datetime(date).strftime("%Y-%m-%d")
            except Exception:
                return None

        return {
            "Оригинальный ISIN": isin,
            "Наименование инструмента": secname,
            "Дата погашения": fmt(maturity_date),
            "Дата оферты Put": fmt(put_date),
            "Дата оферты Call": fmt(call_date),
            "Дата фиксации купона": fmt(record_date),
            "Дата купона": fmt(coupon_date),
        }

    except Exception:
        return None

# === Стили таблицы ===
def style_df(row):
    today = datetime.today().date()
    danger_threshold = today + timedelta(days=days_threshold)
    key_dates = [
        "Дата погашения",
        "Дата оферты Put",
        "Дата оферты Call",
        "Дата фиксации купона",
        "Дата купона",
    ]
    if all(pd.isna(row[col]) for col in key_dates):
        return ["background-color: DimGray"] * len(row)
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

# === Загрузка файла ===
uploaded_file = st.file_uploader("Загрузите Excel или CSV с колонкой ISIN:", type=["xlsx", "xls", "csv"])
if uploaded_file:
    if not st.session_state["file_loaded"] or uploaded_file.name != st.session_state["last_file_name"]:
        st.session_state["file_loaded"] = True
        st.session_state["last_file_name"] = uploaded_file.name

        status_area = st.empty()
        status_area.info("🔍 Этап 1: поиск через стандартное API...")

        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        if "ISIN" not in df.columns:
            st.error("❌ В файле должна быть колонка 'ISIN'.")
            st.stop()

        isins = df["ISIN"].dropna().unique().tolist()
        results = []
        unfound = []
        progress_bar = st.progress(0)

        # === Этап 1 ===
        for idx, isin in enumerate(isins, start=1):
            data = get_bond_data(isin)
            if data:
                results.append(data)
            else:
                unfound.append(isin)
            progress_bar.progress(idx / len(isins))

        status_area.info(f"✅ Найдено {len(results)} инструментов. Осталось {len(unfound)} для дополнительной проверки.")

        # === Этап 2 ===
        if unfound:
            status_area.info("🔍 Этап 2: поиск оставшихся через общий список (ОФЗ)...")
            for idx, isin in enumerate(unfound, start=1):
                data = get_bond_data(isin)
                if data:
                    results.append(data)

        # --- Финальные данные ---
        st.session_state["results"] = pd.DataFrame(results)
        status_area.empty()
        st.success("✅ Данные успешно получены!")

# === Вывод таблицы ===
if st.session_state["results"] is not None:
    styled_df = st.session_state["results"].style.apply(style_df, axis=1)
    st.dataframe(styled_df, use_container_width=True)

    def to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Данные")
        return output.getvalue()

    st.download_button(
        label="💾 Скачать результат (Excel)",
        data=to_excel(st.session_state["results"]),
        file_name="bond_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("👆 Загрузите файл с ISIN для начала.")
