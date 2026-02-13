# Bond Date Frontend (React + Tailwind) + Python Backend

## Directory structure

```text
Bond_date/
├── app.py
├── requirements.txt
├── templates/
│   └── index.html
├── static/
│   ├── css/
│   │   └── styles.css
│   └── js/
│       └── app.js
└── README.md
```

## Run

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Start backend + frontend server (Flask serves API and frontend files):

```bash
python app.py
```

3. Open in browser:

```text
http://127.0.0.1:8000
```

## Frontend/Backend integration notes

- `templates/index.html` loads React and Tailwind via CDN and mounts app into `#root`.
- `static/js/app.js` contains modular React components:
  - Menu cards (`Претрейд РЕПО`, `Календарь выплат`, `Расчет VM`, `Sell_stres`)
  - Mobile drawer navigation
  - Loader + error UI
  - Dynamic table rendering
- `fetch()` calls backend endpoints:
  - `GET /api/menu` — card metadata
  - `POST /api/repo` — ISIN analysis for repo pretrade
  - `POST /api/calendar` — coupon/amortization schedule
  - `GET /api/vm` — VM placeholder response
  - `GET /api/sell-stres` — Sell_stres placeholder response

## API request examples

### Repo

```bash
curl -X POST http://127.0.0.1:8000/api/repo \
  -H "Content-Type: application/json" \
  -d '{"isins":"RU000A0JWSQ7 RU000A1033M8"}'
```

### Calendar

```bash
curl -X POST http://127.0.0.1:8000/api/calendar \
  -H "Content-Type: application/json" \
  -d '{"isins":"RU000A0JWSQ7 RU000A1033M8"}'
```
