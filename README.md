# Bond Date Frontend (React + Tailwind) + Python Backend

## Directory structure

```text
Bond_date/
├── app.py
├── requirements.txt
├── Procfile
├── render.yaml
├── templates/
│   └── index.html
├── static/
│   ├── css/
│   │   └── styles.css
│   └── js/
│       └── app.js
└── README.md
```

## Local run

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

## Render deployment (optimized)

### What is optimized for Render

- App reads `PORT` from environment (required by Render runtime).
- Debug mode is disabled by default (`FLASK_DEBUG=0`).
- Production server uses Gunicorn instead of Flask dev server.
- Dependencies minimized to only required runtime packages for faster builds.
- Added `render.yaml` blueprint for one-click infra setup.

### Option A: deploy using `render.yaml`

1. Push repository to GitHub.
2. In Render: **New +** → **Blueprint**.
3. Select repository and confirm creation.
4. Render will use:
   - `buildCommand: pip install -r requirements.txt`
   - `startCommand: gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 60`

### Option B: deploy manually in Render UI

- **Runtime**: Python 3
- **Build Command**:

```bash
pip install -r requirements.txt
```

- **Start Command**:

```bash
gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 60
```

- **Environment variables**:
  - `FLASK_DEBUG=0`

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
  - `GET /api/vm` — таблица расчета VM (лонг/шорт на 1 лот)
  - `GET /api/sell-stres` — таблица стресс-метрик продажи

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


## VM / Sell_stres

- `GET /api/vm` — таблица расчета VM (оценка long/short на 1 лот по TQBR)
- `GET /api/sell-stres` — таблица стресс-метрик продажи (изменение цены + спред)
- `GET /api/sell-strass` — алиас для совместимости с альтернативным названием блока
