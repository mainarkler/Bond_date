const { useEffect, useMemo, useState } = React;

const API = {
  menu: "/api/menu",
  repo: "/api/repo",
  calendar: "/api/calendar",
  vm: "/api/vm",
  sellStres: "/api/sell-stres",
};

function Loader() {
  return <div className="animate-pulse text-blue-300">Загрузка...</div>;
}

function ErrorBox({ message }) {
  if (!message) return null;
  return <div className="rounded-lg border border-red-500/60 bg-red-950/50 p-3 text-red-200">{message}</div>;
}

function DataTable({ rows }) {
  if (!rows?.length) return <p className="text-slate-400">Нет данных.</p>;
  const headers = Object.keys(rows[0]);
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm border-collapse">
        <thead>
          <tr className="bg-slate-800 text-slate-200">
            {headers.map((h) => (
              <th key={h} className="border border-slate-700 px-3 py-2">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={idx} className="odd:bg-slate-900 even:bg-slate-900/60">
              {headers.map((h) => (
                <td key={h} className="border border-slate-800 px-3 py-2">{String(row[h] ?? "-")}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function MobileNav({ open, onClose, cards, onOpenModule }) {
  if (!open) return null;
  return (
    <div className="mobile-drawer fixed inset-0 z-40 bg-slate-950/80 lg:hidden">
      <div className="absolute right-0 top-0 h-full w-72 border-l border-slate-800 bg-slate-900 p-4">
        <div className="mb-4 flex items-center justify-between">
          <h3 className="text-lg font-semibold">Навигация</h3>
          <button onClick={onClose} className="rounded-md bg-slate-800 px-3 py-1">✕</button>
        </div>
        <div className="space-y-2">
          {cards.map((card) => (
            <button
              key={card.id}
              onClick={() => {
                onOpenModule(card.id);
                onClose();
              }}
              className="w-full rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 text-left hover:bg-slate-700"
            >
              {card.icon} {card.title}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

function App() {
  const [cards, setCards] = useState([]);
  const [menuLoading, setMenuLoading] = useState(true);
  const [menuError, setMenuError] = useState("");

  const [activeModule, setActiveModule] = useState(null);
  const [isins, setIsins] = useState("RU000A0JWSQ7 RU000A1033M8");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [rows, setRows] = useState([]);
  const [drawerOpen, setDrawerOpen] = useState(false);

  useEffect(() => {
    // Main menu loads list of modules from backend to keep frontend dynamic.
    fetch(API.menu)
      .then((r) => {
        if (!r.ok) throw new Error("Не удалось загрузить меню");
        return r.json();
      })
      .then((data) => setCards(data.modules || []))
      .catch((e) => setMenuError(e.message))
      .finally(() => setMenuLoading(false));
  }, []);

  const activeCard = useMemo(() => cards.find((c) => c.id === activeModule), [cards, activeModule]);

  async function openModule(moduleId) {
    setActiveModule(moduleId);
    setRows([]);
    setError("");

    if (moduleId === "vm") {
      await runSimpleFetch(API.vm);
    }
    if (moduleId === "sell_stres") {
      await runSimpleFetch(API.sellStres);
    }
  }

  async function runSimpleFetch(url) {
    setLoading(true);
    try {
      const res = await fetch(url);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Ошибка API");
      setRows([data]);
    } catch (e) {
      setError(e.message || "Сетевая ошибка");
    } finally {
      setLoading(false);
    }
  }

  async function submitIsinForm(endpoint) {
    setLoading(true);
    setError("");
    setRows([]);
    try {
      // Fetch/AJAX integration with Flask API for async UI updates.
      const res = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ isins }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Ошибка API");
      setRows(data.rows || []);
    } catch (e) {
      setError(e.message || "Ошибка сети");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="mx-auto max-w-7xl px-4 py-6 lg:px-8">
      <header className="mb-8 flex items-center justify-between gap-3 rounded-2xl border border-slate-800 bg-slate-900/70 p-5 shadow-card">
        <div>
          <h1 className="text-2xl font-bold md:text-3xl">🏠 Главное меню</h1>
          <p className="mt-1 text-sm text-slate-400">React + Tailwind frontend с подключением к Python API.</p>
        </div>
        <button className="rounded-lg border border-slate-700 bg-slate-800 px-4 py-2 lg:hidden" onClick={() => setDrawerOpen(true)}>
          ☰
        </button>
      </header>

      <MobileNav open={drawerOpen} onClose={() => setDrawerOpen(false)} cards={cards} onOpenModule={openModule} />

      {menuLoading ? <Loader /> : <ErrorBox message={menuError} />}

      <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {cards.map((card) => (
          <article key={card.id} className="glow-card rounded-2xl border border-slate-800 bg-slate-900 p-5 shadow-card">
            <div className="mb-2 text-3xl">{card.icon}</div>
            <h2 className="text-xl font-semibold">{card.title}</h2>
            <p className="mt-2 min-h-16 text-sm text-slate-400">{card.description}</p>
            <button
              className="mt-4 w-full rounded-lg bg-blue-600 px-4 py-2 text-base font-medium hover:bg-blue-500 active:scale-[0.98]"
              onClick={() => openModule(card.id)}
            >
              Открыть
            </button>
          </article>
        ))}
      </section>

      <section className="mt-8 rounded-2xl border border-slate-800 bg-slate-900/80 p-5">
        <h3 className="text-xl font-semibold">{activeCard ? `${activeCard.icon} ${activeCard.title}` : "Выберите модуль"}</h3>

        {(activeModule === "repo" || activeModule === "calendar") && (
          <div className="mt-4 space-y-3">
            <label className="text-sm text-slate-300">ISIN (через пробел/запятую)</label>
            <textarea
              value={isins}
              onChange={(e) => setIsins(e.target.value)}
              rows={4}
              className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2"
            />
            <button
              onClick={() => submitIsinForm(activeModule === "repo" ? API.repo : API.calendar)}
              className="rounded-lg bg-emerald-600 px-4 py-2 font-medium hover:bg-emerald-500"
            >
              Загрузить данные
            </button>
          </div>
        )}

        <div className="mt-4 space-y-3">
          {loading && <Loader />}
          <ErrorBox message={error} />
          {!loading && !error && rows.length > 0 && <DataTable rows={rows} />}
        </div>
      </section>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
