import React, { useEffect, useMemo, useState } from "react";

const API_BASE =
  import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, "") || "http://127.0.0.1:8000";

function fmt(n, digits = 2) {
  const v = Number(n);
  if (!Number.isFinite(v)) return "-";
  return v.toFixed(digits);
}

function pct(n, digits = 1) {
  const v = Number(n);
  if (!Number.isFinite(v)) return "-";
  return `${(v * 100).toFixed(digits)}%`;
}

function safeArray(x) {
  return Array.isArray(x) ? x : [];
}

function metricClass(v, good = 0.75, ok = 0.45) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "metric-neutral";
  if (x >= good) return "metric-good";
  if (x >= ok) return "metric-ok";
  return "metric-bad";
}

export default function MMAOptimizerPage() {
  const [slates, setSlates] = useState([]);
  const [selectedSlateId, setSelectedSlateId] = useState("");
  const [mode, setMode] = useState("gpp");

  const [inputs, setInputs] = useState(null);
  const [analysis, setAnalysis] = useState(null);
  const [optimize, setOptimize] = useState(null);

  const [search, setSearch] = useState("");
  const [salaryMin, setSalaryMin] = useState("");
  const [salaryMax, setSalaryMax] = useState("");
  const [sortKey, setSortKey] = useState("proj_mean");
  const [sortDir, setSortDir] = useState("desc");

  const [loadingSlates, setLoadingSlates] = useState(false);
  const [loadingSlate, setLoadingSlate] = useState(false);
  const [error, setError] = useState("");

  async function fetchJson(path) {
    const res = await fetch(`${API_BASE}${path}`);
    if (!res.ok) {
      const text = await res.text();
      throw new Error(text || `Request failed: ${res.status}`);
    }
    return res.json();
  }

  useEffect(() => {
    let live = true;
    async function loadSlates() {
      try {
        setLoadingSlates(true);
        setError("");
        const data = await fetchJson("/mma/dk/list_slates");
        if (!live) return;
        const nextSlates = safeArray(data.slates);
        setSlates(nextSlates);
        if (!selectedSlateId && nextSlates.length > 0) {
          setSelectedSlateId(nextSlates[0].slate_id);
        }
      } catch (err) {
        if (!live) return;
        setError(err.message || "Failed to load slates.");
      } finally {
        if (live) setLoadingSlates(false);
      }
    }
    loadSlates();
    return () => {
      live = false;
    };
  }, [selectedSlateId]);

  useEffect(() => {
    if (!selectedSlateId) return;

    let live = true;
    async function loadSlate() {
      try {
        setLoadingSlate(true);
        setError("");

        const [inputsData, analysisData, optimizeData] = await Promise.all([
          fetchJson(`/mma/dk/optimizer_inputs/${selectedSlateId}`),
          fetchJson(`/mma/dk/slate_analysis/${selectedSlateId}`),
          fetchJson(`/mma/dk/optimize/${selectedSlateId}?mode=${mode}`),
        ]);

        if (!live) return;
        setInputs(inputsData);
        setAnalysis(analysisData);
        setOptimize(optimizeData);
      } catch (err) {
        if (!live) return;
        setError(err.message || "Failed to load optimizer slate.");
      } finally {
        if (live) setLoadingSlate(false);
      }
    }

    loadSlate();
    return () => {
      live = false;
    };
  }, [selectedSlateId, mode]);

  const players = useMemo(() => safeArray(inputs?.players), [inputs]);
  const fights = useMemo(() => {
  const grouped = new Map();

  for (const p of safeArray(inputs?.players)) {
    const fid = p.fight_id || "unknown";
    if (!grouped.has(fid)) {
      grouped.set(fid, {
        fight_id: fid,
        fight_time: p.fight_time || null,
        fighters: [],
      });
    }
    grouped.get(fid).fighters.push(p);
  }

  return Array.from(grouped.values()).sort((a, b) =>
    String(a.fight_time || "").localeCompare(String(b.fight_time || ""))
  );
}, [inputs]);  
  const lineups = useMemo(() => safeArray(optimize?.optimizer?.lineups), [optimize]);

  const filteredPlayers = useMemo(() => {
    let out = [...players];

    const q = search.trim().toLowerCase();
    if (q) {
      out = out.filter((p) => {
        const a = String(p.player_name || "").toLowerCase();
        const b = String(p.opponent_name || "").toLowerCase();
        return a.includes(q) || b.includes(q);
      });
    }

    if (salaryMin !== "") {
      const min = Number(salaryMin);
      out = out.filter((p) => Number(p.salary || 0) >= min);
    }

    if (salaryMax !== "") {
      const max = Number(salaryMax);
      out = out.filter((p) => Number(p.salary || 0) <= max);
    }

    out.sort((a, b) => {
      const av = a?.[sortKey];
      const bv = b?.[sortKey];

      const an = Number(av);
      const bn = Number(bv);

      let cmp = 0;
      if (Number.isFinite(an) && Number.isFinite(bn)) {
        cmp = an - bn;
      } else {
        cmp = String(av ?? "").localeCompare(String(bv ?? ""));
      }

      return sortDir === "asc" ? cmp : -cmp;
    });

    return out;
  }, [players, search, salaryMin, salaryMax, sortKey, sortDir]);

  const slateSummary = useMemo(() => {
    const summary = analysis?.analysis?.summary || {};
    const leaders = analysis?.analysis?.leaders || {};

    return {
      playerCount: players.length,
      fightCount: fights.length,
      lineupCount: Number(optimize?.optimizer?.count || 0),
      excluded: Number(inputs?.excluded_count || 0),
      value75: summary.value_p75,
      ceiling90: summary.ceiling_p90,
      own90: summary.own_p90,
      topMeanLeader: leaders.top_mean?.[0]?.name || "-",
      topCeilingLeader: leaders.top_ceiling?.[0]?.name || "-",
      topValueLeader: leaders.top_value?.[0]?.name || "-",
    };
  }, [analysis, optimize, inputs, players, fights]);

  return (
    <div className="page-shell">
      <div className="optimizer-page">
        <div className="page-hero">
          <div>
            <div className="page-kicker">DraftMindIQ // MMA</div>
            <h1 className="page-title">Optimizer Lab</h1>
            <p className="page-subtitle">
              Full slate view with player pool, fight cards, lineup generation, and slate
              analysis in one place.
            </p>
          </div>

          <div className="hero-actions">
            <div className="control-block">
              <label className="control-label">Slate</label>
              <select
                className="control-input"
                value={selectedSlateId}
                onChange={(e) => setSelectedSlateId(e.target.value)}
                disabled={loadingSlates}
              >
                {slates.map((s) => (
                  <option key={s.slate_id} value={s.slate_id}>
                    {s.slate_name || s.slate_id}
                  </option>
                ))}
              </select>
            </div>

            <div className="control-block">
              <label className="control-label">Mode</label>
              <div className="mode-switch">
                <button
                  className={`mode-btn ${mode === "gpp" ? "active" : ""}`}
                  onClick={() => setMode("gpp")}
                >
                  GPP
                </button>
                <button
                  className={`mode-btn ${mode === "cash" ? "active" : ""}`}
                  onClick={() => setMode("cash")}
                >
                  Cash
                </button>
              </div>
            </div>
          </div>
        </div>

        {error ? <div className="error-banner">{error}</div> : null}

        <div className="summary-grid">
          <div className="summary-card">
            <div className="summary-label">Players</div>
            <div className="summary-value">{slateSummary.playerCount}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Fights</div>
            <div className="summary-value">{slateSummary.fightCount}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Lineups</div>
            <div className="summary-value">{slateSummary.lineupCount}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Excluded</div>
            <div className="summary-value">{slateSummary.excluded}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Top Mean</div>
            <div className="summary-value summary-text">{slateSummary.topMeanLeader}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Top Ceiling</div>
            <div className="summary-value summary-text">{slateSummary.topCeilingLeader}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Top Value</div>
            <div className="summary-value summary-text">{slateSummary.topValueLeader}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Ceiling P90</div>
            <div className="summary-value">{fmt(slateSummary.ceiling90)}</div>
          </div>
        </div>

        <div className="page-grid">
          <section className="panel panel-wide">
            <div className="panel-header">
              <h2>Fight Cards</h2>
              <span className="panel-meta">{loadingSlate ? "Loading..." : `${fights.length} fights`}</span>
            </div>

            <div className="fight-grid">
              {fights.map((fight) => {
                const fightersInCard = safeArray(fight.fighters);
                return (
                  <div className="fight-card" key={fight.fight_id}>
                    <div className="fight-time">{fight.fight_time || "TBD"}</div>
                    {fightersInCard.map((fp, idx) => (
                      <div className="fight-fighter" key={`${fight.fight_id}-${idx}`}>
                        <div className="fight-name">{fp.player_name || fp.name}</div>
                        <div className="fight-stats">
                          <span>${fp.salary || "-"}</span>
                          <span>Mean {fmt(fp.proj_mean || fp.proj)}</span>
                          <span>Win {pct(fp.p_win)}</span>
                          <span>Fin {pct(fp.finish_equity)}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                );
              })}
            </div>
          </section>

          <section className="panel">
            <div className="panel-header">
              <h2>Slate Controls</h2>
              <span className="panel-meta">Pool filters</span>
            </div>

            <div className="filter-grid">
              <div className="control-block">
                <label className="control-label">Search fighter</label>
                <input
                  className="control-input"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Name or opponent"
                />
              </div>

              <div className="control-block">
                <label className="control-label">Min Salary</label>
                <input
                  className="control-input"
                  value={salaryMin}
                  onChange={(e) => setSalaryMin(e.target.value)}
                  placeholder="6700"
                />
              </div>

              <div className="control-block">
                <label className="control-label">Max Salary</label>
                <input
                  className="control-input"
                  value={salaryMax}
                  onChange={(e) => setSalaryMax(e.target.value)}
                  placeholder="9500"
                />
              </div>

              <div className="control-block">
                <label className="control-label">Sort</label>
                <select
                  className="control-input"
                  value={sortKey}
                  onChange={(e) => setSortKey(e.target.value)}
                >
                  <option value="proj_mean">Projection</option>
                  <option value="proj_ceiling">Ceiling</option>
                  <option value="value">Value</option>
                  <option value="p_win">Win %</option>
                  <option value="finish_equity">Finish Equity</option>
                  <option value="salary">Salary</option>
                  <option value="risk">Risk</option>
                </select>
              </div>

              <div className="control-block">
                <label className="control-label">Direction</label>
                <select
                  className="control-input"
                  value={sortDir}
                  onChange={(e) => setSortDir(e.target.value)}
                >
                  <option value="desc">Desc</option>
                  <option value="asc">Asc</option>
                </select>
              </div>
            </div>
          </section>

          <section className="panel panel-wide">
            <div className="panel-header">
              <h2>Player Pool</h2>
              <span className="panel-meta">{filteredPlayers.length} visible</span>
            </div>

            <div className="table-wrap">
              <table className="optimizer-table">
                <thead>
                  <tr>
                    <th>Fighter</th>
                    <th>Opp</th>
                    <th>Salary</th>
                    <th>Mean</th>
                    <th>Floor</th>
                    <th>Ceiling</th>
                    <th>Win %</th>
                    <th>Finish</th>
                    <th>Value</th>
                    <th>Risk</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredPlayers.map((p) => (
                    <tr key={p.player_id}>
                      <td>
                        <div className="cell-name">{p.player_name}</div>
                        <div className="cell-sub">{p.edge_note || "-"}</div>
                      </td>
                      <td>{p.opponent_name || "-"}</td>
                      <td>{p.salary || "-"}</td>
                      <td>{fmt(p.proj_mean)}</td>
                      <td>{fmt(p.proj_floor)}</td>
                      <td>{fmt(p.proj_ceiling)}</td>
                      <td className={metricClass(p.p_win, 0.7, 0.55)}>{pct(p.p_win)}</td>
                      <td className={metricClass(p.finish_equity, 0.4, 0.2)}>
                        {pct(p.finish_equity)}
                      </td>
                      <td>{fmt(p.value, 3)}</td>
                      <td>{fmt(p.risk, 3)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="panel panel-wide">
            <div className="panel-header">
              <h2>Generated Lineups</h2>
              <span className="panel-meta">{lineups.length} built</span>
            </div>

            <div className="lineup-list">
              {lineups.map((lu, idx) => (
                <div className="lineup-card" key={`lineup-${idx}`}>
                  <div className="lineup-top">
                    <div>
                      <div className="lineup-title">Lineup {idx + 1}</div>
                      <div className="lineup-sub">
                        Score {fmt(lu.score, 2)} · Salary {lu.salary} · Mean {fmt(lu.total_mean)}
                      </div>
                    </div>
                    <div className="lineup-metrics">
                      <span>Ceil {fmt(lu.total_ceiling)}</span>
                      <span>Own {fmt(lu.total_ownership)}</span>
                      <span>Risk {fmt(lu.avg_risk, 3)}</span>
                    </div>
                  </div>

                  <div className="lineup-players">
                    {safeArray(lu.players).map((p) => (
                      <div className="lineup-player" key={`${idx}-${p.player_id}`}>
                        <div className="lineup-player-name">{p.player_name}</div>
                        <div className="lineup-player-meta">
                          <span>${p.salary}</span>
                          <span>M {fmt(p.proj_mean)}</span>
                          <span>C {fmt(p.proj_ceiling)}</span>
                          <span>W {pct(p.p_win)}</span>
                          <span>F {pct(p.finish_equity)}</span>
                          <span>V {fmt(p.value, 3)}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </section>
        </div>
      </div>

      <style>{`
        .page-shell {
          padding: 24px;
          color: #e8ecf3;
          background:
            radial-gradient(circle at top left, rgba(102, 126, 234, 0.12), transparent 30%),
            radial-gradient(circle at top right, rgba(16, 185, 129, 0.10), transparent 28%),
            #0b1020;
          min-height: 100vh;
        }

        .optimizer-page {
          max-width: 1600px;
          margin: 0 auto;
        }

        .page-hero {
          display: flex;
          justify-content: space-between;
          gap: 20px;
          align-items: end;
          margin-bottom: 24px;
          padding: 24px;
          border: 1px solid rgba(255,255,255,0.08);
          border-radius: 24px;
          background: linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02));
          box-shadow: 0 20px 40px rgba(0,0,0,0.25);
        }

        .page-kicker {
          font-size: 12px;
          letter-spacing: 0.18em;
          text-transform: uppercase;
          color: #8ea2c9;
          margin-bottom: 8px;
        }

        .page-title {
          margin: 0;
          font-size: 36px;
          line-height: 1.05;
        }

        .page-subtitle {
          margin: 10px 0 0;
          color: #9fb0cf;
          max-width: 760px;
        }

        .hero-actions {
          display: flex;
          gap: 16px;
          align-items: end;
          flex-wrap: wrap;
        }

        .summary-grid {
          display: grid;
          grid-template-columns: repeat(8, minmax(0, 1fr));
          gap: 14px;
          margin-bottom: 24px;
        }

        .summary-card,
        .panel {
          border: 1px solid rgba(255,255,255,0.08);
          border-radius: 22px;
          background: rgba(14, 20, 38, 0.92);
          box-shadow: 0 14px 34px rgba(0,0,0,0.20);
        }

        .summary-card {
          padding: 16px;
        }

        .summary-label {
          color: #8ea2c9;
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }

        .summary-value {
          margin-top: 8px;
          font-size: 28px;
          font-weight: 700;
        }

        .summary-text {
          font-size: 18px;
          line-height: 1.2;
        }

        .page-grid {
          display: grid;
          grid-template-columns: 2fr 1fr;
          gap: 18px;
        }

        .panel {
          padding: 18px;
        }

        .panel-wide {
          grid-column: span 2;
        }

        .panel-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 14px;
        }

        .panel-header h2 {
          margin: 0;
          font-size: 20px;
        }

        .panel-meta {
          color: #8ea2c9;
          font-size: 13px;
        }

        .fight-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
          gap: 14px;
        }

        .fight-card {
          padding: 16px;
          border-radius: 18px;
          background: rgba(255,255,255,0.03);
          border: 1px solid rgba(255,255,255,0.06);
        }

        .fight-time {
          color: #87c9ff;
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          margin-bottom: 10px;
        }

        .fight-fighter + .fight-fighter {
          margin-top: 10px;
          padding-top: 10px;
          border-top: 1px solid rgba(255,255,255,0.06);
        }

        .fight-name {
          font-weight: 700;
          margin-bottom: 4px;
        }

        .fight-stats {
          display: flex;
          gap: 10px;
          flex-wrap: wrap;
          color: #9fb0cf;
          font-size: 13px;
        }

        .filter-grid {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 14px;
        }

        .control-block {
          display: flex;
          flex-direction: column;
          gap: 6px;
        }

        .control-label {
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          color: #8ea2c9;
        }

        .control-input {
          height: 42px;
          border-radius: 12px;
          border: 1px solid rgba(255,255,255,0.10);
          background: rgba(255,255,255,0.04);
          color: #eef3ff;
          padding: 0 12px;
          outline: none;
        }

        .mode-switch {
          display: flex;
          gap: 8px;
        }

        .mode-btn {
          height: 42px;
          min-width: 88px;
          border-radius: 12px;
          border: 1px solid rgba(255,255,255,0.10);
          background: rgba(255,255,255,0.03);
          color: #cdd7ec;
          cursor: pointer;
          font-weight: 700;
        }

        .mode-btn.active {
          background: linear-gradient(90deg, rgba(99,102,241,0.35), rgba(16,185,129,0.22));
          color: #fff;
          border-color: rgba(99,102,241,0.45);
        }

        .table-wrap {
          overflow-x: auto;
        }

        .optimizer-table {
          width: 100%;
          border-collapse: collapse;
        }

        .optimizer-table th,
        .optimizer-table td {
          padding: 12px 10px;
          border-bottom: 1px solid rgba(255,255,255,0.06);
          text-align: left;
          vertical-align: top;
          font-size: 14px;
        }

        .optimizer-table th {
          color: #8ea2c9;
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }

        .cell-name {
          font-weight: 700;
        }

        .cell-sub {
          margin-top: 4px;
          color: #8ea2c9;
          font-size: 12px;
          max-width: 420px;
          word-break: break-word;
        }

        .metric-good { color: #57d38c; }
        .metric-ok { color: #f7c66b; }
        .metric-bad { color: #ff7b7b; }
        .metric-neutral { color: #cdd7ec; }

        .lineup-list {
          display: grid;
          gap: 14px;
        }

        .lineup-card {
          padding: 16px;
          border-radius: 18px;
          background: rgba(255,255,255,0.03);
          border: 1px solid rgba(255,255,255,0.06);
        }

        .lineup-top {
          display: flex;
          justify-content: space-between;
          gap: 16px;
          margin-bottom: 12px;
          align-items: start;
        }

        .lineup-title {
          font-size: 18px;
          font-weight: 800;
        }

        .lineup-sub {
          color: #9fb0cf;
          margin-top: 4px;
          font-size: 13px;
        }

        .lineup-metrics {
          display: flex;
          gap: 10px;
          flex-wrap: wrap;
          color: #8ea2c9;
          font-size: 13px;
        }

        .lineup-players {
          display: grid;
          gap: 10px;
        }

        .lineup-player {
          border-radius: 14px;
          padding: 12px;
          background: rgba(255,255,255,0.03);
        }

        .lineup-player-name {
          font-weight: 700;
          margin-bottom: 6px;
        }

        .lineup-player-meta {
          display: flex;
          gap: 10px;
          flex-wrap: wrap;
          color: #9fb0cf;
          font-size: 13px;
        }

        .error-banner {
          margin-bottom: 16px;
          border: 1px solid rgba(255,99,99,0.35);
          background: rgba(255,99,99,0.10);
          color: #ffd4d4;
          border-radius: 16px;
          padding: 12px 14px;
        }

        @media (max-width: 1280px) {
          .summary-grid {
            grid-template-columns: repeat(4, minmax(0, 1fr));
          }
          .page-grid {
            grid-template-columns: 1fr;
          }
          .panel-wide {
            grid-column: span 1;
          }
        }

        @media (max-width: 820px) {
          .page-shell {
            padding: 16px;
          }
          .page-hero {
            flex-direction: column;
            align-items: stretch;
          }
          .summary-grid {
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }
          .filter-grid {
            grid-template-columns: 1fr;
          }
        }
      `}</style>
    </div>
  );
}