import React, { useEffect, useMemo, useState } from "react";
import "./mmaDatabase.css";

const API_BASE = import.meta?.env?.VITE_API_BASE || "http://127.0.0.1:8000";

/* ---------------------------------
   API
---------------------------------- */
async function apiGet(path) {
  const res = await fetch(`${API_BASE}${path}`);
  const json = await res.json().catch(() => ({}));
  if (!res.ok || json?.ok === false) {
    throw new Error(json?.detail || json?.error || `HTTP ${res.status}`);
  }
  return json;
}

/* ---------------------------------
   Helpers
---------------------------------- */
function fmtPct(x) {
  if (x === null || x === undefined) return "—";
  return `${Math.round(Number(x) * 100)}%`;
}

function fmtNum(x, digits = 1) {
  if (x === null || x === undefined) return "—";
  const n = Number(x);
  if (Number.isNaN(n)) return "—";
  return n.toFixed(digits);
}

function fmtHeightIn(heightIn) {
  if (heightIn === null || heightIn === undefined) return "—";
  const inches = Math.round(Number(heightIn));
  if (Number.isNaN(inches)) return "—";
  const feet = Math.floor(inches / 12);
  const rem = inches % 12;
  return `${feet}'${rem}"`;
}

function fmtWeightLbs(weight) {
  if (weight === null || weight === undefined) return "—";
  const n = Number(weight);
  if (Number.isNaN(n)) return "—";
  return `${Math.round(n)} lb`;
}

function fmtReachIn(reach) {
  if (reach === null || reach === undefined) return "—";
  const n = Number(reach);
  if (Number.isNaN(n)) return "—";
  return `${Math.round(n)} in`;
}

function resultChip(win) {
  if (win === 1) return { label: "W", cls: "mmaChip mmaChipWin" };
  if (win === 0) return { label: "L", cls: "mmaChip mmaChipLoss" };
  return { label: "NC", cls: "mmaChip" };
}

function safeFighterName(name) {
  const s = String(name || "").trim();
  if (s.length < 3 || s.length > 45) return false;

  const bad = [
    "Sports-Statistics",
    "Fight Statistics",
    "Upcoming UFC Events",
    "Scheduled UFC Events",
    "Privacy Policy",
    "Contact Us",
    "Odds",
    "How Odds Work",
    "Home >",
    "UFC Fight Statistics",
  ];

  return !bad.some((b) => s.includes(b));
}

function alphaSortFighters(list) {
  return [...list].sort((a, b) => {
    const aa = String(a?.fighter || "").toLowerCase();
    const bb = String(b?.fighter || "").toLowerCase();
    return aa.localeCompare(bb);
  });
}

function buildRecord(fights = [], latestRollup = null) {
  if (latestRollup) {
    const wins = Number(latestRollup?.wins || 0);
    const losses = Number(latestRollup?.losses || 0);
    const draws = Number(latestRollup?.draws || 0);
    const noContests = Number(latestRollup?.no_contests || 0);

    let out = `${wins}-${losses}`;
    if (draws > 0) out += `-${draws}`;
    if (noContests > 0) out += ` (${noContests} NC)`;
    return out;
  }

  let wins = 0;
  let losses = 0;
  let draws = 0;
  let noContests = 0;

  fights.forEach((f) => {
    if (f?.is_win === 1) wins += 1;
    else if (f?.is_win === 0) losses += 1;
    else if (String(f?.result || "").toLowerCase().includes("draw")) draws += 1;
    else noContests += 1;
  });

  if (!wins && !losses && !draws && !noContests) return "—";

  let out = `${wins}-${losses}`;
  if (draws > 0) out += `-${draws}`;
  if (noContests > 0) out += ` (${noContests} NC)`;
  return out;
}

function statValue(row, keys = []) {
  for (const key of keys) {
    if (row?.[key] !== null && row?.[key] !== undefined) return row[key];
  }
  return null;
}

/* ---------------------------------
   Fight Modal
---------------------------------- */
function FightDetailModal({ open, onClose, fightUrl }) {
  const [loading, setLoading] = useState(false);
  const [detail, setDetail] = useState(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    if (!open || !fightUrl) return;

    let alive = true;
    setLoading(true);
    setErr("");
    setDetail(null);

    apiGet(`/mma/history/fight_v1/detail?fight_url=${encodeURIComponent(fightUrl)}`)
      .then((d) => alive && setDetail(d))
      .catch((e) => alive && setErr(String(e.message || e)))
      .finally(() => alive && setLoading(false));

    return () => {
      alive = false;
    };
  }, [open, fightUrl]);

  if (!open) return null;

  const meta = detail?.meta || [];
  const fighters = Array.from(new Set(meta.map((r) => r.fighter))).sort();
  const totals = detail?.totals || [];
  const totalKeys = Array.from(new Set(totals.map((r) => r.stat_key))).sort();
  const rounds = detail?.rounds || [];
  const roundsList = Array.from(new Set(rounds.map((r) => r.round))).sort((a, b) => a - b);

  function getTotalVal(fighter, key) {
    const row = totals.find((r) => r.fighter === fighter && r.stat_key === key);
    return row ? row.a_value ?? row.value ?? row.a_landed ?? "—" : "—";
  }

  function getRoundVal(round, fighter, key) {
    const row = rounds.find((r) => r.round === round && r.fighter === fighter && r.stat_key === key);
    return row ? row.a_value ?? row.value ?? row.a_landed ?? "—" : "—";
  }

  return (
    <div className="mmaModalBackdrop" onClick={onClose}>
      <div className="mmaModal" onClick={(e) => e.stopPropagation()}>
        <div className="mmaModalHead">
          <div>
            <div className="mmaModalEyebrow">Fight Breakdown</div>
            <div className="mmaModalTitle">Round-by-Round Fight Detail</div>
          </div>
          <button className="mmaBtn" onClick={onClose}>Close</button>
        </div>

        <div className="mmaMuted mmaSmall mmaBreak mmaModalUrl">{fightUrl}</div>

        {loading && <div className="mmaPanelCard">Loading fight…</div>}
        {err && <div className="mmaPanelCard mmaErrorCard">Error: {err}</div>}

        {detail && (
          <>
            <div className="mmaTwoUp">
              <div className="mmaPanelCard">
                <div className="mmaSectionTitle">Fight Summary</div>
                {fighters.map((f) => {
                  const m = meta.find((x) => x.fighter === f) || {};
                  const c = resultChip(m.is_win);
                  return (
                    <div key={f} className="mmaLineRow">
                      <span className={c.cls}>{c.label}</span>
                      <span className="mmaStrong">{f}</span>
                      <span className="mmaMuted">vs {m.opponent}</span>
                      <span className="mmaRight mmaMuted">
                        {m.method || "—"} · R{m.round || "—"} · {m.time || "—"}
                      </span>
                    </div>
                  );
                })}
              </div>

              <div className="mmaPanelCard">
                <div className="mmaSectionTitle">Fight Totals</div>
                <div className="mmaTableWrap">
                  <table className="mmaTable">
                    <thead>
                      <tr>
                        <th>Stat</th>
                        {fighters.map((f) => (
                          <th key={f}>{f}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {totalKeys.map((k) => (
                        <tr key={k}>
                          <td>{k}</td>
                          {fighters.map((f) => (
                            <td key={f}>{getTotalVal(f, k)}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            <div className="mmaPanelCard">
              <div className="mmaSectionTitle">Per-Round Stats</div>
              {roundsList.map((roundNo) => (
                <div key={roundNo} className="mmaRoundBlock">
                  <div className="mmaRoundTitle">Round {roundNo}</div>
                  <div className="mmaTableWrap">
                    <table className="mmaTable">
                      <thead>
                        <tr>
                          <th>Stat</th>
                          {fighters.map((f) => (
                            <th key={f}>{f}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {totalKeys.map((k) => (
                          <tr key={`${roundNo}-${k}`}>
                            <td>{k}</td>
                            {fighters.map((f) => (
                              <td key={f}>{getRoundVal(roundNo, f, k)}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

/* ---------------------------------
   Main Page
---------------------------------- */
export default function MMADatabasePage() {
  const [q, setQ] = useState("");
  const [fighters, setFighters] = useState([]);
  const [selected, setSelected] = useState("");

  const [profile, setProfile] = useState(null);
  const [fightList, setFightList] = useState([]);
  const [loadingList, setLoadingList] = useState(false);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [err, setErr] = useState("");

  const [modalOpen, setModalOpen] = useState(false);
  const [modalFightUrl, setModalFightUrl] = useState("");

  /* fighter list */
  useEffect(() => {
    let alive = true;
    setErr("");
    setLoadingList(true);

    apiGet(`/mma/history/fighters?q=${encodeURIComponent(q)}&limit=500`)
      .then((d) => {
        if (!alive) return;

        const cleaned = (d.fighters || []).filter((x) => safeFighterName(x?.fighter));
        const sorted = alphaSortFighters(cleaned);

        setFighters(sorted);

        if ((!selected || !sorted.some((f) => f.fighter === selected)) && sorted.length) {
          setSelected(sorted[0].fighter);
        }
      })
      .catch((e) => alive && setErr(String(e.message || e)))
      .finally(() => alive && setLoadingList(false));

    return () => {
      alive = false;
    };
  }, [q, selected]);

  /* selected fighter detail */
  useEffect(() => {
    if (!selected) return;

    let alive = true;
    setLoadingProfile(true);
    setErr("");
    setProfile(null);
    setFightList([]);

    Promise.all([
      apiGet(`/mma/history/fighter_v1/profile?fighter=${encodeURIComponent(selected)}`),
      apiGet(`/mma/history/fighter_v1/fight_list?fighter=${encodeURIComponent(selected)}&limit=250&offset=0`),
    ])
      .then(([p, fl]) => {
        if (!alive) return;
        setProfile(p);
        setFightList(fl.fights || []);
      })
      .catch((e) => alive && setErr(String(e.message || e)))
      .finally(() => alive && setLoadingProfile(false));

    return () => {
      alive = false;
    };
  }, [selected]);

  const bio = profile?.bio || {};
  const latest = profile?.latest_rollup || {};
  const last5 = profile?.last5_summary || {};
  const last5Fights = profile?.last5_fights || [];

  const fighterRecord = useMemo(() => buildRecord(fightList, latest), [fightList, latest]);

  const careerStats = useMemo(() => {
    return [
      { label: "Career Record", value: fighterRecord },
      { label: "Career Win Rate", value: fmtPct(latest.career_win_rate) },
      { label: "Last 5 Win Rate", value: fmtPct(last5.win_rate) },
      { label: "Sig Strikes Landed Avg", value: fmtNum(last5.sig_landed_avg, 1) },
      { label: "Sig Strikes Attempted Avg", value: fmtNum(last5.sig_attempted_avg, 1) },
      { label: "Takedowns Avg", value: `${fmtNum(last5.td_landed_avg, 2)} / ${fmtNum(last5.td_attempted_avg, 2)}` },
      { label: "Knockdowns Avg", value: fmtNum(last5.kd_avg, 2) },
      { label: "Fights Loaded", value: fightList.length || "—" },
      { label: "Current Weight Class", value: latest.weight_class ?? "—" },
    ];
  }, [fighterRecord, fightList.length, last5, latest]);

  const recentRoundsPreview = useMemo(() => {
    return last5Fights.slice(0, 5).map((f) => ({
      fight_url: f.fight_url,
      opponent: f.opponent,
      result: f.is_win,
      method: f.method,
      date: f.event_date ? String(f.event_date).slice(0, 10) : "—",
      round: f.finish_round ?? "—",
      time: f.finish_time ?? "—",
      sig_landed: statValue(f, ["sig_landed", "sig_strikes_landed", "sig_landed_total"]),
      sig_attempted: statValue(f, ["sig_attempted", "sig_strikes_attempted", "sig_attempted_total"]),
      td_landed: statValue(f, ["td_landed", "takedowns_landed"]),
      td_attempted: statValue(f, ["td_attempted", "takedowns_attempted"]),
      kd: statValue(f, ["kd", "knockdowns"]),
    }));
  }, [last5Fights]);

  return (
    <div className="mmaDbPage">
      <div className="mmaDbHeader">
        <div>
          <div className="mmaEyebrow">MMA Database</div>
          <h1 className="mmaMainTitle">Fighter Library</h1>
          <div className="mmaSubTitle">
            Search fighters alphabetically, open a premium fighter card, and drill into fight and round-level stats.
          </div>
        </div>
      </div>

      {err ? <div className="mmaGlobalError">Error: {err}</div> : null}

      <div className="mmaDbLayout">
        <aside className="mmaSidebar">
          <div className="mmaSidebarCard">
            <div className="mmaSidebarTitle">Fighters</div>

            <input
              className="mmaSearch"
              placeholder="Search fighter name..."
              value={q}
              onChange={(e) => setQ(e.target.value)}
            />

            <div className="mmaSidebarMeta">
              {loadingList ? "Loading fighters..." : `${fighters.length} fighters`}
            </div>

            <div className="mmaFighterList">
              {fighters.map((f) => (
                <button
                  key={f.fighter}
                  className={`mmaFighterListItem ${selected === f.fighter ? "active" : ""}`}
                  onClick={() => setSelected(f.fighter)}
                >
                  <span className="mmaFighterListName">{f.fighter}</span>
                </button>
              ))}
            </div>
          </div>
        </aside>

        <section className="mmaContent">
          {loadingProfile ? (
            <div className="mmaHeroCard">
              <div className="mmaMuted">Loading fighter card...</div>
            </div>
          ) : null}

          {!loadingProfile && selected ? (
            <>
              <div className="mmaHeroCard">
                <div className="mmaHeroTop">
                  <div>
                    <div className="mmaHeroName">{selected}</div>
                    <div className="mmaHeroNick">
                      {bio.nickname ? `"${bio.nickname}"` : "No nickname available"}
                    </div>
                  </div>

                  <div className="mmaHeroRecordBlock">
                    <div className="mmaRecordLabel">Record</div>
                    <div className="mmaRecordValue">{fighterRecord}</div>
                  </div>
                </div>

                <div className="mmaBioGrid">
                  <div className="mmaBioStat">
                    <span>Stance</span>
                    <strong>{bio.stance ?? "—"}</strong>
                  </div>
                  <div className="mmaBioStat">
                    <span>Height</span>
                    <strong>{fmtHeightIn(bio.height_in)}</strong>
                  </div>
                  <div className="mmaBioStat">
                    <span>Weight</span>
                    <strong>{fmtWeightLbs(bio.weight_lbs ?? latest.weight_lbs)}</strong>
                  </div>
                  <div className="mmaBioStat">
                    <span>Reach</span>
                    <strong>{fmtReachIn(bio.reach_in)}</strong>
                  </div>
                </div>
              </div>

              <div className="mmaInfoGrid">
                <div className="mmaPanelCard">
                  <div className="mmaSectionTitle">Fighter Profile</div>
                  <div className="mmaInfoList">
                    <div className="mmaLineRow"><span className="mmaMuted">Name</span><span className="mmaRight">{selected}</span></div>
                    <div className="mmaLineRow"><span className="mmaMuted">Nickname</span><span className="mmaRight">{bio.nickname ?? "—"}</span></div>
                    <div className="mmaLineRow"><span className="mmaMuted">Stance</span><span className="mmaRight">{bio.stance ?? "—"}</span></div>
                    <div className="mmaLineRow"><span className="mmaMuted">Height</span><span className="mmaRight">{fmtHeightIn(bio.height_in)}</span></div>
                    <div className="mmaLineRow"><span className="mmaMuted">Weight</span><span className="mmaRight">{fmtWeightLbs(bio.weight_lbs ?? latest.weight_lbs)}</span></div>
                    <div className="mmaLineRow"><span className="mmaMuted">Reach</span><span className="mmaRight">{fmtReachIn(bio.reach_in)}</span></div>
                    <div className="mmaLineRow"><span className="mmaMuted">Weight Class</span><span className="mmaRight">{latest.weight_class ?? "—"}</span></div>
                  </div>
                </div>

                <div className="mmaPanelCard">
                  <div className="mmaSectionTitle">Career Snapshot</div>
                  <div className="mmaStatGrid">
                    {careerStats.map((s) => (
                      <div key={s.label} className="mmaMiniStat">
                        <span>{s.label}</span>
                        <strong>{s.value}</strong>
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              <div className="mmaPanelCard">
                <div className="mmaSectionTitle">Recent Fight Log</div>
                <div className="mmaFightLog">
                  {fightList.map((f) => {
                    const c = resultChip(f.is_win);
                    return (
                      <button
                        key={f.fight_url}
                        className="mmaFightRow"
                        onClick={() => {
                          setModalFightUrl(f.fight_url);
                          setModalOpen(true);
                        }}
                      >
                        <span className={c.cls}>{c.label}</span>
                        <span className="mmaStrong">{f.opponent}</span>
                        <span className="mmaMuted">{f.method || "—"}</span>
                        <span className="mmaRight mmaMuted mmaSmall">
                          {f.event_date ? String(f.event_date).slice(0, 10) : "—"} · R{f.finish_round || "—"} · {f.finish_time || "—"}
                        </span>
                      </button>
                    );
                  })}
                  {!fightList.length ? <div className="mmaMuted">No fights found.</div> : null}
                </div>
              </div>

              <div className="mmaPanelCard">
                <div className="mmaSectionTitle">Fight Stats Preview</div>
                <div className="mmaTableWrap">
                  <table className="mmaTable">
                    <thead>
                      <tr>
                        <th>Date</th>
                        <th>Opponent</th>
                        <th>Result</th>
                        <th>Method</th>
                        <th>Round</th>
                        <th>Time</th>
                        <th>Sig Lnd</th>
                        <th>Sig Att</th>
                        <th>TD Lnd</th>
                        <th>TD Att</th>
                        <th>KD</th>
                      </tr>
                    </thead>
                    <tbody>
                      {recentRoundsPreview.map((r) => (
                        <tr key={r.fight_url}>
                          <td>{r.date}</td>
                          <td>{r.opponent}</td>
                          <td>{r.result === 1 ? "W" : r.result === 0 ? "L" : "NC"}</td>
                          <td>{r.method || "—"}</td>
                          <td>{r.round}</td>
                          <td>{r.time}</td>
                          <td>{r.sig_landed ?? "—"}</td>
                          <td>{r.sig_attempted ?? "—"}</td>
                          <td>{r.td_landed ?? "—"}</td>
                          <td>{r.td_attempted ?? "—"}</td>
                          <td>{r.kd ?? "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="mmaMuted mmaSmall mmaTableHint">
                  Click any fight in the log above to open full round-by-round fight detail.
                </div>
              </div>
            </>
          ) : null}
        </section>
      </div>

      <FightDetailModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        fightUrl={modalFightUrl}
      />
    </div>
  );
}