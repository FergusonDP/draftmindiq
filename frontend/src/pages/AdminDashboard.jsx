import React, { useEffect, useMemo, useState } from "react";
import { Link, NavLink } from "react-router-dom";
import { useSettings } from "../app/SettingsContext.jsx";
import "./adminDashboard.css";

const SPORTS = [
  { key: "ALL", label: "All Sports" },
  { key: "MMA", label: "MMA" },
  { key: "NFL", label: "NFL" },
  { key: "NBA", label: "NBA" },
  { key: "MLB", label: "MLB" },
  { key: "NHL", label: "NHL" },
];

const SPORT_KEYS = ["MMA", "NFL", "NBA", "MLB", "NHL"];

const MODULES = [
  {
    key: "MMA",
    title: "MMA Operations Hub",
    desc: "Checks • balances • reconciliation • optimizer controls",
    to: "/admin/mma",
    highlights: [
      "Checks & balances monitor",
      "Fight, slate, and odds reconciliation",
      "Optimizer rebuild and cleanup tools",
    ],
    tone: "good",
  },
  {
    key: "NFL",
    title: "NFL Operations",
    desc: "Ingestion • integrity • admin tooling",
    to: "/optimizer/nfl",
    highlights: [
      "Pipeline scaffold",
      "Future validation layer",
      "Admin hooks reserved",
    ],
    tone: "neutral",
  },
  {
    key: "NBA",
    title: "NBA Operations",
    desc: "Feed controls • status • upcoming admin tools",
    to: "/optimizer/nba",
    highlights: [
      "Module placeholder",
      "Operational tooling later",
      "UI ready",
    ],
    tone: "neutral",
  },
  {
    key: "MLB",
    title: "MLB Operations",
    desc: "Weather • stacks • admin monitoring",
    to: "/optimizer/mlb",
    highlights: [
      "Module placeholder",
      "Data model TBD",
      "Admin hub later",
    ],
    tone: "neutral",
  },
  {
    key: "NHL",
    title: "NHL Operations",
    desc: "Lines • correlation • control panel",
    to: "/optimizer/nhl",
    highlights: [
      "Module placeholder",
      "Data model TBD",
      "Admin hub later",
    ],
    tone: "neutral",
  },
];

const ADMIN_TOOLS = [
  {
    title: "Run Checks & Balances",
    desc: "Verifies slate rows, fighter pairings, odds coverage, duplicates, and optimizer readiness.",
    tone: "good",
  },
  {
    title: "Reconcile Slate vs Odds",
    desc: "Finds mismatches between the active DK slate and current odds/fight mappings.",
    tone: "warn",
  },
  {
    title: "Rebuild Fight Map",
    desc: "Re-links fighters into valid fights and flags orphaned or changed records.",
    tone: "neutral",
  },
  {
    title: "Rebuild Optimizer Inputs",
    desc: "Refreshes optimizer-ready data after slate, odds, or fight changes.",
    tone: "neutral",
  },
];

function safeJson(x) {
  return x && typeof x === "object" ? x : null;
}

function formatPublished(iso) {
  try {
    if (!iso) return "";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "";
    return d.toLocaleString();
  } catch {
    return "";
  }
}

function parseTime(iso) {
  try {
    const t = new Date(iso).getTime();
    return Number.isNaN(t) ? 0 : t;
  } catch {
    return 0;
  }
}

function getAdminHealthFallback() {
  return {
    code: "attention_needed",
    label: "Attention Needed",
    summary: "2 fight changes detected • optimizer refresh recommended",
    tone: "warn",
    counts: {
      activeFights: 12,
      changedFights: 2,
      missingOdds: 1,
      orphanRows: 0,
      duplicateRows: 0,
    },
    lastSync: "10 minutes ago",
    optimizerReady: false,
  };
}

export default function AdminDashboard() {
  const { settings } = useSettings();
  const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

  const [sport, setSport] = useState(settings.favoriteSport || "ALL");
  const [tick, setTick] = useState(Date.now());

  const [news, setNews] = useState({ ok: false, items: [], count: 0 });
  const [videos, setVideos] = useState({ ok: false, items: [], count: 0 });
  const [contentLoading, setContentLoading] = useState(false);

  const [videoOffset, setVideoOffset] = useState(0);
  const [mmaHealth, setMmaHealth] = useState(getAdminHealthFallback());

  const shouldCycle = sport === "ALL";

  useEffect(() => {
    const timer = setInterval(() => setTick(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    if (settings.favoriteSport && settings.favoriteSport !== sport) {
      setSport(settings.favoriteSport);
    }
  }, [settings.favoriteSport, sport]);

  const clockLabel = useMemo(() => {
    return new Date(tick).toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      timeZone: settings.timezone,
    });
  }, [tick, settings.timezone]);

  const dateLabel = useMemo(() => {
    return new Date(tick).toLocaleDateString("en-US", {
      weekday: "long",
      month: "short",
      day: "numeric",
      timeZone: settings.timezone,
    });
  }, [tick, settings.timezone]);

  const visibleModules = useMemo(() => {
    if (sport === "ALL") return MODULES;
    return MODULES.filter((m) => m.key === sport);
  }, [sport]);

  const pulse = useMemo(() => {
    return [
      { label: "System Status", value: mmaHealth.label, hint: mmaHealth.summary, tone: mmaHealth.tone },
      {
        label: "Optimizer",
        value: mmaHealth.optimizerReady ? "READY" : "REFRESH NEEDED",
        hint: "MMA optimizer state",
        tone: mmaHealth.optimizerReady ? "good" : "warn",
      },
      {
        label: "Live Feed",
        value: sport,
        hint: shouldCycle ? "mixed sports feed" : "filtered feed",
        tone: "neutral",
      },
      {
        label: "Last Sync",
        value: mmaHealth.lastSync,
        hint: "latest MMA health update",
        tone: "neutral",
      },
    ];
  }, [mmaHealth, sport, shouldCycle]);

  const adminFeed = useMemo(() => {
    return [
      {
        title: "MMA Health",
        sub: mmaHealth.summary,
        pill: mmaHealth.label,
      },
      {
        title: "Changed Fights",
        sub: `${mmaHealth.counts.changedFights} detected in current compare pass`,
        pill: String(mmaHealth.counts.changedFights),
      },
      {
        title: "Missing Odds",
        sub: `${mmaHealth.counts.missingOdds} fights missing odds coverage`,
        pill: String(mmaHealth.counts.missingOdds),
      },
      {
        title: "Optimizer State",
        sub: mmaHealth.optimizerReady ? "Optimizer is up to date" : "Optimizer should be rebuilt",
        pill: mmaHealth.optimizerReady ? "READY" : "STALE",
      },
    ];
  }, [mmaHealth]);

  useEffect(() => {
    let alive = true;

    async function loadMmaHealth() {
      try {
        const res = await fetch(`${API_BASE}/admin/mma/health`);
        if (!res.ok) throw new Error("health_failed");
        const json = safeJson(await res.json());
        if (!alive || !json) return;

        setMmaHealth({
          code: json.status || "attention_needed",
          label:
            json.status === "healthy"
              ? "System Healthy"
              : json.status === "broken"
              ? "System Problem"
              : json.status === "stale"
              ? "Refresh Recommended"
              : "Attention Needed",
          summary: json.summary || "MMA health summary unavailable",
          tone:
            json.status === "healthy"
              ? "good"
              : json.status === "broken"
              ? "bad"
              : "warn",
          counts: {
            activeFights: json?.counts?.active_fights ?? 0,
            changedFights: json?.counts?.changed_fights ?? 0,
            missingOdds: json?.counts?.missing_odds ?? 0,
            orphanRows: json?.counts?.orphan_rows ?? 0,
            duplicateRows: json?.counts?.duplicate_rows ?? 0,
          },
          lastSync: json.last_sync_at
            ? formatPublished(json.last_sync_at)
            : "Unavailable",
          optimizerReady: Boolean(json.optimizer_ready),
        });
      } catch {
        if (!alive) return;
        setMmaHealth(getAdminHealthFallback());
      }
    }

    loadMmaHealth();
    const timer = setInterval(loadMmaHealth, 30000);

    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [API_BASE]);

  useEffect(() => {
    let alive = true;

    async function fetchNewsFor(key) {
      const res = await fetch(`${API_BASE}/news/${key}?limit=10`);
      const j = safeJson(await res.json());
      if (!j || !j.ok) return [];
      return (j.items || []).map((it) => ({ ...it, sport: key }));
    }

    async function fetchVideosFor(key) {
      const res = await fetch(`${API_BASE}/video/${key}?limit=8`);
      const j = safeJson(await res.json());
      if (!j || !j.ok) return [];
      return (j.items || []).map((it) => ({ ...it, sport: key }));
    }

    async function load() {
      setContentLoading(true);
      try {
        if ((sport || "ALL").toUpperCase() === "ALL") {
          const [newsLists, videoLists] = await Promise.all([
            Promise.all(SPORT_KEYS.map((k) => fetchNewsFor(k))),
            Promise.all(SPORT_KEYS.map((k) => fetchVideosFor(k))),
          ]);

          if (!alive) return;

          const mergedNews = newsLists.flat().sort((a, b) => parseTime(b.published_at) - parseTime(a.published_at));
          const mergedVideos = videoLists.flat();

          setNews({ ok: true, items: mergedNews.slice(0, 10), count: mergedNews.length });
          setVideos({ ok: true, items: mergedVideos.slice(0, 12), count: mergedVideos.length });
          setVideoOffset(0);
          return;
        }

        const sportKey = (sport || "MMA").toUpperCase();
        const [nRes, vRes] = await Promise.all([
          fetch(`${API_BASE}/news/${sportKey}?limit=10`),
          fetch(`${API_BASE}/video/${sportKey}?limit=12`),
        ]);

        const nJson = safeJson(await nRes.json());
        const vJson = safeJson(await vRes.json());

        if (!alive) return;

        setNews(nJson && nJson.ok ? nJson : { ok: false, items: [], count: 0 });
        setVideos(vJson && vJson.ok ? vJson : { ok: false, items: [], count: 0 });
        setVideoOffset(0);
      } catch {
        if (!alive) return;
        setNews({ ok: false, items: [], count: 0 });
        setVideos({ ok: false, items: [], count: 0 });
      } finally {
        if (alive) setContentLoading(false);
      }
    }

    load();
    return () => {
      alive = false;
    };
  }, [API_BASE, sport]);

  useEffect(() => {
    if (!shouldCycle) return;
    const t = setInterval(() => setVideoOffset((x) => x + 1), 8000);
    return () => clearInterval(t);
  }, [shouldCycle]);

  const railHint = useMemo(() => {
    const base = sport === "ALL" ? "Operations feed + rotating videos" : `${sport} feed + videos`;
    return contentLoading ? `${base} • loading…` : base;
  }, [sport, contentLoading]);

  const cycledVideos = useMemo(() => {
    const items = videos?.items || [];
    if (!items.length) return [];
    const take = 4;
    const start = (videoOffset || 0) % items.length;
    const out = [];

    for (let i = 0; i < Math.min(take, items.length); i++) {
      out.push(items[(start + i) % items.length]);
    }

    return out;
  }, [videos, videoOffset]);

  const shownVideos = useMemo(() => {
    if (!videos?.ok) return [];
    if (shouldCycle) return cycledVideos;
    return (videos.items || []).slice(0, 4);
  }, [videos, shouldCycle, cycledVideos]);

  const pillForNews = (it) => {
    return sport === "ALL" ? it.sport || "NEWS" : sport;
  };

  return (
    <div className="hub adminHub">
      <nav className="topNav" aria-label="Top navigation">
        <Link to="/dashboard" className="brand">
          <img className="brandIcon" src="/draftmindiq_head_transparent.png" alt="DraftMind" />
          <div className="brandText">
            <span className="brandWord">DraftMindIQ</span>
            <span className="brandSub">Admin Control Center</span>
          </div>
          <img className="brandIcon" src="/draftmindiq_iq_transparent.png" alt="IQ" />
        </Link>

        <div className="navCenter">
          <div className="navTabs" role="navigation" aria-label="Primary tabs">
            <NavLink to="/dashboard" className={({ isActive }) => `navTab ${isActive ? "active" : ""}`}>
              Dashboard
            </NavLink>
            <NavLink to="/databases" className={({ isActive }) => `navTab ${isActive ? "active" : ""}`}>
              Databases
            </NavLink>
            <NavLink to="/settings" className={({ isActive }) => `navTab ${isActive ? "active" : ""}`}>
              Settings
            </NavLink>
            <NavLink to="/admin/mma" className={({ isActive }) => `navTab ${isActive ? "active" : ""}`}>
              MMA Hub
            </NavLink>
          </div>

          <div className="navSport">
            <div className="sportSelectLabel">Feed</div>
            <select className="sportSelect" value={sport} onChange={(e) => setSport(e.target.value)}>
              {SPORTS.map((s) => (
                <option key={s.key} value={s.key}>
                  {s.label}
                </option>
              ))}
            </select>
          </div>
        </div>

        <div className="navRight">
          <div className="navTime">
            <span className="hubDot" aria-hidden="true" />
            <span className="navClock">{clockLabel}</span>
            <span className="navTz">{settings.timezone}</span>
          </div>
          <div className="navDateSmall">{dateLabel}</div>
        </div>
      </nav>

      <header className="hubHeader compact">
        <div className="hubHeaderLeft">
          <div className="hubKicker">Operations</div>
          <div className="hubH1">Admin Control Center</div>
          <div className="hubMeta">
            <span className="hubMetaMuted">
              Monitor health, reconcile data, and control optimizer readiness.
            </span>
          </div>
        </div>
      </header>

      <section className="pulseRow" aria-label="Admin pulse">
        {pulse.map((p) => (
          <div key={p.label} className={`pulseCard tone-${p.tone}`}>
            <div className="pulseLabel">{p.label}</div>
            <div className="pulseValue">{p.value}</div>
            <div className="pulseHint">{p.hint}</div>
          </div>
        ))}
      </section>

      <div className="hubGrid">
        <div className="leftCol">
          <Link to="/admin/mma" className="heroCard adminHeroCard">
            <div className="heroOverlay" aria-hidden="true" />
            <div className="heroTop">
              <div className="heroTag">Featured Admin Module • MMA</div>
              <div className={`heroChip adminHealthChip tone-${mmaHealth.tone}`}>{mmaHealth.label}</div>
            </div>

            <div className="heroTitle">MMA Operations Hub</div>
            <div className="heroSub">
              Clear operational status, issue visibility, and button-driven admin controls for your MMA workflow.
            </div>

            <div className="adminHealthGrid">
              <div className="adminHealthCell">
                <span>Active Fights</span>
                <strong>{mmaHealth.counts.activeFights}</strong>
              </div>
              <div className="adminHealthCell">
                <span>Changed</span>
                <strong>{mmaHealth.counts.changedFights}</strong>
              </div>
              <div className="adminHealthCell">
                <span>Missing Odds</span>
                <strong>{mmaHealth.counts.missingOdds}</strong>
              </div>
              <div className="adminHealthCell">
                <span>Optimizer</span>
                <strong>{mmaHealth.optimizerReady ? "Ready" : "Refresh"}</strong>
              </div>
            </div>

            <div className="heroBullets">
              <div className="heroBullet">
                <span className="heroBulletDot" aria-hidden="true" />
                <span>{mmaHealth.summary}</span>
              </div>
              <div className="heroBullet">
                <span className="heroBulletDot" aria-hidden="true" />
                <span>Last sync: {mmaHealth.lastSync}</span>
              </div>
            </div>

            <div className="heroCtaRow">
              <div className="heroCta">Open MMA Admin Hub</div>
              <div className="heroCtaArrow" aria-hidden="true">
                →
              </div>
            </div>
          </Link>

          <section className="moduleGrid" aria-label="Admin modules">
            {visibleModules.map((m) => (
              <div key={m.key} className={`moduleCard tone-${m.tone}`}>
                <div className="moduleTop">
                  <div className="moduleTitle">{m.title}</div>
                  <div className="modulePill adminModulePill">{m.key}</div>
                </div>

                <div className="moduleDesc">{m.desc}</div>

                <div className="moduleList">
                  {m.highlights.map((x) => (
                    <div key={x} className="moduleItem">
                      <span className="moduleDot" aria-hidden="true" />
                      <span>{x}</span>
                    </div>
                  ))}
                </div>

                <div className="moduleActions">
                  <Link to={m.to} className="moduleBtn">
                    Open <span aria-hidden="true">→</span>
                  </Link>
                  <div className="moduleHint">Admin module</div>
                </div>
              </div>
            ))}
          </section>

          <section className="adminToolGrid" aria-label="Primary admin actions">
            {ADMIN_TOOLS.map((tool) => (
              <div key={tool.title} className={`adminToolCard tone-${tool.tone}`}>
                <div className="adminToolTitle">{tool.title}</div>
                <div className="adminToolDesc">{tool.desc}</div>
                <Link to="/admin/mma" className="moduleBtn">
                  Open Tool
                </Link>
              </div>
            ))}
          </section>

          <section className="blockGrid" aria-label="Admin blocks">
            <div className="blockCard adminBlock">
              <div className="blockHeader">MMA Hub Status</div>
              <div className="blockBody">
                {mmaHealth.label} • {mmaHealth.summary}
              </div>
              <div className="blockFooter">
                <span className="pill">ACTIVE FIGHTS {mmaHealth.counts.activeFights}</span>
                <span className="pill">CHANGED {mmaHealth.counts.changedFights}</span>
                <span className="pill">MISSING ODDS {mmaHealth.counts.missingOdds}</span>
              </div>
            </div>

            <div className="blockCard adminBlock">
              <div className="blockHeader">Checks & Balances Queue</div>
              <div className="blockBody">
                Run validation before optimizer export. Use the MMA Hub to reconcile changed fights, missing odds, and stale inputs.
              </div>
              <div className="blockFooter">
                <span className="pill">VALIDATION</span>
                <span className="pill">RECONCILE</span>
                <span className="pill">OPTIMIZER</span>
              </div>
            </div>

            <div className="blockCard adminBlock">
              <div className="blockHeader">Cleanup Targets</div>
              <div className="blockBody">
                Dead rows, duplicates, orphan pairings, and outdated optimizer payloads should all be cleaned from one place.
              </div>
              <div className="blockFooter">
                <span className="pill">DUPES {mmaHealth.counts.duplicateRows}</span>
                <span className="pill">ORPHANS {mmaHealth.counts.orphanRows}</span>
                <span className="pill">STALE INPUTS</span>
              </div>
            </div>

            <div className="blockCard adminBlock">
              <div className="blockHeader">Release Notes</div>
              <div className="blockBody">
                Use this section later for system releases, scraper changes, optimizer updates, and reconciliation logic changes.
              </div>
              <div className="blockFooter">
                <span className="pill">RELEASES</span>
                <span className="pill">SCRAPER</span>
                <span className="pill">PATCHES</span>
              </div>
            </div>
          </section>
        </div>

        <aside className="rightCol" aria-label="Right rail">
          <div className="railHeader">
            <div className="railTitle">Operations Feed</div>
            <div className="railHint">{railHint}</div>
          </div>

          <div className="railStack">
            {adminFeed.map((n) => (
              <div key={n.title} className="newsCard">
                <div className="newsTop">
                  <div className="newsTitle">{n.title}</div>
                  <div className="newsPill">{n.pill}</div>
                </div>
                <div className="newsSub">{n.sub}</div>
              </div>
            ))}

            {news.ok &&
              news.items &&
              news.items.slice(0, 3).map((it) => {
                const published = formatPublished(it.published_at);
                const sub = published || it.source || "News";
                return (
                  <a
                    key={it.url}
                    className="newsCard"
                    href={it.url}
                    target="_blank"
                    rel="noreferrer"
                    style={{ textDecoration: "none" }}
                  >
                    <div className="newsTop">
                      <div className="newsTitle">{it.title}</div>
                      <div className="newsPill">{pillForNews(it)}</div>
                    </div>
                    <div className="newsSub">{sub}</div>
                  </a>
                );
              })}
          </div>

          <div className="railCTA">
            <div className="railCTATitle">Latest Videos</div>
            <div className="railCTASub">
              Admin side keeps videos secondary. Core purpose here is system visibility and controls.
            </div>

            <div className="toolLinks">
              {shownVideos && shownVideos.length > 0 ? (
                shownVideos.map((v) => (
                  <a key={v.url} href={v.url} target="_blank" rel="noreferrer">
                    {v.title}
                  </a>
                ))
              ) : (
                <>
                  <a href="https://www.youtube.com/@ufc" target="_blank" rel="noreferrer">
                    UFC Channel
                  </a>
                  <a href="https://www.youtube.com/@NFL" target="_blank" rel="noreferrer">
                    NFL Channel
                  </a>
                </>
              )}
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}