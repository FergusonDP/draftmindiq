import React, { useEffect, useMemo, useState } from "react";
import { Link, NavLink } from "react-router-dom";
import { useSettings } from "../app/SettingsContext.jsx";
import "./dashboard.css";
import "./userDashboard.css";

const SPORTS = [
  { key: "ALL", label: "All Sports" },
  { key: "MMA", label: "MMA" },
  { key: "NFL", label: "NFL" },
  { key: "NBA", label: "NBA" },
  { key: "MLB", label: "MLB" },
  { key: "NHL", label: "NHL" },
];

const SPORT_KEYS = ["MMA", "NFL", "NBA", "MLB", "NHL"];

const USER_MODULES = [
  {
    key: "MMA",
    status: "LIVE",
    title: "MMA Optimizer",
    desc: "Build lineups, review projections, and export with confidence.",
    to: "/optimizer/mma",
    highlights: ["Player pool controls", "Fight-level context", "CSV export ready"],
    tone: "good",
  },
  {
    key: "NFL",
    status: "NEXT",
    title: "NFL Hub",
    desc: "Projections, contests, and optimizer workflow are coming next.",
    to: "/optimizer/nfl",
    highlights: ["Slate flow planned", "Projection layer upcoming", "UI route ready"],
    tone: "warn",
  },
  {
    key: "NBA",
    status: "SOON",
    title: "NBA Hub",
    desc: "Late news and usage-aware optimizer workflow will live here.",
    to: "/optimizer/nba",
    highlights: ["Placeholder module", "Late news hooks", "Frontend ready"],
    tone: "neutral",
  },
  {
    key: "MLB",
    status: "SOON",
    title: "MLB Hub",
    desc: "Stacks, weather, leverage, and slate tools are planned.",
    to: "/optimizer/mlb",
    highlights: ["Placeholder module", "Data model TBD", "Frontend ready"],
    tone: "neutral",
  },
  {
    key: "NHL",
    status: "SOON",
    title: "NHL Hub",
    desc: "Lines, goalies, and correlations will live here later.",
    to: "/optimizer/nhl",
    highlights: ["Placeholder module", "Data model TBD", "Frontend ready"],
    tone: "neutral",
  },
];

const QUICK_ACTIONS = [
  { title: "Open Optimizer", desc: "Jump into the active optimizer workflow.", to: "/optimizer/mma" },
  { title: "Saved Builds", desc: "Review your latest lineups and drafts.", to: "/optimizer/mma" },
  { title: "Exports", desc: "Check your recent CSV-ready outputs.", to: "/optimizer/mma" },
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

function getStoredUser() {
  try {
    return JSON.parse(localStorage.getItem("dm_user") || "{}");
  } catch {
    return {};
  }
}

export default function UserDashboard() {
  const { settings } = useSettings();
  const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";
  const user = getStoredUser();

  const [sport, setSport] = useState(settings.favoriteSport || "ALL");
  const [tick, setTick] = useState(Date.now());

  const [news, setNews] = useState({ ok: false, items: [], count: 0 });
  const [videos, setVideos] = useState({ ok: false, items: [], count: 0 });
  const [contentLoading, setContentLoading] = useState(false);
  const [videoOffset, setVideoOffset] = useState(0);

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
    if (sport === "ALL") return USER_MODULES;
    return USER_MODULES.filter((m) => m.key === sport);
  }, [sport]);

  const firstName = useMemo(() => {
    const raw = String(user?.name || user?.username || "Player").trim();
    return raw.split(" ")[0] || "Player";
  }, [user]);

  const pulse = useMemo(() => {
    return [
      { label: "Selected Sport", value: sport, hint: shouldCycle ? "all sports view" : "single sport view", tone: "good" },
      { label: "Optimizer", value: "READY", hint: "core workflow available", tone: "good" },
      { label: "News Feed", value: contentLoading ? "LOADING" : "LIVE", hint: "latest platform content", tone: "neutral" },
      { label: "Exports", value: "READY", hint: "csv workflow available", tone: "neutral" },
    ];
  }, [sport, shouldCycle, contentLoading]);

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
    const base = sport === "ALL" ? "Mixed headlines + rotating videos" : `${sport} headlines + videos`;
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
    <div className="hub userHub">
      <nav className="topNav" aria-label="Top navigation">
        <Link to="/dashboard" className="brand">
          <img className="brandIcon" src="/draftmindiq_head_transparent.png" alt="DraftMind" />
          <div className="brandText">
            <span className="brandWord">DraftMindIQ</span>
            <span className="brandSub">User Dashboard</span>
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
          </div>

          <div className="navSport">
            <div className="sportSelectLabel">Sport</div>
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
          <div className="hubKicker">Welcome Back</div>
          <div className="hubH1">{firstName}'s Dashboard</div>
          <div className="hubMeta">
            <span className="hubMetaMuted">
              Open the optimizer, follow your sport, and move fast from research to lineup build.
            </span>
          </div>
        </div>
      </header>

      <section className="pulseRow" aria-label="User pulse">
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
          <Link to="/optimizer/mma" className="heroCard userHeroCard">
            <div className="heroOverlay" aria-hidden="true" />
            <div className="heroTop">
              <div className="heroTag">Featured Workflow • MMA</div>
              <div className="heroChip">READY</div>
            </div>

            <div className="heroTitle">Open the MMA Optimizer</div>
            <div className="heroSub">
              Build lineups from a cleaner, player-facing dashboard without admin controls or operational clutter.
            </div>

            <div className="heroBullets">
              <div className="heroBullet">
                <span className="heroBulletDot" aria-hidden="true" />
                <span>Open optimizer workflow fast</span>
              </div>
              <div className="heroBullet">
                <span className="heroBulletDot" aria-hidden="true" />
                <span>Review feeds, videos, and updates by sport</span>
              </div>
              <div className="heroBullet">
                <span className="heroBulletDot" aria-hidden="true" />
                <span>Keep focus on building, not admin maintenance</span>
              </div>
            </div>

            <div className="heroCtaRow">
              <div className="heroCta">Start Building</div>
              <div className="heroCtaArrow" aria-hidden="true">
                →
              </div>
            </div>
          </Link>

          <section className="userQuickGrid" aria-label="Quick actions">
            {QUICK_ACTIONS.map((action) => (
              <Link key={action.title} to={action.to} className="userQuickCard">
                <div className="userQuickTitle">{action.title}</div>
                <div className="userQuickDesc">{action.desc}</div>
              </Link>
            ))}
          </section>

          <section className="moduleGrid" aria-label="User modules">
            {visibleModules.map((m) => (
              <div key={m.key} className={`moduleCard tone-${m.tone}`}>
                <div className="moduleTop">
                  <div className="moduleTitle">{m.title}</div>
                  <div className={`modulePill status-${m.status.toLowerCase()}`}>{m.status}</div>
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
                  <div className="moduleHint">Module: {m.key}</div>
                </div>
              </div>
            ))}
          </section>

          <section className="blockGrid" aria-label="User blocks">
            <div className="blockCard userBlock">
              <div className="blockHeader">My Workflow</div>
              <div className="blockBody">
                Start with the optimizer, review current slate context, and export when your build is ready.
              </div>
              <div className="blockFooter">
                <span className="pill">OPTIMIZER</span>
                <span className="pill">SLATES</span>
                <span className="pill">EXPORTS</span>
              </div>
            </div>

            <div className="blockCard userBlock">
              <div className="blockHeader">News & Research</div>
              <div className="blockBody">
                Headlines, videos, and slate notes stay visible here without mixing in backend system status.
              </div>
              <div className="blockFooter">
                <span className="pill">NEWS</span>
                <span className="pill">VIDEOS</span>
                <span className="pill">NOTES</span>
              </div>
            </div>

            <div className="blockCard userBlock">
              <div className="blockHeader">Defaults</div>
              <div className="blockBody">
                Favorite sport: {settings.favoriteSport || "ALL"} • Timezone: {settings.timezone} • Notifications: ON
              </div>
              <div className="blockFooter">
                <span className="pill">SETTINGS</span>
                <span className="pill">TIMEZONE</span>
                <span className="pill">PREFERENCES</span>
              </div>
            </div>

            <div className="blockCard userBlock">
              <div className="blockHeader">Coming Soon</div>
              <div className="blockBody">
                Saved builds, historical comparison, and per-lineup notes can expand here once the optimizer flow is finalized.
              </div>
              <div className="blockFooter">
                <span className="pill">SAVES</span>
                <span className="pill">COMPARE</span>
                <span className="pill">NOTES</span>
              </div>
            </div>
          </section>
        </div>

        <aside className="rightCol" aria-label="Right rail">
          <div className="railHeader">
            <div className="railTitle">Live Feed</div>
            <div className="railHint">{railHint}</div>
          </div>

          <div className="railStack">
            {news.ok && news.items && news.items.length > 0 ? (
              news.items.slice(0, 6).map((it) => {
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
              })
            ) : (
              <>
                <div className="newsCard">
                  <div className="newsTop">
                    <div className="newsTitle">Feed Loading</div>
                    <div className="newsPill">LIVE</div>
                  </div>
                  <div className="newsSub">News and video content will appear here.</div>
                </div>
              </>
            )}
          </div>

          <div className="railCTA">
            <div className="railCTATitle">Latest Videos</div>
            <div className="railCTASub">
              {shouldCycle ? "Auto-cycling across sports." : "Latest updates for the selected sport."}
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