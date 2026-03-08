import React, { useEffect, useMemo, useRef, useState } from "react";
import { Link, NavLink } from "react-router-dom";
import { useSettings } from "../app/SettingsContext.jsx";
import { DAILY_SLATE_NOTES } from "../data/dailySlateNotes.js";
import "./Dashboard.css";
import "./userDashboard.css";

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

const MAIN_SPORTS = ["MMA", "NFL", "NBA", "MLB", "NHL"];
const MORE_SPORTS = ["CFB", "PGA", "SOCCER"];
const HERO_VIDEO_SPORTS = ["MMA", "NFL", "NBA", "MLB", "NHL"];
const NEWS_SPORTS = ["MMA", "NFL", "NBA", "MLB", "NHL"];

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

function getVideoThumb(item) {
  return (
    item?.thumbnail ||
    item?.thumbnail_url ||
    item?.image ||
    item?.image_url ||
    item?.thumb ||
    ""
  );
}

function getVideoMeta(item) {
  const source = item?.source || "Video";
  const published = formatPublished(item?.published_at);
  return published ? `${source} • ${published}` : source;
}

export default function UserDashboard() {
  const { settings } = useSettings();
  const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";
  const user = getStoredUser();

  const [tick, setTick] = useState(Date.now());
  const [news, setNews] = useState({ ok: false, items: [], count: 0 });
  const [newsLoading, setNewsLoading] = useState(false);

  const [heroVideos, setHeroVideos] = useState([]);
  const [heroVideosLoading, setHeroVideosLoading] = useState(false);
  const [heroIndex, setHeroIndex] = useState(0);

  const [moreOpen, setMoreOpen] = useState(false);
  const moreRef = useRef(null);

  useEffect(() => {
    const timer = setInterval(() => setTick(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    function onDocClick(e) {
      if (!moreRef.current?.contains(e.target)) {
        setMoreOpen(false);
      }
    }

    document.addEventListener("mousedown", onDocClick);
    return () => document.removeEventListener("mousedown", onDocClick);
  }, []);

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

  const firstName = useMemo(() => {
    const raw = String(user?.name || user?.username || "Player").trim();
    return raw.split(" ")[0] || "Player";
  }, [user]);

  const sportCards = useMemo(() => {
    return Object.entries(DAILY_SLATE_NOTES).map(([key, value]) => ({
      key,
      ...value,
    }));
  }, []);

  useEffect(() => {
    let alive = true;

    async function loadNews() {
      setNewsLoading(true);
      try {
        const all = await Promise.all(
          NEWS_SPORTS.map(async (key) => {
            const res = await fetch(`${API_BASE}/news/${key}?limit=6`);
            const j = safeJson(await res.json());
            if (!j || !j.ok) return [];
            return (j.items || []).map((it) => ({ ...it, sport: key }));
          })
        );

        if (!alive) return;

        const merged = all
          .flat()
          .sort((a, b) => parseTime(b.published_at) - parseTime(a.published_at));

        const seen = new Set();
        const deduped = merged.filter((item) => {
          const dedupeKey = item.url || `${item.sport}-${item.title}`;
          if (seen.has(dedupeKey)) return false;
          seen.add(dedupeKey);
          return true;
        });

        setNews({
          ok: true,
          items: deduped.slice(0, 8),
          count: deduped.length,
        });
      } catch {
        if (!alive) return;
        setNews({ ok: false, items: [], count: 0 });
      } finally {
        if (alive) setNewsLoading(false);
      }
    }

    async function loadHeroVideos() {
      setHeroVideosLoading(true);
      try {
        const all = await Promise.all(
          HERO_VIDEO_SPORTS.map(async (key) => {
            const res = await fetch(`${API_BASE}/video/${key}?limit=4`);
            const j = safeJson(await res.json());
            if (!j || !j.ok) return [];
            return (j.items || [])
              .map((it) => ({ ...it, sport: key }))
              .sort((a, b) => parseTime(b.published_at) - parseTime(a.published_at));
          })
        );

        if (!alive) return;

        const buckets = all.map((items) => [...items]);
        const mixed = [];

        let added = true;
        while (added) {
          added = false;
          for (const bucket of buckets) {
            if (bucket.length) {
              mixed.push(bucket.shift());
              added = true;
            }
          }
        }

        setHeroVideos(mixed);
        setHeroIndex(0);
      } catch {
        if (!alive) return;
        setHeroVideos([]);
      } finally {
        if (alive) setHeroVideosLoading(false);
      }
    }

    loadNews();
    loadHeroVideos();

    return () => {
      alive = false;
    };
  }, [API_BASE]);

  useEffect(() => {
    if (!heroVideos.length) return;
    const t = setInterval(() => {
      setHeroIndex((x) => (x + 1) % heroVideos.length);
    }, 8000);
    return () => clearInterval(t);
  }, [heroVideos]);

  const heroCount = heroVideos.length;

  function goPrevHero(e) {
    e.preventDefault();
    if (!heroCount) return;
    setHeroIndex((x) => (x - 1 + heroCount) % heroCount);
  }

  function goNextHero(e) {
    e.preventDefault();
    if (!heroCount) return;
    setHeroIndex((x) => (x + 1) % heroCount);
  }

  function goToHero(idx, e) {
    e.preventDefault();
    setHeroIndex(idx);
  }

  const activeHero = useMemo(() => {
    if (!heroVideos.length) return null;
    return heroVideos[heroIndex % heroVideos.length];
  }, [heroVideos, heroIndex]);

  const visibleHeroDots = useMemo(() => {
    return heroVideos.slice(0, Math.min(heroVideos.length, 8));
  }, [heroVideos]);

  return (
    <div className="hub userHub">
      <nav className="topNav" aria-label="Top navigation">
        <Link to="/dashboard" className="brand">
          <img className="brandIcon2" src="/draftmindiq_head_transparent.png" alt="DraftMind" />
          <div className="brandText">
            <span className="brandWord">DraftMind</span>
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

      <section className="sportsBar" aria-label="Sports navigation">
        <div className="sportsTabs">
          {MAIN_SPORTS.map((key) => (
            <Link key={key} to={DAILY_SLATE_NOTES[key]?.to || "#"} className="sportsTab">
              {key}
            </Link>
          ))}

          <div className="sportsMoreWrap" ref={moreRef}>
            <button
              type="button"
              className="sportsTab sportsTabMore"
              onClick={() => setMoreOpen((x) => !x)}
              aria-expanded={moreOpen}
              aria-label="More sports"
            >
              ...
            </button>

            {moreOpen && (
              <div className="sportsMoreMenu">
                {MORE_SPORTS.map((key) => (
                  <Link
                    key={key}
                    to={DAILY_SLATE_NOTES[key]?.to || "#"}
                    className="sportsMoreItem"
                    onClick={() => setMoreOpen(false)}
                  >
                    {key}
                  </Link>
                ))}
              </div>
            )}
          </div>
        </div>
      </section>

      <header className="hubHeader compact">
        <div className="hubHeaderLeft">
          <div className="hubKicker">Welcome Back</div>
          <div className="hubH1">{firstName}'s Dashboard</div>
          <div className="hubMeta">
            <span className="hubMetaMuted">
              Scan the latest clips, check the prep notes, and jump straight into the optimizer when you are ready.
            </span>
          </div>
        </div>
      </header>

      <div className="hubGrid">
        <div className="leftCol">
          <a
            href={activeHero?.url || "#"}
            target={activeHero?.url ? "_blank" : undefined}
            rel={activeHero?.url ? "noreferrer" : undefined}
            className="heroCard userHeroCard videoHeroCard"
            style={
              getVideoThumb(activeHero)
                ? {
                    backgroundImage: `
                      linear-gradient(180deg, rgba(5,10,20,0.18), rgba(5,10,20,0.78)),
                      linear-gradient(135deg, rgba(79,70,229,0.22), rgba(34,197,94,0.14)),
                      url("${getVideoThumb(activeHero)}")
                    `,
                    backgroundSize: "cover",
                    backgroundPosition: "center",
                  }
                : undefined
            }
          >
            <div className="heroOverlay heroOverlayVideo" aria-hidden="true" />

            <div className="heroTop">
              <div className="heroTag">Featured Clips • All Sports</div>
              <div className="heroChip">
                {heroVideosLoading ? "LOADING" : activeHero?.sport || "LIVE"}
              </div>
            </div>

            <div className="heroMediaTopRow">
              <div className="heroMediaMeta">
                {activeHero ? getVideoMeta(activeHero) : "Recent sports clips will appear here"}
              </div>

              <div className="heroControls">
                <button type="button" className="heroControlBtn" onClick={goPrevHero} aria-label="Previous clip">
                  ‹
                </button>
                <button type="button" className="heroControlBtn" onClick={goNextHero} aria-label="Next clip">
                  ›
                </button>
              </div>
            </div>

            <div className="heroVideoMeta">
              <div className="heroVideoEyebrow">{activeHero?.sport || "ALL SPORTS"}</div>

              <div className="heroTitle">
                {activeHero?.title || "Recent cross-sport clips will rotate here."}
              </div>

              <div className="heroSub">
                {activeHero?.description ||
                  activeHero?.source ||
                  "Use the arrows to move through recent clips or open the one currently featured."}
              </div>
            </div>

            <div className="heroCtaRow">
              <div className="heroCta">Open Clip</div>
              <div className="heroCtaArrow" aria-hidden="true">
                →
              </div>
            </div>

            {!!visibleHeroDots.length && (
              <div className="heroDots" aria-label="Clip selection">
                {visibleHeroDots.map((_, idx) => (
                  <button
                    key={idx}
                    type="button"
                    className={`heroDot ${idx === (heroIndex % visibleHeroDots.length) ? "active" : ""}`}
                    onClick={(e) => goToHero(idx, e)}
                    aria-label={`Go to clip ${idx + 1}`}
                  />
                ))}
              </div>
            )}
          </a>

          <section className="moduleGrid" aria-label="Upcoming slate prep notes">
            {sportCards.map((m) => (
              <div key={m.key} className="moduleCard slateNoteCard">
                <div className="moduleTop">
                  <div>
                    <div className="slateLabel">{m.slate}</div>
                    <div className="moduleTitle">{m.key}</div>
                  </div>

                  <div className="modulePill status-soon">{m.status}</div>
                </div>

                <div className="moduleDesc">{m.summary}</div>

                <div className="moduleList">
                  {m.angles.map((x) => (
                    <div key={x} className="moduleItem">
                      <span className="moduleDot" aria-hidden="true" />
                      <span>{x}</span>
                    </div>
                  ))}
                </div>

                <div className="moduleActions">
                  <Link to={m.to} className="moduleBtn">
                    Open Optimizer <span aria-hidden="true">→</span>
                  </Link>
                  <div className="moduleHint">{m.updatedAt}</div>
                </div>
              </div>
            ))}
          </section>
        </div>

        <aside className="rightCol" aria-label="Critical headlines across all sports">
          <div className="railStack">
            {news.ok && news.items && news.items.length > 0 ? (
              news.items.slice(0, 8).map((it) => {
                const published = formatPublished(it.published_at);
                const sub = published || it.source || "Update";

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
                      <div className="newsPill">{it.sport || "NEWS"}</div>
                    </div>
                    <div className="newsSub">{sub}</div>
                  </a>
                );
              })
            ) : (
              <div className="newsCard">
                <div className="newsTop">
                  <div className="newsTitle">
                    {newsLoading ? "Loading headlines" : "Critical headlines will appear here"}
                  </div>
                  <div className="newsPill">ALL</div>
                </div>
                <div className="newsSub">
                  The right rail stays focused on important cross-sport headlines and updates.
                </div>
              </div>
            )}
          </div>
        </aside>
      </div>
    </div>
  );
}