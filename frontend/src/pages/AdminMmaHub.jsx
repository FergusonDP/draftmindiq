import React, { useEffect, useMemo, useState } from "react";
import { Link, NavLink } from "react-router-dom";
import { useSettings } from "../app/SettingsContext.jsx";
import "./Dashboard.css";
import "./adminDashboard.css";
import "./adminMmaHub.css";

function safeJson(x) {
  return x && typeof x === "object" ? x : null;
}

function formatTs(iso) {
  try {
    if (!iso) return "Unavailable";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "Unavailable";
    return d.toLocaleString();
  } catch {
    return "Unavailable";
  }
}

function getStoredUser() {
  try {
    return JSON.parse(localStorage.getItem("dm_user") || "{}");
  } catch {
    return {};
  }
}

function getFallbackHealth() {
  return {
    status: "attention_needed",
    label: "Attention Needed",
    summary: "2 fight changes detected • optimizer refresh recommended",
    tone: "warn",
    optimizerReady: false,
    activeSlateId: "ufc-demo-slate",
    lastSyncAt: null,
    lastReconcileAt: null,
    counts: {
      activeFights: 12,
      changedFights: 2,
      missingOdds: 1,
      orphanRows: 0,
      duplicateRows: 0,
    },
  };
}

function getFallbackIssues() {
  return [
    {
      level: "warn",
      title: "Fight changed",
      detail: "One fight appears in slate compare but is missing in current odds map.",
    },
    {
      level: "info",
      title: "Optimizer stale",
      detail: "Optimizer inputs should be rebuilt after reconciliation.",
    },
    {
      level: "good",
      title: "No duplicate rows",
      detail: "Current compare pass did not detect duplicate slate rows.",
    },
  ];
}

function getInitialResult() {
  return {
    title: "No tool run yet",
    status: "idle",
    ranAt: null,
    summary: "Select a tool to inspect what it does or run it once the endpoint is wired.",
    details: [
      "This panel will show the result payload, summary counts, warnings, and follow-up actions.",
    ],
  };
}

const TOOL_GROUPS = [
  {
    key: "integrity",
    title: "Integrity & Validation",
    items: [
      {
        key: "run-checks",
        title: "Run Checks & Balances",
        desc: "Verifies slate rows, fighter pairings, odds coverage, duplicates, orphan rows, and optimizer readiness.",
        method: "POST",
        endpoint: "/admin/mma/run-checks",
      },
      {
        key: "reconcile",
        title: "Reconcile Slate vs Odds",
        desc: "Compares the active DraftKings slate against current fight and odds mappings and flags differences.",
        method: "POST",
        endpoint: "/admin/mma/reconcile",
      },
      {
        key: "view-issues",
        title: "Load Issue Snapshot",
        desc: "Loads the current open issues list without changing data.",
        method: "GET",
        endpoint: "/admin/mma/issues",
      },
    ],
  },
  {
    key: "mapping",
    title: "Fight Mapping",
    items: [
      {
        key: "fight-map",
        title: "Rebuild Fight Map",
        desc: "Re-links fighters into fight pairs, updates keys, and flags records that can no longer be mapped cleanly.",
        method: "POST",
        endpoint: "/admin/mma/rebuild-fight-map",
      },
      {
        key: "change-log",
        title: "Load Change Log",
        desc: "Loads recent fight-status or slate-status changes for operator review.",
        method: "GET",
        endpoint: "/admin/mma/changes",
      },
    ],
  },
  {
    key: "optimizer",
    title: "Optimizer Controls",
    items: [
      {
        key: "optimizer-inputs",
        title: "Rebuild Optimizer Inputs",
        desc: "Refreshes optimizer-ready tables after fight, slate, odds, or projection changes.",
        method: "POST",
        endpoint: "/admin/mma/rebuild-optimizer",
      },
      {
        key: "match-report",
        title: "Recompute Match Report",
        desc: "Regenerates the matchup report and any downstream optimizer-supporting summaries.",
        method: "POST",
        endpoint: "/admin/mma/recompute-match-report",
      },
    ],
  },
  {
    key: "cleanup",
    title: "Cleanup & Maintenance",
    items: [
      {
        key: "cleanup",
        title: "Remove Dead Data",
        desc: "Deletes stale, orphaned, or invalid records no longer linked to the active slate pipeline.",
        method: "POST",
        endpoint: "/admin/mma/cleanup",
      },
      {
        key: "snapshot",
        title: "Export Admin Snapshot",
        desc: "Returns a concise operational snapshot you can use for debugging or state review.",
        method: "GET",
        endpoint: "/admin/mma/snapshot",
      },
    ],
  },
];

export default function AdminMmaHub() {
  const { settings } = useSettings();
  const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";
  const user = getStoredUser();

  const [tick, setTick] = useState(Date.now());
  const [health, setHealth] = useState(getFallbackHealth());
  const [issues, setIssues] = useState(getFallbackIssues());
  const [selectedTool, setSelectedTool] = useState(TOOL_GROUPS[0].items[0]);
  const [toolResult, setToolResult] = useState(getInitialResult());
  const [busyKey, setBusyKey] = useState("");
  const [loadingHealth, setLoadingHealth] = useState(false);

  useEffect(() => {
    const timer = setInterval(() => setTick(Date.now()), 1000);
    return () => clearInterval(timer);
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

  const adminName = useMemo(() => {
    return String(user?.name || user?.username || "Admin").trim() || "Admin";
  }, [user]);

  async function loadHealth() {
    setLoadingHealth(true);
    try {
      const res = await fetch(`${API_BASE}/admin/mma/health`);
      if (!res.ok) throw new Error("health_failed");

      const json = safeJson(await res.json());
      if (!json) throw new Error("bad_health_json");

      setHealth({
        status: json.status || "attention_needed",
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
        optimizerReady: Boolean(json.optimizer_ready),
        activeSlateId: json.active_slate_id || "Unknown",
        lastSyncAt: json.last_sync_at || null,
        lastReconcileAt: json.last_reconcile_at || null,
        counts: {
          activeFights: json?.counts?.active_fights ?? 0,
          changedFights: json?.counts?.changed_fights ?? 0,
          missingOdds: json?.counts?.missing_odds ?? 0,
          orphanRows: json?.counts?.orphan_rows ?? 0,
          duplicateRows: json?.counts?.duplicate_rows ?? 0,
        },
      });
    } catch {
      setHealth(getFallbackHealth());
    } finally {
      setLoadingHealth(false);
    }
  }

  async function loadIssues() {
    try {
      const res = await fetch(`${API_BASE}/admin/mma/issues`);
      if (!res.ok) throw new Error("issues_failed");

      const json = safeJson(await res.json());
      const raw = Array.isArray(json?.items) ? json.items : [];

      if (!raw.length) {
        setIssues([
          {
            level: "good",
            title: "No open issues",
            detail: "The current issue list is empty.",
          },
        ]);
        return;
      }

      setIssues(
        raw.map((item, idx) => ({
          level: item.level || "info",
          title: item.title || `Issue ${idx + 1}`,
          detail: item.detail || item.reason || "No detail provided.",
        }))
      );
    } catch {
      setIssues(getFallbackIssues());
    }
  }

  useEffect(() => {
    loadHealth();
    loadIssues();
    const timer = setInterval(() => {
      loadHealth();
    }, 30000);

    return () => clearInterval(timer);
  }, [API_BASE]);

  function inspectTool(tool) {
    setSelectedTool(tool);
    setToolResult({
      title: tool.title,
      status: "inspect",
      ranAt: null,
      summary: tool.desc,
      details: [
        `Method: ${tool.method}`,
        `Endpoint: ${tool.endpoint}`,
        "This tool is currently in inspect mode. Press Run Tool to call the endpoint.",
      ],
    });
  }

  async function runTool(tool) {
    setSelectedTool(tool);
    setBusyKey(tool.key);

    try {
      const url = `${API_BASE}${tool.endpoint}`;
      const res = await fetch(url, {
        method: tool.method,
        headers: {
          "Content-Type": "application/json",
        },
      });

      let payload = null;
      try {
        payload = safeJson(await res.json());
      } catch {
        payload = null;
      }

      if (!res.ok) {
        throw new Error(payload?.detail || `${tool.title} failed`);
      }

      const detailLines = [];

      if (payload?.summary) detailLines.push(payload.summary);
      if (payload?.message) detailLines.push(payload.message);
      if (payload?.counts && typeof payload.counts === "object") {
        Object.entries(payload.counts).forEach(([k, v]) => {
          detailLines.push(`${k}: ${v}`);
        });
      }
      if (Array.isArray(payload?.warnings)) {
        payload.warnings.forEach((w) => detailLines.push(`warning: ${w}`));
      }
      if (Array.isArray(payload?.items)) {
        payload.items.slice(0, 8).forEach((item, idx) => {
          if (typeof item === "string") {
            detailLines.push(item);
          } else if (item && typeof item === "object") {
            detailLines.push(
              `${idx + 1}. ${item.title || item.name || item.reason || JSON.stringify(item)}`
            );
          }
        });
      }

      setToolResult({
        title: tool.title,
        status: "success",
        ranAt: new Date().toISOString(),
        summary: payload?.summary || payload?.message || `${tool.title} completed successfully.`,
        details:
          detailLines.length > 0
            ? detailLines
            : ["Tool completed successfully, but no extra details were returned."],
      });

      if (tool.key === "view-issues") {
        loadIssues();
      }
      if (["run-checks", "reconcile", "fight-map", "cleanup", "optimizer-inputs"].includes(tool.key)) {
        loadHealth();
        loadIssues();
      }
    } catch (err) {
      setToolResult({
        title: tool.title,
        status: "error",
        ranAt: new Date().toISOString(),
        summary: err?.message || `${tool.title} failed.`,
        details: [
          "The endpoint may not be wired yet.",
          `Expected endpoint: ${tool.method} ${tool.endpoint}`,
        ],
      });
    } finally {
      setBusyKey("");
    }
  }

  const summaryCards = [
    {
      label: "System Status",
      value: loadingHealth ? "Loading..." : health.label,
      hint: health.summary,
      tone: health.tone,
    },
    {
      label: "Active Slate",
      value: health.activeSlateId,
      hint: "Current working slate",
      tone: "neutral",
    },
    {
      label: "Optimizer",
      value: health.optimizerReady ? "READY" : "REFRESH NEEDED",
      hint: "Current optimizer readiness",
      tone: health.optimizerReady ? "good" : "warn",
    },
    {
      label: "Last Reconcile",
      value: formatTs(health.lastReconcileAt),
      hint: "Latest compare pass",
      tone: "neutral",
    },
  ];

  return (
    <div className="hub adminHub mmaHubPage">
      <nav className="topNav" aria-label="Top navigation">
        <Link to="/dashboard" className="brand">
          <img className="brandIcon" src="/draftmindiq_head_transparent.png" alt="DraftMind" />
          <div className="brandText">
            <span className="brandWord">DraftMindIQ</span>
            <span className="brandSub">MMA Admin Hub</span>
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
          <div className="hubKicker">MMA Operations</div>
          <div className="hubH1">Admin MMA Hub</div>
          <div className="hubMeta">
            <span className="hubMetaMuted">
              {adminName} • monitor health, run tools, reconcile changes, and keep the optimizer clean.
            </span>
          </div>
        </div>
      </header>

      <section className="pulseRow" aria-label="MMA summary">
        {summaryCards.map((card) => (
          <div key={card.label} className={`pulseCard tone-${card.tone}`}>
            <div className="pulseLabel">{card.label}</div>
            <div className="pulseValue">{card.value}</div>
            <div className="pulseHint">{card.hint}</div>
          </div>
        ))}
      </section>

      <div className="mmaHubLayout">
        <section className="mmaHubMain">
          <div className="mmaHubHero">
            <div className="mmaHubHeroTop">
              <div className="mmaHubHeroTag">Operational Status</div>
              <div className={`mmaHubHeroPill tone-${health.tone}`}>{health.label}</div>
            </div>

            <div className="mmaHubHeroTitle">MMA system state at a glance</div>
            <div className="mmaHubHeroSub">{health.summary}</div>

            <div className="mmaHubStatGrid">
              <div className="mmaHubStatCard">
                <span>Active Fights</span>
                <strong>{health.counts.activeFights}</strong>
              </div>
              <div className="mmaHubStatCard">
                <span>Changed Fights</span>
                <strong>{health.counts.changedFights}</strong>
              </div>
              <div className="mmaHubStatCard">
                <span>Missing Odds</span>
                <strong>{health.counts.missingOdds}</strong>
              </div>
              <div className="mmaHubStatCard">
                <span>Orphan Rows</span>
                <strong>{health.counts.orphanRows}</strong>
              </div>
              <div className="mmaHubStatCard">
                <span>Duplicate Rows</span>
                <strong>{health.counts.duplicateRows}</strong>
              </div>
              <div className="mmaHubStatCard">
                <span>Optimizer</span>
                <strong>{health.optimizerReady ? "Ready" : "Refresh"}</strong>
              </div>
            </div>

            <div className="mmaHubHeroMeta">
              <div>Last sync: {formatTs(health.lastSyncAt)}</div>
              <div>Last reconcile: {formatTs(health.lastReconcileAt)}</div>
              <div>Slate: {health.activeSlateId}</div>
            </div>
          </div>

          <section className="mmaToolSections" aria-label="Tool groups">
            {TOOL_GROUPS.map((group) => (
              <div key={group.key} className="mmaToolSection">
                <div className="mmaSectionHeader">
                  <div className="mmaSectionTitle">{group.title}</div>
                  <div className="mmaSectionSub">Button-driven internal controls with plain-English descriptions.</div>
                </div>

                <div className="mmaToolGrid">
                  {group.items.map((tool) => {
                    const active = selectedTool?.key === tool.key;
                    const isBusy = busyKey === tool.key;

                    return (
                      <div
                        key={tool.key}
                        className={`mmaToolCard ${active ? "active" : ""}`}
                      >
                        <div className="mmaToolCardTop">
                          <div className="mmaToolTitle">{tool.title}</div>
                          <div className="mmaToolMethod">{tool.method}</div>
                        </div>

                        <div className="mmaToolDesc">{tool.desc}</div>
                        <div className="mmaToolEndpoint">{tool.endpoint}</div>

                        <div className="mmaToolActions">
                          <button
                            type="button"
                            className="mmaToolBtn secondary"
                            onClick={() => inspectTool(tool)}
                          >
                            Inspect
                          </button>
                          <button
                            type="button"
                            className="mmaToolBtn primary"
                            onClick={() => runTool(tool)}
                            disabled={isBusy}
                          >
                            {isBusy ? "Running..." : "Run Tool"}
                          </button>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
          </section>
        </section>

        <aside className="mmaHubSide" aria-label="MMA detail panels">
          <div className="mmaPanel">
            <div className="mmaPanelHeader">
              <div className="mmaPanelTitle">Selected Tool</div>
              <div className="mmaPanelBadge">{selectedTool?.method || "GET"}</div>
            </div>

            <div className="mmaSelectedToolName">{selectedTool?.title}</div>
            <div className="mmaSelectedToolDesc">{selectedTool?.desc}</div>
            <div className="mmaSelectedToolEndpoint">{selectedTool?.endpoint}</div>
          </div>

          <div className="mmaPanel">
            <div className="mmaPanelHeader">
              <div className="mmaPanelTitle">Open Issues</div>
              <button type="button" className="mmaMiniBtn" onClick={loadIssues}>
                Refresh
              </button>
            </div>

            <div className="mmaIssueList">
              {issues.map((issue, idx) => (
                <div key={`${issue.title}-${idx}`} className={`mmaIssueCard level-${issue.level}`}>
                  <div className="mmaIssueTitle">{issue.title}</div>
                  <div className="mmaIssueDetail">{issue.detail}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="mmaPanel">
            <div className="mmaPanelHeader">
              <div className="mmaPanelTitle">Result Console</div>
              <div className={`mmaPanelBadge status-${toolResult.status}`}>{toolResult.status}</div>
            </div>

            <div className="mmaConsoleTitle">{toolResult.title}</div>
            <div className="mmaConsoleSummary">{toolResult.summary}</div>
            <div className="mmaConsoleMeta">
              {toolResult.ranAt ? `Ran at ${formatTs(toolResult.ranAt)}` : "No run timestamp yet"}
            </div>

            <div className="mmaConsoleLog">
              {toolResult.details.map((line, idx) => (
                <div key={`${line}-${idx}`} className="mmaConsoleLine">
                  {line}
                </div>
              ))}
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}