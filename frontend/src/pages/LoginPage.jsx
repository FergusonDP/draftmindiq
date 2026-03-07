import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import "./auth.css";

export default function LoginPage() {
  const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";
  const navigate = useNavigate();

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [rememberMe, setRememberMe] = useState(true);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function handleLogin(e) {
    e.preventDefault();
    setError("");
    setBusy(true);

    try {
      const res = await fetch(`${API_BASE}/auth/login`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          username,
          password,
        }),
      });

      const data = await res.json();

      if (!res.ok || !data?.ok) {
        throw new Error(data?.detail || "Login failed");
      }

      localStorage.setItem("dm_user", JSON.stringify(data.user));

      if (rememberMe) {
        localStorage.setItem("beta_ok", "true");
      } else {
        sessionStorage.setItem("beta_ok", "true");
      }

      navigate("/dashboard");
    } catch (err) {
      setError(err.message || "Unable to sign in.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="authShell">
      <div className="authBrandPanel">
        <div className="authBrandTop">
          <img src="/draftmindiq_head_transparent.png" alt="DraftMindIQ" className="authLogo" />
          <div className="authBadge">FOUNDING ACCESS</div>
        </div>

        <h1 className="authBrandTitle">DraftMindIQ</h1>
        <p className="authBrandSub">
          Premium DFS tools built for a serious multi-sport future, starting with MMA.
        </p>

        <div className="authPoints">
          <div className="authPoint">Role-based dashboards</div>
          <div className="authPoint">Optimizer-focused workflow</div>
          <div className="authPoint">Admin control + clean product UI</div>
          <div className="authPoint">Paid platform foundation</div>
        </div>
      </div>

      <div className="authCardWrap">
        <form className="authCard" onSubmit={handleLogin}>
          <div className="authCardHeader">
            <div className="authEyebrow">Account Access</div>
            <h2>Welcome back</h2>
            <p>Sign in to access your dashboard, optimizer tools, and account settings.</p>
          </div>

          <div className="authField">
            <label>Username</label>
            <input
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="Enter your username"
              autoComplete="username"
            />
          </div>

          <div className="authField">
            <label>Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Enter your password"
              autoComplete="current-password"
            />
          </div>

          <div className="authRow">
            <label className="authCheck">
              <input
                type="checkbox"
                checked={rememberMe}
                onChange={(e) => setRememberMe(e.target.checked)}
              />
              <span>Remember me</span>
            </label>

            <button type="button" className="authTextBtn">
              Forgot password
            </button>
          </div>

          {error ? <div className="authError">{error}</div> : null}

          <button type="submit" className="authPrimaryBtn" disabled={busy}>
            {busy ? "Signing In..." : "Sign In"}
          </button>

          <div className="authFooterText">
            Don’t have an account? <Link to="/signup">Create one</Link>
          </div>
        </form>
      </div>
    </div>
  );
}