import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import "./auth.css";

export default function SignupPage() {
  const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";
  const navigate = useNavigate();

  const [fullName, setFullName] = useState("");
  const [username, setUsername] = useState("");
  const [email, setEmail] = useState("");
  const [promoCode, setPromoCode] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [agree, setAgree] = useState(false);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function handleSignup(e) {
    e.preventDefault();
    setError("");

    if (password !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }

    if (!agree) {
      setError("You must accept the terms to continue.");
      return;
    }

    setBusy(true);

    try {
      const res = await fetch(`${API_BASE}/auth/register`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          full_name: fullName,
          username,
          email,
          password,
          promo_code: promoCode || null,
        }),
      });

      const data = await res.json();

      if (!res.ok || !data?.ok) {
        throw new Error(data?.detail || "Signup failed");
      }

      navigate("/login");
    } catch (err) {
      setError(err.message || "Unable to create account.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="authShell">
      <div className="authBrandPanel">
        <div className="authBrandTop">
          <img src="/draftmindiq_head_transparent.png" alt="DraftMindIQ" className="authLogo" />
          <div className="authBadge">BETA ACCESS</div>
        </div>

        <h1 className="authBrandTitle">Create your account</h1>
        <p className="authBrandSub">
          Start with a clean account foundation now and expand into premium tools as the platform grows.
        </p>

        <div className="authPoints">
          <div className="authPoint">MMA live first</div>
          <div className="authPoint">More sports planned</div>
          <div className="authPoint">Subscription-ready account system</div>
          <div className="authPoint">Admin-managed promotions and access</div>
        </div>
      </div>

      <div className="authCardWrap">
        <form className="authCard" onSubmit={handleSignup}>
          <div className="authCardHeader">
            <div className="authEyebrow">New Account</div>
            <h2>Create your account</h2>
            <p>Join DraftMindIQ and get your account ready for optimizer access and premium tools.</p>
          </div>

          <div className="authField">
            <label>Full name</label>
            <input
              value={fullName}
              onChange={(e) => setFullName(e.target.value)}
              placeholder="Enter your full name"
              autoComplete="name"
            />
          </div>

          <div className="authField">
            <label>Username</label>
            <input
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="Choose a username"
              autoComplete="username"
            />
          </div>

          <div className="authField">
            <label>Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="Enter your email"
              autoComplete="email"
            />
          </div>

          <div className="authField">
            <label>Promo code <span className="authOptional">(optional)</span></label>
            <input
              value={promoCode}
              onChange={(e) => setPromoCode(e.target.value)}
              placeholder="Enter promo code if you have one"
            />
          </div>

          <div className="authField">
            <label>Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Create a password"
              autoComplete="new-password"
            />
          </div>

          <div className="authField">
            <label>Confirm password</label>
            <input
              type="password"
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              placeholder="Confirm your password"
              autoComplete="new-password"
            />
          </div>

          <label className="authCheck">
            <input
              type="checkbox"
              checked={agree}
              onChange={(e) => setAgree(e.target.checked)}
            />
            <span>I agree to the terms and account policies.</span>
          </label>

          {error ? <div className="authError">{error}</div> : null}

          <button type="submit" className="authPrimaryBtn" disabled={busy}>
            {busy ? "Creating Account..." : "Create Account"}
          </button>

          <div className="authFooterText">
            Already have an account? <Link to="/login">Sign in</Link>
          </div>
        </form>
      </div>
    </div>
  );
}